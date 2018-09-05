package server

import (
	"bytes"
	"fmt"
	"strconv"
	"time"
	"proxy/gateway-server/mysql"
	"proxy/gateway-server/sqlparser"
	"model/pkg/kvrpcpb"
	"model/pkg/timestamp"
	"pkg-go/ds_client"
	"util"
	"util/hack"
	"util/log"
	"proxy/store/dskv"
	"sort"
)

func (p *Proxy) HandleInsert(db string, stmt *sqlparser.Insert, args []interface{}) (*mysql.Result, error) {
	//var parseTime time.Time
	//start := time.Now()
	//defer func() {
	//	delay := time.Since(start)
	//	trace := sqlparser.NewTrackedBuffer(nil)
	//	stmt.Format(trace)
	//	//p.sqlStats(trace.String(), time.Since(start), time.Since(parseTime))
	//	//p.metric.AddApiWithDelay("insert", true, delay)
	//	if delay > time.Duration(p.config.InsertSlowLog)*time.Millisecond {
	//		log.Info("[insert slow log] %v %v", delay.String(), trace.String())
	//	}
	//}()

	parser := &StmtParser{}

	// 解析表名
	tableName := parser.parseTable(stmt)
	t := p.router.FindTable(db, tableName)
	if t == nil {
		log.Error("[insert] table %s.%s doesn.t exist", db, tableName)
		return nil, fmt.Errorf("Table '%s.%s' doesn't exist", db, tableName)
	}

	// 解析插入列名
	cols, err := parser.parseInsertCols(stmt)
	if err != nil {
		log.Error("[insert] parse columns error(%v)", err)
		return nil, fmt.Errorf("handle insert parseColumn err %s", err.Error())
	}
	// 没有指定列名，添加表的所有列
	if len(cols) == 0 {
		columns := t.GetAllColumns()
		if len(columns) == 0 {
			log.Error("[insert] get table(%s.%s) all columns from router failed", db, tableName)
			return nil, fmt.Errorf("could not get colums info table(%s.%s)", db, tableName)
		}
		for _, c := range columns {
			cols = append(cols, c.Name)
		}
	}

	// 解析插入行值（可能有多行）
	rows, err := parser.parseInsertValues(stmt)
	if err != nil {
		log.Error("[insert] table %s.%s parse row values error(%v)", db, tableName, err)
		return nil, fmt.Errorf("handle insert parseRow err %s", err.Error())
	}
	// 检查每行值的个数跟列名个数是否相等
	for i, r := range rows {
		if len(r) != len(cols) {
			log.Error("[insert] table %s.%s Column count doesn't match value count at row %d(%d != %d)", db, tableName, i, len(r), len(cols))
			return nil, fmt.Errorf("Column count doesn't match value count at row %d", i)
		}
	}

	// 按照表的每个列查找对应列值位置
	colMap, t, err := p.matchInsertValues(t, cols)
	if err != nil {
		log.Error("[insert] table %s.%s match column values error(%v)", db, tableName, err)
		return nil, err
	}
	// 检查是否缺少主键列
	pkName, err := p.checkPKMissing(t, colMap)
	if err != nil {
		log.Error("[insert] table %s.%s missing column(%v)", db, tableName, err)
		return nil, err
	}
	lasInsertId := uint64(0)
	//填充自增id值
	if len(pkName) > 0 {
		colMap[pkName] = len(colMap)
		ids, err := p.msCli.GetAutoIncId(t.GetDbId(), t.GetId(), uint32(len(rows)))
		if err != nil {
			log.Error("[insert] table %s.%s get auto_increment value err, %v", db, tableName, err)
			return nil, err
		}
		if len(ids) != len(rows) {
			log.Error("[insert] table %s.%s get auto_increment value err, %v", db, tableName, err)
			return nil, fmt.Errorf("get auto increment id size %d not equal insert size %d", len(ids), len(rows))
		}
		for i, row := range rows {
			row = append(row, []byte(fmt.Sprintf("%v", ids[i])))
			rows[i] = row
		}
		//insert multiple rows, returns the value generated for the first inserted row only
		//see http://dev.mysql.com/doc/refman/5.6/en/information-functions.html#function_last-insert-id
		lasInsertId = uint64(ids[0])
	}

	//parseTime = time.Now()
	// 编码、执行插入
	res := new(mysql.Result)
	affected, duplicateKey, err := p.insertRows(t, colMap, rows)
	if err != nil {
		log.Error("insert error table[%s:%s], err %s", db, tableName, err.Error())
		return nil, err
	} else if affected != uint64(len(rows)) {
		log.Error("insert error table[%s:%s],request num:%d,inserted num:%d", db, tableName, len(rows), affected)
		return nil, ErrAffectRows
	}
	if len(duplicateKey) != 0 {
		resErr := new(mysql.SqlError)
		resErr.Code = mysql.ER_DUP_ENTRY
		resErr.State = "23000"
		message := ` Duplicate entry `
		message += `for key 'PRIMARY'`
		resErr.Message = message
		return nil, resErr
	}
	res.AffectedRows = affected
	res.InsertId = lasInsertId
	res.Status = 0
	return res, nil
}

// 查找每列对应的列值的偏移，处理自动添加列逻辑
func (p *Proxy) matchInsertValues(t *Table, cols []string) (colMap map[string]int, newTable *Table, err error) {
	newTable = t
	colMap = make(map[string]int)
	unrecognized := make(map[string]int) // 未识别的列，不在原表定义中的列
	for i, c := range cols {
		col := t.FindColumn(c)
		if col != nil { // 存在该列
			if _, ok := colMap[col.Name]; ok { // 重复了
				err = fmt.Errorf("duplicate column(%v) for insert", c)
				return
			}
			colMap[col.Name] = i
		} else { // 表中没有该列
			if _, ok := unrecognized[c]; ok {
				err = fmt.Errorf("duplicate unrecognized column(%v) for insert", c)
				return
			}
			unrecognized[c] = i
		}
	}

	db := t.DbName()
	table := t.Name()

	// 自动添加列
	if len(unrecognized) > 0 {
		addcols := make([]string, 0, len(unrecognized))
		for k := range unrecognized {
			addcols = append(addcols, k)
			log.Info("%s-%s col[%v] is null, prepare add", db, table, k)
		}
		log.Debug("autoAddColumn %v", unrecognized)
		err = p.autoAddColumn(t, addcols)
		if err != nil {
			log.Error("auto add column[%s:%s:%v] failed, err[%v]", db, table, addcols, err)
			return
		}
		newTable = p.router.FindTable(db, table)
		if newTable == nil {
			err = fmt.Errorf("update table %s.%s info failed", db, table)
			return
		}
		for c, index := range unrecognized {
			col := newTable.FindColumn(c)
			if col == nil {
				log.Info("%s-%s col[%s] is null, may be add failure ", db, table, c)
				err = fmt.Errorf("invalid column %s-%s col[%s] ", db, table, c)
				return
			}
			colMap[col.Name] = index
		}
	}
	return
}

// 检查插入时是否少了主键列： 如果缺少的列是自增id，返回要填充的col
func (p *Proxy) checkPKMissing(t *Table, colMap map[string]int) (string, error) {
	var pkName string
	// 是否缺少主键
	for _, pk := range t.PKS() {
		if _, ok := colMap[pk]; !ok {
			if col := t.FindColumn(pk); col != nil && col.AutoIncrement { //表定义时建，限制只能有一个主键列为自增
				pkName = pk
				continue
			}
			return "", fmt.Errorf("pk(%s) is required for insert", pk)
		}
	}
	return pkName, nil
}

// EncodeRow 编码一行
func (p *Proxy) EncodeRow(t *Table, colMap map[string]int, rowValue InsertRowValue) (*kvrpcpb.KeyValue, error) {
	key := util.EncodeStorePrefix(util.Store_Prefix_KV, t.GetId())
	var value []byte
	var err error

	// 编码主键
	for _, pk := range t.PKS() {
		i, ok := colMap[pk]
		if !ok {
			return nil, fmt.Errorf("pk(%s) is missing", pk)
		}
		col := t.FindColumn(pk)
		if col == nil {
			return nil, fmt.Errorf("invalid pk column(%s)", pk)
		}
		if i >= len(rowValue) {
			return nil, fmt.Errorf("invalid pk(%s) value", pk)
		}
		if rowValue[i] == nil {
			return nil, fmt.Errorf("pk(%s) could not be NULL", pk)
		}
		key, err = util.EncodePrimaryKey(key, col, rowValue[i])
		if err != nil {
			return nil, err
		}
	}

	// 编码列值
	/**
	for _, col := range t.table.Columns {
		// TODO: 不需要编码主键列
		// if col.PrimaryKey == 1 { // 跳过主键列
		// 	continue
		// }
		i, ok := colMap[col.Name]
		if !ok {
			return nil, fmt.Errorf("column(%s) is missing", col.Name)
		}
		if i >= len(rowValue) {
			return nil, fmt.Errorf("invalid column(%s)", col.Name)
		}
		value, err = util.EncodeColumnValue(value, col, rowValue[i])
		if err != nil {
			return nil, err
		}
	}
	*/
	for colName, colIndex := range colMap {
		col := t.FindColumn(colName)
		if col == nil {
			return nil, fmt.Errorf("invalid table(%s) column(%s)", t.GetName(), colName)
		}
		if col.GetPrimaryKey() == 1 {
			continue
		}
		if colIndex >= len(rowValue) {
			return nil, fmt.Errorf("invalid column(%s)", col.Name)
		}
		value, err = util.EncodeColumnValue(value, col, rowValue[colIndex])
		if err != nil {
			return nil, err
		}
	}

	expireAt, err := findRowExpire(colMap, rowValue)
	if err != nil {
		return nil, fmt.Errorf("find row ttl error(%s)", err)
	}

	return &kvrpcpb.KeyValue{
		Key:      key,
		Value:    value,
		ExpireAt: expireAt,
	}, nil
}

func (p *Proxy) batchInsert(context *dskv.ReqContext, t *Table, kvPairs []*kvrpcpb.KeyValue) (affected uint64, duplicateKey []byte, retryKVPairs []*kvrpcpb.KeyValue, err error) {
	// 首先排序,这个很重要
	sort.Sort(KvParisSlice(kvPairs))

	var kvGroup [][]*kvrpcpb.KeyValue
	retryKVPairs = make([]*kvrpcpb.KeyValue, 0)
	// 按照route的范围划分kv group
	ggroup := make(map[uint64][]*kvrpcpb.KeyValue)
	for _, kv := range kvPairs {
		log.Debug("task insert add key[%v]", kv.GetKey())
		l, _err := t.ranges.LocateKey(context.GetBackOff(), kv.GetKey())
		if _err != nil {
			err = _err
			log.Warn("locate key failed, err %v", err)
			return
		}
		var group []*kvrpcpb.KeyValue
		var ok bool
		if group, ok = ggroup[l.Region.Id]; !ok {
			group = make([]*kvrpcpb.KeyValue, 0)
			ggroup[l.Region.Id] = group
		}
		group = append(group, kv)

		//map copy value must reset
		ggroup[l.Region.Id] = group
		// 每100个kv切割一下
		if len(group) >= 100 {
			kvGroup = append(kvGroup, group)
			delete(ggroup, l.Region.Id)
		}
	}
	for _, group := range ggroup {
		if len(group) > 0 {
			kvGroup = append(kvGroup, group)
		}
	}
	log.Debug("%s, task insert %s group size: %d", context, t.GetName(), len(kvGroup))
	// 只需要访问一个range
	if len(kvGroup) == 1 {
		affected, duplicateKey, err = p.insert(context, t, kvGroup[0])
		if err != nil && err == dskv.ErrRouteChange {
			retryKVPairs = append(retryKVPairs, kvGroup[0]...)
			return 0, nil, retryKVPairs, err
		} else {
			return affected, duplicateKey, retryKVPairs, err
		}
	}
	startTime := time.Now()
	// for more range batch insert
	var tasks []*InsertTask
	for _, rows := range kvGroup {
		task := GetInsertTask()
		cClone := context.Clone()
		task.init(cClone, p, t, rows)
		err = p.Submit(task)
		if err != nil {
			// release task
			PutInsertTask(task)
			log.Error("submit insert task failed, err[%v]", err)
			return
		}
		tasks = append(tasks, task)
	}
	// 存在部分task不能被回收的问题，但是不会造成内存泄漏
	retryKVPairs = make([]*kvrpcpb.KeyValue, 0)
	for _, task := range tasks {
		err_ := task.Wait()
		if err_ != nil {
			err = err_
			if err == dskv.ErrRouteChange {
				retryKVPairs = append(retryKVPairs, task.rows...)
				log.Debug("insert task route change, table:%s, err[%v]", t.GetName(), err)
				PutInsertTask(task)
				continue
			} else {
				log.Error("insert task do failed, table:%s, err[%v]", t.GetName(), err)
				PutInsertTask(task)
				return
			}

		}
		if task.rest.GetDuplicateKey() != nil {
			duplicateKey = task.rest.GetDuplicateKey()
			PutInsertTask(task)
			return
		}
		affected += task.rest.GetAffected()
		PutInsertTask(task)
	}
	context.GetBackOff().CombineTime(int(time.Since(startTime).Nanoseconds()/1000000))
	log.Debug("%s execute batch task finish", context)
	return
}

func (p *Proxy) insertRows(t *Table, colMap map[string]int, rows []InsertRowValue) (affected uint64, duplicateKey []byte, err error) {
	var kvPairs []*kvrpcpb.KeyValue
	var kv *kvrpcpb.KeyValue
	for i, r := range rows {
		kv, err = p.EncodeRow(t, colMap, r)
		if err != nil {
			log.Error("[insert] table %s.%s encode row at %d failed: %v", t.DbName(), t.Name(), i, err)
			return
		}
		kvPairs = append(kvPairs, kv)
	}

	var affectedTp uint64
	var duplicateKeyTp []byte
	var errTp error

	context  := dskv.NewPRConext(dskv.InsertMaxBackoff)
	var errForRetry error
	for metricLoop := 0; ; metricLoop++ {
		if kvPairs == nil || len(kvPairs) == 0 {
			break
		}
		if errForRetry != nil {
			errForRetry = context.GetBackOff().Backoff(dskv.BoMSRPC, errForRetry)
			if errForRetry != nil {
				log.Error("%s execute timeout", context)
				return
			}
		}
		if metricLoop > 0 {
			log.Info("%s, retry insert table:%s, row size:%d, loop: %v", context, t.GetName(), len(kvPairs), metricLoop)
		} else {
			log.Debug("%s, insert table:%s, row size:%d", context, t.GetName(), len(kvPairs))
		}

		if len(kvPairs) > 1 {
			affectedTp, duplicateKeyTp, kvPairs, errTp = p.batchInsert(context, t, kvPairs)
		} else {
			affectedTp, duplicateKeyTp, errTp = p.insert(context, t, kvPairs)
		}

		if errTp != nil && errTp == dskv.ErrRouteChange {
			log.Warn("%s route change ,retry table:%s, row size:%v", context, t.GetName(), len(kvPairs))
			duplicateKey = duplicateKeyTp
			affected += affectedTp
			errForRetry = errTp
			err = errTp
			continue
		}
		duplicateKey = duplicateKeyTp
		affected += affectedTp
		err = errTp
		break
	}
	log.Debug("%s execute finish", context)
	return
}

func (p *Proxy) insert(context *dskv.ReqContext, t *Table, rows []*kvrpcpb.KeyValue) (affected uint64, duplicateKey []byte, err error) {
	if len(rows) == 0 {
		err = ErrEmptyRow
		return
	}
	now := p.clock.Now()
	req := &kvrpcpb.InsertRequest{
		Rows:           rows,
		CheckDuplicate: t.PkDupCheck(),
		Timestamp:      &timestamp.Timestamp{WallTime: now.WallTime, Logical: now.Logical},
	}
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
	//单range写入，包括单行数据，同range多行数据
	var resp *kvrpcpb.InsertResponse
	resp, _, err = proxy.Insert(context, req, rows[0].GetKey())
	if err != nil {
		return
	}
	if resp.GetCode() == 0 {
		affected = resp.GetAffectedKeys()
	} else if resp.GetDuplicateKey() != nil {
		duplicateKey = resp.GetDuplicateKey()
	}
	if resp.GetCode() > 0 {
		err = CodeToErr(int(resp.GetCode()))
		return
	}
	return
}

func (p *Proxy) autoAddColumn(t *Table, cols []string) error {
	if err := p.router.addColumnToRemote(t.GetDbId(), t.GetId(), cols); err != nil {
		return err
	}
	db := p.router.findDBById(t.GetDbId())
	if db != nil {
		_t, err := db.loadTableFromRemote(t.GetName())
		if err != nil {
			return err
		}
		if t != nil {
			table := NewTable(_t, db.cli, 5*time.Minute)
			table.ranges = t.ranges
			db.AddTable(table)
		}
	}
	return nil
}

type KvParisSlice []*kvrpcpb.KeyValue

func (p KvParisSlice) Len() int {
	return len(p)
}

func (p KvParisSlice) Swap(i int, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p KvParisSlice) Less(i int, j int) bool {
	return bytes.Compare(p[i].GetKey(), p[j].GetKey()) < 0
}

func findRowExpire(colMap map[string]int, rowValue InsertRowValue) (int64, error) {
	idx, ok := colMap[util.TTL_COL_NAME]
	if !ok {
		return 0, nil
	}
	if idx >= len(rowValue) {
		return 0, fmt.Errorf("invalid column(%s) pos", util.TTL_COL_NAME)
	}
	ttl, err := strconv.ParseInt(hack.String(rowValue[idx]), 10, 64)
	if err != nil {
		return 0, err
	}
	// ms to nano seconds
	return ttl * 1000000, nil
}
