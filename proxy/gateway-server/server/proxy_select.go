package server

import (
	"fmt"

	"bytes"
	"model/pkg/kvrpcpb"
	"model/pkg/timestamp"
	"pkg-go/ds_client"
	"proxy/gateway-server/mysql"
	"proxy/gateway-server/sqlparser"
	//"util"
	"proxy/store/dskv"
	"util/log"
	"master-server/engine/errors"
)

func (p *Proxy) HandleSelect(db string, stmt *sqlparser.Select, args []interface{}) (*mysql.Result, error) {
	//var parseTime time.Time
	//start := time.Now()
	//defer func() {
	//	delay := time.Since(start)
	//	trace := sqlparser.NewTrackedBuffer(nil)
	//	stmt.Format(trace)
	//	//p.sqlStats(trace.String(), time.Since(start), time.Since(parseTime))
	//	//p.metric.AddApiWithDelay("select", true, delay)
	//	if delay > time.Duration(p.config.SelectSlowLog)*time.Millisecond {
	//		log.Info("[select slow log %v %v ", delay.String(), trace.String())
	//	}
	//}()
	parser := &StmtParser{}

	// 解析表名
	tableName := parser.parseTable(stmt)
	t := p.router.FindTable(db, tableName)
	if t == nil {
		log.Error("[select] table %s.%s doesn.t exist", db, tableName)
		return nil, fmt.Errorf("Table '%s.%s' doesn't exist", db, tableName)
	}

	// 解析选择列
	cols, err := parser.parseSelectCols(stmt)
	if err != nil {
		log.Error("[select] parse colum error: %v", err)
		return nil, fmt.Errorf("handle select parseColumn err %s", err.Error())
	}
	fieldList, err := makeFieldList(t, cols)
	if err != nil {
		log.Error("[select] find %s.%s field list error(%s), ", t.DbName(), t.Name(), err)
		return nil, err
	}

	// 解析where条件
	var matchs []Match
	if stmt.Where != nil {
		// TODO: 支持OR表达式
		matchs, err = parser.parseWhere(stmt.Where)
		if err != nil {
			log.Error("handle select parse where error(%v)", err.Error())
			return nil, err
		}
	}

	var limit *Limit
	if stmt.Limit != nil {
		offset, count, err := parseLimit(stmt.Limit)
		if err != nil {
			log.Error("select parse limit error[%v]", err)
			return nil, err
		}
		if count > DefaultMaxRawCount {
			log.Warn("limit count exceeding the maximum limit")
			return nil, ErrExceedMaxLimit
		}
		limit = &Limit{offset: offset, rowCount: count}
	}

	if log.GetFileLogger().IsEnableDebug() {
		log.Debug("where %v", stmt.Where)
		log.Debug("have %v", stmt.Having)
		log.Debug("cols %v", cols)
		log.Debug("matchs %v", matchs)
	}

	//parseTime = time.Now()
	// 向dataserver查询
	rowss, err := p.doSelect(t, fieldList, matchs, limit, nil)
	if err != nil {
		return nil, err
	}

	columns, err := fieldList2ColNames(fieldList)
	if err != nil {
		log.Error("[select] Table %s.%s covert field list to column name failed(%v)", t.DbName(), t.Name(), err)
		return nil, fmt.Errorf("covert field list error(%v)", err)
	}

	// 合并结果
	return buildSelectResult(stmt, rowss, columns)
}

func (p *Proxy) doSelect(t *Table, fieldList []*kvrpcpb.SelectField, matches []Match, limit *Limit, userScope *Scope) ([][]*Row, error) {
	var err error

	pbMatches, err := makePBMatches(t, matches)
	if err != nil {
		log.Error("[select]covert filter failed(%v), Table: %s.%s", err, t.DbName(), t.Name())
		return nil, err
	}

	pbLimit, err := makePBLimit(p, limit)
	if err != nil {
		log.Error("[select]covert limit failed(%v), Table: %s.%s", err, t.DbName(), t.Name())
		return nil, err
	}

	var key []byte
	var scope *kvrpcpb.Scope
	if userScope != nil {
		// TODO
		scope = &kvrpcpb.Scope{
			Start: userScope.Start,
			Limit: userScope.End,
		}
	} else {
		key, scope, err = findPKScope(t, pbMatches)
		if err != nil {
			log.Error("[select]get pk scope failed(%v), Table: %s.%s", err, t.DbName(), t.Name())
			return nil, err
		}
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("[select]pk key: [%v], scope: %v", key, scope)
		}
	}
	/** maybe repeat prefix 2017-12-22 dj
	if key != nil {
		key = append(util.EncodeStorePrefix(util.Store_Prefix_KV, t.GetId()), key...)
	}
	*/
	// TODO: pool
	now := p.clock.Now()
	sreq := &kvrpcpb.SelectRequest{
		Key:          key,
		Scope:        scope,
		FieldList:    fieldList,
		WhereFilters: pbMatches,
		Limit:        pbLimit,
		Timestamp:    &timestamp.Timestamp{WallTime: now.WallTime, Logical: now.Logical},
		ReadPoint:    t.ReadFromNode,
	}
	return p.selectRemote(t, sreq)
}

func (p *Proxy) selectRemote(t *Table, req *kvrpcpb.SelectRequest) ([][]*Row, error) {
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)

	var pbRows [][]*kvrpcpb.Row
	var err error
	// single get
	if len(req.Key) != 0 {
		pbRows, err = p.singleSelectRemote(proxy, req, req.GetKey())
	} else {
		// 聚合函数，并行执行, 并且没有limit、offset逻辑
		if len(req.FieldList) > 0 && req.FieldList[0].Typ == kvrpcpb.SelectField_AggreFunction {
			pbRows, err = p.selectAggre(t, proxy, req)
		} else { // 普通的范围查询
			pbRows, err = p.rangeSelectRemote(proxy, req)
		}
	}
	if err != nil {
		return nil, err
	}

	return decodeRows(t, req.FieldList, pbRows)
}

func (p *Proxy) singleSelectRemote(kvproxy *dskv.KvProxy, req *kvrpcpb.SelectRequest, key []byte) ([][]*kvrpcpb.Row, error) {
	resp, _, err := kvproxy.SqlQuery(req, key)
	if err != nil {
		return nil, err
	}

	var rows []*kvrpcpb.Row
	if resp.GetCode() == 0 {
		rows = resp.GetRows()
	} else {
		return nil, fmt.Errorf("remote server return error. Code=%d", resp.Code)
	}
	if log.GetFileLogger().IsEnableDebug() {
		log.Debug("query rows[%v]", rows)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return [][]*kvrpcpb.Row{rows}, nil
}

func (p *Proxy) rangeSelectRemote(kvproxy *dskv.KvProxy, sreq *kvrpcpb.SelectRequest) ([][]*kvrpcpb.Row, error) {
	var key, start, end []byte
	var resp *kvrpcpb.SelectResponse
	var route *dskv.KeyLocation
	var err error
	var allRows [][]*kvrpcpb.Row
	var all, count uint64
	var offset, rawCount uint64
	scope := sreq.Scope
	limit := sreq.Limit
	var subLimit *kvrpcpb.Limit

	start = scope.Start
	end = scope.Limit
	var rangeCount int
	for {
		if key == nil {
			key = start
		} else if route != nil {
			key = route.EndKey
			// check key in range
			if bytes.Compare(key, start) < 0 || bytes.Compare(key, end) >= 0 {
				// 遍历完成，直接退出循环
				break
			}
		}
		if limit != nil {
			if limit.Offset > all {
				offset = limit.Offset - all
				rawCount = limit.Count
			} else {
				offset = 0
				if limit.Count > (all - limit.Offset) {
					rawCount = limit.Count - (all - limit.Offset)
				} else {
					break
				}
			}
			subLimit = &kvrpcpb.Limit{Offset: offset, Count: rawCount}
			log.Debug("limit %v", subLimit)
		}
		now := p.clock.Now()
		req := &kvrpcpb.SelectRequest{
			Scope:        scope,
			FieldList:    sreq.FieldList,
			WhereFilters: sreq.WhereFilters,
			Limit:        subLimit,
			Timestamp:    &timestamp.Timestamp{WallTime: now.WallTime, Logical: now.Logical},
		}
		resp, route, err = kvproxy.SqlQuery(req, key)
		if err != nil {
			return nil, err
		}
		if resp.GetCode() != 0 {
			log.Error("remote server return code: %v", resp.GetCode())
			return nil,errors.New(fmt.Sprintf("response code is err %v",resp.GetCode()))
		}
		rangeCount++

		if log.GetFileLogger().IsEnableDebug() {
			if len(resp.GetRows()) > 64 {
				log.Debug("===route %d offset %d rows(%d)", route.Region.Id, resp.GetOffset(), len(resp.GetRows()))
			} else {
				log.Debug("===route %d offset %d rows(%d) %v", route.Region.Id, resp.GetOffset(), len(resp.GetRows()), resp.GetRows())
			}
		}
		rows := resp.GetRows()
		all += resp.GetOffset()

		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("----- offset: %v", resp.GetOffset())
		}
		if limit != nil && (uint64(len(rows))+count >= limit.Count) {
			rows = rows[:limit.Count-count]
			allRows = append(allRows, rows)
			return allRows, nil
		}
		if len(rows) > 0 {
			allRows = append(allRows, rows)
			count += uint64(len(rows))
		}
	}

	if rangeCount >= 3 {
		log.Warn("request to too much ranges(%d): req: %v", rangeCount, sreq)
	}

	return allRows, nil
}
