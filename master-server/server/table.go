package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"model/pkg/metapb"
	"util/deepcopy"
	"util/log"

	"github.com/gogo/protobuf/proto"
	"golang.org/x/net/context"
)

var (
	MAX_COLUMN_NAME_LENGTH = 128
)

var PREFIX_AUTO_TRANSFER_TABLE string = fmt.Sprintf("$auto_transfer_table_%d")
var PREFIX_AUTO_FAILOVER_TABLE string = fmt.Sprintf("$auto_failover_table_%d")

type Table struct {
	*metapb.Table
	// 表属性锁
	schemaLock sync.RWMutex
	// 路由锁
	lock     sync.Mutex
	maxColId uint64

	// 删除时间,删除的table会保留三天，之后正式删除
	deleteTime time.Time
}

type rangeToCreate struct {
	startKey []byte
	endKey   []byte
}

func NewTable(t *metapb.Table) *Table {
	var maxColId uint64
	for _, col := range t.GetColumns() {
		if col.GetId() > maxColId {
			maxColId = col.GetId()
		}
	}
	table := &Table{
		Table:    t,
		maxColId: maxColId,
	}
	return table
}

func (t *Table) GenColId() uint64 {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.maxColId++
	return t.maxColId
}

type TableProperty struct {
	Columns []*metapb.Column `json:"columns"`
	Regxs   []*metapb.Column `json:"regxs"`
}

func (t *Table) Name() string {
	return t.GetName()
}

type ByPrimaryKey []*metapb.Column

func (s ByPrimaryKey) Len() int           { return len(s) }
func (s ByPrimaryKey) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ByPrimaryKey) Less(i, j int) bool { return s[i].PrimaryKey < s[j].PrimaryKey }

func (t *Table) GetColumns() []*metapb.Column {
	var columns []*metapb.Column
	_columns := t.Columns
	for _, c := range _columns {
		columns = append(columns, c)
	}
	return columns
}

func (t *Table) GetPkColumns() []*metapb.Column {
	var columns []*metapb.Column
	_columns := t.Columns
	for _, c := range _columns {
		if c.PrimaryKey > 0 {
			columns = append(columns, c)
		}
	}
	return columns
}

func (t *Table) GetColumnByName(name string) (*metapb.Column, bool) {
	_columns := t.Columns
	for _, c := range _columns {
		if c.GetName() == name {
			return c, true
		}
	}
	return nil, false
}

func (t *Table) GetColumnById(id uint64) (*metapb.Column, bool) {
	_columns := t.Columns
	for _, c := range _columns {
		if c.Id == id {
			return c, true
		}
	}
	return nil, false
}

func (t *Table) MergeColumn(source []*metapb.Column, cluster *Cluster) error {
	t.schemaLock.Lock()
	defer t.schemaLock.Unlock()
	table := deepcopy.Iface(t.Table).(*metapb.Table)

	newColMap := make(map[string]uint64, 0)

	for _, col := range source {
		col.Name = strings.ToLower(col.GetName())

		if len(col.GetName()) > MAX_COLUMN_NAME_LENGTH {
			return ErrColumnNameTooLong
		}
		if isSqlReservedWord(col.GetName()) {
			log.Warn("col[%s] is sql reserved word", col.Name)
			return ErrSqlReservedWord
		}

		newColMap[col.Name] = col.GetId()

		if col.GetId() == 0 { // add column
			if col.PrimaryKey == 1 {
				log.Warn("pk column is not allow change")
				return ErrInvalidColumn
			}
			if _, find := t.GetColumnByName(col.Name); find {
				log.Warn("column[%s:%s:%s] is already existed", t.GetDbName(), t.GetName(), col.Name)
				return ErrDupColumnName
			}
			col.Id = t.GenColId()
			table.Columns = append(table.Columns, col)
			table.Epoch.ConfVer++
		} else { // column maybe rename
			tt := NewTable(table)
			col_, find := tt.GetColumnById(col.GetId())
			if !find {
				log.Warn("column[%s:%s:%s] is not exist", tt.GetDbName(), tt.GetName(), col.GetId())
				return ErrInvalidColumn
			}
			if col_.Name == col.GetName() {
				continue
			}

			col_.Name = col.GetName()
			table.Epoch.ConfVer++
		}
	}

	//删除列处理
	var tartCols []*metapb.Column
	for _, col := range table.GetColumns() {
		_, found := newColMap[col.GetName()]
		if col.PrimaryKey == 1 || found{
			tartCols = append(tartCols, col)
		}
	}
	table.Columns = tartCols
	table.Epoch.ConfVer++

	err := cluster.storeTable(table)
	if err != nil {
		log.Error("store table failed, err[%v]", err)
		return err
	}
	t.Table = table
	return nil
}

func checkTTLDataType(dataType metapb.DataType) bool {
	return metapb.DataType_BigInt == dataType
}

func (t *Table) UpdateSchema(columns []*metapb.Column, store Store) ([]*metapb.Column, error) {
	t.schemaLock.Lock()
	defer t.schemaLock.Unlock()
	table := deepcopy.Iface(t.Table).(*metapb.Table)
	//not concurrent safy,need in syncronized block
	cols := make([]*metapb.Column, 0)
	allCols := make([]*metapb.Column, 0)
	colMap := make(map[string]*metapb.Column)
	for _, tempCol := range table.Columns {
		colMap[tempCol.GetName()] = tempCol
		allCols = append(allCols, tempCol)
	}

	var match bool
	for _, newCol := range columns {
		_, ok := colMap[newCol.GetName()]
		if ok {
			continue
		}
		//scan template column
		for _, tempCol := range table.Regxs {
			isMatch, err := regexp.Match(tempCol.GetName(), []byte(newCol.Name))
			if err == nil && isMatch {
				if isSqlReservedWord(newCol.Name) {
					log.Warn("col[%s:%s:%s] is sql reserved word",
						table.GetDbName(), table.GetName(), newCol.Name)
					return nil, ErrSqlReservedWord
				}
				addCol := deepcopy.Iface(tempCol).(*metapb.Column)
				addCol.Name = newCol.Name
				addCol.Id = t.GenColId()
				cols = append(cols, addCol)
				allCols = append(allCols, addCol)
				match = true
				break
			}
		}
	}
	if match == false {
		return nil, errors.New("none of columns matches")
	}
	props, err := ToTableProperty(allCols)
	if err != nil {
		return nil, err
	}
	table.Columns = allCols
	table.Properties = props
	table.Epoch.ConfVer++
	data, err := proto.Marshal(table)
	if err != nil {
		return nil, err
	}
	batch := store.NewBatch()
	key := []byte(fmt.Sprintf("%s%d", PREFIX_TABLE, t.GetId()))
	batch.Put(key, data)
	err = batch.Commit()
	if err != nil {
		return nil, err
	}
	t.Table = table
	//t.routes = t.genRoutes()
	return cols, nil
}

type TableCache struct {
	lock    sync.RWMutex
	tableIs map[uint64]*Table
	tableNs map[string]*Table
}

func NewTableCache() *TableCache {
	return &TableCache{
		tableIs: make(map[uint64]*Table),
		tableNs: make(map[string]*Table)}
}

func (tc *TableCache) Add(t *Table) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	tc.tableIs[t.GetId()] = t
	tc.tableNs[t.GetName()] = t
}

func (tc *TableCache) DeleteById(id uint64) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if t, find := tc.tableIs[id]; find {
		delete(tc.tableIs, t.GetId())
		delete(tc.tableNs, t.GetName())
		return
	}
}

func (tc *TableCache) DeleteByName(name string) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if t, find := tc.tableNs[name]; find {
		delete(tc.tableIs, t.GetId())
		delete(tc.tableNs, t.GetName())
		return
	}
}

func (tc *TableCache) FindTableById(id uint64) (*Table, bool) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	if t, find := tc.tableIs[id]; find {
		return t, true
	}
	return nil, false
}

func (tc *TableCache) FindTableByName(name string) (*Table, bool) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	if t, find := tc.tableNs[name]; find {
		return t, true
	}
	return nil, false
}

func (tc *TableCache) GetAllTable() []*Table {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	var tables []*Table
	for _, t := range tc.tableIs {
		tables = append(tables, t)
	}
	return tables
}

func (tc *TableCache) Size() int {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	return len(tc.tableIs)
}

type GlobalTableCache struct {
	lock    sync.RWMutex
	tableIs map[uint64]*Table
}

func NewGlobalTableCache() *GlobalTableCache {
	return &GlobalTableCache{
		tableIs: make(map[uint64]*Table)}
}

func (tc *GlobalTableCache) Add(t *Table) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	tc.tableIs[t.GetId()] = t
}

func (tc *GlobalTableCache) DeleteById(id uint64) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if t, find := tc.tableIs[id]; find {
		delete(tc.tableIs, t.GetId())
		return
	}
}

func (tc *GlobalTableCache) FindTableById(id uint64) (*Table, bool) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	if t, find := tc.tableIs[id]; find {
		return t, true
	}
	return nil, false
}

func (tc *GlobalTableCache) GetAllTable() []*Table {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	var tables []*Table
	for _, t := range tc.tableIs {
		tables = append(tables, t)
	}
	return tables
}

func (tc *GlobalTableCache) Size() int {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	return len(tc.tableIs)
}

type CreateTable struct {
	*Table
	sync.RWMutex
	ranges             map[uint64]*Range
	rangesToCreateList chan *rangeToCreate
}

func NewCreateTable(t *Table, n uint64) *CreateTable {
	return &CreateTable{
		Table:              t,
		ranges:             make(map[uint64]*Range),
		rangesToCreateList: make(chan *rangeToCreate, n),
	}
}

func (t *CreateTable) AddRange(r *Range) {
	t.Lock()
	defer t.Unlock()
	t.ranges[r.GetId()] = r
}

func (t *CreateTable) DeleteRange(rangeID uint64) {
	t.Lock()
	defer t.Unlock()
	delete(t.ranges, rangeID)
}

func (t *CreateTable) GetAllRanges() []*Range {
	t.RLock()
	defer t.RUnlock()
	var ranges []*Range
	for _, r := range t.ranges {
		ranges = append(ranges, r)
	}
	return ranges
}

type CreateTableCache struct {
	lock    sync.RWMutex
	tableIs map[uint64]*CreateTable
}

func NewCreateTableCache() *CreateTableCache {
	return &CreateTableCache{
		tableIs: make(map[uint64]*CreateTable),
	}
}

func (tc *CreateTableCache) Add(t *CreateTable) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	tc.tableIs[t.GetId()] = t
}

func (tc *CreateTableCache) Delete(id uint64) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if v, find := tc.tableIs[id]; find {
		if v.rangesToCreateList != nil {
			close(v.rangesToCreateList)
		}

		delete(tc.tableIs, id)
		return
	}
}

func (tc *CreateTableCache) FindTable(id uint64) (*CreateTable, bool) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	if t, find := tc.tableIs[id]; find {
		return t, true
	}
	return nil, false
}

func (tc *CreateTableCache) GetAllTable() []*CreateTable {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	var tables []*CreateTable
	for _, t := range tc.tableIs {
		tables = append(tables, t)
	}
	return tables
}

func (tc *CreateTableCache) Size() int {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	return len(tc.tableIs)
}

func ToTableProperty(cols []*metapb.Column) (string, error) {
	tp := &TableProperty{
		Columns: make([]*metapb.Column, 0),
	}
	for _, c := range cols {
		tp.Columns = append(tp.Columns, c)
	}
	bytes, err := json.Marshal(tp)
	if err != nil {
		return "", err
	} else {
		return string(bytes), nil
	}
}

func EditProperties(properties string) ([]*metapb.Column, error) {
	tp := new(TableProperty)
	log.Debug("edit properties string: %v", properties)
	if err := json.Unmarshal([]byte(properties), tp); err != nil {
		return nil, err
	}
	log.Debug("edit properties struct: %v", *tp)
	if tp.Columns == nil {
		return nil, ErrInvalidColumn
	}

	return tp.Columns, nil
}

func parseColumn(cols []*metapb.Column) error {
	var hasPk bool
	for _, c := range cols {
		c.Name = strings.ToLower(c.Name)
		if len(c.GetName()) > MAX_COLUMN_NAME_LENGTH {
			return ErrColumnNameTooLong
		}
		if c.PrimaryKey == 1 {
			if c.Nullable {
				return ErrPkMustNotNull
			}
			if len(c.DefaultValue) > 0 {
				return ErrPkMustNotSetDefaultValue
			}
			hasPk = true
		}
		if c.DataType == metapb.DataType_Invalid {
			return ErrInvalidColumn
		}
	}
	if !hasPk {
		return ErrMissingPk
	}
	columnName := make(map[string]interface{})
	//sort.Sort(ByPrimaryKey(cols))
	var id uint64 = 1
	for _, c := range cols {

		// check column name
		if _, ok := columnName[c.Name]; ok {
			return ErrDupColumnName
		} else {
			columnName[c.Name] = nil
		}

		// set column id
		c.Id = id
		id++

	}
	return nil
}

func ParseProperties(properties string) ([]*metapb.Column, []*metapb.Column, error) {
	tp := new(TableProperty)
	if err := json.Unmarshal([]byte(properties), tp); err != nil {
		log.Error("deserialize table property failed, err:[%v]", err)
		return nil, nil, err
	}
	if tp.Columns == nil {
		log.Error("column is nil")
		return nil, nil, ErrInvalidColumn
	}

	err := parseColumn(tp.Columns)
	if err != nil {
		log.Error("parse table column failed, err:[%v]", err)
		return nil, nil, err
	}

	for _, c := range tp.Regxs {
		// TODO check regx compile if error or not, error return
		if c.DataType == metapb.DataType_Invalid {
			return nil, nil, ErrInvalidColumn
		}
	}

	return tp.Columns, tp.Regxs, nil
}

func GetTypeByName(name string) metapb.DataType {
	for k, v := range metapb.DataType_name {
		if strings.Compare(strings.ToLower(v), strings.ToLower(name)) == 0 {
			return metapb.DataType(k)
		}
	}
	return metapb.DataType_Invalid
}

var DefaultRetentionTime = time.Hour * time.Duration(72)

type DeleteTableWorker struct {
	name     string
	ctx      context.Context
	cancel   context.CancelFunc
	interval time.Duration
}

func NewDeleteTableWorker(wm *WorkerManager, interval time.Duration) Worker {
	ctx, cancel := context.WithCancel(wm.ctx)
	return &DeleteTableWorker{
		name:     deleteTableWorkerName,
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
	}
}

func (dt *DeleteTableWorker) GetName() string {
	return dt.name
}

func (dt *DeleteTableWorker) Work(cluster *Cluster) {
	for _, table := range cluster.deletingTables.GetAllTable() {
		select {
		case <-dt.ctx.Done():
			return
		default:
		}
		// 超过三天，正式删除
		if table.Status == metapb.TableStatus_TableDeleting || time.Since(table.deleteTime) > DefaultRetentionTime {
			table.Status = metapb.TableStatus_TableDeleting
			key := []byte(fmt.Sprintf("%s%d", PREFIX_TABLE, table.GetId()))
			if err := cluster.store.Delete(key); err != nil {
				log.Error("MS worker delete expired table:[%s][%d] from store is failed.",
					table.GetName(), table.GetId())
				return
			}
			cluster.deletingTables.DeleteById(table.GetId())
		}
	}
	return
}

func (dt *DeleteTableWorker) AllowWork(cluster *Cluster) bool {
	return true
}

func (dt *DeleteTableWorker) GetInterval() time.Duration {
	return dt.interval
}

func (dt *DeleteTableWorker) Stop() {
	dt.cancel()
}

type CreateTableWorker struct {
	name     string
	ctx      context.Context
	cancel   context.CancelFunc
	interval time.Duration
}

func NewCreateTableWorker(wm *WorkerManager, interval time.Duration) Worker {
	ctx, cancel := context.WithCancel(wm.ctx)
	return &CreateTableWorker{
		name:     createTableWorkerName,
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
	}
}

func (dt *CreateTableWorker) GetName() string {
	return dt.name
}

func (dt *CreateTableWorker) Work(c *Cluster) {
	for _, table := range c.creatingTables.GetAllTable() {
		select {
		case <-dt.ctx.Done():
			return
		default:
		}

		log.Debug("CreateTable: table %v is creating", table.GetName())
		switch table.Status {
		case metapb.TableStatus_TableInit:
			err := dt.createRange(c, table)
			if err != nil {
				log.Error("create table %v failed, err: %v", table.Table.Table, err)
				if db, find := c.FindDatabaseById(table.GetDbId()); find {
					db.DeleteTableById(table.GetId())
				}
				c.creatingTables.Delete(table.GetId())
				c.workingTables.DeleteById(table.GetId())
				for _, r := range table.GetAllRanges() {
					c.DeleteRange(r.GetId())
				}
			}
		case metapb.TableStatus_TablePrepare:
			// 等待初始创建的分片都满足了三副本的要求
			dt.waitRangePeerReady(c, table)
		default:
			c.creatingTables.Delete(table.GetId())
			continue
		}
	}

	return
}

func (dt *CreateTableWorker) AllowWork(cluster *Cluster) bool {
	return true
}

func (dt *CreateTableWorker) GetInterval() time.Duration {
	return dt.interval
}

func (dt *CreateTableWorker) Stop() {
	dt.cancel()
}

func (dt *CreateTableWorker) createRange(c *Cluster, table *CreateTable) error {
	start := time.Now()
	var err error
	for {
		log.Debug("table %v is creating range", table.GetName())
		select {
		case <-dt.ctx.Done():
			return nil
		case create, ok := <-table.rangesToCreateList:
			if !ok {
				// 异常关闭
				log.Error("table %v rangesToCreateList is closed", table.GetName())
				return ErrInternalError
			}
			region, err := c.newRangeByScope(create.startKey, create.endKey, table.Table)
			if err != nil {
				return err
			}
			table.AddRange(region)
			c.AddRange(region)
			// create range remote
			if err = c.createRangeRemote(region.Range); err != nil {
				return fmt.Errorf("create range[%v, %v] failed, err[%v]", region.GetTableId(), region.GetId(), err)
			}
		default:
			// 创建分片结束
			if table.Status != metapb.TableStatus_TableInit {
				return nil
			}
			t := deepcopy.Iface(table.Table.Table).(*metapb.Table)
			t.Status = metapb.TableStatus_TablePrepare
			err = c.storeTable(t)
			if err != nil {
				log.Warn("store table %v failed, err %v", t, err)
				return err
			}
			table.Table.Table = t
			log.Info("table %v create range finish, used %v", t.GetName(), time.Since(start))
			return nil
		} // select end
	} // for end
	return nil
}

func (dt *CreateTableWorker) waitRangePeerReady(c *Cluster, table *CreateTable) {
	log.Debug("create table %v ranges: %v", table.GetName(), table.GetAllRanges())
	for _, r := range table.GetAllRanges() {
		log.Debug("check table: %v, range: %v peer number", table.GetName(), r.GetId())
		if len(r.GetPeers()) < c.opt.GetMaxReplicas() {
			log.Info("check table: %v, range: %v peers number %v != %v",
				table.GetName(), r.GetId(), len(r.GetPeers()), c.opt.GetMaxReplicas())
			return
		}
	}
	tt := deepcopy.Iface(table.Table.Table).(*metapb.Table)
	tt.Status = metapb.TableStatus_TableRunning
	err := c.storeTable(tt)
	if err != nil {
		return
	}
	c.creatingTables.Delete(table.GetId())
	table.Table.Table = tt
	log.Info("table %s:%s ready", table.GetDbName(), table.GetName())
	return
}
