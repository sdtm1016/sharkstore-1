package server

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"model/pkg/metapb"
	"pkg-go/ds_client"
	"util"
	"util/deepcopy"
	"util/log"
	"util/ttlcache"

	"encoding/binary"
	sErr "master-server/engine/errors"

	"github.com/gogo/protobuf/proto"
)

// NOTE: prefix's first char must not be '\xff'
var SCHEMA_SPLITOR string = " "
var PREFIX_DB string = fmt.Sprintf("schema%sdb%s", SCHEMA_SPLITOR, SCHEMA_SPLITOR)
var PREFIX_TABLE string = fmt.Sprintf("schema%stable%s", SCHEMA_SPLITOR, SCHEMA_SPLITOR)
var PREFIX_NODE string = fmt.Sprintf("schema%snode%s", SCHEMA_SPLITOR, SCHEMA_SPLITOR)
var PREFIX_RANGE string = fmt.Sprintf("schema%srange%s", SCHEMA_SPLITOR, SCHEMA_SPLITOR)
var PREFIX_DELETED_RANGE string = fmt.Sprintf("schema%sdeleted_range%s", SCHEMA_SPLITOR, SCHEMA_SPLITOR)
var AUTO_INCREMENT_ID string = fmt.Sprintf("$auto_increment_id")
var PREFIX_TASK string = fmt.Sprintf("schema%stask%s", SCHEMA_SPLITOR, SCHEMA_SPLITOR)
var PREFIX_REPLICA string = fmt.Sprintf("schema%sreplica%s", SCHEMA_SPLITOR, SCHEMA_SPLITOR)
var PREFIX_PRE_GC string = fmt.Sprintf("schema%spre_gc%s", SCHEMA_SPLITOR, SCHEMA_SPLITOR)
var PREFIX_AUTO_TRANSFER string = fmt.Sprintf("$auto_transfer_%d")
var PREFIX_AUTO_FAILOVER string = fmt.Sprintf("$auto_failover_%d")

type Cluster struct {
	clusterId uint64
	nodeId    uint64
	leader    *Peer

	cli client.SchClient

	idGener IDGenerator
	opt     *scheduleOption

	lock sync.Mutex
	dbs  *DbCache
	// 存放正在创建中的table
	creatingTables *CreateTableCache
	// 存放正在提供服务的table
	workingTables *GlobalTableCache
	// 存放标记为删除的table
	deletingTables *GlobalTableCache

	//心跳不健康的range记录[不是实时的，不落盘]
	unhealthyRanges *ttlcache.TTLCache

	//落盘，切换leader时，load
	preGCRanges *GlobalPreGCRange

	deletedRanges *GlobalDeletedRange

	nodes  *NodeCache
	ranges *RangeCache

	workerPool map[string]bool
	//coordinator   *Coordinator
	hbManager       *hb_range_manager
	workerManger    *WorkerManager
	eventDispatcher *EventDispatcher
	metric          *Metric

	close bool

	store Store

	writeStatistics *lruCache

	autoFailoverUnable bool
	autoTransferUnable bool
}

func NewCluster(clusterId, nodeId uint64, store Store, opt *scheduleOption) *Cluster {
	cluster := &Cluster{
		clusterId:       clusterId,
		nodeId:          nodeId,
		cli:             client.NewSchRPCClient(),
		store:           store,
		opt:             opt,
		dbs:             NewDbCache(),
		nodes:           NewNodeCache(),
		ranges:          NewRangeCache(),
		writeStatistics: newLRUCache(writeStatLRUMaxLen),
		creatingTables:  NewCreateTableCache(),
		workingTables:   NewGlobalTableCache(),
		deletingTables:  NewGlobalTableCache(),
		unhealthyRanges: ttlcache.NewTTLCache(2 * time.Second * 60),
		preGCRanges:     NewGlobalPreGCRange(),
		deletedRanges:   NewGlobalDeletedRange(),
		idGener:         NewClusterIDGenerator(store),
	}
	cluster.workerPool = initWorkerPool()
	cluster.metric = NewMetric(cluster, opt.GetMetricAddress(), opt.GetMetricInterval())
	//cluster.coordinator = NewCoordinator(cluster, opt)
	cluster.workerManger = NewWorkerManager(cluster, opt)
	cluster.hbManager = NewHBRangeManager(cluster)
	cluster.eventDispatcher = NewEventDispatcher(cluster, opt)
	return cluster
}

func (c *Cluster) LoadCache() error {
	c.dbs = NewDbCache()
	c.nodes = NewNodeCache()
	c.ranges = NewRangeCache()
	c.writeStatistics = newLRUCache(writeStatLRUMaxLen)
	c.creatingTables = NewCreateTableCache()
	c.workingTables = NewGlobalTableCache()
	c.deletingTables = NewGlobalTableCache()
	c.idGener = NewClusterIDGenerator(c.store)
	c.preGCRanges = NewGlobalPreGCRange()
	c.deletedRanges = NewGlobalDeletedRange()

	err := c.loadNodes()
	if err != nil {
		log.Error("load node from store failed, err[%v]", err)
		return err
	}
	err = c.loadDatabases()
	if err != nil {
		log.Error("load database from store failed, err[%v]", err)
		return err
	}
	err = c.loadTables()
	if err != nil {
		log.Error("load table from store failed, err[%v]", err)
		return err
	}

	err = c.loadRanges()
	if err != nil {
		log.Error("load range from store failed, err[%v]", err)
		return err
	}

	err = c.loadTrashReplicas()
	if err != nil {
		log.Error("load range from store failed, err[%v]", err)
		return err
	}

	err = c.loadPreGCRanges()
	if err != nil {
		log.Error("load pre gc range from store failed, err[%v]", err)
		return err
	}

	err = c.loadDeletedRanges()
	if err != nil {
		log.Error("load deleted ranges from store failed, err[%v]", err)
		return err
	}

	return nil
}

func (c *Cluster) Start() {
	c.close = false
	c.metric.Run()
	c.eventDispatcher.Run()
	c.workerManger.Run()
}

func (c *Cluster) Close() {
	if c.close {
		return
	}
	c.cli.Close()
	//c.coordinator.Stop()
	c.eventDispatcher.Stop()
	c.workerManger.Stop()
	c.metric.Stop()
	c.close = true
}

func (c *Cluster) GenId() (uint64, error) {
	return c.idGener.GenID()
}

func (c *Cluster) FindDatabase(name string) (*Database, bool) {
	return c.dbs.FindDb(name)
}

func (c *Cluster) FindDatabaseById(id uint64) (*Database, bool) {
	return c.dbs.FindDbById(id)
}

func (c *Cluster) DeleteDatabase(name string) error {
	return nil
}

func (c *Cluster) CreateDatabase(name string, properties string) (*Database, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, ok := c.FindDatabase(name); ok {
		log.Error("database name:%s is existed!", name)
		return nil, ErrDupDatabase
	}

	id, err := c.idGener.GenID()
	if err != nil {
		log.Error("gen database ID failed, err[%v]", err)
		return nil, ErrGenID
	}
	db := &metapb.DataBase{
		Name:       name,
		Id:         id,
		Properties: properties,
		Version:    0,
		CreateTime: time.Now().Unix(),
	}
	err = c.storeDatabase(db)
	if err != nil {
		log.Error("store database[%s] failed", name)
		return nil, err
	}
	database := NewDatabase(db)
	c.dbs.Add(database)
	log.Info("create database[%s] success", name)
	return database, nil
}

func (c *Cluster) GetAllDatabase() []*Database {
	return c.dbs.GetAllDatabase()
}

func (c *Cluster) FindTableById(tableId uint64) (*Table, bool) {
	return c.workingTables.FindTableById(tableId)
}

func (c *Cluster) FindDeleteTableById(tableId uint64) (*Table, bool) {
	return c.deletingTables.FindTableById(tableId)
}

func (c *Cluster) GetAllUnhealthyRanges() []*Range {
	var ranges []*Range
	for _, r := range c.unhealthyRanges.GetAll() {
		ranges = append(ranges, r.(*Range))
	}
	return ranges
}

func (c *Cluster) FindPreGCRangeById(rangeId uint64) (*metapb.Range, bool) {
	return c.preGCRanges.FindRange(rangeId)
}

func (c *Cluster) EditTable(t *Table, properties string) error {
	columns, err := EditProperties(properties)
	if err != nil {
		return err
	}
	err = t.MergeColumn(columns, c)
	if err != nil {
		return err
	}
	return nil
}

type ByLetter [][]byte

func (s ByLetter) Len() int           { return len(s) }
func (s ByLetter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ByLetter) Less(i, j int) bool { return bytes.Compare(s[i], s[j]) == -1 }

func rangeKeysSplit(keys, sep string) ([][]byte, error) {
	ks := strings.Split(keys, sep)
	var ks_ [][]byte

	kmap := make(map[string]interface{})
	for _, k := range ks {
		if _, found := kmap[k]; !found {
			kmap[k] = nil
		} else {
			return nil, fmt.Errorf("dup key in split keys: %v", k)
		}
		ks_ = append(ks_, []byte(k))
	}
	return ks_, nil
}

func encodeSplitKeys(keys [][]byte, columns []*metapb.Column) ([][]byte, error) {
	var ret [][]byte
	for _, c := range columns {
		// 只按照第一主键编码
		if c.GetPrimaryKey() == 1 {
			for _, k := range keys {
				buf, err := util.EncodePrimaryKey(nil, c, k)
				if err != nil {
					return nil, err
				}
				ret = append(ret, buf)
			}
			// 只按照第一主键编码
			break
		}
	}
	return ret, nil
}

// step 1. create table
// step 2. create range in remote
// step 3. add range in cache and disk
func (c *Cluster) CreateTable(dbName, tableName string, columns, regxs []*metapb.Column, pkDupCheck bool, sliceKeys [][]byte) (*Table, error) {
	// todo override
	var isolationLabel string
	for _, col := range columns {
		if isSqlReservedWord(col.Name) {
			log.Warn("col[%s] is sql reserved word", col.Name)
			return nil, ErrSqlReservedWord
		}
	}

	// check the table if exist
	db, find := c.FindDatabase(dbName)
	if !find {
		return nil, ErrNotExistDatabase
	}
	db.Lock()
	defer db.UnLock()
	_t, find := db.FindTable(tableName)
	if find {
		log.Warn("dup Table %v", _t)
		return nil, ErrDupTable
	}

	// create table
	tableId, err := c.idGener.GenID()
	if err != nil {
		log.Error("cannot generate table[%s:%s] ID, err[%v]", dbName, tableName, err)
		return nil, ErrGenID
	}
	var start, end []byte
	start = util.EncodeStorePrefix(util.Store_Prefix_KV, tableId)
	_, end = bytesPrefix(start)
	t := &metapb.Table{
		Name:   tableName,
		DbName: dbName,
		Id:     tableId,
		DbId:   db.GetId(),
		//Properties: properties,
		Columns:    columns,
		Regxs:      regxs,
		Epoch:      &metapb.TableEpoch{ConfVer: uint64(1), Version: uint64(1)},
		CreateTime: time.Now().Unix(),
		PkDupCheck: pkDupCheck,
		IsolationLabel: isolationLabel,
	}

	var sharingKeys [][]byte
	table := NewTable(t)
	table.Status = metapb.TableStatus_TableInit
	db.AddTable(table)
	c.workingTables.Add(table)
	if len(sliceKeys) != 0 {
		_keys, err := encodeSplitKeys(sliceKeys, columns)
		if err != nil {
			log.Error("encode table preSplit keys failed(%v), keys: %v", err, _keys)
			db.DeleteTableById(tableId)
			c.workingTables.DeleteById(tableId)
			return nil, err
		}
		sort.Sort(ByLetter(_keys))
		var _sliceKeys [][]byte
		for _, key := range _keys {
			_sliceKeys = append(_sliceKeys, append(start, key...))
		}
		sharingKeys = append(sharingKeys, start)
		sharingKeys = append(sharingKeys, _sliceKeys...)
		sharingKeys = append(sharingKeys, end)
	} else {
		sharingKeys = append(sharingKeys, start)
		sharingKeys = append(sharingKeys, end)
	}
	keysNum := len(sharingKeys)
	createTable := NewCreateTable(table, uint64(keysNum-1))
	for i := 0; i < keysNum-1; i++ {
		createTable.rangesToCreateList <- &rangeToCreate{
			startKey: sharingKeys[i],
			endKey:   sharingKeys[i+1],
		}
	}
	// 准备完成后再加入创建队列
	c.creatingTables.Add(createTable)
	return table, nil
}

func (c *Cluster) DeleteTable(dbName, tableName string, fast bool) (*Table, error) {
	db, find := c.FindDatabase(dbName)
	if !find {
		return nil, ErrNotExistDatabase
	}
	db.Lock()
	defer db.UnLock()
	table, find := db.FindTable(tableName)
	if !find {
		return nil, ErrNotExistTable
	}

	batch := c.store.NewBatch()
	tt := deepcopy.Iface(table.Table).(*metapb.Table)
	var delTime time.Time
	if fast {
		// 强制修改为三天前，触发立即删除
		delTime = time.Now().Add(DefaultRetentionTime * time.Duration(-1))
		tt.Status = metapb.TableStatus_TableDeleting
	} else {
		delTime = time.Now()
		tt.Status = metapb.TableStatus_TableDelete
	}
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, uint64(delTime.Unix()))
	// 存放删除标记时间
	tt.Expand = value
	tbData, _ := proto.Marshal(tt)
	key := []byte(fmt.Sprintf("%s%d", PREFIX_TABLE, table.GetId()))
	batch.Put(key, tbData)
	// close auto switch
	key = []byte(fmt.Sprintf(PREFIX_AUTO_TRANSFER_TABLE, table.GetId()))
	batch.Delete(key)
	key = []byte(fmt.Sprintf(PREFIX_AUTO_FAILOVER_TABLE, table.GetId()))
	batch.Delete(key)
	err := batch.Commit()
	if err != nil {
		log.Warn("store task failed, err[%v]", err)
		return nil, err
	}
	table.Table = tt
	table.deleteTime = delTime
	c.deletingTables.Add(table)
	db.DeleteTableByName(tableName)
	c.workingTables.DeleteById(table.GetId())
	return table, nil
}

func (c *Cluster) CancelTable(dbName, tName string) error {
	db, find := c.FindDatabase(dbName)
	if !find {
		log.Warn("db[%s] not exist", dbName)
		return ErrNotExistDatabase
	}
	table, find := db.FindTable(tName)
	if !find {
		log.Warn("table[%s:%s] not exist", dbName, tName)
		return ErrNotExistTable
	}
	table.schemaLock.Lock()
	defer table.schemaLock.Unlock()
	if table.Status != metapb.TableStatus_TableInit && table.Status != metapb.TableStatus_TablePrepare {
		log.Warn("table[%s:%s] can not cancel", dbName, tName)
		return ErrNotCancel
	}
	if table.Status == metapb.TableStatus_TablePrepare {
		key := []byte(fmt.Sprintf("%s%d", PREFIX_TABLE, table.GetId()))
		if err := c.store.Delete(key); err != nil {
			log.Error("MS scheduler delete expired table:[%s][%d] from store is failed.",
				table.GetName(), table.GetId())
			return err
		}
	}
	table.Status = metapb.TableStatus_TableDelete
	db.DeleteTableById(table.GetId())
	c.creatingTables.Delete(table.GetId())
	return nil
}

func (c *Cluster) UpdateLeader(leader *Peer) {
	c.leader = leader
}

func (c *Cluster) GetLeader() *Peer {
	return c.leader
}

func (c *Cluster) GetClusterId() uint64 {
	return c.clusterId
}

func (c *Cluster) IsLeader() bool {
	return c.nodeId == c.leader.GetId()
}

func (c *Cluster) UpdateAutoScheduleInfo(autoFailoverUnable, autoTransferUnable bool) error {
	if c.autoFailoverUnable == autoFailoverUnable && c.autoTransferUnable == autoTransferUnable {
		return nil
	}
	batch := c.store.NewBatch()
	var key, value []byte
	key = []byte(fmt.Sprintf(PREFIX_AUTO_TRANSFER, c.clusterId))
	if autoTransferUnable {
		value = uint64ToBytes(uint64(1))
	} else {
		value = uint64ToBytes(uint64(0))
	}
	batch.Put(key, value)
	key = []byte(fmt.Sprintf(PREFIX_AUTO_FAILOVER, c.clusterId))
	if autoFailoverUnable {
		value = uint64ToBytes(uint64(1))
	} else {
		value = uint64ToBytes(uint64(0))
	}
	batch.Put(key, value)
	err := batch.Commit()
	if err != nil {
		log.Error("batch commit failed, err[%v]", err)
		return err
	}
	c.autoTransferUnable = autoTransferUnable
	c.autoFailoverUnable = autoFailoverUnable
	log.Info("auto[T:%t F:%t]", c.autoTransferUnable, c.autoFailoverUnable)
	return nil
}

func (c *Cluster) GetAllWorker() map[string]bool {
	list := c.workerManger.GetAllWorker()
	workers := make(map[string]bool)
	temp := make(map[string]bool)
	for _, name := range list {
		temp[name] = true
	}
	for s, _ := range c.workerPool {
		_, found := temp[s]
		workers[s] = found
	}
	return workers
}

func (c *Cluster) AddFailoverWorker() {
	c.workerManger.addWorker(NewFailoverWorker(c.workerManger, time.Second))
}

func (c *Cluster) AddDeleteTableWorker() {
	c.workerManger.addWorker(NewDeleteTableWorker(c.workerManger, 10*time.Minute))
}

func (c *Cluster) AddTrashReplicaGCWorker() {
	c.workerManger.addWorker(NewTrashReplicaGCWorker(c.workerManger, time.Minute))
}

func (c *Cluster) AddCreateTableWorker() {
	c.workerManger.addWorker(NewCreateTableWorker(c.workerManger, time.Second))
}

func (c *Cluster) AddRangeHbCheckWorker() {
	c.workerManger.addWorker(NewRangeHbCheckWorker(c.workerManger, 2*time.Minute))
}

func (c *Cluster) AddBalanceLeaderWorker() {
	c.workerManger.addWorker(NewBalanceNodeLeaderWorker(c.workerManger, 5 * defaultWorkerInterval))
}

func (c *Cluster) AddBalanceRangeWorker() {
	c.workerManger.addWorker(NewBalanceNodeRangeWorker(c.workerManger, 5 * defaultWorkerInterval))
}

func (c *Cluster) AddBalanceNodeOpsWorker() {
	c.workerManger.addWorker(NewBalanceNodeOpsWorker(c.workerManger, 5 * defaultWorkerInterval))
}

func (c *Cluster) RemoveWorker(name string) error {
	return c.workerManger.removeWorker(name)
}

func (c *Cluster) GetAllEvent() []RangeEvent {
	return c.eventDispatcher.getEvents()
}

func (c *Cluster) GetEvent(rangeID uint64) RangeEvent {
	return c.eventDispatcher.peekEvent(rangeID)
}

func (c *Cluster) AddEvent(event RangeEvent) bool {
	return c.eventDispatcher.pushEvent(event)
}

func (c *Cluster) RemoveEvent(event RangeEvent) {
	c.eventDispatcher.removeEvent(event)
}

func (c *Cluster) loadAutoTransfer() error {
	var s uint64
	s = uint64(1)
	key := fmt.Sprintf(PREFIX_AUTO_TRANSFER, c.clusterId)
	value, err := c.store.Get([]byte(key))
	if err != nil {
		if err == sErr.ErrNotFound {
			s = uint64(0)
		} else {
			return err
		}
	}
	var auto bool
	auto = true // enable by default
	if value != nil {
		s, err = bytesToUint64(value)
		if err != nil {
			return err
		}
	}
	if s == 0 {
		auto = false
	}
	c.autoTransferUnable = auto
	return nil
}

func (c *Cluster) loadAutoFailover() error {
	var s uint64
	s = uint64(1)
	key := fmt.Sprintf(PREFIX_AUTO_FAILOVER, c.clusterId)
	value, err := c.store.Get([]byte(key))
	if err != nil {
		if err == sErr.ErrNotFound {
			s = uint64(0)
		} else {
			return err
		}
	}
	var auto bool
	auto = true
	if value != nil {
		s, err = bytesToUint64(value)
		if err != nil {
			return err
		}
	}
	if s == 0 {
		auto = false
	}
	c.autoFailoverUnable = auto
	return nil
}

func (c *Cluster) loadScheduleSwitch() error {
	if err := c.loadAutoFailover(); err != nil {
		log.Error("load auto failover failed, err[%v]", err)
		return err
	}
	if err := c.loadAutoTransfer(); err != nil {
		log.Error("load auto transfer failed, err[%v]", err)
		return err
	}
	return nil
}

func (c *Cluster) storeDatabase(db *metapb.DataBase) error {
	database := deepcopy.Iface(db).(*metapb.DataBase)
	data, err := proto.Marshal(database)
	if err != nil {
		//TODO error
		return err
	}
	key := []byte(fmt.Sprintf("%s%d", PREFIX_DB, db.GetId()))
	return c.store.Put(key, data)
}

func (c *Cluster) storeTable(t *metapb.Table) error {
	table := deepcopy.Iface(t).(*metapb.Table)
	data, err := proto.Marshal(table)
	if err != nil {
		//TODO error
		return err
	}
	key := []byte(fmt.Sprintf("%s%d", PREFIX_TABLE, t.GetId()))
	return c.store.Put(key, data)
}

func (c *Cluster) deleteTable(tableId uint64) error {
	key := []byte(fmt.Sprintf("%s%d", PREFIX_TABLE, tableId))
	return c.store.Delete(key)
}

func (c *Cluster) getRange(rangeId uint64) ([]byte, error) {
	key := []byte(fmt.Sprintf("%s%d", PREFIX_RANGE, rangeId))
	return c.store.Get(key)
}

func (c *Cluster) deleteRange(rangeId uint64) error {
	key := []byte(fmt.Sprintf("%s%d", PREFIX_RANGE, rangeId))
	return c.store.Delete(key)
}

func (c *Cluster) storeDeleteRange(r *metapb.Range) error {
	b := c.store.NewBatch()

	for _, peer := range r.GetPeers() {
		replica := &metapb.Replica{RangeId: r.GetId(), Peer: peer, StartKey: r.GetStartKey(), EndKey: r.GetEndKey()}
		v, err := proto.Marshal(replica)
		if err != nil {
			return err
		}
		key := []byte(fmt.Sprintf("%s%d", PREFIX_REPLICA, peer.GetId()))
		b.Put(key, v)
	}

	key := []byte(fmt.Sprintf("%s%d", PREFIX_RANGE, r.GetId()))
	deletedKey := []byte(fmt.Sprintf("%s%d", PREFIX_DELETED_RANGE, r.GetId()))
	rng := deepcopy.Iface(r).(*metapb.Range)
	data, err := proto.Marshal(rng)
	if err != nil {
		return err
	}
	b.Delete(key)
	b.Put(deletedKey, data)

	return b.Commit()
}

func (c *Cluster) storeReplaceRange(old, new *metapb.Range, toGc []*metapb.Peer) error {
	b := c.store.NewBatch()

	for _, peer := range toGc {
		replica := &metapb.Replica{RangeId: old.GetId(), Peer: peer, StartKey: old.GetStartKey(), EndKey: old.GetEndKey()}
		v, err := proto.Marshal(replica)
		if err != nil {
			return err
		}
		key := []byte(fmt.Sprintf("%s%d", PREFIX_REPLICA, peer.GetId()))
		b.Put(key, v)
	}

	oldKey := []byte(fmt.Sprintf("%s%d", PREFIX_RANGE, old.GetId()))
	deletedKey := []byte(fmt.Sprintf("%s%d", PREFIX_DELETED_RANGE, old.GetId()))
	newKey := []byte(fmt.Sprintf("%s%d", PREFIX_RANGE, new.GetId()))
	rng := deepcopy.Iface(new).(*metapb.Range)
	data, err := proto.Marshal(rng)
	if err != nil {
		return err
	}
	oldRng := deepcopy.Iface(old).(*metapb.Range)
	oldData, err := proto.Marshal(oldRng)
	if err != nil {
		return err
	}
	b.Delete(oldKey)
	b.Put(deletedKey, oldData)
	b.Put(newKey, data)

	return b.Commit()
}

func (c *Cluster) storeRange(r *metapb.Range) error {
	rng := deepcopy.Iface(r).(*metapb.Range)
	data, err := proto.Marshal(rng)
	if err != nil {
		//TODO error
		return err
	}
	key := []byte(fmt.Sprintf("%s%d", PREFIX_RANGE, r.GetId()))
	return c.store.Put(key, data)
}

func (c *Cluster) storeNode(n *metapb.Node) error {
	node := deepcopy.Iface(n).(*metapb.Node)
	data, err := proto.Marshal(node)
	if err != nil {
		//TODO error
		log.Error("store node info is failed. error:[%v]", err)
		return err
	}
	key := []byte(fmt.Sprintf("%s%d", PREFIX_NODE, n.GetId()))
	return c.store.Put(key, data)
}

func (c *Cluster) storeRangeGC(r *metapb.Range) error {
	rng := deepcopy.Iface(r).(*metapb.Range)
	data, err := proto.Marshal(rng)
	if err != nil {
		log.Error("store node info is failed. error:[%v]", err)
		return err
	}
	key := []byte(fmt.Sprintf("%s%d", PREFIX_PRE_GC, rng.GetId()))
	return c.store.Put(key, data)
}

func (c *Cluster) deleteRangeGC(rangeId uint64) error {
	key := []byte(fmt.Sprintf("%s%d", PREFIX_PRE_GC, rangeId))
	return c.store.Delete(key)
}

func (c *Cluster) loadDatabases() error {
	prefix := []byte(PREFIX_DB)
	startKey, limitKey := bytesPrefix(prefix)
	it := c.store.Scan(startKey, limitKey)
	defer it.Release()
	for it.Next() {
		k := it.Key()
		if k == nil {
			log.Error("load databases key is nil")
			continue
		}
		db := new(metapb.DataBase)
		err := proto.Unmarshal(it.Value(), db)
		if err != nil {
			return err
		}
		database := NewDatabase(db)
		c.dbs.Add(database)
	}
	return nil
}

func (c *Cluster) loadTables() error {
	prefix := []byte(PREFIX_TABLE)
	startKey, limitKey := bytesPrefix(prefix)
	it := c.store.Scan(startKey, limitKey)
	defer it.Release()
	for it.Next() {
		k := it.Key()
		if k == nil {
			log.Error("load tables key is nil")
			continue
		}
		t := new(metapb.Table)
		err := proto.Unmarshal(it.Value(), t)
		if err != nil {
			return err
		}

		db, find := c.FindDatabase(t.DbName)
		if !find {
			log.Error("database[%s] not found", t.DbName)
			return ErrNotExistDatabase
		}

		table := NewTable(t)
		switch t.GetStatus() {
		case metapb.TableStatus_TablePrepare:
			db.AddTable(table)
			c.workingTables.Add(table)
			c.creatingTables.Add(&CreateTable{Table: table})
		case metapb.TableStatus_TableRunning:
			db.AddTable(table)
			c.workingTables.Add(table)
		case metapb.TableStatus_TableDelete:
			if len(t.GetExpand()) != 8 {
				log.Error("must bug for table %v delete", t)
				return ErrInternalError
			}
			delTime := binary.BigEndian.Uint64(t.GetExpand())
			table.deleteTime = time.Unix(int64(delTime), 0)
			c.deletingTables.Add(table)
		case metapb.TableStatus_TableDeleting:
			if len(t.GetExpand()) != 8 {
				log.Error("must bug for table %v delete", t)
				return ErrInternalError
			}
			delTime := binary.BigEndian.Uint64(t.GetExpand())
			table.deleteTime = time.Unix(int64(delTime), 0)
			c.deletingTables.Add(table)
		default:
			log.Error("invalid table %v", t)
			return ErrInternalError
		}
	}
	return nil
}

func (c *Cluster) loadRanges() error {
	prefix := []byte(PREFIX_RANGE)
	startKey, limitKey := bytesPrefix(prefix)
	it := c.store.Scan(startKey, limitKey)
	defer it.Release()
	var trashRanges []*metapb.Range
	for it.Next() {
		k := it.Key()
		if k == nil {
			log.Error("load ranges key is nil")
			continue
		}
		r := new(metapb.Range)
		err := proto.Unmarshal(it.Value(), r)
		if err != nil {
			return err
		}
		leader := deepcopy.Iface(r.GetPeers()[0]).(*metapb.Peer)
		rr := NewRange(r, leader)
		c.AddRange(rr)
	}
	// 删除垃圾分片(无归属分片)
	var batch Batch
	count := 0
	for _, r := range trashRanges {
		if batch == nil {
			batch = c.store.NewBatch()
		}
		key := []byte(fmt.Sprintf("%s%d", PREFIX_RANGE, r.GetId()))
		batch.Delete(key)
		count++
		if count == 100 {
			err := batch.Commit()
			if err != nil {
				// leader change ?????
				log.Warn("delete trash range failed, err[%v]", err)
				return nil
			}
			count = 0
			batch = c.store.NewBatch()
		}
	}
	if count > 0 && batch != nil {
		batch.Commit()
	}

	return nil
}

func (c *Cluster) loadTrashReplicas() error {
	prefix := []byte(PREFIX_REPLICA)
	startKey, limitKey := bytesPrefix(prefix)
	it := c.store.Scan(startKey, limitKey)
	defer it.Release()
	for it.Next() {
		k := it.Key()
		if k == nil {
			log.Error("load trash replicas key is nil")
			continue
		}
		rep := new(metapb.Replica)
		err := proto.Unmarshal(it.Value(), rep)
		if err != nil {
			return err
		}
		node := c.FindNodeById(rep.GetPeer().GetNodeId())
		node.AddTrashReplica(rep)
	}

	return nil
}

func (c *Cluster) loadDeletedRanges() error {
	prefix := []byte(PREFIX_DELETED_RANGE)
	startKey, limitKey := bytesPrefix(prefix)
	it := c.store.Scan(startKey, limitKey)
	defer it.Release()
	for it.Next() {
		k := it.Key()
		if k == nil {
			log.Error("load deleted ranges is nil")
			continue
		}
		r := new(metapb.Range)
		err := proto.Unmarshal(it.Value(), r)
		if err != nil {
			return err
		}
		c.deletedRanges.Add(r)
	}
	return nil

}

func (c *Cluster) loadPreGCRanges() error {
	prefix := []byte(PREFIX_PRE_GC)
	startKey, limitKey := bytesPrefix(prefix)
	it := c.store.Scan(startKey, limitKey)
	defer it.Release()
	for it.Next() {
		k := it.Key()
		if k == nil {
			log.Error("load pre gc ranges is nil")
			continue
		}
		r := new(metapb.Range)
		err := proto.Unmarshal(it.Value(), r)
		if err != nil {
			return err
		}
		c.preGCRanges.Add(r)
	}
	return nil
}

func (c *Cluster) loadNodes() error {
	prefix := []byte(PREFIX_NODE)
	startKey, limitKey := bytesPrefix(prefix)
	it := c.store.Scan(startKey, limitKey)
	defer it.Release()
	for it.Next() {
		k := it.Key()
		if k == nil {
			log.Error("load node key is nil")
			continue
		}
		n := new(metapb.Node)
		err := proto.Unmarshal(it.Value(), n)
		if err != nil {
			return err
		}
		node := NewNode(n)
		c.nodes.Add(node)
	}
	return nil
}

func (c *Cluster) loadRange(rangeId uint64) (*metapb.Range, error) {
	key := []byte(fmt.Sprintf("%s%d", PREFIX_RANGE, rangeId))
	value, err := c.store.Get(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, ErrNotFound
	}
	r := new(metapb.Range)
	err = proto.Unmarshal(value, r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (c *Cluster) loadTable(tableId uint64) (*metapb.Table, error) {
	key := []byte(fmt.Sprintf("%s%d", PREFIX_TABLE, tableId))
	value, err := c.store.Get(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, ErrNotFound
	}
	t := new(metapb.Table)
	err = proto.Unmarshal(value, t)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (c *Cluster) loadDatabase(dbId uint64) (*metapb.DataBase, error) {
	key := []byte(fmt.Sprintf("%s%d", PREFIX_DB, dbId))
	value, err := c.store.Get(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, ErrNotFound
	}
	db := new(metapb.DataBase)
	err = proto.Unmarshal(value, db)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (c *Cluster) loadNode(nodeId uint64) (*metapb.Node, error) {
	key := []byte(fmt.Sprintf("%s%d", PREFIX_NODE, nodeId))
	value, err := c.store.Get(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, ErrNotFound
	}
	n := new(metapb.Node)
	err = proto.Unmarshal(value, n)
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (c *Cluster) loadTrashReplica(peerId uint64) (*metapb.Replica, error) {
	key := []byte(fmt.Sprintf("%s%d", PREFIX_REPLICA, peerId))
	value, err := c.store.Get(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, ErrNotFound
	}
	rep := new(metapb.Replica)
	err = proto.Unmarshal(value, rep)
	if err != nil {
		return nil, err
	}
	return rep, nil
}

func (c *Cluster) allocPeer(nodeId uint64) (*metapb.Peer, error) {
	peerId, err := c.idGener.GenID()
	if err != nil {
		return nil, ErrGenID
	}
	return &metapb.Peer{Id: peerId, NodeId: nodeId}, nil
}
/**
选择节点需要排除已有peer相同的IP
 */
func (c *Cluster) allocPeerAndSelectNode(rng *Range) (*metapb.Peer, error) {
	node := c.selectNodeForAddPeer(rng)
	if node == nil {
		return nil, ERR_NO_SELECTED_NODE
	}
	newPeer, err := c.allocPeer(node.GetId())
	if err != nil {
		return nil, err
	}

	if node != nil {
		node.stats.RangeCount++
	}
	return newPeer, nil
}

func (c *Cluster) allocRange(startKey, endKey []byte, table *Table) (*metapb.Range, error) {
	rangeId, err := c.idGener.GenID()
	if err != nil {
		return nil, ErrGenID
	}
	return &metapb.Range{
		Id:          rangeId,
		StartKey:    startKey,
		EndKey:      endKey,
		RangeEpoch:  &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
		TableId:     table.GetId(),
		PrimaryKeys: table.GetPkColumns(),
	}, nil
}

func (c *Cluster) createRangeByScope(startKey, endKey []byte, table *Table) error {
	rng, err := c.allocRange(startKey, endKey, table)
	if err != nil {
		return err
	}
	region := NewRange(rng, nil)
	newPeer, err := c.allocPeerAndSelectNode(region)
	if err != nil {
		return err
	}
	var peers []*metapb.Peer
	peers = append(peers, newPeer)
	region.Peers = peers

	c.AddRange(region)
	// create range remote
	if err = c.createRangeRemote(rng); err != nil {
		return fmt.Errorf("create range[%s, %s] failed", startKey, endKey)
	}
	return nil
}

func initWorkerPool() map[string]bool {
	pool := make(map[string]bool)
	pool[failoverWorkerName] = true
	pool[deleteTableWorkerName] = true
	pool[trashReplicaGcWorkerName] = true
	pool[createTableWorkerName] = true
	pool[rangeHbCheckWorkerName] = true

	pool[balanceRangeWorkerName] = true
	pool[balanceLeaderWorkerName] = true
	pool[balanceNodeOpsWorkerName] = true
	return pool
}

func (c *Cluster) selectNodeForAddPeer(rng *Range) *Node {

	newSelectors := []NodeSelector{
		NewNodeLoginSelector(c.opt),
		NewDifferIPSelector(rng.GetNodes(c)),
		NewWriterOpsThresholdSelector(c.opt),
		NewStorageThresholdSelector(c.opt),
	}

	log.Debug("select node for add Peer node size:%d",len(c.GetAllNode()))
	nodes := make([]*Node, 0)
	for _, node := range c.GetAllNode() {

		var tableIsolationLabel string
		table, found := c.FindTableById(rng.Range.TableId)
		if found {
			tableIsolationLabel = table.Table.IsolationLabel
		}

		if tableIsolationLabel != "" && node.IsolationLabel != "" && tableIsolationLabel != node.IsolationLabel {
			continue
		}
		flag := true
		for _, selector := range newSelectors {
			if !selector.CanSelect(node) {
				log.Debug("addPeer: node %v cannot select, because of %v", node.GetId(), selector.Name())
				flag = false
				break
			}
		}
		if flag {
			nodes = append(nodes, node)
		}
	}

	// none of the nodes selected (e.g. wrong isolation label)
	if len(nodes) == 0 {
		for _, node := range c.GetAllNode() {
			flag := true
			for _, selector := range newSelectors {
				if !selector.CanSelect(node) {
					log.Debug("addPeer: node %v cannot select, because of %v", node.GetId(), selector.Name())
					flag = false
					break
				}
			}
			if flag {
				nodes = append(nodes, node)
			}
		}
	}

	log.Debug("selected node size:%d",len(nodes))
	//TODO：选择一个最好的节点，目前只通过range数来判断
	var bestNode *Node
	for _, node := range nodes {
		if bestNode == nil || node.GetRangesCount() < bestNode.GetRangesCount() {
			bestNode = node
		}
	}
	log.Debug("best node %v", bestNode)
	return bestNode

}

func (c *Cluster) checkSameIpNode(nodes []*Node) (string, bool) {
	m := make(map[string]struct{})
	for _, n := range nodes {
		ip := strings.Split(n.GetServerAddr(), ":")[0]
		if len(ip) != 0 {
			if _, ok := m[ip]; ok {
				return ip, true
			} else {
				m[ip] = struct {
				}{}
			}
		}
	}
	return "", false
}

func (c *Cluster) selectWorstPeer(rng *Range) *metapb.Peer {
	if len(rng.DownPeers) > 0 {
		return rng.DownPeers[0].Peer
	}

	//TODO:复制位置落后的peer

	// 检查相同ip的peer
	var nodes []*Node
	allNodes := rng.GetNodes(c)
	ip, ok := c.checkSameIpNode(allNodes)
	if ok {
		nodes = func() (ret []*Node) {
			for _, n := range allNodes {
				nIp := strings.Split(n.GetServerAddr(), ":")[0]
				if nIp == ip {
					ret = append(ret, n)
				}
			}
			return
		}()
	} else {
		nodes = c.getRangeNodes(rng)
	}

	var worstNode *Node

	if worstNode == nil {
		for _, node := range nodes {
			if worstNode == nil || node.availableRatio() < worstNode.availableRatio() {
				worstNode = node
			}
		}
	}

	if worstNode == nil {
		return nil
	}

	for _, peer := range rng.Peers {
		if peer.NodeId == worstNode.GetId() {
			return peer
		}
	}
	return nil
}
