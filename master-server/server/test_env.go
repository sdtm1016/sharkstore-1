package server

import (
	"os"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
	"model/pkg/metapb"
	"sync/atomic"
	"model/pkg/schpb"
)

// local store, for case test
type LevelDBDriver struct {
	path  string
	db    *leveldb.DB
}

func NewLevelDBDriver(path string) (Store, error) {
	return &LevelDBDriver{path: path}, nil
}

func (ld *LevelDBDriver)Open() error {
	db, err := leveldb.OpenFile(ld.path, nil)
	if err != nil {
		return err
	}

	ld.db = db
	return nil
}

func (ld *LevelDBDriver)Get(key []byte) (value []byte, err error) {
	if ld == nil {
		return nil, nil
	}
	value, err = ld.db.Get(key, nil)
	if err != nil && err == leveldb.ErrNotFound {
		value = nil
		err = nil
		return
	}
	return
}

func (ld *LevelDBDriver)Put(key []byte, value []byte) error {
	if ld == nil {
		return nil
	}
	return ld.db.Put(key, value, nil)
}

func (ld *LevelDBDriver)Delete(key []byte) error {
	if ld == nil {
		return nil
	}
	return ld.db.Delete(key, nil)
}

func (ld *LevelDBDriver)Close() error {
	if ld == nil {
		return nil
	}
	if ld.db != nil {
		return ld.db.Close()
	}
	return nil
}

func (ld *LevelDBDriver)Scan (startKey, endKey []byte) Iterator {
	if ld == nil {
		return nil
	}
	return &LevelDBIter{ld.db.NewIterator(&util.Range{Start: startKey, Limit: endKey}, nil)}
}

// 批量写入，提交时保证batch里的修改同时对外可见
func (ld *LevelDBDriver)NewBatch() Batch {
	return &LevelDBBatch{db: ld.db, batch: &leveldb.Batch{}}
}

type LevelDBIter struct {
	iter   iterator.Iterator
}

func (i *LevelDBIter)Next() bool {
	return i.iter.Next()
}

func (i *LevelDBIter)Key() []byte {
	return i.iter.Key()
}

func (i *LevelDBIter)Value() []byte {
	return i.iter.Value()
}

func (i *LevelDBIter)Error() error {
	return i.iter.Error()
}

// Release iterator使用完需要释放
func (i *LevelDBIter)Release() {
	i.iter.Release()
}

type LevelDBBatch struct {
	db    *leveldb.DB
	batch *leveldb.Batch
}

func (b *LevelDBBatch)Put(key []byte, value []byte) {
	b.batch.Put(key, value)
}

func (b *LevelDBBatch)Delete(key []byte) {
	b.batch.Delete(key)
}

func (b *LevelDBBatch)Commit() error {
	return b.db.Write(b.batch, nil)
}

var path = "/tmp/Data"
var dsPath = "/tmp/Data"

func initDataPath() {
	os.RemoveAll(path)
	err := os.MkdirAll(path, 0755)
	if err != nil {
		fmt.Sprintf("init data path %s err %v", path, err)
		os.Exit(-1)
		return
	}
}

func clearData() {
	os.RemoveAll(path)
}

type LocalDSClient struct {

}

// Close should release all data.
func (lc *LocalDSClient) Close() error {
	return nil
}
// SendKVReq sends kv request.
func (lc *LocalDSClient) CreateRange(addr string, r *metapb.Range) error {
	fmt.Println(fmt.Sprintf("invoke createRange, addr: %v, rangeID: %v", addr, r.GetId()))
	return nil
}
func (lc *LocalDSClient) DeleteRange(addr string, rangeId uint64) error {
	fmt.Println(fmt.Sprintf("invoke deleteRange, addr: %v, rangeID: %v", addr, rangeId))
	return nil
}
func (lc *LocalDSClient) TransferLeader(addr string, rangeId uint64) error {
	fmt.Println(fmt.Sprintf("invoke transferLeader, addr: %v, rangeID: %v", addr, rangeId))
	return nil
}

func (lc *LocalDSClient) UpdateRange(addr string, r *metapb.Range) error {
	fmt.Println(fmt.Sprintf("invoke updateRange, addr: %v, rangeID: %v", addr, r.GetId()))
	return nil
}

func (lc *LocalDSClient) GetPeerInfo(addr string, rangeId uint64) (*schpb.GetPeerInfoResponse, error){
	fmt.Println(fmt.Sprintf("invoke getPeerInfo, addr: %v, rangeID: %v", addr, rangeId))
	return &schpb.GetPeerInfoResponse{}, nil
}

func (lc *LocalDSClient) SetNodeLogLevel(addr string, level string) error {
	fmt.Println(fmt.Sprintf("invoke setNodeLogLevel, addr: %v, level: %v", addr, level))
	return nil
}

func (lc *LocalDSClient) OffLineRange(addr string, rangeId uint64) error {
	fmt.Println(fmt.Sprintf("invoke offlineRange, addr: %v, rangeID: %v", addr, rangeId))
	return nil
}

func (lc *LocalDSClient) ReplaceRange(addr string, oldRangeId uint64, newRange *metapb.Range) error {
	fmt.Println(fmt.Sprintf("invoke replaceRange, addr: %v, old rangeID: %v, new rangeID: %v", addr, oldRangeId, newRange.GetId()))
	return nil
}


/*********mock ds **************/
type MockDs struct {
	NodeId 		uint64
	RpcAddr 	string
}

func NewMockDs(rpcAddr string) *MockDs {
	return &MockDs{
		RpcAddr:rpcAddr,
	}
}

func (ds *MockDs)SetNodeId(id uint64) {
	ds.NodeId = id
}

// mockIDAllocator mocks IDAllocator and it is only used for test.
type mockIDAllocator struct {
	base uint64
}

func newMockIDAllocator() *mockIDAllocator {
	return &mockIDAllocator{base: 0}
}

func (alloc *mockIDAllocator) GenID() (uint64, error) {
	return atomic.AddUint64(&alloc.base, 1), nil
}

func (alloc *mockIDAllocator) GetBatchIds(size uint32) ([]uint64, error) {
	ids := make([]uint64, size)

	for len(ids) < int(size) {
		ids = append(ids, atomic.AddUint64(&alloc.base, 1))
	}
	return ids, nil
}
