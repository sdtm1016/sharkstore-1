package server

import (
	"testing"
	"bytes"
	"time"
	"encoding/binary"

	"master-server/raft"
)

func TestRaftStore(t *testing.T) {
	var lleader uint64
	cnf := &StoreConfig{
		RaftRetainLogs:        int64(100),
		RaftHeartbeatInterval: time.Millisecond * 500,
		RaftHeartbeatAddr:     "127.0.0.1:1234",
		RaftReplicateAddr:     "127.0.0.1:1235",
		RaftPeers: []*Peer{&Peer{
			ID:                1,
			WebManageAddr:     "127.0.0.1:8080",
			RpcServerAddr:     "127.0.0.1:8887",
			RaftHeartbeatAddr: "127.0.0.1:1234",
			RaftReplicateAddr: "127.0.0.1:1235"}},

		NodeID:   uint64(1),
		DataPath: "/tmp/data",

		LeaderChangeHandler: func(leader uint64) {
			lleader = leader
		},
		FatalHandler: func(err *raft.FatalError) {
			// TODO event
			t.Errorf("raft fatal: id[%d], err[%v]", err.ID, err.Err)
		},
	}
	saveStore, err := NewRaftStore(cnf)
	if err != nil {
		t.Error("test failed")
		return
	}
	err = saveStore.Open()
	if err != nil {
		t.Error("test failed")
		return
	}
	time.Sleep(time.Second * 5)
	if lleader == 0 {
		t.Error("test failed")
		return
	}
	defer saveStore.Close()
	key := []byte("key")
	value := []byte("value")
	err = saveStore.Put(key, value)
	if err != nil {
		t.Error("test failed")
		return
	}

	v, err := saveStore.Get(key)
	if err != nil {
		t.Error("test failed")
		return
	}
	if bytes.Compare(value, v) != 0 {
		t.Error("test failed")
		return
	}

	err = saveStore.Delete(key)
	if err != nil {
		t.Error("test failed")
		return
	}
	v, err = saveStore.Get(key)
	if err != nil || v != nil {
		t.Error("test failed")
		return
	}
}

func TestRaftStoreIterAndBatch(t *testing.T) {
	var lleader uint64
	cnf := &StoreConfig{
		RaftRetainLogs:        int64(100),
		RaftHeartbeatInterval: time.Millisecond * 500,
		RaftHeartbeatAddr:     "127.0.0.1:2234",
		RaftReplicateAddr:     "127.0.0.1:2235",
		RaftPeers: []*Peer{&Peer{
			ID:                1,
			WebManageAddr:     "127.0.0.1:8080",
			RpcServerAddr:     "127.0.0.1:8887",
			RaftHeartbeatAddr: "127.0.0.1:2234",
			RaftReplicateAddr: "127.0.0.1:2235"}},

		NodeID:   uint64(1),
		DataPath: "/tmp/data",

		LeaderChangeHandler: func(leader uint64) {
			lleader = leader
		},
		FatalHandler: func(err *raft.FatalError) {
			// TODO event
		},
	}
	saveStore, err := NewRaftStore(cnf)
	if err != nil {
		t.Error("test failed")
		return
	}
	err = saveStore.Open()
	if err != nil {
		t.Error("test failed")
		return
	}
	time.Sleep(time.Second * 5)
	if lleader == 0 {
		t.Error("test failed")
		return
	}
	defer saveStore.Close()

	// batch
	batch := saveStore.NewBatch()
	var buff [8]byte
	for i := 0; i < 100; i++ {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(i))
		key = append(key, []byte("key_")...)
		key = append(key, buff[:]...)
		value = append(value, []byte("value_")...)
		value = append(value, buff[:]...)
		batch.Put(key, value)
	}
	err = batch.Commit()
	if err != nil {
		t.Error("test failed")
		return
	}

	var start, limit []byte
	binary.BigEndian.PutUint64(buff[:], uint64(2))
	start = append(start, []byte("key_")...)
	start = append(start, buff[:]...)

	binary.BigEndian.PutUint64(buff[:], uint64(43))
	limit = append(limit, []byte("key_")...)
	limit = append(limit, buff[:]...)
	iter := saveStore.Scan(start, limit)
	defer iter.Release()
	index := 2
	for iter.Next() {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(index))
		key = append(key, []byte("key_")...)
		key = append(key, buff[:]...)

		binary.BigEndian.PutUint64(buff[:], uint64(index))
		value = append(value, []byte("value_")...)
		value = append(value, buff[:]...)
		if bytes.Compare(key, iter.Key()) != 0 || bytes.Compare(value, iter.Value()) != 0 {
			t.Error("test failed")
			return
		}
		index++
	}
	if index != 43 {
		t.Error("test failed")
		return
	}
}

func TestRaftStoreSnapshot(t *testing.T) {
	var lleader uint64
	cnf := &StoreConfig{
		RaftRetainLogs:        int64(100),
		RaftHeartbeatInterval: time.Millisecond * 500,
		RaftHeartbeatAddr:     "127.0.0.1:3234",
		RaftReplicateAddr:     "127.0.0.1:3235",
		RaftPeers: []*Peer{&Peer{
			ID:                1,
			WebManageAddr:     "127.0.0.1:8080",
			RpcServerAddr:     "127.0.0.1:8887",
			RaftHeartbeatAddr: "127.0.0.1:3234",
			RaftReplicateAddr: "127.0.0.1:3235"}},

		NodeID:   uint64(1),
		DataPath: "/tmp/data",

		LeaderChangeHandler: func(leader uint64) {
			lleader = leader
		},
		FatalHandler: func(err *raft.FatalError) {
			// TODO event
		},
	}
	saveStore, err := NewRaftStore(cnf)
	if err != nil {
		t.Error("test failed")
		return
	}
	err = saveStore.Open()
	if err != nil {
		t.Error("test failed")
		return
	}
	time.Sleep(time.Second * 5)
	if lleader == 0 {
		t.Error("test failed")
		return
	}
	defer saveStore.Close()

	// batch
	batch := saveStore.NewBatch()
	var buff [8]byte
	for i := 0; i < 100; i++ {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(i))
		key = append(key, []byte("key_")...)
		key = append(key, buff[:]...)
		value = append(value, []byte("value_")...)
		value = append(value, buff[:]...)
		batch.Put(key, value)
	}
	err = batch.Commit()
	if err != nil {
		t.Error("test failed")
		return
	}
	snap, err := saveStore.GetSnapshot()
	if err != nil {
		t.Error("test failed")
		return
	}
	defer snap.Release()
	for i := 0; i < 100; i++ {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(i))
		key = append(key, []byte("key_")...)
		key = append(key, buff[:]...)
		value = append(value, []byte("value_")...)
		value = append(value, buff[:]...)
		v, err := snap.Get(key)
		if err != nil {
			t.Error("test failed")
			return
		}
		if bytes.Compare(v, value) != 0 {
			t.Error("test failed")
			return
		}
	}

	var start, limit []byte
	binary.BigEndian.PutUint64(buff[:], uint64(2))
	start = append(start, []byte("key_")...)
	start = append(start, buff[:]...)

	binary.BigEndian.PutUint64(buff[:], uint64(43))
	limit = append(limit, []byte("key_")...)
	limit = append(limit, buff[:]...)
	iter := snap.NewIterator(start, limit)
	defer iter.Release()
	index := 2
	for iter.Next() {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(index))
		key = append(key, []byte("key_")...)
		key = append(key, buff[:]...)

		binary.BigEndian.PutUint64(buff[:], uint64(index))
		value = append(value, []byte("value_")...)
		value = append(value, buff[:]...)
		if bytes.Compare(key, iter.Key()) != 0 || bytes.Compare(value, iter.Value()) != 0 {
			t.Error("test failed")
			return
		}
		index++
	}
	if index != 43 {
		t.Error("test failed")
		return
	}

}
