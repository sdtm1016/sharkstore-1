package server

import (
	"testing"
	"time"
	"model/pkg/metapb"
)

func TestFindAndDeleteTable(t *testing.T) {
	cluster := newBoltDbCluster(t, newMockIDAllocator())
	defer closeLocalCluster(cluster)

	// 第一步.　添加新的节点
	node1, _, err := cluster.GetNodeId("127.0.0.1:6060", "127.0.0.1:6061", "127.0.0.1:6062", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	node2, _, err := cluster.GetNodeId("127.0.0.1:7060", "127.0.0.1:7061", "127.0.0.1:7062", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	node3, _, err := cluster.GetNodeId("127.0.0.1:8060", "127.0.0.1:8061", "127.0.0.1:8062", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	err = cluster.NodeLogin(node1.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node1.GetId(), err)
		return
	}
	err = cluster.NodeLogin(node2.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node2.GetId(), err)
		return
	}
	err = cluster.NodeLogin(node3.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node3.GetId(), err)
		return
	}

	// 第二步.　创建Database
	db, err := cluster.CreateDatabase("test", "")
	if err != nil {
		t.Errorf("create database failed, err %v", node3.GetId(), err)
		return
	}
	// 第三步.　创建table
	tableId, err := cluster.idGener.GenID()
	if err != nil {
		t.Errorf("cannot generte ID, err[%v]", err)
		return
	}
	_table := &metapb.Table{
		Name:   "t1",
		DbName: "test",
		Id:     tableId,
		DbId:   db.GetId(),
		//Properties: properties,
		Columns:    nil,
		Regxs:      nil,
		Epoch:      &metapb.TableEpoch{ConfVer: uint64(1), Version: uint64(1)},
		CreateTime: time.Now().Unix(),
		PkDupCheck: false,
	}
	err = cluster.storeTable(_table)
	if err != nil {
		t.Errorf("store table failed, err[%v]", err)
		return
	}
	table := NewTable(_table)
	db.AddTable(table)
	cluster.workingTables.Add(table)

	tt, find := db.FindTable("t1")
	if tt == nil || !find {
		t.Error("test failed")
		return
	}
	tt, find = db.FindTableById(table.GetId())
	if tt == nil || !find {
		t.Error("test failed")
		return
	}
	err = db.DeleteTableByName("t1")
	if err != nil {
		t.Error("test failed")
		return
	}

	err = db.DeleteTableById(table.GetId())
	if err == nil {
		t.Error("test failed")
		return
	}
}

func TestDbCache(t *testing.T) {
	cache := NewDbCache()
	cache.Add(&Database{DataBase: &metapb.DataBase{Name: "test1", Id: 1}})
	cache.Add(&Database{DataBase: &metapb.DataBase{Name: "test2", Id: 2}})
	db, find := cache.FindDb("test1")
	if db == nil || !find {
		t.Error("test failed")
		return
	}
	db, find = cache.FindDbById(uint64(2))
	if db == nil || !find {
		t.Error("test failed")
		return
	}
	if len(cache.GetAllDatabase()) != 2 {
		t.Error("test failed")
		return
	}

	cache.Delete("test1")
	db, find = cache.FindDb("test1")
	if db != nil && find {
		t.Error("test failed")
		return
	}
	db, find = cache.FindDbById(uint64(1))
	if db != nil && find {
		t.Error("test failed")
		return
	}
}
