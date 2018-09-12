package server

import (
	"testing"
	"model/pkg/metapb"
	"time"
)

func TestCreatingTable(t *testing.T) {
	initDataPath()
	defer clearData()
	store, err := NewLevelDBDriver(path)
	if err != nil {
		t.Errorf("new store failed, err %v", err)
		return
	}
	err = store.Open()
	if err != nil {
		t.Errorf("open store failed, err %v", err)
		return
	}
	defer store.Close()
	var clusterId uint64 =  1
	var nodeId uint64 = 1
	cfg := NewDefaultConfig()
	cluster := NewCluster(clusterId, nodeId, store, newScheduleOption(cfg))
	err = cluster.LoadCache()
	if err != nil {
		t.Errorf("load cache failed, err %v", err)
		return
	}
	// 指定自己为leader
	cluster.UpdateLeader(&Peer{ID: nodeId})
	cluster.cli = &LocalDSClient{}

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

	// make disk available
	node1.stats.Available, node1.stats.Capacity = 80, 100
	node1.stats.Available, node2.stats.Capacity = 80, 100
	node1.stats.Available, node2.stats.Capacity = 80, 100

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

	if _, err := cluster.CreateDatabase(DB_NAME, ""); err != nil {
		t.Fatalf("create db error: %v", err)
	}
	var sliceKeys [][]byte
	var rangeKeysStart = "a"
	var rangeKeysEnd = "b"
	var rangeKeysNum uint64 = 1
	sliceKeys, err = ScopeSplit([]byte(rangeKeysStart), []byte(rangeKeysEnd), rangeKeysNum, nil)
	if err != nil {
		t.Fatalf("split scope error: %v", err)
	}
	table, err := cluster.CreateTableSimply(DB_NAME, TABLE_NAME, TABLE_PK_VARCHAR, nil, false, sliceKeys)
	if err != nil {
		t.Fatalf("create table error: %v", err)
	}
	t.Logf("table: %v", *table)

	scheduler := NewCreateTableWorker(cluster.workerManger, time.Second)
	// 第一次调度，创建range
	scheduler.Work(cluster)
	if table.Status != metapb.TableStatus_TablePrepare {
		t.Error("test failed")
		return
	}
	// 第二次调度，等待分片副本满足需求
	scheduler.Work(cluster)
	if table.Status != metapb.TableStatus_TablePrepare {
		t.Error("test failed")
		return
	}
	ranges := cluster.GetAllRanges()
	if len(ranges) != 2 {
		t.Errorf("test failed, ranges %v", ranges)
		return
	}
	// 模拟一个分片的副本ＯＫ
	var peers []*metapb.Peer
	nodes := make([]*Node, 3)
	nodes[0] = node1
	nodes[1] = node2
	nodes[2] = node3
	for j := 0; j < 3; j++ {
		peerId, err := cluster.idGener.GenID()
		if err != nil {
			t.Errorf("cannot generte ID, err[%v]", err)
			return
		}
		peers = append(peers, &metapb.Peer{Id: peerId, NodeId: nodes[j].GetId()})
	}
	ranges[0].Peers = peers
    // 第三次调度
	scheduler.Work(cluster)
	if table.Status != metapb.TableStatus_TablePrepare {
		t.Error("test failed")
		return
	}
	// 模拟所有的分片的副本都OK
	for j := 0; j < 3; j++ {
		peerId, err := cluster.idGener.GenID()
		if err != nil {
			t.Errorf("cannot generte ID, err[%v]", err)
			return
		}
		peers = append(peers, &metapb.Peer{Id: peerId, NodeId: nodes[j].GetId()})
	}
	ranges[1].Peers = peers
	scheduler.Work(cluster)
	if table.Status != metapb.TableStatus_TableRunning {
		t.Error("table not running")
		return
	}
	tt, _ := cluster.loadTable(table.GetId())
	if tt == nil {
		t.Error("load table failed")
		return
	}
	if tt.Status != metapb.TableStatus_TableRunning {
		t.Error("test failed")
		return
	}

	t.Logf("create table finished, table status: %v", *table)
}

func TestDeletingTable(t *testing.T) {
	initDataPath()
	defer clearData()
	store, err := NewLevelDBDriver(path)
	if err != nil {
		t.Errorf("new store failed, err %v", err)
		return
	}
	err = store.Open()
	if err != nil {
		t.Errorf("open store failed, err %v", err)
		return
	}
	defer store.Close()
	var clusterId uint64 =  1
	var nodeId uint64 = 1
	cfg := NewDefaultConfig()
	cluster := NewCluster(clusterId, nodeId, store, newScheduleOption(cfg))
	err = cluster.LoadCache()
	if err != nil {
		t.Errorf("load cache failed, err %v", err)
		return
	}
	// 指定自己为leader
	cluster.UpdateLeader(&Peer{ID: nodeId})
	cluster.cli = &LocalDSClient{}

	if _, err := cluster.CreateDatabase(DB_NAME, ""); err != nil {
		t.Fatalf("create db error: %v", err)
	}

	table, err := cluster.CreateTableSimply(DB_NAME, TABLE_NAME, TABLE_PK_INT, nil, false, nil)
	if err != nil {
		t.Fatalf("create table error: %v", err)
	}
	t.Logf("table: %v", *table)
	// 模拟table创建成功
	table.Status = metapb.TableStatus_TableRunning
	cluster.storeTable(table.Table)
	cluster.creatingTables.Delete(table.GetId())
	tt, err := cluster.DeleteTable(DB_NAME, TABLE_NAME, true)
	if err != nil || tt == nil {
		t.Error("test failed")
		return
	}
	scheduler := NewDeleteTableWorker(cluster.workerManger, time.Second)
	scheduler.Work(cluster)
	if _, find := cluster.deletingTables.FindTableById(table.GetId()); find {
		t.Error("test failed")
		return
	}
	_t, err :=  cluster.loadTable(table.GetId())
	if _t != nil {
		t.Error("test failed")
		return
	}
}
