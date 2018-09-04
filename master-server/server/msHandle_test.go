package server

import (
	"testing"
	"time"
	"model/pkg/metapb"
	"math/rand"
	"util"
	"golang.org/x/net/context"
	"model/pkg/mspb"
	"util/deepcopy"
	"model/pkg/taskpb"
)

func TestRpcColumn(t *testing.T) {
	cluster, server := createCluster(t)
	defer closeCluster(cluster)

	// 第二步.　创建Database
	db, err := cluster.CreateDatabase("test", "")
	if err != nil {
		t.Errorf("create database failed, err %v", err)
		return
	}
	// 第三步.　创建table
	tableId, err := cluster.idGener.GenID()
	if err != nil {
		t.Errorf("cannot generte ID, err[%v]", err)
		return
	}
	cols := []*metapb.Column{
		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, Id: 1},
		&metapb.Column{Name: "ctime", DataType: metapb.DataType_BigInt, PrimaryKey: 1, Id: 2},
		&metapb.Column{Name: "c_size", DataType: metapb.DataType_BigInt, Id: 3},
	}
	regxs := []*metapb.Column{
		&metapb.Column{Name: "test_.*", DataType: metapb.DataType_BigInt, Id: 4},
	}
	_table := &metapb.Table{
		Name:   "t1",
		DbName: "test",
		Id:     tableId,
		DbId:   db.GetId(),
		//Properties: properties,
		Columns:    cols,
		Regxs:      regxs,
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

	// 第四步.　创建table的分片
	ctx := context.Background()
	_, err = server.handleGetDb(ctx, &mspb.GetDBRequest{
		Header: &mspb.RequestHeader{},
		Name:   "test",
	})
	if err != nil {
		t.Error("test failed, err %v", err)
		return
	}

	_, err = server.handleGetTable(ctx, &mspb.GetTableRequest{
		Header:    &mspb.RequestHeader{},
		DbName:    db.GetName(),
		TableName: "t1",
	})
	if err != nil {
		t.Error("test failed, err %v", err)
		return
	}
	_, err = server.handleGetTableById(ctx, &mspb.GetTableByIdRequest{
		Header:  &mspb.RequestHeader{},
		DbId:    db.GetId(),
		TableId: table.GetId(),
	})
	if err != nil {
		t.Error("test failed, err %v", err)
		return
	}
	resp, err := server.handleGetColumns(ctx, &mspb.GetColumnsRequest{
		Header:  &mspb.RequestHeader{},
		DbId:    db.GetId(),
		TableId: table.GetId(),
	})
	if err != nil {
		t.Error("test failed, err %v", err)
		return
	}
	if len(resp.GetColumns()) != 3 {
		t.Error("test failed")
		return
	}
	_, err = server.handleGetColumnById(ctx, &mspb.GetColumnByIdRequest{
		Header:  &mspb.RequestHeader{},
		DbId:    db.GetId(),
		TableId: table.GetId(),
		ColId:   1,
	})
	if err != nil {
		t.Error("test failed, err %v", err)
		return
	}
	_, err = server.handleGetColumnByName(ctx, &mspb.GetColumnByNameRequest{
		Header:  &mspb.RequestHeader{},
		DbId:    db.GetId(),
		TableId: table.GetId(),
		ColName: "rangeid",
	})
	if err != nil {
		t.Error("test failed, err %v", err)
		return
	}
	cols_new := []*metapb.Column{
		&metapb.Column{Name: "test_1", DataType: metapb.DataType_BigInt,},
		&metapb.Column{Name: "test_2", DataType: metapb.DataType_BigInt,},
		&metapb.Column{Name: "test_3", DataType: metapb.DataType_BigInt,},
	}
	_, err = server.handleAddColumns(ctx, &mspb.AddColumnRequest{
		Header:  &mspb.RequestHeader{},
		DbId:    db.GetId(),
		TableId: table.GetId(),
		Columns: cols_new,
	})
	if err != nil {
		t.Error("test failed, err %v", err)
		return
	}
}

func TestRpcRangeSplit(t *testing.T) {
	cluster, server := createCluster(t)
	defer closeCluster(cluster)

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
		Status:     metapb.TableStatus_TableRunning,
	}
	err = cluster.storeTable(_table)
	if err != nil {
		t.Errorf("store table failed, err[%v]", err)
		return
	}
	table := NewTable(_table)
	db.AddTable(table)
	cluster.workingTables.Add(table)

	// 第四步.　创建table的分片
	nodes := make([]*Node, 3)
	nodes[0] = node1
	nodes[1] = node2
	nodes[2] = node3
	num := 100
	var start, end []byte
	start = util.EncodeStorePrefix(util.Store_Prefix_KV, tableId)
	_, end = bytesPrefix(start)
	var ranges []*Range
	for i := 0; i < num; i++ {
		rangeId, err := cluster.idGener.GenID()
		if err != nil {
			t.Errorf("cannot generte ID, err[%v]", err)
			return
		}
		var peers []*metapb.Peer
		for j := 0; j < 3; j++ {
			peerId, err := cluster.idGener.GenID()
			if err != nil {
				t.Errorf("cannot generte ID, err[%v]", err)
				return
			}
			peers = append(peers, &metapb.Peer{Id: peerId, NodeId: nodes[j].GetId()})
		}
		var _start, _end []byte
		if i == 0 {
			_start = start
		} else {
			_start = append(_start, start...)
			_start = append(_start, byte(i))
		}
		if i == num-1 {
			_end = end
		} else {
			_end = append(_end, end...)
			_end = append(_end, byte(i+1))
		}
		r := &metapb.Range{
			Id:         rangeId,
			StartKey:   _start,
			EndKey:     _end,
			RangeEpoch: &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
			Peers:      peers,
			TableId:    tableId,
		}
		err = cluster.storeRange(r)
		if err != nil {
			t.Errorf("store range failed, err[%v]", err)
			return
		}
		rng := NewRange(r, nil)
		// 随机指定peer为leader
		index := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(3)
		rng.Leader = rng.GetPeers()[index]
		// 添加到节点
		for _, peer := range rng.GetPeers() {
			if node := cluster.FindNodeById(peer.GetNodeId()); node != nil {
				node.AddRange(rng)
				node.stats.RangeCount++
			}
		}
		nodes[index].stats.RangeLeaderCount++
		cluster.AddRange(rng)
		ranges = append(ranges, rng)
	}

	// 第五步.　添加新的节点
	node4, _, err := cluster.GetNodeId("127.0.0.1:6070", "127.0.0.1:6071", "127.0.0.1:6072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	node5, _, err := cluster.GetNodeId("127.0.0.1:7070", "127.0.0.1:7071", "127.0.0.1:7072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	node6, _, err := cluster.GetNodeId("127.0.0.1:8070", "127.0.0.1:8071", "127.0.0.1:8072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	err = cluster.NodeLogin(node4.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node4.GetId(), err)
		return
	}
	err = cluster.NodeLogin(node5.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node5.GetId(), err)
		return
	}
	err = cluster.NodeLogin(node6.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node6.GetId(), err)
		return
	}

	ctx := context.Background()
	rng := ranges[0]
	var splitKey []byte
	splitKey = append(splitKey, rng.GetStartKey()...)
	splitKey = append(splitKey, []byte("a")...)
	_, err = server.handleAskSplit(ctx, &mspb.AskSplitRequest{
		Header:   &mspb.RequestHeader{},
		Range:    rng.Range,
		SplitKey: splitKey,
	})
	if err != nil {
		t.Errorf("test failed, %v", err)
		return
	}

	left := deepcopy.Iface(rng.Range).(*metapb.Range)
	left.RangeEpoch.Version++
	left.EndKey = splitKey
	right := deepcopy.Iface(rng.Range).(*metapb.Range)
	right.StartKey = splitKey
	_, err = server.handleReportSplit(ctx, &mspb.ReportSplitRequest{
		Header: &mspb.RequestHeader{},
		Left:   left,
		Right:  right,
	})
	if err != nil {
		t.Error("test failed")
		return
	}

	// 模拟异常的分裂请求
	rng = ranges[1]
	r := deepcopy.Iface(rng.Range).(*metapb.Range)
	r.EndKey = []byte("invalid")
	rng = NewRange(r, nil)
	_, err = server.handleAskSplit(ctx, &mspb.AskSplitRequest{
		Header:   &mspb.RequestHeader{},
		Range:    rng.Range,
		SplitKey: splitKey,
	})
	if err == nil {
		t.Error("test failed")
		return
	}

	rng = ranges[2]
	r = deepcopy.Iface(rng.Range).(*metapb.Range)
	r.RangeEpoch.Version++
	rng = NewRange(r, nil)
	_, err = server.handleAskSplit(ctx, &mspb.AskSplitRequest{
		Header:   &mspb.RequestHeader{},
		Range:    rng.Range,
		SplitKey: splitKey,
	})
	if err == nil {
		t.Error("test failed")
		return
	}
}

func TestRpcRangeHeartbeat(t *testing.T) {
	cluster, server := createCluster(t)
	defer closeCluster(cluster)

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
		Status:     metapb.TableStatus_TableRunning,
	}
	err = cluster.storeTable(_table)
	if err != nil {
		t.Errorf("store table failed, err[%v]", err)
		return
	}
	table := NewTable(_table)
	db.AddTable(table)
	cluster.workingTables.Add(table)

	// 第四步.　创建table的分片
	nodes := make([]*Node, 3)
	nodes[0] = node1
	nodes[1] = node2
	nodes[2] = node3
	num := 100
	var start, end []byte
	start = util.EncodeStorePrefix(util.Store_Prefix_KV, tableId)
	_, end = bytesPrefix(start)
	var ranges []*Range
	for i := 0; i < num; i++ {
		rangeId, err := cluster.idGener.GenID()
		if err != nil {
			t.Errorf("cannot generte ID, err[%v]", err)
			return
		}
		var peers []*metapb.Peer
		for j := 0; j < 3; j++ {
			peerId, err := cluster.idGener.GenID()
			if err != nil {
				t.Errorf("cannot generte ID, err[%v]", err)
				return
			}
			peers = append(peers, &metapb.Peer{Id: peerId, NodeId: nodes[j].GetId()})
		}
		var _start, _end []byte
		if i == 0 {
			_start = start
		} else {
			_start = append(_start, start...)
			_start = append(_start, byte(i))
		}
		if i == num-1 {
			_end = end
		} else {
			_end = append(_end, end...)
			_end = append(_end, byte(i+1))
		}
		r := &metapb.Range{
			Id:         rangeId,
			StartKey:   _start,
			EndKey:     _end,
			RangeEpoch: &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
			Peers:      peers,
			TableId:    tableId,
		}
		err = cluster.storeRange(r)
		if err != nil {
			t.Errorf("store range failed, err[%v]", err)
			return
		}
		rng := NewRange(r, nil)
		// 随机指定peer为leader
		index := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(peers))
		rng.Leader = rng.GetPeers()[index]
		// 添加到节点
		for _, peer := range rng.GetPeers() {
			if node := cluster.FindNodeById(peer.GetNodeId()); node != nil {
				node.AddRange(rng)
				node.stats.RangeCount++
			}
		}
		nodes[index].stats.RangeLeaderCount++
		cluster.AddRange(rng)
		ranges = append(ranges, rng)
	}

	// 第五步.　添加新的节点
	node4, _, err := cluster.GetNodeId("127.0.0.1:6070", "127.0.0.1:6071", "127.0.0.1:6072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	node5, _, err := cluster.GetNodeId("127.0.0.1:7070", "127.0.0.1:7071", "127.0.0.1:7072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	node6, _, err := cluster.GetNodeId("127.0.0.1:8070", "127.0.0.1:8071", "127.0.0.1:8072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	err = cluster.NodeLogin(node4.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node4.GetId(), err)
		return
	}
	err = cluster.NodeLogin(node5.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node5.GetId(), err)
		return
	}
	err = cluster.NodeLogin(node6.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node6.GetId(), err)
		return
	}

	ctx := context.Background()
	rng := ranges[0]
	server.handleRangeHeartbeat(ctx, &mspb.RangeHeartbeatRequest{
		Header: &mspb.RequestHeader{},
		Range:  rng.Range,
		Leader: rng.GetLeader(),
	})

	// 模拟range发生副本变化
	rng = ranges[1]
	r := deepcopy.Iface(rng.Range).(*metapb.Range)
	r.RangeEpoch.ConfVer++
	id, err := cluster.GenId()
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	upPeer := &metapb.Peer{Id: id, NodeId: node4.GetId()}
	var peers []*metapb.Peer
	peers = append(peers, upPeer)
	peers = append(peers, rng.GetPeers()[1:]...)
	r.Peers = peers
	rng = NewRange(r, nil)
	server.handleRangeHeartbeat(ctx, &mspb.RangeHeartbeatRequest{
		Header: &mspb.RequestHeader{},
		Range:  rng.Range,
		Leader: rng.GetLeader(),
	})
	if _, find := node1.GetRange(rng.GetId()); find {
		t.Error("test failed")
		return
	}
	if _, find := node4.GetRange(rng.GetId()); !find {
		t.Error("test failed")
		return
	}

	// 模拟脑裂
	rng = ranges[2]
	r = deepcopy.Iface(rng.Range).(*metapb.Range)
	var downPeer []*mspb.PeerStatus
	for _, peer := range rng.GetPeers() {
		downPeer = append(downPeer, &mspb.PeerStatus{Peer: peer, DownSeconds: uint64(1)})
		if len(downPeer) == 2 {
			break
		}
	}
	rng.Leader = rng.GetPeers()[2]
	server.handleRangeHeartbeat(ctx, &mspb.RangeHeartbeatRequest{
		Header:  &mspb.RequestHeader{},
		Range: r,
		Leader: rng.GetPeers()[0],
		PeersStatus: downPeer,
	})
	if len(rng.GetDownPeers()) > 0 {
		t.Error("test failed")
		return
	}
}

func TestRpcRangeHeartbeatWhenTableInit(t *testing.T) {
	cluster, server := createCluster(t)
	defer closeCluster(cluster)

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
		Status:     metapb.TableStatus_TableInit,
	}
	err = cluster.storeTable(_table)
	if err != nil {
		t.Errorf("store table failed, err[%v]", err)
		return
	}
	table := NewTable(_table)
	db.AddTable(table)
	cluster.workingTables.Add(table)

	// 第四步.　创建table的分片
	nodes := make([]*Node, 3)
	nodes[0] = node1
	nodes[1] = node2
	nodes[2] = node3
	num := 100
	var start, end []byte
	start = util.EncodeStorePrefix(util.Store_Prefix_KV, tableId)
	_, end = bytesPrefix(start)
	var ranges []*Range
	for i := 0; i < num; i++ {
		rangeId, err := cluster.idGener.GenID()
		if err != nil {
			t.Errorf("cannot generte ID, err[%v]", err)
			return
		}
		var peers []*metapb.Peer
		// 只有一个副本
		for j := 0; j < 1; j++ {
			peerId, err := cluster.idGener.GenID()
			if err != nil {
				t.Errorf("cannot generte ID, err[%v]", err)
				return
			}
			peers = append(peers, &metapb.Peer{Id: peerId, NodeId: nodes[j].GetId()})
		}
		var _start, _end []byte
		if i == 0 {
			_start = start
		} else {
			_start = append(_start, start...)
			_start = append(_start, byte(i))
		}
		if i == num-1 {
			_end = end
		} else {
			_end = append(_end, end...)
			_end = append(_end, byte(i+1))
		}
		r := &metapb.Range{
			Id:         rangeId,
			StartKey:   _start,
			EndKey:     _end,
			RangeEpoch: &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
			Peers:      peers,
			TableId:    tableId,
		}
		err = cluster.storeRange(r)
		if err != nil {
			t.Errorf("store range failed, err[%v]", err)
			return
		}
		rng := NewRange(r, nil)
		// 随机指定peer为leader
		index := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(peers))
		rng.Leader = rng.GetPeers()[index]
		// 添加到节点
		for _, peer := range rng.GetPeers() {
			if node := cluster.FindNodeById(peer.GetNodeId()); node != nil {
				node.AddRange(rng)
				node.stats.RangeCount++
			}
		}
		nodes[index].stats.RangeLeaderCount++
		cluster.AddRange(rng)
		ranges = append(ranges, rng)
	}

	// 第五步.　添加新的节点
	node4, _, err := cluster.GetNodeId("127.0.0.1:6070", "127.0.0.1:6071", "127.0.0.1:6072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	node5, _, err := cluster.GetNodeId("127.0.0.1:7070", "127.0.0.1:7071", "127.0.0.1:7072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	node6, _, err := cluster.GetNodeId("127.0.0.1:8070", "127.0.0.1:8071", "127.0.0.1:8072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	err = cluster.NodeLogin(node4.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node4.GetId(), err)
		return
	}
	err = cluster.NodeLogin(node5.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node5.GetId(), err)
		return
	}
	err = cluster.NodeLogin(node6.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node6.GetId(), err)
		return
	}

	ctx := context.Background()
	rng := ranges[0]
	resp := server.handleRangeHeartbeat(ctx, &mspb.RangeHeartbeatRequest{
		Header: &mspb.RequestHeader{},
		Range:  rng.Range,
		Leader: rng.GetLeader(),
	})
	if resp.GetTask() != nil {
		t.Error("test failed")
		return
	}
}

func TestRpcRangeHeartbeatWhenTablePrepare(t *testing.T) {
	cluster, server := createCluster(t)
	defer closeCluster(cluster)

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
		Status:     metapb.TableStatus_TablePrepare,
	}
	err = cluster.storeTable(_table)
	if err != nil {
		t.Errorf("store table failed, err[%v]", err)
		return
	}
	table := NewTable(_table)
	db.AddTable(table)
	cluster.workingTables.Add(table)

	// 第四步.　创建table的分片
	nodes := make([]*Node, 3)
	nodes[0] = node1
	nodes[1] = node2
	nodes[2] = node3
	num := 100
	var start, end []byte
	start = util.EncodeStorePrefix(util.Store_Prefix_KV, tableId)
	_, end = bytesPrefix(start)
	var ranges []*Range
	for i := 0; i < num; i++ {
		rangeId, err := cluster.idGener.GenID()
		if err != nil {
			t.Errorf("cannot generte ID, err[%v]", err)
			return
		}
		var peers []*metapb.Peer
		for j := 0; j < 1; j++ {
			peerId, err := cluster.idGener.GenID()
			if err != nil {
				t.Errorf("cannot generte ID, err[%v]", err)
				return
			}
			peers = append(peers, &metapb.Peer{Id: peerId, NodeId: nodes[j].GetId()})
		}
		var _start, _end []byte
		if i == 0 {
			_start = start
		} else {
			_start = append(_start, start...)
			_start = append(_start, byte(i))
		}
		if i == num-1 {
			_end = end
		} else {
			_end = append(_end, end...)
			_end = append(_end, byte(i+1))
		}
		r := &metapb.Range{
			Id:         rangeId,
			StartKey:   _start,
			EndKey:     _end,
			RangeEpoch: &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
			Peers:      peers,
			TableId:    tableId,
		}
		err = cluster.storeRange(r)
		if err != nil {
			t.Errorf("store range failed, err[%v]", err)
			return
		}
		rng := NewRange(r, nil)
		// 随机指定peer为leader
		index := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(peers))
		rng.Leader = rng.GetPeers()[index]
		// 添加到节点
		for _, peer := range rng.GetPeers() {
			if node := cluster.FindNodeById(peer.GetNodeId()); node != nil {
				node.AddRange(rng)
				node.stats.RangeCount++
			}
		}
		nodes[index].stats.RangeLeaderCount++
		cluster.AddRange(rng)
		ranges = append(ranges, rng)
	}

	// 第五步.　添加新的节点
	node4, _, err := cluster.GetNodeId("127.0.0.1:6070", "127.0.0.1:6071", "127.0.0.1:6072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	node5, _, err := cluster.GetNodeId("127.0.0.1:7070", "127.0.0.1:7071", "127.0.0.1:7072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	node6, _, err := cluster.GetNodeId("127.0.0.1:8070", "127.0.0.1:8071", "127.0.0.1:8072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	err = cluster.NodeLogin(node4.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node4.GetId(), err)
		return
	}
	err = cluster.NodeLogin(node5.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node5.GetId(), err)
		return
	}
	err = cluster.NodeLogin(node6.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node6.GetId(), err)
		return
	}

	ctx := context.Background()
	rng := ranges[0]
	resp := server.handleRangeHeartbeat(ctx, &mspb.RangeHeartbeatRequest{
		Header: &mspb.RequestHeader{},
		Range:  rng.Range,
		Leader: rng.GetLeader(),
	})
	if resp.GetTask() == nil {
		t.Error("test failed")
		return
	}
	if resp.GetTask().GetType() != taskpb.TaskType_RangeAddPeer {
		t.Error("test failed")
		return
	}
}

func TestRpcRangeHeartbeatWhenTableDeleting(t *testing.T) {
	cluster, server := createCluster(t)
	defer closeCluster(cluster)

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
		Status:     metapb.TableStatus_TableDeleting,
	}
	err = cluster.storeTable(_table)
	if err != nil {
		t.Errorf("store table failed, err[%v]", err)
		return
	}
	table := NewTable(_table)
	db.AddTable(table)
	cluster.workingTables.Add(table)

	// 第四步.　创建table的分片
	nodes := make([]*Node, 3)
	nodes[0] = node1
	nodes[1] = node2
	nodes[2] = node3
	num := 100
	var start, end []byte
	start = util.EncodeStorePrefix(util.Store_Prefix_KV, tableId)
	_, end = bytesPrefix(start)
	var ranges []*Range
	for i := 0; i < num; i++ {
		rangeId, err := cluster.idGener.GenID()
		if err != nil {
			t.Errorf("cannot generte ID, err[%v]", err)
			return
		}
		var peers []*metapb.Peer
		for j := 0; j < 3; j++ {
			peerId, err := cluster.idGener.GenID()
			if err != nil {
				t.Errorf("cannot generte ID, err[%v]", err)
				return
			}
			peers = append(peers, &metapb.Peer{Id: peerId, NodeId: nodes[j].GetId()})
		}
		var _start, _end []byte
		if i == 0 {
			_start = start
		} else {
			_start = append(_start, start...)
			_start = append(_start, byte(i))
		}
		if i == num-1 {
			_end = end
		} else {
			_end = append(_end, end...)
			_end = append(_end, byte(i+1))
		}
		r := &metapb.Range{
			Id:         rangeId,
			StartKey:   _start,
			EndKey:     _end,
			RangeEpoch: &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
			Peers:      peers,
			TableId:    tableId,
		}
		err = cluster.storeRange(r)
		if err != nil {
			t.Errorf("store range failed, err[%v]", err)
			return
		}
		rng := NewRange(r, nil)
		// 随机指定peer为leader
		index := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(peers))
		rng.Leader = rng.GetPeers()[index]
		// 添加到节点
		for _, peer := range rng.GetPeers() {
			if node := cluster.FindNodeById(peer.GetNodeId()); node != nil {
				node.AddRange(rng)
				node.stats.RangeCount++
			}
		}
		nodes[index].stats.RangeLeaderCount++
		cluster.AddRange(rng)
		ranges = append(ranges, rng)
	}

	// 第五步.　添加新的节点
	node4, _, err := cluster.GetNodeId("127.0.0.1:6070", "127.0.0.1:6071", "127.0.0.1:6072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	node5, _, err := cluster.GetNodeId("127.0.0.1:7070", "127.0.0.1:7071", "127.0.0.1:7072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	node6, _, err := cluster.GetNodeId("127.0.0.1:8070", "127.0.0.1:8071", "127.0.0.1:8072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	err = cluster.NodeLogin(node4.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node4.GetId(), err)
		return
	}
	err = cluster.NodeLogin(node5.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node5.GetId(), err)
		return
	}
	err = cluster.NodeLogin(node6.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node6.GetId(), err)
		return
	}

	ctx := context.Background()
	rng := ranges[0]
	resp := server.handleRangeHeartbeat(ctx, &mspb.RangeHeartbeatRequest{
		Header: &mspb.RequestHeader{},
		Range:  rng.Range,
		Leader: rng.GetLeader(),
	})

	if resp.GetTask() == nil {
		t.Error("test failed")
		return
	}
	if resp.GetTask().GetType() != taskpb.TaskType_RangeDelPeer {
		t.Error("test failed")
		return
	}
}

func TestRpcRangeHeartbeatWhenTableMiss(t *testing.T) {
	cluster, server := createCluster(t)
	defer closeCluster(cluster)

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
		Status:     metapb.TableStatus_TableRunning,
	}
	err = cluster.storeTable(_table)
	if err != nil {
		t.Errorf("store table failed, err[%v]", err)
		return
	}
	table := NewTable(_table)
	db.AddTable(table)
	cluster.workingTables.Add(table)

	// 第四步.　创建table的分片
	nodes := make([]*Node, 3)
	nodes[0] = node1
	nodes[1] = node2
	nodes[2] = node3
	num := 100
	var start, end []byte
	start = util.EncodeStorePrefix(util.Store_Prefix_KV, tableId)
	_, end = bytesPrefix(start)
	var ranges []*Range
	for i := 0; i < num; i++ {
		rangeId, err := cluster.idGener.GenID()
		if err != nil {
			t.Errorf("cannot generte ID, err[%v]", err)
			return
		}
		var peers []*metapb.Peer
		for j := 0; j < 1; j++ {
			peerId, err := cluster.idGener.GenID()
			if err != nil {
				t.Errorf("cannot generte ID, err[%v]", err)
				return
			}
			peers = append(peers, &metapb.Peer{Id: peerId, NodeId: nodes[j].GetId()})
		}
		var _start, _end []byte
		if i == 0 {
			_start = start
		} else {
			_start = append(_start, start...)
			_start = append(_start, byte(i))
		}
		if i == num-1 {
			_end = end
		} else {
			_end = append(_end, end...)
			_end = append(_end, byte(i+1))
		}
		r := &metapb.Range{
			Id:         rangeId,
			StartKey:   _start,
			EndKey:     _end,
			RangeEpoch: &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
			Peers:      peers,
			TableId:    tableId,
		}
		err = cluster.storeRange(r)
		if err != nil {
			t.Errorf("store range failed, err[%v]", err)
			return
		}
		rng := NewRange(r, nil)
		// 随机指定peer为leader
		index := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(1)
		rng.Leader = rng.GetPeers()[index]
		// 添加到节点
		for _, peer := range rng.GetPeers() {
			if node := cluster.FindNodeById(peer.GetNodeId()); node != nil {
				node.AddRange(rng)
				node.stats.RangeCount++
			}
		}
		nodes[index].stats.RangeLeaderCount++
		cluster.AddRange(rng)
		ranges = append(ranges, rng)
	}

	// 第五步.　添加新的节点
	node4, _, err := cluster.GetNodeId("127.0.0.1:6070", "127.0.0.1:6071", "127.0.0.1:6072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	node5, _, err := cluster.GetNodeId("127.0.0.1:7070", "127.0.0.1:7071", "127.0.0.1:7072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	node6, _, err := cluster.GetNodeId("127.0.0.1:8070", "127.0.0.1:8071", "127.0.0.1:8072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	err = cluster.NodeLogin(node4.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node4.GetId(), err)
		return
	}
	err = cluster.NodeLogin(node5.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node5.GetId(), err)
		return
	}
	err = cluster.NodeLogin(node6.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node6.GetId(), err)
		return
	}

	_, err = cluster.DeleteTable("test", "t1", true)
	if err != nil {
		t.Error("test failed")
		return
	}
	cluster.deletingTables.DeleteById(table.GetId())
	ctx := context.Background()
	rng := ranges[0]
	resp := server.handleRangeHeartbeat(ctx, &mspb.RangeHeartbeatRequest{
		Header: &mspb.RequestHeader{},
		Range:  rng.Range,
		Leader: rng.GetLeader(),
	})
	if resp.GetTask() == nil {
		t.Error("test failed")
		return
	}
	if resp.GetTask().GetType() != taskpb.TaskType_RangeDelete {
		t.Error("test failed")
		return
	}

}

func TestRpcNodeHeartbeat(t *testing.T) {
	cluster, server := createCluster(t)
	modifyDefault(1, 2, 5)
	defer closeCluster(cluster)

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

	// 第四步.　创建table的分片
	nodes := make([]*Node, 3)
	nodes[0] = node1
	nodes[1] = node2
	nodes[2] = node3
	num := 100
	var start, end []byte
	start = util.EncodeStorePrefix(util.Store_Prefix_KV, tableId)
	_, end = bytesPrefix(start)
	for i := 0; i < num; i++ {
		rangeId, err := cluster.idGener.GenID()
		if err != nil {
			t.Errorf("cannot generte ID, err[%v]", err)
			return
		}
		var peers []*metapb.Peer
		for j := 0; j < 3; j++ {
			peerId, err := cluster.idGener.GenID()
			if err != nil {
				t.Errorf("cannot generte ID, err[%v]", err)
				return
			}
			peers = append(peers, &metapb.Peer{Id: peerId, NodeId: nodes[j].GetId()})
		}
		var _start, _end []byte
		if i == 0 {
			_start = start
		} else {
			_start = append(_start, start...)
			_start = append(_start, byte(i))
		}
		if i == num-1 {
			_end = end
		} else {
			_end = append(_end, end...)
			_end = append(_end, byte(i+1))
		}
		r := &metapb.Range{
			Id:         rangeId,
			StartKey:   _start,
			EndKey:     _end,
			RangeEpoch: &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
			Peers:      peers,
			TableId:    tableId,
		}
		err = cluster.storeRange(r)
		if err != nil {
			t.Errorf("store range failed, err[%v]", err)
			return
		}
		rng := NewRange(r, nil)
		// 随机指定peer为leader
		index := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(3)
		rng.Leader = rng.GetPeers()[index]
		// 添加到节点
		for _, peer := range rng.GetPeers() {
			if node := cluster.FindNodeById(peer.GetNodeId()); node != nil {
				node.AddRange(rng)
				node.stats.RangeCount++
			}
		}
		nodes[index].stats.RangeLeaderCount++
		cluster.AddRange(rng)
	}

	// 第五步.　添加新的节点
	node4, _, err := cluster.GetNodeId("127.0.0.1:6070", "127.0.0.1:6071", "127.0.0.1:6072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	node5, _, err := cluster.GetNodeId("127.0.0.1:7070", "127.0.0.1:7071", "127.0.0.1:7072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	node6, _, err := cluster.GetNodeId("127.0.0.1:8070", "127.0.0.1:8071", "127.0.0.1:8072", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	err = cluster.NodeLogin(node4.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node4.GetId(), err)
		return
	}
	err = cluster.NodeLogin(node5.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node5.GetId(), err)
		return
	}
	err = cluster.NodeLogin(node6.GetId())
	if err != nil {
		t.Errorf("node %d login failed, err %v", node6.GetId(), err)
		return
	}

	ctx := context.Background()
	// 模拟node offline 恢复
	node1.State = metapb.NodeState_N_Offline
	server.handleNodeHeartbeat(ctx, &mspb.NodeHeartbeatRequest{
		Header: &mspb.RequestHeader{},
		NodeId: node1.GetId(),
		Stats: &mspb.NodeStats{
			RangeCount:       uint32(len(node1.GetAllRanges())),
			RangeLeaderCount: 1,
			Capacity:         100000,
			UsedSize:         1000,
		},
	})
	n, err := cluster.loadNode(node1.GetId())
	if err != nil {
		t.Errorf("load node failed, err %v", err)
		return
	}
	if n.State != metapb.NodeState_N_Login {
		t.Error("test failed")
		return
	}

	node2.State = metapb.NodeState_N_Tombstone
	time.Sleep(time.Second * 100)
	server.handleNodeHeartbeat(ctx, &mspb.NodeHeartbeatRequest{
		Header: &mspb.RequestHeader{},
		NodeId: node2.GetId(),
		Stats: &mspb.NodeStats{
			RangeCount:       uint32(len(node1.GetAllRanges())),
			RangeLeaderCount: 1,
			Capacity:         100000,
			UsedSize:         1000,
		},
	})
	n, err = cluster.loadNode(node2.GetId())
	if err != nil {
		t.Errorf("load node failed, err %v", err)
		return
	}
	if n.State != metapb.NodeState_N_Offline {
		t.Error("test failed")
		return
	}
	server.handleNodeHeartbeat(ctx, &mspb.NodeHeartbeatRequest{
		Header: &mspb.RequestHeader{},
		NodeId: node2.GetId(),
		Stats: &mspb.NodeStats{
			RangeCount:       uint32(len(node1.GetAllRanges())),
			RangeLeaderCount: 1,
			Capacity:         100000,
			UsedSize:         1000,
		},
	})
	n, err = cluster.loadNode(node2.GetId())
	if err != nil {
		t.Errorf("load node failed, err %v", err)
		return
	}
	if n.State != metapb.NodeState_N_Login {
		t.Error("test failed")
		return
	}
}
