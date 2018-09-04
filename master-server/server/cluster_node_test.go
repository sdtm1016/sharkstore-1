package server

import (
	"testing"
	"model/pkg/metapb"
)

func TestNodeLogin(t *testing.T) {
	cluster := newBoltDbCluster(t, newMockIDAllocator())
	if cluster == nil {
		return
	}
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

	// 模拟invalid node
	err = cluster.NodeLogin(10000)
	if err == nil {
		t.Errorf("test failed, %v", err)
		return
	}
	// 模拟node logout
	node1.State = metapb.NodeState_N_Logout
	node1, clean, err := cluster.GetNodeId("127.0.0.1:6060", "127.0.0.1:6061", "127.0.0.1:6062", "v1")
	if err != nil || !clean {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	err = cluster.NodeLogin(node1.GetId())
	if err != nil {
		t.Error("test failed")
		return
	}
}

func TestDeleteNode(t *testing.T) {
	cluster := newBoltDbCluster(t, newMockIDAllocator())
	if cluster == nil {
		return
	}
	defer closeLocalCluster(cluster)

	// 第一步.　添加新的节点
	node1, _, err := cluster.GetNodeId("127.0.0.1:6060", "127.0.0.1:6061", "127.0.0.1:6062", "v1")
	if err != nil {
		t.Errorf("get node ID failed, err %v", err)
		return
	}
	node2, _,err := cluster.GetNodeId("127.0.0.1:7060", "127.0.0.1:7061", "127.0.0.1:7062", "v1")
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

	if node1M, err := cluster.loadNode(node1.GetId()); err != nil {
		t.Error("test failed")
		return
	} else {
		t.Logf("node1 info %v", node1M)
	}
	err = cluster.DeleteNodeById(node1.GetId())
	if err != nil {
		t.Errorf("delete node by ID failed, err %v", err)
		return
	}
	n, err := cluster.loadNode(node1.GetId())
	if n != nil {
		t.Error("test failed")
		return
	}
	err = cluster.DeleteNodeByAddr(node2.GetServerAddr())
	if err != nil {
		t.Errorf("delete node by ID failed, err %v", err)
		return
	}
	n, err = cluster.loadNode(node2.GetId())
	if n != nil {
		t.Error("test failed")
		return
	}
}

func TestFindNode(t *testing.T) {
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

	n := cluster.FindNodeById(node1.GetId())
	if n == nil {
		t.Error("test failed")
		return
	}

	n = cluster.FindNodeByAddr(node2.GetServerAddr())
	if n == nil {
		t.Error("test failed")
		return
	}
}
