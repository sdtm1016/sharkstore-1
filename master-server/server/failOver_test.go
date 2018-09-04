package server

import (
	"testing"
	"model/pkg/metapb"
	"model/pkg/mspb"
	"time"
	"github.com/stretchr/testify/assert"
)

var (
	dsAddr_1 = "127.0.0.1:18887"
	dsAddr_2 = "127.0.0.1:28887"
)

/**
 * 正常流程:GetNodeId->NodeLogin->handleNodeHeartbeat->schedule
 */
func TestNodeFailOverNormal(t *testing.T) {
	cluster, server := createCluster(t)
	defer closeCluster(cluster)

	ds_1 := NewMockDs(dsAddr_1)
	node1, _, err := cluster.GetNodeId(dsAddr_1, "", "", "")
	if err != nil {
		t.Fatalf("GetNodeId error:[%v]", err)
	}
	assert.NotEqual(t, node1.Id, 0, "Donot generate node id")
	assert.Equal(t, node1.State, metapb.NodeState_N_Initial, "node state error")
	ds_1.SetNodeId(node1.Id)

	if err := cluster.NodeLogin(ds_1.NodeId); err != nil {
		t.Fatal("NodeLogin error:[%v]", err)
	}
	assert.Equal(t, node1.State, metapb.NodeState_N_Login, "node state error")

	req := &mspb.NodeHeartbeatRequest{
		NodeId: ds_1.NodeId,
	}
	server.handleNodeHeartbeat(nil, req)
	assert.Equal(t, node1.State, metapb.NodeState_N_Login, "node state error")

	failOver := NewFailoverWorker(cluster.workerManger, time.Second)
	failOver.Work(cluster)
	assert.Equal(t, node1.State, metapb.NodeState_N_Login, "node state error")
}

/**
 * 异常流程:GetNodeId->NodeLogin->handleNodeHeartbeat->schedule 心跳偶尔延迟，但可恢复
 */
func TestNodeFailOverRecovery(t *testing.T) {
	cluster, server := createCluster(t)
	defer closeCluster(cluster)

	ds_1 := NewMockDs(dsAddr_1)
	node1, _, err := cluster.GetNodeId(dsAddr_1, "", "", "")
	if err != nil {
		t.Fatal("GetNodeId error:[%v]", err)
	}
	assert.NotEqual(t, node1.Id, 0, "Donot generate node id")
	assert.Equal(t, node1.State, metapb.NodeState_N_Initial, "node state error")
	ds_1.SetNodeId(node1.Id)

	if err := cluster.NodeLogin(ds_1.NodeId); err != nil {
		t.Fatal("NodeLogin error:[%v]", err)
	}
	assert.Equal(t, node1.State, metapb.NodeState_N_Login, "node state error")

	time.Sleep(2 * time.Second)
	failOver := NewFailoverWorker(cluster.workerManger, time.Second)
	failOver.Work(cluster)
	assert.Equal(t, node1.State, metapb.NodeState_N_Offline, "node state error")

	time.Sleep(2 * time.Second)
	req := &mspb.NodeHeartbeatRequest{
		NodeId: ds_1.NodeId,
	}
	server.handleNodeHeartbeat(nil, req)
	assert.Equal(t, node1.State, metapb.NodeState_N_Login, "node state error")
}

/**
 * 异常流程:GetNodeId->NodeLogin->handleNodeHeartbeat->schedule 心跳严重延迟，但可起死回生
 */
func TestNodeFailOverBornAgain(t *testing.T) {
	cluster, server := createCluster(t)
	defer closeCluster(cluster)

	ds_1 := NewMockDs(dsAddr_1)
	node1, _, err := cluster.GetNodeId(dsAddr_1, "", "", "")
	if err != nil {
		t.Fatal("GetNodeId error:[%v]", err)
	}
	assert.NotEqual(t, node1.Id, 0, "Donot generate node id")
	assert.Equal(t, node1.State, metapb.NodeState_N_Initial, "node state error")
	ds_1.SetNodeId(node1.Id)

	if err := cluster.NodeLogin(ds_1.NodeId); err != nil {
		t.Fatal("NodeLogin error:[%v]", err)
	}
	assert.Equal(t, node1.State, metapb.NodeState_N_Login, "node state error")

	time.Sleep(2 * time.Second)
	failOver := NewFailoverWorker(cluster.workerManger, time.Second)
	failOver.Work(cluster)
	assert.Equal(t, node1.State, metapb.NodeState_N_Offline, "node state error")

	time.Sleep(4 * time.Second)
	failOver.Work(cluster)
	assert.Equal(t, node1.State, metapb.NodeState_N_Tombstone, "node state error")

	time.Sleep(1 * time.Second)
	// 连续发送DefaultDsRecoveryInterim / DefaultDsHearbeatInterval次心跳
	req := &mspb.NodeHeartbeatRequest{
		NodeId: ds_1.NodeId,
	}
	server.handleNodeHeartbeat(nil, req)
	assert.Equal(t, node1.State, metapb.NodeState_N_Offline, "node state error")
	server.handleNodeHeartbeat(nil, req)
	assert.Equal(t, node1.State, metapb.NodeState_N_Login, "node state error")
}

/**
 * 异常流程:GetNodeId->NodeLogin->handleNodeHeartbeat->schedule 心跳严重延迟，直到死亡
 */
func TestNodeFailOverToDead(t *testing.T) {
	cluster, server := createCluster(t)
	defer closeCluster(cluster)

	ds_1 := NewMockDs(dsAddr_1)
	node1, _, err := cluster.GetNodeId(dsAddr_1, "", "", "")
	if err != nil {
		t.Fatal("GetNodeId error:[%v]", err)
	}
	assert.NotEqual(t, node1.Id, 0, "Donot generate node id")
	assert.Equal(t, node1.State, metapb.NodeState_N_Initial, "node state error")
	ds_1.SetNodeId(node1.Id)

	if err := cluster.NodeLogin(ds_1.NodeId); err != nil {
		t.Fatal("NodeLogin error:[%v]", err)
	}
	assert.Equal(t, node1.State, metapb.NodeState_N_Login, "node state error")

	time.Sleep(2 * time.Second)
	failOver := NewFailoverWorker(cluster.workerManger, time.Second)
	failOver.Work(cluster)
	assert.Equal(t, node1.State, metapb.NodeState_N_Offline, "node state error")

	time.Sleep(4 * time.Second)
	failOver.Work(cluster)
	assert.Equal(t, node1.State, metapb.NodeState_N_Tombstone, "node state error")

	time.Sleep(1 * time.Second)
	// 不连续发送DefaultDsRecoveryInterim / DefaultDsHearbeatInterval次心跳
	req := &mspb.NodeHeartbeatRequest{
		NodeId: ds_1.NodeId,
	}
	server.handleNodeHeartbeat(nil, req)
	assert.Equal(t, node1.State, metapb.NodeState_N_Offline, "node state error")

	time.Sleep(4 * time.Second)
	failOver.Work(cluster)

	cluster.opt.MaxNodeDownTime = time.Second
	time.Sleep(1 * time.Second)
	failOver.Work(cluster)
	assert.Equal(t, node1.State, metapb.NodeState_N_Logout, "node state error")
}

/**
 * 正常流程:已经死亡的节点，救不回来了
 */
func TestNodeFailOverNeverBorn(t *testing.T) {
	cluster, server := createCluster(t)
	defer closeCluster(cluster)

	ds_1 := NewMockDs(dsAddr_1)
	node1, _, err := cluster.GetNodeId(dsAddr_1, "", "", "")
	if err != nil {
		t.Fatalf("GetNodeId error:[%v]", err)
	}
	assert.NotEqual(t, node1.Id, 0, "Donot generate node id")
	assert.Equal(t, node1.State, metapb.NodeState_N_Initial, "node state error")
	ds_1.SetNodeId(node1.Id)

	if err := cluster.NodeLogin(ds_1.NodeId); err != nil {
		t.Fatalf("NodeLogin error:[%v]", err)
	}
	assert.Equal(t, node1.State, metapb.NodeState_N_Login, "node state error")

	time.Sleep(2 * time.Second)
	failOver := NewFailoverWorker(cluster.workerManger, time.Second)
	failOver.Work(cluster)
	assert.Equal(t, node1.State, metapb.NodeState_N_Offline, "node state error")

	time.Sleep(4 * time.Second)
	failOver.Work(cluster)
	assert.Equal(t, node1.State, metapb.NodeState_N_Tombstone, "node state error")

	time.Sleep(1 * time.Second)
	cluster.opt.MaxNodeDownTime = time.Second
	failOver.Work(cluster)
	assert.Equal(t, node1.State, metapb.NodeState_N_Logout, "node state error")

	// 连续发送DefaultDsRecoveryInterim / DefaultDsHearbeatInterval次心跳
	req := &mspb.NodeHeartbeatRequest{
		NodeId: ds_1.NodeId,
	}
	server.handleNodeHeartbeat(nil, req)
	assert.Equal(t, node1.State, metapb.NodeState_N_Logout, "node state error")

	server.handleNodeHeartbeat(nil, req)
	assert.Equal(t, node1.State, metapb.NodeState_N_Logout, "node state error")
}

func createCluster(t *testing.T) (c *Cluster, s *Server) {
	modifyDefault(1, 2, 5)

	cluster := newBoltDbCluster(t, newMockIDAllocator())
	server := new(Server)
	server.cluster = cluster
	return cluster, server
}

func closeCluster(c *Cluster)  {
	closeLocalCluster(c)
}

func modifyDefault(t1, t2, t3 uint32) {
	DefaultDownTimeLimit = time.Duration(t1) * time.Second
	DefaultDsHearbeatInterval = time.Duration(t2) * time.Second
	DefaultDsRecoveryInterim = time.Duration(t3) * time.Second
}
