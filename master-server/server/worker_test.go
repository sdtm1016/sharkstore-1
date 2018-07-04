package server

import (
	"testing"
	"fmt"
	"os"
	"model/pkg/mspb"
	"model/pkg/metapb"
	"sync"
	"util/deepcopy"
	"time"
	"math/rand"
)

var wg sync.WaitGroup  //定义一个同步等待的组

func TestAdjust_Interval(t *testing.T) {
	interval := 5 * defaultWorkerInterval
	adjustThreshold := maxFloat64(60, float64(Min_leader_adjust_num))
	if 100 > adjustThreshold {
		interval = maxDuration(time.Duration(float64(interval)*scheduleIntervalFactor), minScheduleInterval)
	} else {
		interval = 5 * defaultWorkerInterval
	}

	t.Logf("fist adjust, interval: %v", interval)

	adjustThreshold = maxFloat64(60, float64(Min_leader_adjust_num))
	if 100 > adjustThreshold {
		interval = maxDuration(time.Duration(float64(interval)*scheduleIntervalFactor), minScheduleInterval)
	} else {
		interval = 5 * defaultWorkerInterval
	}

	t.Logf("second adjust, interval: %v", interval)

	for i := 0; i< 100; i++ {
		interval = maxDuration(time.Duration(float64(interval)*scheduleIntervalFactor), minScheduleInterval)
		t.Logf("adjust, interval: %v", interval)
	}

	adjustThreshold = maxFloat64(60, float64(Min_leader_adjust_num))
	if 50 > adjustThreshold {
		interval = maxDuration(time.Duration(float64(interval)*scheduleIntervalFactor), minScheduleInterval)
	} else {
		interval = 5 * defaultWorkerInterval
	}

	t.Logf("thred recover, interval: %v", interval)
}

func TestBalanceLeader(t *testing.T) {
	mockCluster := MockCluster(t)
	defer closeCluster(mockCluster)
	addRangeForLeaderBalance(mockCluster, 100, t)
	fmt.Println(fmt.Sprintf("current range size: [%v]", len(mockCluster.GetAllRanges())))
	mockCluster.AddBalanceLeaderWorker()
	for i := 0; i < 7; i++ {
		NodeBalanceCondition(i, mockCluster, t)
		time.Sleep(time.Minute)
	}
	wg.Add(1)
	wg.Wait()
}

func TestBalanceRangeNum(t *testing.T) {
	mockCluster := MockCluster(t)
	defer closeCluster(mockCluster)
	addRangeForRangeBalance(mockCluster, 100, t)
	fmt.Println(fmt.Sprintf("current range size: [%v]", len(mockCluster.GetAllRanges())))
	mockCluster.AddBalanceRangeWorker()
	for i := 0; i < 7; i++ {
		NodeBalanceCondition(i, mockCluster, t)
		time.Sleep(time.Minute)
	}

	wg.Add(1)
	wg.Wait()
}

func TestBalanceRangeOps(t *testing.T) {
	mockCluster := MockCluster(t)
	defer closeCluster(mockCluster)
	addRangeForRangeBalance(mockCluster, 100, t)
	fmt.Println(fmt.Sprintf("current range size: [%v]", len(mockCluster.GetAllRanges())))
	mockCluster.AddBalanceNodeOpsWorker()
	for i := 0; i < 8; i++ {
		NodeBalanceCondition(i, mockCluster, t)
		time.Sleep(time.Minute)
	}
	wg.Add(1)
	wg.Wait()
}

func MockCluster(t *testing.T) *Cluster {
	mockCluster := newLocalCluster(newMockIDAllocator())
	addNodes(mockCluster)
	fmt.Println(fmt.Sprintf("current node size : %v", len(mockCluster.GetAllNode())))
	return mockCluster
}

func NodeBalanceCondition(expr int, c *Cluster, t *testing.T) {
	switch expr {
	case 1:
		//update node state
		if err := c.UpdateNodeState(c.FindNodeById(1), metapb.NodeState_N_Logout); err != nil {
			t.Error("update node 1 state error")
		}
		return
	case 2:
		//stat ops
		node := c.FindNodeById(2)
		node.stats.BytesWritten = 100000000
		node.opsStat.Hit(node.stats.GetBytesWritten())
		node = c.FindNodeById(3)
		node.stats.BytesWritten = 100000000
		node.opsStat.Hit(node.stats.GetBytesWritten())
		return
	case 3:
		//stat storage
		node := c.FindNodeById(4)
		node.stats.Available = 10
		return
	case 4:
		//cache
		for i := 0; i < 100; i++ {
			c.hbManager.dealIngNodes.set(5)
			time.Sleep(time.Second)
		}
		return
	case 5:
		//range state
		i := 0
		for _, rng := range c.GetAllRanges() {
			if i%6 == 0 {
				rng.State = metapb.RangeState_R_Offline
			}
			i++
		}
		return
	case 6:
		//leader count [默认代码里面生产的leader 就是不均衡的]
		return
	case 7:
		//range count  [代码控制]
		return
	case 8:
		// range ops
		i := 0
		for _, rng := range c.GetAllRanges() {
			if i%5 == 0 {
				rng.BytesWritten = 100000000
				rng.opsStat.Hit(rng.BytesWritten)
			}
			i++
		}
		return
	}
}

// range 均衡， 但  leader 不均衡
func addRangeForLeaderBalance(cluster *Cluster, rangeSize int, t *testing.T) {
	tableId, _ := cluster.idGener.GenID()
	for j := 0; j < rangeSize; j++ {
		rangeId, err := cluster.idGener.GenID()
		if err != nil {
			t.Errorf("generate id error")
			continue
		}
		rngM := &metapb.Range{
			Id:      rangeId,
			TableId: tableId,
		}
		rng := NewRange(rngM, nil)
		addPeersBalance(cluster, rng, t)
		cluster.AddRange(rng)
	}
	for _, node := range cluster.GetAllNode() {
		var leaderNodeNum uint32
		for _, rng := range node.GetAllRanges() {
			if rng.GetLeader().GetNodeId() == node.GetId() {
				leaderNodeNum++
			}
		}
		node.stats.RangeLeaderCount = leaderNodeNum
		fmt.Println(fmt.Sprintf("current node [%v]  has range size  [%v], leader size [%v]",
			node.GetId(), len(node.GetAllRanges()), leaderNodeNum))
	}

}

// range 不均衡 ( 不关心leader)
func addRangeForRangeBalance(cluster *Cluster, rangeSize int, t *testing.T) {
	tableId, _ := cluster.idGener.GenID()
	for j := 0; j < rangeSize; j++ {
		rangeId, err := cluster.idGener.GenID()
		if err != nil {
			t.Errorf("generate id error")
			continue
		}
		rngM := &metapb.Range{
			Id:      rangeId,
			Peers:   addPeersRandom(cluster),
			TableId: tableId,
		}
		rng := NewRange(rngM, nil)
		cluster.AddRange(rng)
	}
	for _, node := range cluster.GetAllNode() {
		node.stats.RangeCount = uint32(len(node.GetAllRanges()))
		var leaderNodeNum uint32
		for _, rng := range node.GetAllRanges() {
			if rng.GetLeader().GetNodeId() == node.GetId() {
				leaderNodeNum++
			}
		}
		node.stats.RangeLeaderCount = leaderNodeNum
		fmt.Println(fmt.Sprintf("current node [%v]  has range size  [%v], leader size [%v]",
			node.GetId(), len(node.GetAllRanges()), leaderNodeNum))
	}

}

func addPeersBalance(cluster *Cluster, rng *Range, t *testing.T) {
	for index := 0; index < cluster.opt.GetMaxReplicas(); index++ {
		peers := rng.GetPeers()
		newPeer, err := cluster.allocPeerAndSelectNode(rng)
		if err != nil {
			t.Errorf("errr: %v", err)
			continue
		}
		peers = append(peers, newPeer)
		if index == 0 {
			rng.Leader = deepcopy.Iface(newPeer).(*metapb.Peer)
		}
		rng.Peers = peers
	}
}

func addPeersRandom(cluster *Cluster) []*metapb.Peer {
	var peers []*metapb.Peer
	nodes := cluster.GetAllNode()
	for index := 0; index < cluster.opt.GetMaxReplicas(); index++ {
		newPeer, _ := cluster.allocPeer(nodes[rand.Intn(len(nodes))].GetId())
		peers = append(peers, newPeer)
	}
	return peers
}

//7 个 node
func addNodes(cluster *Cluster) {
	cluster.AddNode(createNode(cluster, "192.168.0.1:6180", 2, 12121212))
	cluster.AddNode(createNode(cluster, "192.168.0.1:6280", 1, 2312))
	cluster.AddNode(createNode(cluster, "192.168.0.2:6180", 2, 87122))
	cluster.AddNode(createNode(cluster, "192.168.0.3:6180", 1, 42321))
	cluster.AddNode(createNode(cluster, "192.168.0.4:6180", 1, 98238))
	cluster.AddNode(createNode(cluster, "192.168.0.4:6280", 1, 323121))
	cluster.AddNode(createNode(cluster, "192.168.0.5:6180", 1, 9871))
}

func createNode(cluster *Cluster, addr string, count uint32, written uint64) *Node {
	nodeId, _ := cluster.idGener.GenID()
	node := NewNode(&metapb.Node{Id: nodeId, ServerAddr: addr, State: metapb.NodeState_N_Login})
	node.stats = &mspb.NodeStats{RangeCount: count, BytesWritten: written, Capacity: 1048576, Available: 894323}
	return node
}

func closeLocalCluster(cluster *Cluster) {
	if cluster != nil {
		cluster.store.Close()
	}
	clearData()
}

func newLocalCluster(id IDGenerator) *Cluster {
	initDataPath()
	store, err := NewLevelDBDriver(path)
	if err != nil {
		fmt.Printf("new store failed, err %v\n", err)
		os.Exit(-1)
	}
	err = store.Open()
	if err != nil {
		fmt.Printf("open store failed, err %v\n", err)
		os.Exit(-1)
	}
	var clusterId uint64 = 1
	var nodeId uint64 = 1
	cfg := NewDefaultConfig()
	cluster := NewCluster(clusterId, nodeId, store, newScheduleOption(cfg))
	err = cluster.LoadCache()
	if err != nil {
		fmt.Printf("load cache failed, err %v", err)
		os.Exit(-1)
		return nil
	}
	cluster.idGener = id
	// 指定自己为leader
	cluster.UpdateLeader(&Peer{ID: nodeId})
	cluster.cli = &LocalDSClient{}
	return cluster
}
