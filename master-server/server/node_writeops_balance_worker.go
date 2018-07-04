package server

import (
	"context"
	"model/pkg/metapb"
	"time"
	"util/log"
)

type balanceNodeOpsWorker struct {
	opt      *scheduleOption
	limit    uint64
	name     string
	ctx      context.Context
	cancel   context.CancelFunc
	interval time.Duration
}

func NewBalanceNodeOpsWorker(wm *WorkerManager, interval time.Duration) *balanceNodeOpsWorker {
	ctx, cancel := context.WithCancel(wm.ctx)
	return &balanceNodeOpsWorker{
		name:     balanceNodeOpsWorkerName,
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
	}
}

func (w *balanceNodeOpsWorker) GetName() string {
	return w.name
}

func (w *balanceNodeOpsWorker) Work(cluster *Cluster) {
	log.Debug("start  %s", w.GetName())
	cluster.metric.CollectScheduleCounter(w.GetName(), "schedule")
	// Select a peer from the node with most regions.
	rng, oldPeer := scheduleRemoveMaxOpsPeer(cluster, w.GetName())
	if rng == nil || oldPeer == nil {
		log.Debug("%s: no range need balance", w.GetName())
		return
	}

	sourceNode := cluster.FindNodeById(oldPeer.GetNodeId())
	newPeer, err := cluster.allocPeerAndSelectNode(rng, true)
	if newPeer == nil || err != nil {
		cluster.metric.CollectScheduleCounter(w.GetName(), "no_peer")
		log.Error("alloc peer failure rngId:%d err:%s", rng.GetId(), err.Error())
		return
	}

	//避免下次马上调度，清理旧的数据
	rng.opsStat.Clear()
	sourceNode.opsStat.Clear()

	taskID, err := cluster.GenId()
	if err != nil {
		return
	}

	cluster.metric.CollectScheduleCounter(w.GetName(), "new_operator")
	log.Debug("start to balance region and transfer peer, region:[%v], old peer:[%v], old node:[%v], new node:[%v]",
		rng.GetId(), oldPeer.GetId(), oldPeer.GetNodeId(), newPeer.GetNodeId())
	tc := NewTransferPeerTasks(taskID, rng, "ops-range-tranfer", oldPeer)
	// TODO: check return
	cluster.taskManager.Add(tc)
	return
}

func scheduleRemoveMaxOpsPeer(cluster *Cluster, workerName string) (*Range, *metapb.Peer) {
	nodes := cluster.GetAllActiveNode()
	if len(nodes) == 0 {
		log.Debug("%v: node is nil", workerName)
		cluster.metric.CollectScheduleCounter(workerName, "no_node")
		return nil, nil
	}

	var sourceNode *Node
	for _, node := range nodes {
		if node.IsLogin() && node.opsStat.GetMax() > uint64(cluster.opt.GetWriteByteOpsThreshold()) {
			sourceNode = node
			break
		}
	}

	if sourceNode == nil {
		cluster.metric.CollectScheduleCounter(workerName, "no_node")
		log.Debug("%v: no node is over threshold ops", workerName)
		return nil, nil
	}

	// find the most ops range on sourceNode
	var rng *Range
	for _, peer := range sourceNode.GetAllRanges() {
		r := cluster.FindRange(peer.GetId())
		if r == nil || !r.require(cluster) {
			continue
		}
		if rng == nil || (r.opsStat.hit > 10 && rng.opsStat.GetMax() < r.opsStat.GetMax()) {
			rng = r
		}
	}

	if rng == nil {
		log.Debug("%v: select range is nil", workerName)
		cluster.metric.CollectScheduleCounter(workerName, "no_range")
		return nil, nil
	}
	return rng, rng.GetNodePeer(sourceNode.GetId())
}

func (w *balanceNodeOpsWorker) AllowWork(cluster *Cluster) bool {
	if cluster.autoTransferUnable {
		return false
	}
	return true
}

func (w *balanceNodeOpsWorker) GetInterval() time.Duration {
	return w.interval
}

func (w *balanceNodeOpsWorker) Stop() {
	w.cancel()
}
