package server

import (
	"model/pkg/metapb"
	"time"
	"util/log"

	"golang.org/x/net/context"
)

var (
	Min_range_balance_num = 10
	Min_range_adjust_num  = 50
)

type balanceNodeRangeWorker struct {
	opt             *scheduleOption
	limit           uint64
	name            string
	ctx             context.Context
	cancel          context.CancelFunc
	interval        time.Duration
	defaultInterval time.Duration
}

func NewBalanceNodeRangeWorker(wm *WorkerManager, interval time.Duration) *balanceNodeRangeWorker {
	ctx, cancel := context.WithCancel(wm.ctx)
	return &balanceNodeRangeWorker{
		name:            balanceRangeWorkerName,
		ctx:             ctx,
		cancel:          cancel,
		interval:        interval,
		defaultInterval: interval,
	}
}
func (w *balanceNodeRangeWorker) GetName() string {
	return w.name
}

func (w *balanceNodeRangeWorker) Work(cluster *Cluster) {
	log.Debug("start %s", w.GetName())
	cluster.metric.CollectScheduleCounter(w.GetName(), "schedule")
	rng, oldPeer, targetNodeId := w.selectRemovePeer(cluster)
	if rng == nil || oldPeer == nil {
		log.Debug("no range need balance")
		return
	}

	id, err := cluster.GenId()
	if err != nil {
		return
	}
	cluster.metric.CollectScheduleCounter(w.GetName(), "new_operator")
	log.Debug("start to balance region and remove peer, region:[%v], old peer:[%v], old node:[%v], new node:[%v]",
		rng.GetId(), oldPeer.GetId(), oldPeer.GetNodeId(), targetNodeId)
	tc := NewTransferPeerTasks(id, rng, "balance-range-tranfer", oldPeer)
	// TODO: check return
	cluster.taskManager.Add(tc)
	return
}

func (w *balanceNodeRangeWorker) AllowWork(cluster *Cluster) bool {
	if cluster.autoTransferUnable {
		return false
	}
	return true
}

func (w *balanceNodeRangeWorker) GetInterval() time.Duration {
	return w.interval
}

func (w *balanceNodeRangeWorker) Stop() {
	w.cancel()
}

// selectRemovePeer schedule to transfer the range by removing the peer
// for balancing the node range
func (w *balanceNodeRangeWorker) selectRemovePeer(cluster *Cluster) (*Range, *metapb.Peer, uint64) {
	nodes := cluster.GetAllActiveNode()
	if len(nodes) == 0 {
		log.Debug("%v: node is nil", w.GetName())
		cluster.metric.CollectScheduleCounter(w.GetName(), "no_node")
		return nil, nil, 0
	}

	newSelectors := []NodeSelector{
		NewWriterOpsThresholdSelector(cluster.opt),
		NewStorageThresholdSelector(cluster.opt),
		NewDifferCacheNodeSelector(cluster.hbManager.dealIngNodes),
	}

	mostRangeNode, leastRangeNode, force := SelectMostAndLeastRangeNode(cluster.opt, nodes, newSelectors)
	var mostRangeNum, leastRangeNum = uint32(0), uint32(0)
	if mostRangeNode != nil {
		mostRangeNum = mostRangeNode.GetRangesCount()
	}
	if leastRangeNode != nil {
		leastRangeNum = leastRangeNode.GetRangesCount()
	} else {
		log.Debug("no target node ")
		return nil, nil, 0
	}

	avgRangerNum := countRangeAvg(nodes)
	balanceThreshold := maxFloat64(avgRangerNum/20, float64(Min_range_balance_num))

	if !force && float64(mostRangeNum-leastRangeNum) < balanceThreshold {
		log.Debug("mostNode %v mostRangeNum %v , leastNode %v leastRangeNum %v, don't need balance",
			mostRangeNode.GetId(), mostRangeNum, leastRangeNode.GetId(), leastRangeNum)
		return nil, nil, 0
	}

	w.adjustNextInterval(force, mostRangeNum, leastRangeNum, avgRangerNum)

	//选节点的peer不在mostRangeNode的 range, 且该range  没有 peer在leastRangeNode 上
	var rng *Range
	for _, r := range mostRangeNode.GetAllRanges() {
		if r.GetLeader().GetNodeId() != mostRangeNode.GetId() && r.require(cluster) {
			ipSelector := NewDifferIPSelector(r.GetNodes(cluster))
			if !ipSelector.CanSelect(leastRangeNode) {
				continue
			}
			rng = r
			break
		}
	}

	if rng == nil {
		log.Debug("%v: select follower range than exclude leastRangeNode %v is nil ", w.GetName(), leastRangeNode)
		for _, r := range mostRangeNode.GetAllRanges() {
			if r.GetLeader().GetNodeId() == mostRangeNode.GetId() && r.require(cluster) {
				ipSelector := NewDifferIPSelector(r.GetNodes(cluster))
				if !ipSelector.CanSelect(leastRangeNode) {
					continue
				}
				rng = r
				break
			}
		}
	}

	if rng == nil {
		log.Debug("%v: select leader range that exclude leastRangeNode %v is nil ", w.GetName(), leastRangeNode)
		for _, r := range mostRangeNode.GetAllRanges() {
			if r.GetLeader().GetNodeId() != mostRangeNode.GetId() && r.require(cluster) {
				leastRangeNode = cluster.selectNodeForAddPeer(r)
				if leastRangeNode != nil {
					rng = r
					break
				}
			}
		}
	}

	if rng == nil {
		log.Debug("%v: select follow range to best node is nil  %v", w.GetName(), leastRangeNode)
		for _, r := range mostRangeNode.GetAllRanges() {
			if r.GetLeader().GetNodeId() == mostRangeNode.GetId() && r.require(cluster) {
				leastRangeNode = cluster.selectNodeForAddPeer(r)
				if leastRangeNode != nil {
					rng = r
					break
				}
			}
		}
	}

	if rng == nil {
		log.Debug("%v: select leader range to best node is nil  %v", w.GetName(), leastRangeNode)
		cluster.metric.CollectScheduleCounter(w.GetName(), "no_peer")
		cluster.hbManager.dealIngNodes.set(mostRangeNode.GetId())
		return nil, nil, 0
	}

	return rng, rng.GetNodePeer(mostRangeNode.GetId()), leastRangeNode.GetId()
}

func (w *balanceNodeRangeWorker) adjustNextInterval(force bool, mostRangeNum, leastRangeNum uint32, avgRangeNum float64) {
	adjustThreshold := maxFloat64(avgRangeNum/2, float64(Min_range_adjust_num))
	if force || float64(mostRangeNum-leastRangeNum) > adjustThreshold {
		w.interval = maxDuration(time.Duration(float64(w.interval)*scheduleIntervalFactor), minScheduleInterval)
	} else {
		w.interval = w.defaultInterval
	}
}

//count node range average number,
func countRangeAvg(nodes []*Node) float64 {
	var averageLeader float64
	for _, s := range nodes {
		averageLeader += float64(s.GetRangesCount()) / float64(len(nodes))
	}
	return averageLeader
}
