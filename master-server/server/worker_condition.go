package server

import (
	"util/log"
	"model/pkg/metapb"
	"time"
)

var (
	// 单位是秒
	DefaultDownTimeLimit      = 60 * time.Second
	MaxDownReplicaTimeLimit   = 5 * 60 * time.Second
	DefaultDsHearbeatInterval = 10 * time.Second
	DefaultDsRecoveryInterim  = 5 * 60 * time.Second

	DefaultTimeFormat = "2006-01-02 15:04:05"
	// 大于一个调度周期+一个心跳周期，预留冗余
	DefaultChangeLeaderTaskTimeout = time.Second * time.Duration(30)
	DefaultRangeDeleteTaskTimeout  = time.Second * time.Duration(30)
	DefaultRangeAddPeerTaskTimeout = time.Second * time.Duration(300)
	DefaultRangeDelPeerTaskTimeout = time.Second * time.Duration(30)
)

func (node *Node) require() bool {
	if node.IsLogin() {
		return true
	}
	if !node.IsBusy() {
		return true
	}
	if !node.isBlocked() {
		return true
	}
	return false
}

func (rng *Range) require(cluster *Cluster) bool {
	//todo 完善range的state
	if rng.State == metapb.RangeState_R_Remove ||
		rng.State == metapb.RangeState_R_Abnormal ||
		rng.State == metapb.RangeState_R_Init {
		log.Debug("range state is abnormal, cannot be scheduled")
		return false
	}

	//没有table，就不调度
	if _, ok := cluster.FindTableById(rng.GetTableId()); !ok {
		return false
	}
	if len(rng.GetPeers()) != cluster.opt.GetMaxReplicas() {
		log.Debug("range peer is abnormal, cannot be scheduled")
		return false
	}
	return true
}

/**
return:
	the normal node of the most leader number
    the normal node of the least leader number
 */
func SelectMostAndLeastLeaderNode(nodes []*Node, selectors []NodeSelector) (*Node, *Node) {
	var most, least *Node
	for _, node := range nodes {
		if !node.require() {
			continue
		}
		if most == nil || most.GetLeaderCount() < node.GetLeaderCount() {
			most = node
		}

		/**
		满足条件的节点才能被选择为最小数
		 */
		if least == nil || least.GetLeaderCount() > node.GetLeaderCount() {
			var flag = true
			for _, selector := range selectors {
				if !selector.CanSelect(node) {
					log.Debug("worker: cannot select node %v  as least leader count, because of %v", node.GetId(), selector.Name())
					flag = false
					break
				}
			}
			if flag {
				least = node
			}
		}
	}
	return most, least
}

/**
return:
	the normal node of the most leader number or available ration low node
    the normal node of the least leader number
 */
func SelectMostAndLeastRangeNode(opt *scheduleOption, nodes []*Node, selectors []NodeSelector) (*Node, *Node, bool) {
	var most, least *Node
	force := false
	for _, node := range nodes {
		if !node.require() {
			continue
		}

		if !force || most == nil {
			if node.availableRatio()*100 < float64(opt.GetStorageAvailableThreshold()) {
				//maybe caused by master server restart, ignore
				log.Debug("node %v available ratio under threshold",  node.GetId())
				force = true
				most = node
			} else if most == nil || most.GetRangesCount() < node.GetRangesCount() {
				most = node
			}
		}

		/**
		满足条件的节点才能被选择为最小数
		 */
		if least == nil || least.GetRangesCount() > node.GetRangesCount() {
			var flag = true
			for _, selector := range selectors {
				if !selector.CanSelect(node) {
					log.Debug("worker: cannot select node %v  as least range count, because of %v", node.GetId(), selector.Name())
					flag = false
					break
				}
			}
			if flag {
				least = node
			}
		}
	}
	return most, least, force
}

func SelectLeaderNode(nodes []*Node, selectors []NodeSelector, mostLeaderNum float64) *Node {
	for _, node := range nodes {
		if !node.require() {
			continue
		}
		if mostLeaderNum - float64(node.GetLeaderCount()) < float64(Min_leader_balance_num){
			continue
		}
		flag := true
		for _, selector := range selectors {
			if !selector.CanSelect(node) {
				log.Debug("target: node %v cannot select, because of %v ", node.GetId(), selector.Name())
				flag = false
				break
			}
		}
		if flag {
			return node
		}
	}
	return nil
}
