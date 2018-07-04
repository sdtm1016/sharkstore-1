package server

import (
	"time"

	"model/pkg/alarmpb"
	"model/pkg/metapb"
	"util/log"

	"fmt"
	"util/deepcopy"

	"golang.org/x/net/context"
	"util/alarm"
	"strings"
	"strconv"
)

type RegionHbCheckWorker struct {
	name     string
	ctx      context.Context
	cancel   context.CancelFunc
	interval time.Duration
}

func NewRangeHbCheckWorker(wm *WorkerManager, interval time.Duration) Worker {
	ctx, cancel := context.WithCancel(wm.ctx)
	return &RegionHbCheckWorker{
		name:     rangeHbCheckWorkerName,
		ctx:      ctx,
		cancel:   cancel,
		interval: interval}
}

func (hb *RegionHbCheckWorker) GetName() string {
	return hb.name
}

func (hb *RegionHbCheckWorker) Work(cluster *Cluster) {
	log.Debug("RegionHbCheckWorker: start to check region hb")
	for _, table := range cluster.workingTables.GetAllTable() {
		if table.GetStatus() != metapb.TableStatus_TableRunning {
			continue
		}
		for _, r := range cluster.GetTableAllRanges(table.GetId()) {
			if r.State == metapb.RangeState_R_Remove {
				continue
			}
			leader := r.GetLeader()

			//12个周期leader无心跳
			if time.Since(r.LastHbTimeTS) > cluster.opt.GetMaxRangeDownTime() {
				var desc string
				if leader == nil {
					log.Error("must bug !!!  range[%d:%d] no leader, no heartbeat, lastHeartbeat :[%v]",
						table.GetId(), r.GetId(), r.LastHbTimeTS)

					desc = fmt.Sprintf("cluster[%v] table[%v] range[%v] no heartbeat, no leader, lastheartbeat:[%v]",
						cluster.GetClusterId(), table.GetId(), r.GetId(), r.LastHbTimeTS)
				} else {
					log.Error("range[%d:%d] no heartbeat, leader is [%d], lastHeartbeat:[%v]",
						table.GetId(), r.GetId(), leader.GetNodeId(), r.LastHbTimeTS)

					desc = fmt.Sprintf("cluster[%v] table[%v] range[%v] no heartbeat, leader node[id %v, addr %v], lastheartbeat:[%v]",
						cluster.GetClusterId(), table.GetId(), r.GetId(), leader.GetNodeId(), cluster.FindNodeById(leader.GetNodeId()).GetServerAddr(), r.LastHbTimeTS)
				}

				ip := strings.Split(cluster.FindNodeById(leader.GetNodeId()).GetServerAddr(), ":")[0]
				port, _ := strconv.ParseInt(strings.Split(cluster.FindNodeById(leader.GetNodeId()).GetServerAddr(), ":")[1], 10, 64)
				info := make(map[string]interface{})
				info["ip"] = ip
				info["port"] = port
				info["spaceId"] = cluster.GetClusterId()
				info["range_no_heartbeat"] = 1
				info["tableId"] = table.GetId()
				info["rangeId"] = r.GetId()
				info["nodeId"] = leader.GetNodeId()
				sample := alarm.NewSample(ip, int(port), int(cluster.GetClusterId()), info)
				if err := cluster.alarmCli.RangeNoHeartbeatAlarm(int64(cluster.clusterId), &alarmpb.RangeNoHeartbeatAlarm{
					Range:             deepcopy.Iface(r.Range).(*metapb.Range),
					LastHeartbeatTime: r.LastHbTimeTS.String(),
				}, desc, []*alarm.Sample{sample}); err != nil {
					log.Error("range no leader alarm failed: %v", err)
				}

				r.State = metapb.RangeState_R_Abnormal
				cluster.unhealthyRanges.Put(r.GetId(), r)
				nodeAble := retrieveNode(cluster, r) //节点状态并不能完全决定range的状态【正常是可以的，不排除意外】
				if len(nodeAble) > 1 {               //节点正常，range不正常，不符合逻辑，需要特别关注
					//todo alarm
					log.Error("range[%d:%d] is unhealthy, but node is healthy, please attention. normal node: %v, peer:%v",
						table.GetId(), r.GetId(), nodeAble, r.GetPeers())
				}

			} else { //leader上报心跳正常
				if isQuorumDown(r) {
					r.State = metapb.RangeState_R_Abnormal
					cluster.unhealthyRanges.Put(r.GetId(), r)

					var leaderNodeID uint64
					if leader != nil {
						leaderNodeID = leader.GetNodeId()
					}
					log.Error("range[%d:%d] heartbeat normal, but more than half peer down, please attention. leader:[%d], downPeer:[%v]",
						table.GetId(), r.GetId(), leaderNodeID, r.GetDownPeers())

					//TODO: alarm
				} else {
					if len(r.Peers) != cluster.opt.GetMaxReplicas() || len(r.GetDownPeers()) > 0 {
						cluster.unstableRanges.Put(r.GetId(), r)
					}
				}
			}
		}
	}
	log.Debug("RegionHbCheckWorker: end to check region hb")
	return
}

func (hb *RegionHbCheckWorker) AllowWork(cluster *Cluster) bool {
	if !cluster.IsLeader() {
		return false
	}
	return true
}

func (hb *RegionHbCheckWorker) GetInterval() time.Duration {
	return hb.interval
}

func (hb *RegionHbCheckWorker) Stop() {
	hb.cancel()
}

/**
  检查node的状态
*/
func retrieveNode(cluster *Cluster, r *Range) []uint64 {
	var nodeIds []uint64
	for _, p := range r.GetPeers() {
		node := cluster.FindNodeById(p.GetNodeId())
		//检查peer的状态
		if node != nil && node.IsLogin() {
			nodeIds = append(nodeIds, node.Node.GetId())
		}
	}
	return nodeIds
}
