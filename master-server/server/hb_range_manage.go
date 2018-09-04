package server

import (
	"fmt"
	"model/pkg/metapb"
	"time"
	"util/log"

	"github.com/gogo/protobuf/proto"
)

var (
	DefaultMaxNodeDownTimeInterval time.Duration = 60 * time.Second
	DefaultMaxPeerDownTimeInterval time.Duration = 2 * 60 * time.Second

	// 大于一个调度周期+一个心跳周期，预留冗余
	DefaultChangeLeaderTimeout time.Duration = time.Second * time.Duration(30)
	DefaultDelRangeTimeout     time.Duration = time.Second * time.Duration(30)
	DefaultAddPeerTimeout      time.Duration = time.Second * time.Duration(300)
	DefaultDelPeerTimeout      time.Duration = time.Second * time.Duration(30)
)

type hb_range_manager struct {
	cluster      *Cluster
	dealIngNodes *idCache
}

func NewHBRangeManager(cluster *Cluster) *hb_range_manager {
	dealIngNodes := newIDCache(time.Second, 10*time.Second)
	return &hb_range_manager{cluster: cluster, dealIngNodes: dealIngNodes}
}

type RunMode int

const (
	RUN_MODE_INIT   RunMode = iota //永远不要被用到
	RUN_MODE_LOCAL                 //在master执行
	RUN_MODE_REMOTE                //应答给DS执行
)

func isQuorumDown(r *Range) bool {
	totalVoters := 0
	downVoters := 0
	for _, peer := range r.GetPeers() {
		if peer.GetType() != metapb.PeerType_PeerType_Learner {
			totalVoters++
		}
	}
	for _, down := range r.GetDownPeers() {
		if down.Peer.GetType() != metapb.PeerType_PeerType_Learner {
			downVoters++
		}
	}
	quorum := totalVoters/2 + 1
	return totalVoters-downVoters < quorum
}

func peerGC(cluster *Cluster, r *metapb.Range, peer *metapb.Peer) error {
	node := cluster.FindNodeById(peer.GetNodeId())
	replica := &metapb.Replica{RangeId: r.GetId(), Peer: peer, StartKey: r.GetStartKey(), EndKey: r.GetEndKey()}
	v, _ := proto.Marshal(replica)
	key := []byte(fmt.Sprintf("%s%d", PREFIX_REPLICA, peer.GetId()))
	err := cluster.store.Put(key, v)
	if err != nil {
		return err
	}
	node.AddTrashReplica(replica)
	return nil
}

func (manager *hb_range_manager) checkDownPeer(cluster *Cluster, rng *Range) *TaskChain {
	if isQuorumDown(rng) {
		log.Warn("range %v more down peers ", rng)
		return nil
	}
	for _, down := range rng.GetDownPeers() {
		peer := down.Peer
		if peer == nil {
			continue
		}
		if down.DownSeconds < uint64(DefaultMaxPeerDownTimeInterval.Seconds()) {
			continue
		}
		id, err := cluster.GenId()
		if err != nil {
			return nil
		}
		return NewTaskChain(id, rng.GetId(), "hb-down-peer", NewDeletePeerTask(peer))
	}
	return nil
}

func (manager *hb_range_manager) createDelPeerTask(id uint64, r *Range, peer *metapb.Peer, creator string) *TaskChain {
	delPeerTask := NewDeletePeerTask(peer)

	if r.GetLeader() != nil && r.GetLeader().GetId() == peer.GetId() {
		changeLeaderTask := NewChangeLeaderTask(r.GetLeader().GetNodeId(), 0)
		changeLeaderTask.SetAllowFail()
		return NewTaskChain(id, r.GetId(), creator, changeLeaderTask, delPeerTask)
	}

	return NewTaskChain(id, r.GetId(), creator, delPeerTask)
}

func (manager *hb_range_manager) CheckRange(cluster *Cluster, r *Range) *TaskChain {
	// 处理分片回收
	if r.State == metapb.RangeState_R_Remove {
		log.Warn("range %v had been marked remove status but not gc, please attention", r.GetId())
		return nil // XXX master active do delete range
	}

	// range failover
	if !cluster.autoFailoverUnable {
		if tc := manager.checkDownPeer(cluster, r); tc != nil {
			return tc
		}
	}

	// lack of peer
	if len(r.GetPeers()) < cluster.opt.GetMaxReplicas() {
		log.Info("range %d peer %d less than %d", r.GetId(), len(r.GetPeers()), cluster.opt.GetMaxReplicas())
		id, err := cluster.GenId()
		if err != nil {
			log.Error("rangeId:%d,%s", r.GetId(), err.Error())
			return nil
		}
		return NewTaskChain(id, r.GetId(), "hb-lack-peer", NewAddPeerTask())
	}

	// too many peers
	if len(r.GetPeers()) > cluster.opt.GetMaxReplicas() {
		log.Info("range %d peer %d more than %d", r.GetId(), len(r.GetPeers()), cluster.opt.GetMaxReplicas())

		if len(r.GetPendingPeers()) != 0 {
			log.Info("range %v peer number %v / pending peer number %v: ", r.GetId(), len(r.GetPeers()), len(r.GetPendingPeers()))
			return nil
		}
		// 优先下掉ip相同的副本
		oldPeer := cluster.selectWorstPeer(r)
		if oldPeer == nil {
			return nil
		}
		id, err := cluster.GenId()
		if err != nil {
			return nil
		}
		return manager.createDelPeerTask(id, r, oldPeer, "hb-overmuch-peer")
	}

	// 检查是否有ip相同的副本
	if ip, ok := cluster.checkSameIpNode(r.GetNodes(cluster)); ok {
		//先添加，后面再自动删除
		log.Info("range %d exist same ip %v", r.GetId(), ip)
		id, err := cluster.GenId()
		if err != nil {
			log.Error("rangeId:%d,%s", r.GetId(), err.Error())
			return nil
		}
		return NewTaskChain(id, r.GetId(), "hb-same-ip", NewAddPeerTask())
	}

	return nil
}
