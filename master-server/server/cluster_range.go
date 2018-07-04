package server

import (
	"fmt"
	"master-server/http_reply"
	"model/pkg/metapb"
	"model/pkg/mspb"
	"util/deepcopy"
	"util/log"
)

func (c *Cluster) createRangeRemote(r *metapb.Range) error {
	for _, p := range r.GetPeers() {
		node := c.FindNodeById(p.GetNodeId())
		if node == nil {
			continue
		}
		var err error
		var i int
		for i = 0; i < 3; i++ {
			err = c.cli.CreateRange(node.GetServerAddr(), r)
			if err != nil {
				log.Warn("create range[%v] in node[%s] failed, err[%v]", r.String(), node.GetServerAddr(), err)
			} else {
				break
			}
		}
		if i == 3 {
			return err
		}
	}
	return nil
}

func (c *Cluster) memDeleteRange(r *metapb.Range) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("recover memDeleteRange")
		}
	}()
	c.DeleteRange(r.GetId())
	for _, peer := range r.GetPeers() {
		replica := &metapb.Replica{RangeId: r.GetId(), Peer: peer, StartKey: r.GetStartKey(), EndKey: r.GetEndKey()}
		node := c.FindNodeById(peer.GetNodeId())
		node.AddTrashReplica(replica)
	}
	c.deletedRanges.Add(r)
}

func (c *Cluster) ReplaceRange(old *metapb.Range, new *Range, toGc []*metapb.Peer) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("recover ReplaceRange")
		}
	}()
	c.DeleteRange(old.GetId())
	c.AddRange(new)
	for _, peer := range toGc {
		replica := &metapb.Replica{RangeId: old.GetId(), Peer: peer, StartKey: old.GetStartKey(), EndKey: old.GetEndKey()}
		node := c.FindNodeById(peer.GetNodeId())
		node.AddTrashReplica(replica)
	}
	c.deletedRanges.Add(old)
}

func (c *Cluster) AddRange(r *Range) {
	c.ranges.Add(r)
	for _, peer := range r.GetPeers() {
		node := c.FindNodeById(peer.GetNodeId())
		node.AddRange(r)
	}
}

func (c *Cluster) DeleteRange(rangeId uint64) {
	if r := c.ranges.Delete(rangeId); r != nil {
		for _, peer := range r.GetPeers() {
			node := c.FindNodeById(peer.GetNodeId())
			node.DeleteRange(r.GetId())
		}
	}
}

func (c *Cluster) FindRange(id uint64) *Range {
	if r, find := c.ranges.FindRangeByID(id); find {
		return r
	}
	return nil
}

func (c *Cluster) SearchRange(key []byte) *Range {
	if r, find := c.ranges.SearchRange(key); find {
		return r
	}
	return nil
}

func (c *Cluster) MultipleSearchRanges(key []byte, num int) []*Range {
	if rs, find := c.ranges.MultipleSearchRanges(key, num); find {
		return rs
	}
	return nil
}

func (c *Cluster) GetAllRanges() []*Range {
	return c.ranges.GetAllRange()
}

func (c *Cluster) GetTableAllRanges(tableId uint64) []*Range {
	return c.ranges.GetTableAllRanges(tableId)
}

func (c *Cluster) GetNodeRangeStatByTable(tableId uint64) map[uint64]int {
	rngStat := make(map[uint64]int, 0)
	tRanges := c.GetTableAllRanges(tableId)
	for _, r := range tRanges {
		rPeers := r.GetPeers()
		for _, p := range rPeers {
			rngStat[p.GetNodeId()] = rngStat[p.GetNodeId()] + 1
		}
	}
	return rngStat
}

func (c *Cluster) getLeaderNode(r *Range) *Node {
	return c.FindNodeById(r.GetLeader().GetNodeId())
}

func (c *Cluster) randFollowerRange(nodeID uint64) *Range {
	return c.FindNodeById(nodeID).randFollowerRange()
}

func (c *Cluster) randLeaderRange(nodeID uint64) *Range {
	return c.FindNodeById(nodeID).randLeaderRange()
}

func (c *Cluster) getFollowerNodes(r *Range) []*Node {
	var nodes []*Node
	for nodeID, _ := range r.GetFollowers() {
		if node := c.FindNodeById(nodeID); node != nil {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func (c *Cluster) updateStatus(region *Range, stats *mspb.RangeStats) {
	region.BytesWritten = stats.BytesWritten
	region.BytesRead = stats.BytesRead
	region.KeysWritten = stats.KeysWritten
	region.KeysRead = stats.KeysRead
	region.ApproximateSize = stats.GetApproximateSize()
	region.opsStat.Hit(region.BytesWritten)
}

func (c *Cluster) queryPeerRemote(r *metapb.Range) interface{} {
	var result []*http_reply.PeerBrief
	peerLen := len(r.GetPeers())
	resultChannel := make(chan *http_reply.PeerBrief, peerLen)
	for _, p := range r.GetPeers() {
		go func(p *metapb.Peer) {
			peerInfo := &http_reply.PeerBrief{Id: p.GetId()}
			node := c.FindNodeById(p.GetNodeId())
			if node == nil {
				resultChannel <- peerInfo
				return
			}
			peerInfo.NodeId = node.GetId()
			peerInfo.NodeAddress = node.GetServerAddr()
			peerInfo.NodeState = int32(node.GetState())
			var i int
			for i = 0; i < 3; i++ {
				resp, err := c.cli.GetPeerInfo(node.GetServerAddr(), r.GetId())
				if err != nil {
					log.Warn("query peer info: range[%v] peer[%v] node[%s] failed, err[%v]", r.GetId(), p.GetId(), node.GetServerAddr(), err)
				} else {
					log.Debug("query peer info: range[%v] peer[%v] node[%s] success, resp:[%v]", r.GetId(), p.GetId(), node.GetServerAddr(), resp)
					if resp != nil {
						if resp.Replica != nil {
							peerInfo.StartKey = fmt.Sprintf("%v", resp.Replica.StartKey)
							peerInfo.EndKey = fmt.Sprintf("%v", resp.Replica.EndKey)
						}
						peerInfo.Index = resp.GetIndex()
						peerInfo.Term = resp.GetTerm()
						peerInfo.Commit = resp.GetCommit()
					}
					break
				}
			}
			resultChannel <- peerInfo
		}(p)
	}
	for i := 0; i < peerLen; i++ {
		result = append(result, <-resultChannel)
	}
	return result
}

func (c *Cluster) rangeRecreate(r *Range, peerId uint64) (err error) {
	rngCopy := deepcopy.Iface(r.Range).(*metapb.Range)

	var id uint64
	id, err = c.idGener.GenID()
	if err != nil {
		err = fmt.Errorf("cannot generate range ID, err[%v]", err)
		return
	}

	var peers []*metapb.Peer
	var newPeer *metapb.Peer
	var toGcPeer []*metapb.Peer
	needNewPeer := true
	for _, peer := range r.GetPeers() {
		if peer.GetId() == peerId {
			needNewPeer = false
			newPeer, err = c.allocPeer(peer.GetNodeId(), false)
			if err != nil {
				return nil
			}
			continue
		}
		toGcPeer = append(toGcPeer, peer)
	}
	if needNewPeer {
		newPeer, err = c.allocPeerAndSelectNode(r, false)
		if err != nil {
			return
		}
	}
	// get node addr
	node := c.FindNodeById(newPeer.GetNodeId())
	if node == nil {
		err = fmt.Errorf("Node of peer [%d] does not exist.", peerId)
		return
	}
	peers = append(peers, newPeer)

	rngCopy.Id = id
	rngCopy.Peers = peers
	rngCopy.RangeEpoch.ConfVer = uint64(1)
	rngCopy.RangeEpoch.Version = uint64(1)

	if err = c.storeReplaceRange(r.Range, rngCopy, toGcPeer); err != nil {
		return
	}
	c.ReplaceRange(r.Range, NewRange(rngCopy, nil), toGcPeer)

	if err = c.ReplaceRangeRemote(node.GetServerAddr(), r.GetId(), rngCopy); err != nil {
		err = fmt.Errorf("create range[%v]failed, err[%v]", r, err)
		return
	}
	log.Info("range[%v -> %v] recreate with peerId [%v] on nodeId[%v]",
		r.GetId(), rngCopy.GetId(), newPeer.GetId(), newPeer.GetNodeId())
	return
}

func (c *Cluster) updateRangePeerRemote(r *Range, peerId uint64) error {
	rngCopy := deepcopy.Iface(r.Range).(*metapb.Range)
	var peer *metapb.Peer
	var peerUnable []*metapb.Peer
	for _, p := range rngCopy.GetPeers() {
		if peerId == p.GetId() {
			peer = p
		} else {
			peerUnable = append(peerUnable, p)
		}
	}
	if peer == nil {
		return fmt.Errorf("appointed peer [%d] is not exists in range [%d].", peerId, r.GetId())
	}

	node := c.FindNodeById(peer.GetNodeId())
	if node == nil {
		return fmt.Errorf("Node of peer [%d] does not exist.", peerId)
	}

	//避免删掉失败, 先记录【boltstore】
	rngCopy.Peers = peerUnable
	if err := c.storeRangeGC(rngCopy); err != nil {
		log.Error("store range pre gc failed, err[%v]", err)
		return fmt.Errorf("range store pre gc failed")
	}
	c.preGCRanges.Add(rngCopy)

	var peers []*metapb.Peer
	peers = append(peers, peer)
	rngCopy.Peers = peers
	rngCopy.RangeEpoch = &metapb.RangeEpoch{
		ConfVer: rngCopy.RangeEpoch.GetConfVer() + 10,
		Version: rngCopy.RangeEpoch.GetVersion() + 10}

	return c.UpdateRangeRemote(node.GetServerAddr(), rngCopy)
}

func (c *Cluster) UpdateRangeEpochRemote(r *Range, epoch *metapb.RangeEpoch) error {
	rngCopy := deepcopy.Iface(r.Range).(*metapb.Range)
	node := c.FindNodeById(r.GetLeader().GetNodeId())
	if node == nil {
		return fmt.Errorf("Node of peer [%d] does not exist.", r.GetLeader().GetNodeId())
	}

	rngCopy.RangeEpoch = &metapb.RangeEpoch{
		ConfVer: epoch.GetConfVer(),
		Version: epoch.GetVersion(),
	}

	return c.UpdateRangeRemote(node.GetServerAddr(), rngCopy)
}

func (c *Cluster) UpdateRangeRemote(addr string, r *metapb.Range) error {
	var err error
	for i := 0; i < 3; i++ {
		err = c.cli.UpdateRange(addr, r)
		if err != nil {
			log.Warn("update range meta: range[%v] node[%s] failed, err[%v]", r.GetId(), addr, err)
		} else {
			log.Debug("update range meta: range[%v] node[%s] success", r.GetId(), addr)
			break
		}
	}
	return err
}

func (c *Cluster) offlineRangeRemote(r *Range, peerId uint64) error {
	var peer *metapb.Peer
	for _, p := range r.GetPeers() {
		if peerId == p.GetId() {
			peer = p
			break
		}
	}
	if peer == nil {
		return ErrNotExistPeer
	}

	node := c.FindNodeById(peer.GetNodeId())
	if node == nil {
		return fmt.Errorf("Node of peer [%d] does not exist.", peerId)
	}

	err := c.cli.OffLineRange(node.GetServerAddr(), r.GetId())
	if err != nil {
		log.Warn("offline range: range[%v] peer[%v] node[%s] failed, err[%v]", r.GetId(), peerId, node.GetServerAddr(), err)
	}
	return err
}

func (c *Cluster) ReplaceRangeRemote(addr string, oldRangeId uint64, newRange *metapb.Range) error {
	var err error
	for i := 0; i < 3; i++ {
		err = c.cli.ReplaceRange(addr, oldRangeId, newRange)
		if err != nil {
			log.Warn("replace range meta: oldRangeId[%v]， newRangeId[%v] node[%s] failed, err[%v]", oldRangeId, newRange.GetId(), addr, err)
		} else {
			log.Debug("replace range meta: range[%v] node[%s] success", oldRangeId, newRange.GetId(), addr)
			break
		}
	}
	return err
}
