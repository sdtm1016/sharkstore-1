// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package dskv

import (
	"bytes"
	"sync"
	"fmt"
	"errors"

	"model/pkg/metapb"
	"pkg-go/ms_client"
	"util/log"

	"github.com/petar/GoLLRB/llrb"
)

// RegionCache caches Regions loaded from PD.
type RangeCache struct {
	dbId     uint64
	tableId  uint64
	msClient client.Client
	mu       struct {
		         sync.RWMutex
		         regions map[RangeVerID]*Range
		         sorted  *llrb.LLRB
	         }
	nodeCache *NodeCache
}

// NewRegionCache creates a RegionCache.
func NewRangeCache(dbId, tableId uint64, msClient client.Client, nodeCache *NodeCache) *RangeCache {
	c := &RangeCache{
		dbId:      dbId,
		tableId:   tableId,
		msClient:  msClient,
		nodeCache: nodeCache,
	}
	c.mu.regions = make(map[RangeVerID]*Range)
	c.mu.sorted = llrb.New()
	return c
}

// KeyLocation is the region and range that a key is located.
type KeyLocation struct {
	Region   RangeVerID
	StartKey []byte
	EndKey   []byte
	NodeId   uint64
}

// Contains checks if key is in [StartKey, EndKey).
func (l *KeyLocation) Contains(key []byte) bool {
	return bytes.Compare(l.StartKey, key) <= 0 &&
		(bytes.Compare(key, l.EndKey) < 0 || len(l.EndKey) == 0)
}

func (c *RangeCache) locateKey(key []byte) (*KeyLocation, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if r := c.getRegionFromCache(key); r != nil {
		loc := &KeyLocation{
			Region:   r.VerID(),
			StartKey: r.StartKey(),
			EndKey:   r.EndKey(),
			NodeId:   r.Leader().GetNodeId(),
		}
		return loc, true
	}
	return nil, false
}

// LocateKey searches for the region and range that the key is located.
func (c *RangeCache) LocateKey(bo *Backoffer, key []byte) (*KeyLocation, error) {
	if l, find := c.locateKey(key); find {
		return l, nil
	}
	rs, err := c.loadRegion(bo, key)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	for _, _r := range rs {
		_r = c.insertRegionToCache(_r)
	}
	c.mu.Unlock()
	r := rs[0]
	log.Debug("load range key:%v,range:%d [%v-%v] leader:%s",key,r.GetID(),r.StartKey(),r.EndKey(),r.Leader().String())
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
		NodeId:   r.Leader().GetNodeId(),
	}, nil
}

// GroupKeysByRegion separates keys into groups by their belonging Regions.
// Specially it also returns the first key's region which may be used as the
// 'PrimaryLockKey' and should be committed ahead of others.
func (c *RangeCache) GroupKeysByRegion(bo *Backoffer, keys [][]byte) (map[RangeVerID][][]byte, RangeVerID, error) {
	groups := make(map[RangeVerID][][]byte)
	var first RangeVerID
	var lastLoc *KeyLocation
	for i, k := range keys {
		if lastLoc == nil || !lastLoc.Contains(k) {
			var err error
			lastLoc, err = c.LocateKey(bo, k)
			if err != nil {
				return nil, first, err
			}
		}
		id := lastLoc.Region
		if i == 0 {
			first = id
		}
		groups[id] = append(groups[id], k)
	}
	return groups, first, nil
}

// ListRegionIDsInKeyRange lists ids of regions in [start_key,end_key].
func (c *RangeCache) ListRegionIDsInKeyRange(bo *Backoffer, startKey, endKey []byte) (regionIDs []uint64, err error) {
	for {
		curRegion, err := c.LocateKey(bo, startKey)
		if err != nil {
			return nil, err
		}
		regionIDs = append(regionIDs, curRegion.Region.Id)
		if curRegion.Contains(endKey) {
			break
		}
		startKey = curRegion.EndKey
	}
	return regionIDs, nil
}

// DropRegion removes a cached Region.
func (c *RangeCache) DropRegion(id RangeVerID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.dropRegionFromCache(id)
}

// UpdateLeader update some region cache with newer leader info.
func (c *RangeCache) UpdateLeader(regionID RangeVerID, leaderNodeID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	r, ok := c.mu.regions[regionID]
	if !ok {
		log.Debug("regionCache: cannot find region when updating leader %d,%d", regionID, leaderNodeID)
		return
	}

	if !r.SwitchPeer(leaderNodeID) {
		log.Debug("regionCache: cannot find peer when updating leader %d,%d", regionID, leaderNodeID)
		c.dropRegionFromCache(r.VerID())
	}
}

func (c *RangeCache) GetNodeAddr(bo *Backoffer, nodeId uint64) (string, error) {
	if nodeId == 0 {
		// for bug of DS
		return "", ErrInvalidNode
	}
	if node, err := c.nodeCache.GetNode(bo, nodeId); err != nil {
		return "", err
	} else {
		return node.GetServerAddr(), nil
	}
}

func (c *RangeCache) getRegionFromCache(key []byte) *Range {
	var r *Range
	c.mu.sorted.DescendLessOrEqual(newRBSearchItem(key), func(item llrb.Item) bool {
		r = item.(*llrbItem).region
		return false
	})
	if r != nil && r.Contains(key) {
		return r
	}
	return nil
}

// insertRegionToCache tries to insert the Region to cache. If there is an old
// Region with the same VerID, it will return the old one instead.
func (c *RangeCache) insertRegionToCache(r *Range) *Range {
	if old, ok := c.mu.regions[r.VerID()]; ok {
		return old
	}
	old := c.mu.sorted.ReplaceOrInsert(newRBItem(r))
	if old != nil {
		delete(c.mu.regions, old.(*llrbItem).region.VerID())
	}
	c.mu.regions[r.VerID()] = r
	return r
}

// getRegionByIDFromCache tries to get region by regionID from cache
func (c *RangeCache) getRegionByIDFromCache(regionID uint64) *Range {
	for v, r := range c.mu.regions {
		if v.Id == regionID {
			return r
		}
	}
	return nil
}

func (c *RangeCache) dropRegionFromCache(verID RangeVerID) {
	r, ok := c.mu.regions[verID]
	if !ok {
		return
	}
	c.mu.sorted.Delete(newRBItem(r))
	delete(c.mu.regions, r.VerID())
}

// loadRegion loads region from pd client, and picks the first peer as leader.
func (c *RangeCache) loadRegion(bo *Backoffer, key []byte) ([]*Range, error) {
	var ranges []*Range
	var rs []*metapb.Route
	var err error
	for {
		if err != nil {
			err = bo.Backoff(BoCacheLoad, err)
			if err != nil {
				return nil, err
			}
		}
		rs, err = c.msClient.GetRoute(c.dbId, c.tableId, key)
		if err != nil {
			err = fmt.Errorf("loadRanges from MS failed, key: %q, err: %v", key, err)
			continue
		}
		if len(rs) == 0 {
			err = fmt.Errorf("range not found for key %q", key)
			continue
		}
		for _, r := range rs {
			if len(r.GetRange().GetPeers()) == 0 {
				return nil, errors.New("receive Range with no peer")
			}
		}

		err = nil
		for _, r := range rs {
			region := &Range{
				meta: r.Range,
				peer: r.Range.Peers[0],
				peerStatus: r.PeerStatus,
			}
			if r.Leader != nil {
				region.SwitchPeer(r.Leader.GetNodeId())
			}
			ranges = append(ranges, region)
		}
		break
	}
	return ranges, err
}

// OnRequestFail is used for clearing cache when a ds server does not respond.
func (c *RangeCache) OnRequestFail(regionID RangeVerID, nodeId uint64, err error) {
	// Switch region's leader peer to next one.
	c.mu.Lock()
	if region, ok := c.mu.regions[regionID]; ok {
		if !region.OnRequestFail(nodeId) {
			c.dropRegionFromCache(regionID)
		}
	}
	c.mu.Unlock()

	// Store's meta may be out of date.
	c.nodeCache.DeleteNode(nodeId)
	//log.Infof("drop regions of store %d from cache due to request fail, err: %v", storeID, err)

	c.mu.Lock()
	for id, r := range c.mu.regions {
		if r.peer.GetNodeId() == nodeId {
			c.dropRegionFromCache(id)
		}
	}
	c.mu.Unlock()
}

// OnRegionStale removes the old region and inserts new regions into the cache.
func (c *RangeCache) OnRegionStale(ctx *Context, newRegions []*metapb.Range) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.dropRegionFromCache(ctx.VID)

	for _, meta := range newRegions {
		region := &Range{
			meta: meta,
			peer: meta.Peers[0],
		}
		// TODO new range leader In most cases, it is the same as the original.
		region.SwitchPeer(ctx.NodeId)
		log.Info("regionCache add range[%v-%v] %d ",region.StartKey(),region.EndKey(),region.GetID())
		c.insertRegionToCache(region)
	}
	return nil
}

// PDClient returns the pd.Client in RegionCache.
func (c *RangeCache) MsClient() client.Client {
	return c.msClient
}

// moveLeaderToFirst moves the leader peer to the first and makes it easier to
// try the next peer if the current peer does not respond.
func moveLeaderToFirst(r *metapb.Range, leaderNodeID uint64) {
	for i := range r.Peers {
		if r.Peers[i].GetNodeId() == leaderNodeID {
			r.Peers[0], r.Peers[i] = r.Peers[i], r.Peers[0]
			return
		}
	}
}

// llrbItem is llrbTree's Item that uses []byte to compare.
type llrbItem struct {
	key    []byte
	region *Range
}

func newRBItem(r *Range) *llrbItem {
	return &llrbItem{
		key:    r.StartKey(),
		region: r,
	}
}

func newRBSearchItem(key []byte) *llrbItem {
	return &llrbItem{
		key: key,
	}
}

func (item *llrbItem) Less(other llrb.Item) bool {
	return bytes.Compare(item.key, other.(*llrbItem).key) < 0
}

// Region stores region's meta and its leader peer.
type Range struct {
	meta             *metapb.Range
	peer             *metapb.Peer
	peerStatus       []*metapb.PeerStatus
	unreachableNodes []uint64
}

// GetID returns id.
func (r *Range) GetID() uint64 {
	return r.meta.GetId()
}

// RegionVerID is a unique ID that can identify a Region at a specific version.
type RangeVerID struct {
	Id      uint64
	ConfVer uint64
	Cer     uint64
}

// VerID returns the Region's RegionVerID.
func (r *Range) VerID() RangeVerID {
	return RangeVerID{
		Id:      r.meta.GetId(),
		ConfVer: r.meta.GetRangeEpoch().GetConfVer(),
		Cer:     r.meta.GetRangeEpoch().GetVersion(),
	}
}

// StartKey returns StartKey.
func (r *Range) StartKey() []byte {
	return r.meta.StartKey
}

// EndKey returns EndKey.
func (r *Range) EndKey() []byte {
	return r.meta.EndKey
}

func (r *Range) Leader() *metapb.Peer {
	return r.peer
}

// OnRequestFail records unreachable peer and tries to select another valid peer.
// It returns false if all peers are unreachable.
func (r *Range) OnRequestFail(nodeId uint64) bool {
	if r.peer.GetNodeId() != nodeId {
		return true
	}
	r.unreachableNodes = append(r.unreachableNodes, nodeId)
	L:
	for _, p := range r.meta.Peers {
		for _, id := range r.unreachableNodes {
			if p.GetNodeId() == id {
				continue L
			}
		}
		r.peer = p
		return true
	}
	return false
}

// SwitchPeer switches current peer to the one on specific store. It returns
// false if no peer matches the storeID.
func (r *Range) SwitchPeer(nodeId uint64) bool {
	for _, p := range r.meta.Peers {
		if p.GetNodeId() == nodeId {
			r.peer = p
			return true
		}
	}
	return false
}

// Contains checks whether the key is in the region, for the maximum region endKey is empty.
// startKey <= key < endKey.
func (r *Range) Contains(key []byte) bool {
	return bytes.Compare(r.meta.GetStartKey(), key) <= 0 &&
		(bytes.Compare(key, r.meta.GetEndKey()) < 0 || len(r.meta.GetEndKey()) == 0)
}

