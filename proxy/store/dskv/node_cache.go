package dskv

import (
	"sync"
	"fmt"
	"model/pkg/metapb"
	"pkg-go/ms_client"
)

type NodeCache struct {
	cli       client.Client
	lock      sync.RWMutex
	nodeIs    map[uint64]*metapb.Node
}

func NewNodeCache(cli client.Client) *NodeCache {
	return &NodeCache{
		cli: cli,
		nodeIs: make(map[uint64]*metapb.Node)}
}

func (nc *NodeCache) locateNode(nodeId uint64) (*metapb.Node, bool) {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	if n, find := nc.nodeIs[nodeId]; find {
		return n, true
	}
	return nil, false
}

func (nc *NodeCache) loadNode(bo *Backoffer, nodeId uint64) (node *metapb.Node, err error) {
	for {
		if err != nil {
			err = bo.Backoff(BoCacheLoad, err)
			if err != nil {
				return nil, fmt.Errorf("load node %d failed, err %v", nodeId, err)
			}
		}
		node, err = nc.cli.GetNode(nodeId)
		if err != nil {
			err = fmt.Errorf("load node %d failed, err %v", err)
			continue
		}
		err = nil
		break
	}
	return
}

func (nc *NodeCache) GetNode(bo *Backoffer, nodeId uint64) (node *metapb.Node, err error) {
	var find bool
	if node, find = nc.locateNode(nodeId); !find {
		// TODO load node may spend a lot of time, so we do not use lock for concurrency control
		node, err = nc.loadNode(bo, nodeId)
		if err != nil {
			return
		}
		nc.lock.Lock()
		defer nc.lock.Unlock()
		nc.nodeIs[nodeId] = node
	}
	return
}

func (nc *NodeCache) DeleteNode(nodeId uint64) {
	nc.lock.Lock()
	defer nc.lock.Unlock()
	delete(nc.nodeIs, nodeId)
}
