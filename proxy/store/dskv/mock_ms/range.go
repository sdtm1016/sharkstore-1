package mock_ms

import (
	"fmt"
	"sync"

	"model/pkg/metapb"
	"model/pkg/mspb"
)

type Range struct {
	lock sync.RWMutex
	*metapb.Range
	Leader      *metapb.Peer
	PeersStatus []*mspb.PeerStatus

	State metapb.RangeState
}

func NewRange(r *metapb.Range) *Range {
	region := &Range{
		Range: r,
	}
	return region
}

func (r *Range) SString() string {
	return fmt.Sprintf("%d:%d", r.GetTableId(), r.GetId())
}

func (r *Range) ID() uint64 {
	return r.GetId()
}

func (r *Range) GetLeader() *metapb.Peer {
	if r.Leader == nil {
		return nil
	}
	return r.Leader
}

func (r *Range) GetPeer(nodeId uint64) *metapb.Peer {
	for _, p := range r.Peers {
		if p.GetNodeId() == nodeId {
			return p
		}
	}
	return nil
}

func (r *Range) GetPeerById(peerId uint64) *metapb.Peer {
	for _, p := range r.Peers {
		if p.Id == peerId {
			return p
		}
	}
	return nil
}
