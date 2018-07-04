package server

import (
	"sync"
	"time"
	"fmt"
	"sync/atomic"

	"model/pkg/metapb"
	"golang.org/x/net/context"
	"github.com/juju/errors"
	"model/pkg/mspb"
	"util/log"
)

var (
	DefaultFaultTimeout = time.Minute
	DefaultMaxBigTaskNum  = 3
	DefaultMaxTaskNum = 50
)

// TODO 机器不同导致的分片数量
type Node struct {
	*metapb.Node
    // metapb.Node属性锁
	lock          sync.RWMutex

	ranges        *RegionCache
	blocked       bool
	stats         *mspb.NodeStats
	LastHeartbeatTS time.Time
	Trace         bool
	opsStat		  *NodeOpsStat
	// 创建/删除range副本失败产生的垃圾副本,n/2以上的节点挂掉、手动处理不可用的副本
	// 远程GC DS上的垃圾range副本
	trashReplicas    *ReplicaCache
}

const CacheSize = 100
type NodeOpsStat struct {
	writeOps [CacheSize]uint64
	hit uint64
}

func (opsStat *NodeOpsStat) Hit(v uint64){
	hit := atomic.AddUint64(&(opsStat.hit),1)
	opsStat.writeOps[hit%CacheSize] = v
}

func (opsStat *NodeOpsStat) GetMax() uint64{
	var max uint64 = 0
	for i:=0 ; i<CacheSize ;i++ {
		v := opsStat.writeOps[i]
		if  v > max {
			max = v
		}
	}
	return max
}

func (opsStat *NodeOpsStat) Clear() uint64{
	var max uint64 = 0
	for i:=0 ; i<CacheSize ;i++ {
		opsStat.writeOps[i] = 0
	}
	return max
}

func NewNode(node *metapb.Node) *Node {
	return &Node{
		Node: node,
		stats: &mspb.NodeStats{},
		opsStat:&NodeOpsStat{},
		ranges: NewRegionCache(),
		trashReplicas: NewReplicaCache(),
		LastHeartbeatTS: time.Now(),
	}
}

func (n *Node) ID() uint64 {
	if n == nil {
		return 0
	}
	return n.GetId()
}

func (n *Node) AddRange(r *Range) {
	if n == nil {
		return
	}
	n.ranges.Add(r)
}

func (n *Node) DeleteRange(rangeId uint64) {
	if n == nil {
		return
	}
	n.ranges.Delete(rangeId)
}

func (n *Node) GetRange(id uint64) (*Range, bool) {
	if n == nil {
		return nil, false
	}
	return n.ranges.FindRangeByID(id)
}

func (n *Node) GetAllRanges() ([]*Range) {
	if n == nil {
		return nil
	}
	return n.ranges.GetAllRange()
}

func (n *Node) GetAllTrashReplicas() ([]*metapb.Replica) {
	if n == nil {
		return nil
	}
	return n.trashReplicas.GetAllReplica()
}

func (n *Node) GetAllTrashRangeIds() ([]uint64) {
	if n == nil {
		return nil
	}
	return n.trashReplicas.GetAllRangIds()
}

func (n *Node) AddTrashReplica(peer *metapb.Replica) {
	if n == nil {
		return
	}
	n.trashReplicas.Add(peer)
	return
}

func (n *Node) DeleteTrashReplica(id uint64) {
	if n == nil {
		return
	}
	n.trashReplicas.Delete(id)
	return
}

func (n *Node) GetRangesSize() (int) {
	if n == nil {
		return 0
	}
	return n.ranges.Size()
}

func (n *Node) GetRangesCount() uint32 {
	if n == nil {
		return 0
	}
	return n.stats.GetRangeCount()
}

func (n *Node) GetLeaderCount() uint32 {
	if n == nil {
		return 0
	}
	return n.stats.GetRangeLeaderCount()
}

func (n *Node)IsFault() bool {
	if n == nil {
		return false
	}
	return n.State == metapb.NodeState_N_Tombstone ||
		n.State == metapb.NodeState_N_Logout
}

func (n *Node)IsLogout() bool {
	if n == nil {
		return false
	}
	return n.State == metapb.NodeState_N_Logout
}

func (n *Node) IsLogin() bool {
	if n == nil {
		return false
	}
	return n.State == metapb.NodeState_N_Login
}

func (n *Node) IsBusy() bool {
	if n == nil {
		return false
	}
	return n.stats.GetIsBusy()
}

func (n *Node) UpdateState(state metapb.NodeState) {
	if n == nil {
		return
	}
	n.lock.Lock()
	//oldState := n.State
	n.State = state
	n.lock.Unlock()

	//log.Info("Node state had changed [%v]===>[%v]", oldState, state)
}

func (n *Node) downTime() time.Duration {
	if n == nil {
		return 0
	}
	return time.Since(n.LastHeartbeatTS)
}

func (n *Node) block() {
	if n == nil {
		return
	}
	n.blocked = true
}

func (n *Node) unblock() {
	if n == nil {
		return
	}
	n.blocked = false
}

func (n *Node) isBlocked() bool {
	if n == nil {
		return false
	}
	return n.blocked
}

func (n *Node) isUp() bool {
	if n == nil {
		return false
	}
	return n.GetState() == metapb.NodeState_N_Login
}

func (n *Node) isOffline() bool {
	if n == nil {
		return false
	}
	return n.GetState() == metapb.NodeState_N_Offline
}

func (n *Node) isTombstone() bool {
	if n == nil {
		return false
	}
	return n.GetState() == metapb.NodeState_N_Tombstone
}

func (n *Node) isDown() bool {
	if n == nil {
		return false
	}
	return n.GetState() == metapb.NodeState_N_Logout
}


func (n *Node) leaderCount() uint64 {
	if n == nil {
		return 0
	}
	return uint64(n.stats.GetRangeLeaderCount())
}

func (n *Node) leaderScore() float64 {
	if n == nil {
		return 0.0
	}
	return float64(n.stats.GetRangeLeaderCount())
}

func (n *Node) storageSize() uint64 {
	if n == nil {
		return 0
	}
	return n.stats.GetUsedSize()
}

func (n *Node) availableRatio() float64 {
	if n == nil {
		return 0.0
	}
	available := n.stats.GetAvailable()
	capacity := n.stats.GetCapacity()
	if capacity == 0 {
		return 0
	}
	return float64(available) / float64(capacity)
}

func (n *Node) GetSendingSnapCount() uint32 {
	if n == nil {
		return 0
	}
	return n.stats.GetSendingSnapCount()
}

func (n *Node) GetReceivingSnapCount() uint32 {
	if n == nil {
		return 0
	}
	return n.stats.GetReceivingSnapCount()
}

func (n *Node) GetApplyingSnapCount() uint32 {
	if n == nil {
		return 0
	}
	return n.stats.GetApplyingSnapCount()
}

// GetStartTS returns the start timestamp.
func (n *Node) GetStartTS() time.Time {
	if n == nil {
		return time.Time{}
	}

	return time.Unix(int64(n.stats.GetStart()), 0)
}

// GetUptime returns the uptime.
func (n *Node) GetUptime() time.Duration {
	if n == nil {
		return 0
	}
	uptime := n.LastHeartbeatTS.Sub(n.GetStartTS())
	if uptime > 0 {
		return uptime
	}
	return 0
}

const defaultStoreDownTime = time.Minute

// IsDown returns whether the store is down
func (n *Node) IsDown() bool {
	if n == nil {
		return false
	}
	return time.Now().Sub(n.LastHeartbeatTS) > defaultStoreDownTime
}

func (n *Node) getLabelValue(key string) string {
	if n == nil {
		return ""
	}
	for _, label := range n.GetLabels() {
		if label.GetKey() == key {
			return label.GetValue()
		}
	}
	return ""
}

// compareLocation compares 2 stores' labels and returns at which level their
// locations are different. It returns -1 if they are at the same location.
func (n *Node) compareLocation(other *Node, labels []string) int {
	if n == nil {
		return -1
	}
	for i, key := range labels {
		v1, v2 := n.getLabelValue(key), other.getLabelValue(key)
		// If label is not set, the store is considered at the same location
		// with any other store.
		if v1 != "" && v2 != "" && v1 != v2 {
			return i
		}
	}
	return -1
}

func (n *Node) mergeLabels(labels []*metapb.NodeLabel) {
	if n == nil {
		return
	}
	L:
	for _, newLabel := range labels {
		for _, label := range n.Labels {
			if label.Key == newLabel.Key {
				label.Value = newLabel.Value
				continue L
			}
		}
		n.Labels = append(n.Labels, newLabel)
	}
}

func (n *Node) randFollowerRange() *Range {
	if n == nil {
		return nil
	}
	for _, r := range n.GetAllRanges() {
	    if r.GetLeader().GetNodeId() != n.GetId() {
		    return r
	    }
    }
	return nil
}

func (n *Node) randLeaderRange() *Range {
	if n == nil {
		return nil
	}
	for _, r := range n.GetAllRanges() {
		if r.GetLeader().GetNodeId() == n.GetId() {
			return r
		}
	}
	return nil
}

type NodeCache struct {
	lock      sync.RWMutex
	nodeIs    map[uint64]*Node
	nodeNs    map[string]*Node
}

func NewNodeCache() *NodeCache {
	return &NodeCache{nodeIs: make(map[uint64]*Node), nodeNs: make(map[string]*Node)}
}

func (nc *NodeCache) Add(n *Node) {
	nc.lock.Lock()
	defer nc.lock.Unlock()
	nc.nodeIs[n.ID()] = n
	nc.nodeNs[n.GetServerAddr()] = n
}

func (nc *NodeCache) DeleteById(id uint64) {
	nc.lock.Lock()
	defer nc.lock.Unlock()
	if n, find := nc.nodeIs[id]; find {
		delete(nc.nodeIs, id)
		delete(nc.nodeNs, n.GetServerAddr())
	}
}

func (nc *NodeCache) DeleteByAddr(addr string) {
	nc.lock.Lock()
	defer nc.lock.Unlock()
	if n, find := nc.nodeNs[addr]; find {
		delete(nc.nodeIs, n.GetId())
		delete(nc.nodeNs, n.GetServerAddr())
	}
}

func (nc *NodeCache) FindNodeById(id uint64) (*Node, bool) {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	if n, find := nc.nodeIs[id]; find {
		return n, true
	}
	return nil, false
}

func (nc *NodeCache) FindNodeByAddr(addr string) (*Node, bool) {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	if n, find := nc.nodeNs[addr]; find {
		return n, true
	}
	return nil, false
}

func (nc *NodeCache) GetAllActiveNode() []*Node {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	var nodes []*Node
	for _, n := range nc.nodeIs {
		if n.GetState() == metapb.NodeState_N_Login {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

func (nc *NodeCache) GetAllNode() []*Node {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	var nodes []*Node
	for _, n := range nc.nodeIs {
		nodes = append(nodes, n)
	}
	return nodes
}

func (nc *NodeCache) Size() int {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	return len(nc.nodeIs)
}

type TrashReplicaGCWorker struct {
	name             string
	ctx              context.Context
	cancel           context.CancelFunc
	interval         time.Duration
}

func NewTrashReplicaGCWorker(wm *WorkerManager, interval time.Duration) Worker {
	ctx, cancel := context.WithCancel(wm.ctx)
	return &TrashReplicaGCWorker{
		name:     trashReplicaGcWorkerName,
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
	}
}

func (tr *TrashReplicaGCWorker) GetName() string {
	return tr.name
}

func (tr *TrashReplicaGCWorker) Work(cluster *Cluster) {
	nodes := cluster.GetAllNode()
	for _, node := range nodes {
		select {
		case <-tr.ctx.Done():
			return
		default:
		}

		if node.IsLogout() {
			for _, rep := range node.GetAllTrashReplicas() {
				key := []byte(fmt.Sprintf("%s%d", PREFIX_REPLICA, rep.GetPeer().GetId()))
				err := cluster.store.Delete(key)
				if err != nil {
					return
				}
				log.Debug("delete trash replicas from logout node, peerId:[%v]", rep.GetPeer().GetId())
				node.DeleteTrashReplica(rep.GetPeer().GetId())
			}
			continue
		}
		if node.IsLogin() {
			for _, rep := range node.GetAllTrashReplicas() {
				select {
				case <-tr.ctx.Done():
					return
				default:
				}
				log.Debug("delete trash replicas from login node, rangeId:[%d]", rep.GetRangeId())
				err := cluster.cli.DeleteRange(node.GetServerAddr(), rep.GetRangeId())
				if err == nil {
					log.Debug("delete trash replicas from login node, rangeId:[%d], peerId:[%d]", rep.GetRangeId(), rep.GetPeer().GetId())
					key := []byte(fmt.Sprintf("%s%d", PREFIX_REPLICA, rep.GetPeer().GetId()))
					err = cluster.store.Delete(key)
					if err != nil {
						return
					}
					node.DeleteTrashReplica(rep.GetPeer().GetId())
				}
			}
		}
	}
	return
}

func (tr *TrashReplicaGCWorker) AllowWork(cluster *Cluster) bool {
	return true
}

func (tr *TrashReplicaGCWorker) GetInterval() time.Duration {
	return tr.interval
}

func (tr *TrashReplicaGCWorker) Stop() {
	tr.cancel()
}

type HbRingBuf struct {
	lock 	sync.RWMutex
	cap		uint32
	wPos    uint32
	buf		[]time.Time
}

func NewHbRingBuf(cap uint32) *HbRingBuf {
	if cap < 2 {
		return nil
	}
	return &HbRingBuf{
		cap:cap,
		wPos:0,
		buf:make([]time.Time, cap),
	}
}

func (rb *HbRingBuf)SetCurHbTime() {
	rb.lock.Lock()
	rb.buf[rb.wPos] = time.Now()
	rb.wPos++
	if rb.wPos == rb.cap {
		rb.wPos = 0
	}
	rb.lock.Unlock()
}

func (rb *HbRingBuf)GetLastHbTime() time.Time {
	rb.lock.RLock()
	var t time.Time
	if rb.wPos == 0 {
		t = rb.buf[rb.cap - 1]
	} else {
		t = rb.buf[rb.wPos - 1]
	}
	rb.lock.RUnlock()
	return t
}

func (rb *HbRingBuf)ResetHbRingBuf() {
	rb.lock.Lock()
	rb.wPos = 0
	rb.buf = make([]time.Time, rb.cap)
	rb.lock.Unlock()
}

func (rb *HbRingBuf)CalcMaxHbDiff() (time.Duration, error) {
	rb.lock.RLock()
	if rb.buf[rb.wPos].IsZero() {
		rb.lock.RUnlock()
		return time.Duration(0), errors.New(fmt.Sprintf("hbringbuf is not full, cap:[%d], remain:[%d]",
			rb.cap, rb.cap - rb.wPos))
	}
	var d time.Duration
	if rb.wPos == 0 {
		d = rb.buf[rb.cap - 1].Sub(rb.buf[rb.wPos])
	} else {
		d = rb.buf[rb.wPos - 1].Sub(rb.buf[rb.wPos])
	}
	rb.lock.RUnlock()
	return d, nil
}

type Distribution struct {
	nodeId uint64
	count int
}

type Distributions []Distribution

func (d Distributions) Len() int{
	return len(d)
}

func (d Distributions) Less(i, j int) bool {
	return d[i].count > d[j].count
}

func (d Distributions) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}