package server

import (
	"fmt"
	client "pkg-go/ds_client"
	"time"

	"model/pkg/metapb"
	"model/pkg/taskpb"
	"util/deepcopy"
	"util/log"
)

const (
	defaultAddPeerTaskTimeout  = time.Second * time.Duration(300)
	addPeerMaxCreateRetryTimes = 8 // 8 heartbeats =  8 * 10 seconds
)

// AddPeerTask add peer task
type AddPeerTask struct {
	*BaseTask
	peer *metapb.Peer // peer to add

	confRetries   int // TODO: limit max retry
	createRetries int // TODO: limit max retry
}

// NewAddPeerTask new add peer task
func NewAddPeerTask() *AddPeerTask {
	return &AddPeerTask{
		BaseTask: newBaseTask(TaskTypeAddPeer, defaultAddPeerTaskTimeout),
	}
}

func (t *AddPeerTask) String() string {
	return fmt.Sprintf("{%s, \"to_add\":\"%s\"}", t.BaseTask.String(), t.peer.String())
}

// Step step
func (t *AddPeerTask) Step(cluster *Cluster, r *Range) (over bool, task *taskpb.Task) {
	switch t.GetState() {
	case TaskStateStart:
		task = t.stepStart(cluster, r)
		return false, task
	case WaitRaftConfReady:
		task = t.stepWaitConf(cluster, r)
		return false, task
	case WaitRangeCreated:
		return t.stepCreateRange(cluster, r), nil
	case WaitDataSynced:
		return t.stepWaitSync(r), nil
	default:
		log.Error("%s unexpceted add peer task state: %s", t.logID, t.state.String())
	}
	return
}

func (t *AddPeerTask) issueTask() *taskpb.Task {
	return &taskpb.Task{
		Type: taskpb.TaskType_RangeAddPeer,
		RangeAddPeer: &taskpb.TaskRangeAddPeer{
			Peer: t.peer,
		},
	}
}

func (t *AddPeerTask) stepStart(cluster *Cluster, r *Range) (task *taskpb.Task) {
	// not alloc new peer yet
	if t.peer == nil {
		var err error
		t.peer, err = cluster.allocPeerAndSelectNode(r, true)
		if err != nil {
			log.Error("%s alloc peer failed: %s", t.logID, err.Error())
			return nil
		}
	}

	t.state = WaitRaftConfReady

	// return a task to add this peer into raft member
	return t.issueTask()
}

func (t *AddPeerTask) stepWaitConf(cluster *Cluster, r *Range) (task *taskpb.Task) {
	if r.GetPeer(t.peer.GetId()) == nil {
		t.confRetries++

		return t.issueTask()
	}

	log.Info("%s add raft member finsihed, peer: %v.", t.logID, t.peer)

	t.state = WaitRangeCreated

	t.stepCreateRange(cluster, r)
	return nil
}

func prepareAddPeer(cluster *Cluster, r *Range, peer *metapb.Peer) error {
	rng := deepcopy.Iface(r.Range).(*metapb.Range)
	nodeID := peer.GetNodeId()
	node := cluster.FindNodeById(nodeID)
	if node == nil || !node.IsLogin() {
		return ErrRangeStatusErr
	}
	// 即使创建失败，raft leader添加成员
	retry := 3
	var err error
	for i := 0; i < retry; i++ {
		err = cluster.cli.CreateRange(node.GetServerAddr(), rng)
		if err == nil {
			break
		}
		// 链路问题
		if _, ok := err.(*client.RpcError); ok {
			break
		}
		if i+1 < retry {
			time.Sleep(time.Millisecond * time.Duration(10*(i+1)))
		}
	}
	return err
}

// return true if task is over
func (t *AddPeerTask) stepCreateRange(cluster *Cluster, r *Range) bool {
	err := prepareAddPeer(cluster, r, t.peer)
	if err != nil {
		log.Error("%s create new range failed, peer[id:%d, node:%d], retries: %d, err: %v",
			t.logID, t.peer.GetId(), t.peer.GetNodeId(), t.createRetries, err.Error())

		t.createRetries++
		if t.createRetries >= addPeerMaxCreateRetryTimes {
			log.Error("%s create new range max retry limited. task abort, peer[id:%d, node:%d]",
				t.logID, t.peer.GetId(), t.peer.GetNodeId())
			t.state = TaskStateFailed
			return true
		}
		return false
	}

	log.Info("%s create range finshed to node[%d]", t.logID, t.peer.GetNodeId())

	t.state = WaitDataSynced
	return false
}

func (t *AddPeerTask) getProgressInfo(r *Range) string {
	var leaderLogIndex uint64
	leader := r.GetLeader()
	if leader != nil {
		status := r.GetStatus(leader.GetId())
		if status != nil {
			leaderLogIndex = status.GetIndex()
		}
	}

	var peerLogIndex uint64
	var snapshotting bool
	status := r.GetStatus(t.peer.GetId())
	if status != nil {
		peerLogIndex = status.GetIndex()
		snapshotting = status.GetSnapshotting()
	}

	return fmt.Sprintf("[leader:%d, peer:%d, snaping:%v]", leaderLogIndex, peerLogIndex, snapshotting)
}

func (t *AddPeerTask) stepWaitSync(r *Range) bool {

	log.Debug("step range: %v", r.GetPeers())

	peer := r.GetPeer(t.peer.GetId())
	if peer == nil {
		log.Error("%s could not find target peer(%v) when check data sync", t.logID, t.peer)
		return false
	}

	log.Info("%s added peer[id:%d, node:%d] current type: %v, status: %s",
		t.logID, t.peer.GetId(), t.peer.GetNodeId(), peer.Type, t.getProgressInfo(r))

	if peer.Type == metapb.PeerType_PeerType_Learner {
		return false
	}

	log.Info("%s data sync finished, peer: %v", t.logID, t.peer)

	t.state = TaskStateFinished
	return true
}
