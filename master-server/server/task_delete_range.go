package server

import (
	"fmt"
	"model/pkg/taskpb"
	"time"
	"util/log"
)

const (
	defaultDeleteRangeTaskTimeout = time.Second * time.Duration(30)
)

// DeleteRangeTask  delete range task
type DeleteRangeTask struct {
	*BaseTask
}

// NewDeleteRangeTask new delete range task
func NewDeleteRangeTask() *DeleteRangeTask {
	return &DeleteRangeTask{
		BaseTask: newBaseTask(TaskTypeDeleteRange, defaultDeleteRangeTaskTimeout),
	}
}

func (t *DeleteRangeTask) String() string {
	return fmt.Sprintf("{%s}", t.BaseTask.String())
}

// Step step
func (t *DeleteRangeTask) Step(cluster *Cluster, r *Range) (over bool, task *taskpb.Task) {
	switch t.GetState() {
	case TaskStateStart:
		for _, peer := range r.Peers {
			node := cluster.FindNodeById(peer.GetNodeId())
			//TODO:可能对堵塞时间比较长
			err := cluster.cli.DeleteRange(node.GetServerAddr(), r.GetId())
			if err != nil {
				log.Error("%s delete range to node %d err, wait for gc, error: %v", t.logID, peer.GetNodeId(), err)
				peerGC(cluster, r.Range, peer)
			}
		}
		t.state = TaskStateFinished
		return true, nil
	default:
		log.Error("%s unexpceted add peer task state: %s", t.logID, t.state.String())
	}
	return
}
