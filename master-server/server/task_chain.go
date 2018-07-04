package server

import (
	"fmt"
	"model/pkg/metapb"
	"sync/atomic"
	"time"

	"model/pkg/taskpb"
	"util/log"
)

// TaskChain is a list of associated tasks to achive the same goal
type TaskChain struct {
	id      uint64
	rangeID uint64
	name    string

	tasks  []Task // all tasks
	curIdx int    // current running task idx

	begin      time.Time
	lastUpdate time.Time
	running    uint64
	logID      string
}

// NewTaskChain new taskchain
func NewTaskChain(id uint64, rangeID uint64, name string, tasks ...Task) *TaskChain {
	c := &TaskChain{
		id:         id,
		rangeID:    rangeID,
		name:       name,
		tasks:      append([]Task(nil), tasks...),
		curIdx:     0,
		begin:      time.Now(),
		lastUpdate: time.Now(),
		running:    0,
		logID:      fmt.Sprintf("Task[%d:R%d/%s]", id, rangeID, name),
	}

	for _, t := range c.tasks {
		t.SetBegin()
		t.SetLogID(c.logID)
	}

	return c
}

// GetID return taskchain's id
func (c *TaskChain) GetID() uint64 {
	return c.id
}

// GetRangeID return range id
func (c *TaskChain) GetRangeID() uint64 {
	return c.rangeID
}

// GetName return name
func (c *TaskChain) GetName() string {
	return c.name
}

// GetLogID return log id to print log
func (c *TaskChain) GetLogID() string {
	return c.logID
}

// Elapsed time since begin
func (c *TaskChain) Elapsed() time.Duration {
	return time.Since(c.begin)
}

// Next run next step
func (c *TaskChain) Next(cluster *Cluster, r *Range) (over bool, task *taskpb.Task) {
	// only one goroutine can execute it at the same time
	if !atomic.CompareAndSwapUint64(&c.running, 0, 1) {
		log.Warn("%s failed: already running", c.logID)
		return false, nil
	}
	defer atomic.StoreUint64(&c.running, 0)

	c.lastUpdate = time.Now()

	for {
		if c.curIdx >= len(c.tasks) {
			return true, nil
		}

		t := c.tasks[c.curIdx]
		over = t.CheckOver()
		if !over {
			over, task = t.Step(cluster, r)
		}

		// not over, return and wait next Next
		if !over {
			return false, task
		}

		// current task is over but it doesn't finished successfully. so failed at this point
		if t.GetState() != TaskStateFinished {
			if !t.AllowFail() {
				log.Error("%s run %s task failed at %s, detail: %s",
					c.logID, t.GetType().String(), t.GetState().String(), t.String())
				return true, nil
			}
			log.Warn("%s skip %s task failed at %s, detail: %s",
				c.logID, t.GetType().String(), t.GetState().String(), t.String())
		}

		// current task finished successfully and current is the last one
		if c.curIdx == len(c.tasks)-1 {
			log.Info("%s finished. used: %v, last task: %s", c.logID, c.Elapsed(), t.String())
			return true, nil
		}

		// current task finished successfully but there is other task leftover, switch to run next task
		c.curIdx++
		// reset next task's begin time
		c.tasks[c.curIdx].SetBegin()
		log.Info("%s %s task finsished. start next task: %s", c.logID, t.GetType().String(), c.tasks[c.curIdx].String())
	}
}

func (c *TaskChain) String() string {
	return fmt.Sprintf("{\"id\": %d, \"name\": \"%s\", \"range\": %d, \"index\": %d, \"begin\": \"%s\", \"update\": \"%v\", \"tasks\": %v}",
		c.id, c.name, c.rangeID, c.curIdx, c.begin.Format("2006/01/02-03:04:05"),
		c.lastUpdate.Format("2006/01/02-03:04:05"), c.tasks)
}

// NewTransferPeerTasks new transfer peer tasks
func NewTransferPeerTasks(id uint64, r *Range, name string, from *metapb.Peer) *TaskChain {
	addPeerTask := NewAddPeerTask()
	delPeerTask := NewDeletePeerTask(from)

	if r.GetLeader() != nil && r.GetLeader().GetId() == from.GetId() {
		changeLeaderTask := NewChangeLeaderTask(r.GetLeader().GetNodeId(), 0)
		changeLeaderTask.SetAllowFail()
		return NewTaskChain(id, r.GetId(), name, addPeerTask, changeLeaderTask, delPeerTask)
	}
	return NewTaskChain(id, r.GetId(), name, addPeerTask, delPeerTask)
}

// NewDeletePeerTasks new delete peer tasks
func NewDeletePeerTasks(id uint64, r *Range, name string, peer *metapb.Peer) *TaskChain {
	delPeerTask := NewDeletePeerTask(peer)
	if r.GetLeader() != nil && r.GetLeader().GetId() == peer.GetId() {
		changeLeaderTask := NewChangeLeaderTask(r.GetLeader().GetNodeId(), 0)
		changeLeaderTask.SetAllowFail()
		return NewTaskChain(id, r.GetId(), name, changeLeaderTask, delPeerTask)
	}
	return NewTaskChain(id, r.GetId(), name, delPeerTask)
}
