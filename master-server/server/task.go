package server

import (
	"fmt"
	"time"

	"model/pkg/taskpb"
)

// TaskType the task type
type TaskType int

const (
	// TaskTypeAddPeer add peer type
	TaskTypeAddPeer TaskType = iota + 1
	// TaskTypeDeletePeer delete peer
	TaskTypeDeletePeer
	// TaskTypeChangeLeader change leader
	TaskTypeChangeLeader
	// TaskTypeDeleteRange delete range
	TaskTypeDeleteRange
)

// String task type to string name
func (t TaskType) String() string {
	switch t {
	case TaskTypeAddPeer:
		return "add peer"
	case TaskTypeDeletePeer:
		return "delete peer"
	case TaskTypeChangeLeader:
		return "change leader"
	case TaskTypeDeleteRange:
		return "delete range"
	default:
		return "unknown"
	}
}

// TaskState task running state
type TaskState int

// common states, keep less than 100
const (
	// TaskStateStart start
	TaskStateStart TaskState = iota + 1
	// TaskStateFinished finished
	TaskStateFinished
	// TaskStateFailed  failed
	TaskStateFailed
	// TaskStateCanceled canceled
	TaskStateCanceled
	// TaskStateTimeout run timeout
	TaskStateTimeout
)

const (
	// WaitRaftConfReady   wait raft conf ready
	WaitRaftConfReady TaskState = iota + 100
	// WaitRangeCreated wait range created
	WaitRangeCreated
	// WaitDataSynced  wait data synced
	WaitDataSynced
	// WaitRangeDeleted wait range deleted
	WaitRangeDeleted
	// WaitLeaderChanged wait leader moved
	WaitLeaderChanged
)

// String to string name
// TODO: use a table-driven pattern
func (ts TaskState) String() string {
	switch ts {
	case TaskStateStart:
		return "start"
	case TaskStateFinished:
		return "finished"
	case TaskStateFailed:
		return "failed"
	case TaskStateCanceled:
		return "canceled"
	case TaskStateTimeout:
		return "timeout"
	case WaitRaftConfReady:
		return "wait raft conf ready"
	case WaitRangeCreated:
		return "wait range created"
	case WaitDataSynced:
		return "wait data synced"
	case WaitRangeDeleted:
		return "wait range deleted"
	case WaitLeaderChanged:
		return "wait leader changed"
	default:
		return "unknown"
	}
}

// Task range task interface
type Task interface {
	// SetBeginTime set begin time
	SetBegin()

	// SetLogID set a identifer to print log
	SetLogID(id string)

	// GetType return task type
	GetType() TaskType

	// Step next step
	Step(cluster *Cluster, r *Range) (over bool, task *taskpb.Task)

	// CheckOver return true if check is over
	CheckOver() bool

	// AllowFail allow to fail and continue next task
	AllowFail() bool

	// GetState return current state
	GetState() TaskState

	// Elapsed time elapsed since task start
	Elapsed() time.Duration

	// String to string for print
	String() string
}

// BaseTask include task's common attrs
type BaseTask struct {
	typ       TaskType
	state     TaskState
	allowFail bool
	begin     time.Time
	timeout   time.Duration
	logID     string
}

// newBaseTask new base task
func newBaseTask(typ TaskType, timeout time.Duration) *BaseTask {
	return &BaseTask{
		typ:     typ,
		state:   TaskStateStart,
		begin:   time.Now(),
		timeout: timeout,
	}
}

// SetBegin set begin time
func (t *BaseTask) SetBegin() {
	t.begin = time.Now()
}

// SetLogID set logging id
func (t *BaseTask) SetLogID(id string) {
	t.logID = id
}

// GetType return task type
func (t *BaseTask) GetType() TaskType {
	return t.typ
}

// GetState return current state
func (t *BaseTask) GetState() TaskState {
	return t.state
}

// AllowFail return true if is allowed to fail
func (t *BaseTask) AllowFail() bool {
	return t.allowFail
}

// SetAllowFail set allow to fail
func (t *BaseTask) SetAllowFail() {
	t.allowFail = true
}

// Elapsed eplased from start
func (t *BaseTask) Elapsed() time.Duration {
	return time.Since(t.begin)
}

// checkTimeout return true if task is run timeout
func (t *BaseTask) checkTimeout() bool {
	if t.state == TaskStateTimeout {
		return true
	} else if time.Since(t.begin) > t.timeout {
		t.state = TaskStateTimeout
		return true
	} else {
		return false
	}
}

// CheckOver return true if task is over
func (t *BaseTask) CheckOver() bool {
	switch t.state {
	case TaskStateFinished:
		return true
	case TaskStateFailed:
		return true
	case TaskStateCanceled:
		return true
	default:
		return t.checkTimeout()
	}
}

func (t *BaseTask) String() string {
	return fmt.Sprintf("\"type\": \"%s\", \"state\": \"%s\", \"begin\": \"%s\"",
		t.typ.String(), t.state.String(), t.begin.Format("2006/01/02-03:04:05"))
}
