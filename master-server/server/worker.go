package server

import (
	"time"
	"sync"
	"golang.org/x/net/context"
	"util/log"
	"fmt"
)

const (
	defaultWorkerInterval  =  time.Second
	maxScheduleInterval       = time.Minute
	minScheduleInterval       = time.Millisecond * 20
	scheduleIntervalFactor    = 0.8

	writeStatLRUMaxLen            = 1000
	regionHeartBeatReportInterval = 10

	failoverWorkerName 			 = "failover_worker"
	deleteTableWorkerName		 = "delete_table_worker"
	trashReplicaGcWorkerName	 = "trash_replica_gc_worker"
	createTableWorkerName		 = "create_table_worker"
	rangeHbCheckWorkerName		 = "range_hbcheck_worker"

	balanceRangeWorkerName   	 = "balance_range_worker"
	balanceLeaderWorkerName   	 = "balance_leader_worker"
	balanceNodeOpsWorkerName     = "balance_node_ops_worker"

	//balanceStorageWorkerName 	= "balance_node_storage_worker"
	//hotRegionWorkerName        = "balance_hotregion_worker"
	//grantLeaderWorkerName      = "grant_leader_worker"
	//evictLeaderWorkerName      = "evict_leader_worker"
	//shuffleLeaderWorkerName    = "shuffle_leader_worker"
	//shuffleRangeWorkerName    = "shuffle_range_worker"
)


type Worker interface {
	GetName() string
	GetInterval() time.Duration
	AllowWork(cluster *Cluster) bool
	Work(cluster *Cluster)
	Stop()
}

type WorkerManager struct {
	ctx    context.Context
	cancel context.CancelFunc
	lock   sync.RWMutex
	wg     sync.WaitGroup

	cluster    *Cluster
	opt        *scheduleOption
	
	workers map[string]Worker
}

func NewWorkerManager(cluster *Cluster, opt *scheduleOption) *WorkerManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerManager{
		ctx: ctx,
		cancel: cancel,
		cluster: cluster,
		opt: opt,
		workers: make(map[string]Worker),
	}
}

func (wm *WorkerManager) Run() {
	wm.addWorker(NewFailoverWorker(wm, time.Second))
	wm.addWorker(NewDeleteTableWorker(wm, 1 * time.Minute))
	wm.addWorker(NewTrashReplicaGCWorker(wm, time.Minute))
	wm.addWorker(NewCreateTableWorker(wm, time.Second))
	wm.addWorker(NewRangeHbCheckWorker(wm, 2 * time.Minute))

	wm.addWorker(NewBalanceNodeLeaderWorker(wm, 5 * defaultWorkerInterval))
	wm.addWorker(NewBalanceNodeRangeWorker(wm, 2 * defaultWorkerInterval))
	wm.addWorker(NewBalanceNodeOpsWorker(wm, defaultWorkerInterval))
}

func (wm *WorkerManager) Stop() {
	wm.cancel()
	wm.wg.Wait()
}

func (wm *WorkerManager) addWorker(w Worker) error {
	wm.lock.Lock()
	defer wm.lock.Unlock()

	if _, ok := wm.workers[w.GetName()]; ok {
		return ErrWorkerExisted
	}
	log.Debug("add worker %s", w.GetName())

	wm.wg.Add(1)
	go wm.runWorker(w)
	wm.workers[w.GetName()] = w
	return nil
}

func (wm *WorkerManager) runWorker(w Worker) {
	defer wm.wg.Done()

	timer := time.NewTimer(w.GetInterval())
	defer timer.Stop()

	for {
		if !wm.isExistWorker(w.GetName()) {
			return
		}

		select {
		case <-wm.ctx.Done():
			return
		case <-timer.C:
			log.Debug("add worker %s", w.GetName())
			timer.Reset(w.GetInterval())
		    if !w.AllowWork(wm.cluster) {
		    	log.Debug("worker cannot exec, %v", w.GetName())
			    continue
		    }
		    w.Work(wm.cluster)
		}
	}
}

func (wm *WorkerManager) isExistWorker(name string) bool {
	wm.lock.RLock()
	defer wm.lock.RUnlock()

	_, ok := wm.workers[name]
	return ok
}


func (wm *WorkerManager) removeWorker(name string) error {
	wm.lock.Lock()
	defer wm.lock.Unlock()

	_, ok := wm.workers[name]
	if !ok {
		return ErrWorkerNotFound
	}

	delete(wm.workers, name)
	return nil
}

func (wm *WorkerManager) GetAllWorker() []string  {
	wm.lock.RLock()
	defer wm.lock.RUnlock()
	var names []string
	for name := range wm.workers {
		names = append(names, name)
	}
	return names
}


func (wm *WorkerManager) GetWorker(workerName string) string  {
	wm.lock.RLock()
	defer wm.lock.RUnlock()
	for name, w := range wm.workers {
		if workerName == name {
			return fmt.Sprintf("%s: 调度中, interval: %v", workerName, w.GetInterval())
		}
	}
	return fmt.Sprintf("%s: 未被调度", workerName)
}