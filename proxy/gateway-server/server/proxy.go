package server

import (
	"sync"
	dsClient "pkg-go/ds_client"
	msClient "pkg-go/ms_client"
	"util/hlc"

	"golang.org/x/net/context"
)

type Proxy struct {
	msCli  msClient.Client
	dsCli  dsClient.KvClient
	config *Config
	router *Router

	clock *hlc.Clock

	maxWorkNum  uint64
	taskQueues []chan Task
	workRecover chan int
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewProxy(msAddrs []string, config *Config) *Proxy {
	msCli, err := msClient.NewClient(msAddrs)
	if err != nil {
		return nil
	}
	router := NewRouter(msCli)
	if router == nil {
		return nil
	}
	var taskQueues []chan Task
	for i := 0; i < int(config.Performance.MaxWorkNum); i++ {
		queue := make(chan Task, config.Performance.MaxTaskQueueLen)
		taskQueues = append(taskQueues, queue)
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := &Proxy{
		router: router,
		msCli:  msCli,
		dsCli:  dsClient.NewRPCClient(config.Performance.GrpcPoolSize),
		//metric:  metrics.NewMetricMeter("gateway", new(Report)),
		clock:       hlc.NewClock(hlc.UnixNano, 0),
		config:      config,
		ctx:         ctx,
		cancel:      cancel,
		maxWorkNum: config.Performance.MaxWorkNum,
		taskQueues: taskQueues,
		workRecover: make(chan int, config.Performance.MaxWorkNum),
	}

	for i, queue := range taskQueues {
		proxy.wg.Add(1)
		go proxy.work(i, queue)
	}
	proxy.wg.Add(1)
	go proxy.workMonitor()
	return proxy
}

func (p *Proxy) Close() {
	p.cancel()
	p.wg.Wait()
}
