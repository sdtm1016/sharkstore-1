package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"util/log"
	"util/deepcopy"
	"util"
	"model/pkg/statspb"
	"model/pkg/mspb"

	"golang.org/x/net/context"
)

type Metric struct {
	ctx      context.Context
	cancel   context.CancelFunc
	cluster  *Cluster
	cli      *http.Client
	addr     string
	interval time.Duration
	wg       sync.WaitGroup

	lock            sync.Mutex
	taskList        []*TaskChain
	scheduleCounter map[string]map[string]uint64
}

func NewMetric(cluster *Cluster, addr string, interval time.Duration) *Metric {
	if addr == "" || interval == 0 {
		return nil
	}
	cli := &http.Client{
		Transport: &http.Transport{
			Dial: func(netw, addr string) (net.Conn, error) {
				deadline := time.Now().Add(time.Second)
				c, err := net.DialTimeout(netw, addr, time.Second)
				if err != nil {
					return nil, err
				}
				c.SetDeadline(deadline)
				return c, nil
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Metric{cli: cli, addr: addr, interval: interval, cluster: cluster,
		ctx: ctx, cancel: cancel, scheduleCounter: make(map[string]map[string]uint64)}
	return m
}

func UpdateMetric(cluster *Cluster, addr string, interval time.Duration) error {
	metric := cluster.metric
	if metric != nil && metric.addr == addr && metric.interval == interval {
		log.Info("metric server is running on the same config")
		return nil
	}
	//落盘
	err := cluster.StoreMetricConfig(&MetricConfig{Interval: util.NewDuration(interval), Address:addr})
	if err != nil {
		return err
	}

	if metric == nil {
		metric = NewMetric(cluster, addr, interval)
		metric.Run()
	} else {
		metric.lock.Lock()
		if metric.addr != addr && addr != ""{
			cli := &http.Client{
				Transport: &http.Transport{
					Dial: func(netw, addr string) (net.Conn, error) {
						deadline := time.Now().Add(time.Second)
						c, err := net.DialTimeout(netw, addr, time.Second)
						if err != nil {
							return nil, err
						}
						c.SetDeadline(deadline)
						return c, nil
					},
				},
			}
			metric.cli = cli
			metric.addr = addr
		}
		if interval != 0 {
			metric.interval = interval
		}
		metric.lock.Unlock()
	}
	return nil
}

func (m *Metric) GetMetricAddr() string  {
	if m == nil {
		return ""
	}
	return m.addr
}

func (m *Metric) GetMetricInterval() time.Duration  {
	if m == nil {
		return 0
	}
	return m.interval
}

func (m *Metric) run() {
	defer m.wg.Done()
	timer := time.NewTimer(m.interval)
	defer timer.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-timer.C:
			timer.Reset(m.interval)
			// 上报task
			if err := m.eventInfoStats(); err != nil {
				continue
			}
			// 上报调度统计
			if err := m.scheduleCounterStats(); err != nil {
				continue
			}
			// 上报集群统计
			if err := m.clusterInfoStats(); err != nil {
				continue
			}
			// 上报node心跳数据
			if err := m.clusterNodeStats(); err != nil {
				continue
			}
			// 上报range心跳数据
			if err := m.clusterRangeStats(); err != nil {
				continue
			}
			// 上报热点统计
			//m.hotspotStats()

		}
	}
}

func (m *Metric) Run() {
	if m == nil {
		return
	}
	m.wg.Add(1)
	go m.run()
}

func (m *Metric) Stop() {
	if m == nil {
		return
	}
	m.cancel()
	m.wg.Wait()
}

func (m *Metric) CollectEvent(task *TaskChain) {
	if m == nil {
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	list := m.taskList
	list = append(list, task)
	m.taskList = list
	log.Info("add task success %v", task)
}

func (m *Metric) CollectScheduleCounter(name, label string) {
	if m == nil {
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	var labels map[string]uint64
	var count uint64
	var ok bool
	if labels, ok = m.scheduleCounter[name]; !ok {
		labels = make(map[string]uint64)
		m.scheduleCounter[name] = labels
	}
	count = labels[label]
	count++
	labels[label] = count
}

func (m *Metric) eventInfoStats() error {
	m.lock.Lock()
	taskList := m.taskList
	m.taskList = make([]*TaskChain, 0)
	m.lock.Unlock()
	for _, task := range taskList {
		if err := metricEvent(m.cli, m.cluster.GetClusterId(), m.addr, changeEventToMetricTask(task)); err != nil {
			log.Warn("report task to metric server failed, err %v", err)
		}
	}
	return nil
}

func changeEventToMetricTask(t *TaskChain) *statspb.TaskInfo {
	return &statspb.TaskInfo{
		TaskId:   t.GetID(),
		RangeId:  t.GetRangeID(),
		Kind:     t.GetName(),
		Describe: t.String(),
		UsedTime: t.Elapsed().Seconds(),
		// TODO: state
	}
}

func (m *Metric) scheduleCounterStats() error {
	m.lock.Lock()
	schedulerCounter := m.scheduleCounter
	m.scheduleCounter = make(map[string]map[string]uint64)
	m.lock.Unlock()
	for name, lableValues := range schedulerCounter {
		for lable, count := range lableValues {
			stats := &statspb.ScheduleCount{
				Name:  name,
				Label: lable,
				Count: count,
			}
			if err := metricScheduleCount(m.cli, m.cluster.GetClusterId(), m.addr, stats); err != nil {
				log.Warn("report schedule counter to metric server failed, err %v", err)
				return err
			}
		}
	}
	return nil
}

/**
func (m *Metric) hotspotStats() error {
	hotspots := m.cluster.coordinator.collectHotSpotMetrics()
	for _, hotspot := range hotspots {
		err := metricHotSpot(m.cli, m.cluster.GetClusterId(), m.addr, hotspot)
		if err != nil {
			return err
		}
	}
	return nil
}
*/
func (m *Metric) clusterNodeStats() error {
	cluster := m.cluster
	nodeStatss := make(map[uint64]*mspb.NodeStats)
	for _, node := range cluster.GetAllNode() {
		nodeStats := deepcopy.Iface(node.stats).(*mspb.NodeStats)
		nodeStatss[node.GetId()] = nodeStats
	}

	for nodeId, nodeStats := range nodeStatss {
		node := cluster.FindNodeById(nodeId)
		if node == nil {
			continue
		}
		err := metricNode(m.cli, cluster.GetClusterId(), nodeId, m.addr, node.GetServerAddr(), nodeStats)
		if err != nil {
			log.Warn("metric node hb stats failed, err %v", err)
			return err
		}
	}
	return nil
}

func (m *Metric) clusterRangeStats() error {
	cluster := m.cluster
	rngInfos := make([]*statspb.RangeInfo, 0)
	for _, rng := range cluster.GetAllRanges() {
		if rng.GetLeader() == nil {
			log.Warn("range hb metric: range %v no leader", rng.GetId())
			continue
		}
		nodeId := rng.GetLeader().GetNodeId()
		node := cluster.FindNodeById(nodeId)
		if node == nil {
			continue
		}
		rngInfo := &statspb.RangeInfo{
			RangeId:   rng.GetId(),
			LeaderId:  rng.GetLeader().GetId(),
			NodeAdder: node.GetServerAddr(),
			Stats: &mspb.RangeStats{
				BytesWritten:    rng.BytesWritten,
				BytesRead:       rng.BytesRead,
				KeysWritten:     rng.KeysWritten,
				KeysRead:        rng.KeysRead,
				ApproximateSize: rng.ApproximateSize,
			},
		}
		rngInfos = append(rngInfos, rngInfo)
	}

	threadNum := 50
	sendSize := 20
	loop := len(rngInfos)/sendSize/threadNum + 1
	for i := 0; i < loop; i++ {
		for j := 0; j < threadNum; j++ {
			start := j*sendSize + i*threadNum*sendSize
			end := start + sendSize
			if start > len(rngInfos) {
				goto stop
			}
			if end > len(rngInfos) {
				end = len(rngInfos)
			}
			go func() {
				err := metricRanges(m.cli, cluster.GetClusterId(), m.addr, rngInfos[start:end])
				if err != nil {
					log.Warn("metric range hb stats failed, err %v", err)
				}
			}()
		}
	}
stop:
	log.Info("push range metric size: %v", len(rngInfos))
	return nil
}

func (m *Metric) clusterInfoStats() error {
	stats := &statspb.ClusterStats{}
	cluster := m.cluster
	stats.DbNum = uint64(cluster.dbs.Size())
	var dbStatss []*statspb.DatabaseStats
	for _, db := range cluster.dbs.GetAllDatabase() {
		dbStats := &statspb.DatabaseStats{Name: db.Name(), TableNum: uint32(db.tables.Size())}
		dbStatss = append(dbStatss, dbStats)
	}

	var tableStatss []*statspb.TableStats
	for _, table := range cluster.workingTables.GetAllTable() {
		stats.TableNum++
		var size, count uint64
		for _, r := range cluster.GetTableAllRanges(table.GetId()) {
			size += r.ApproximateSize
			count++
		}
		stats.RangeNum += count
		tableStats := &statspb.TableStats{
			DbName:    table.GetDbName(),
			TableName: table.GetName(),
			RangeNum:  count, Size_: size}
		tableStatss = append(tableStatss, tableStats)
	}
	storageSize := uint64(0)
	storageCapacity := uint64(0)
	minLeaderScore, maxLeaderScore := math.MaxFloat64, float64(0.0)
	minRegionScore, maxRegionScore := math.MaxFloat64, float64(0.0)
	for _, node := range cluster.GetAllNode() {
		if node.isOffline() {
			stats.NodeOfflineCount++
		} else if node.isUp() {
			stats.NodeUpCount++
		} else if node.isTombstone() {
			stats.NodeTombstoneCount++
		} else if node.isDown() {
			stats.NodeDownCount++
		}
		if !node.isUp() {
			continue
		}

		// Store stats.
		storageSize += node.storageSize()
		storageCapacity += node.stats.GetCapacity()

		// Balance score.
		minLeaderScore = math.Min(minLeaderScore, node.leaderScore())
		maxLeaderScore = math.Max(maxLeaderScore, node.leaderScore())
		minRegionScore = math.Min(minRegionScore, float64(node.GetRangesCount()))
		maxRegionScore = math.Max(maxRegionScore, float64(node.GetRangesCount()))
	}
	stats.SizeUsed = storageSize
	stats.CapacityTotal = storageCapacity
	if maxLeaderScore != 0.0 {
		stats.LeaderBalanceRatio = 1 - minLeaderScore/maxLeaderScore
	}
	if maxRegionScore != 0.0 {
		stats.RegionBalanceRatio = 1 - minRegionScore/maxRegionScore
	}

	log.Debug("start to schedule metric, and send data to metric service:[%s], data:[%v] ", m.addr, stats)

	err := metricCluster(m.cli, cluster.GetClusterId(), m.addr, stats)
	if err != nil {
		log.Warn("metric cluster stats failed, err %v", err)
		return err
	}
	for _, dbStats := range dbStatss {
		err = metricDb(m.cli, cluster.GetClusterId(), m.addr, dbStats)
		if err != nil {
			log.Warn("metric db stats failed, err %v", err)
		}
	}
	for _, tableStats := range tableStatss {
		err = metricTable(m.cli, cluster.GetClusterId(), m.addr, tableStats)
		if err != nil {
			log.Warn("metric table stats failed, err %v", err)
			return err
		}
	}

	return nil
}

func metricEvent(cli *http.Client, clusterId uint64, addr string, task *statspb.TaskInfo) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s/metric/event?clusterId=%d", addr, clusterId)
	request, err := http.NewRequest("POST", url, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	return send(cli, request)
}

func metricHotSpot(cli *http.Client, clusterId uint64, addr string, spot *statspb.HotSpotStats) error {
	data, err := json.Marshal(spot)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s/metric/hotspot?clusterId=%d", addr, clusterId)
	request, err := http.NewRequest("POST", url, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	return send(cli, request)
}

func metricScheduleCount(cli *http.Client, clusterId uint64, addr string, count *statspb.ScheduleCount) error {
	data, err := json.Marshal(count)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s/metric/schedule?clusterId=%d", addr, clusterId)
	request, err := http.NewRequest("POST", url, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	return send(cli, request)
}

func metricNode(cli *http.Client, clusterId, nodeId uint64, addr, nodeAddr string, stats *mspb.NodeStats) error {
	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s/metric/node?clusterId=%d&namespace=%v&subsystem=%s", addr, clusterId, nodeId, nodeAddr)
	request, err := http.NewRequest("POST", url, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	return send(cli, request)
}

func metricRanges(cli *http.Client, clusterId uint64, addr string, stats []*statspb.RangeInfo) error {
	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s/metric/range?clusterId=%d", addr, clusterId)
	request, err := http.NewRequest("POST", url, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	return send(cli, request)
}

func metricCluster(cli *http.Client, clusterId uint64, addr string, stats *statspb.ClusterStats) error {
	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s/metric/cluster?clusterId=%d", addr, clusterId)
	request, err := http.NewRequest("POST", url, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	return send(cli, request)
}

func metricDb(cli *http.Client, clusterId uint64, addr string, stats *statspb.DatabaseStats) error {
	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s/metric/db?clusterId=%d&namespace=%s", addr, clusterId, stats.Name)
	request, err := http.NewRequest("POST", url, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	return send(cli, request)
}

func metricTable(cli *http.Client, clusterId uint64, addr string, stats *statspb.TableStats) error {
	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s/metric/table?clusterId=%d&namespace=%s&subsystem=%s",
		addr, clusterId, stats.DbName, stats.TableName)
	request, err := http.NewRequest("POST", url, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	return send(cli, request)
}

func send(cli *http.Client, request *http.Request) error {
	response, err := cli.Do(request)
	if err != nil {
		return err
	}
	if response.Body != nil {
		defer response.Body.Close()
		_, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return err
		}
		//log.Debug("response ", string(data))
	}
	if response.StatusCode != http.StatusOK {
		return errors.New("request failed, not ok")
	}

	return nil
}
