package metric

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"encoding/json"
	"model/pkg/statspb"
	"util/log"
	"util/metrics"
	"util/alarm"

	"github.com/gogo/protobuf/proto"
)

var GsMetric *Metric

type SlowLog struct {
	maxSlowLogNum uint64
	currentIndex  uint64
	slowLog       []*statspb.SlowLog
}

type ErrorLog struct {
	maxErrorLogNum 	uint64
	currentIndex  	uint64
	errorLog 	[]string
}

type Metric struct {
	clusterId  uint64
	host       string
	metricAddr string
	cli        *http.Client
	//proxy level
	proxyMeter *metrics.MetricMeter
	//store level
	storeMeter    *metrics.MetricMeter
	maxSlowLogNum uint64

	lock       sync.Mutex
	slowLogger *SlowLog

	connectCount int64

	startTime time.Time

	errorLogLock	sync.Mutex
	errorLogger 	*ErrorLog
	alarmClient 	*alarm.Client
}

func NewMetric(clusterId uint64, host, addr string, maxSlowLogNum uint64, alarmServerAddr string) *Metric {
	metric := &Metric{
		clusterId: clusterId,
		host: host,
		metricAddr: addr,
		maxSlowLogNum: maxSlowLogNum,
		slowLogger: &SlowLog{
			maxSlowLogNum: maxSlowLogNum,
			slowLog:       make([]*statspb.SlowLog, 0, maxSlowLogNum),
		},
		errorLogger: &ErrorLog{
			maxErrorLogNum: maxSlowLogNum,
			errorLog: make([]string, 0, maxSlowLogNum),
		},
	}
	if addr != "" {
		metric.cli = &http.Client{
			Transport: &http.Transport{
				Dial: func(network, addr string) (net.Conn, error) {
					return net.DialTimeout(network, addr, time.Second)
				},
				ResponseHeaderTimeout: time.Second,
			},
		}
	}
	metric.proxyMeter = metrics.NewMetricMeter("GS-Proxy", time.Second * 60, metric)
	metric.storeMeter = metrics.NewMetricMeter("GS-Store", time.Second * 60, metric)

	if len(alarmServerAddr) != 0 {
		var err error
		metric.alarmClient, err = alarm.NewAlarmClient(alarmServerAddr)
		if err != nil {
			log.Error("create alarm client failed, err[%v]", err)
		}
	}
	go metric.Run()

	return metric
}

func UpdateMetric(addr string) error {
	if GsMetric == nil {
		return errors.New("wait init")
	}
	if GsMetric.metricAddr == addr {
		log.Info("metric server is running on the same config")
		return nil
	}
	if addr != "" {
		GsMetric.cli = &http.Client{
			Transport: &http.Transport{
				Dial: func(network, addr string) (net.Conn, error) {
					return net.DialTimeout(network, addr, time.Second)
				},
				ResponseHeaderTimeout: time.Second,
			},
		}
	} else {
		GsMetric.cli = nil
	}
	GsMetric.metricAddr = addr
	return nil
}

func (m *Metric) GetMetricAddress() string  {
	if m == nil {
		return ""
	}
	return m.metricAddr
}

func (m *Metric) gatewayErrorlogAlarm(clusterId, ipAddr string, errorlogs []string) {
	var samples []*alarm.Sample

	for _, errorlog := range errorlogs {
		info := make(map[string]interface{})
		info["spaceId"] = clusterId
		info["ip"] = ipAddr
		info["gateway_errorlog"] = 1
		info["errorlog"] = errorlog
		samples = append(samples, alarm.NewSample("", 0, 0, info))
	}

	log.Warn("errorlog alarm do send")
	if err := m.alarmClient.SimpleAlarm(m.clusterId, "errorlog alarm", "", samples); err != nil {
		log.Error("errorlog alarm do failed: %v", err)
	}
}

func (m *Metric) gatewaySlowlogAlarm(clusterId, ipAddr string, slowlogs []*statspb.SlowLog) {
	var samples []*alarm.Sample

	for _, slowlog := range slowlogs {
		info := make(map[string]interface{})
		info["spaceId"] = clusterId
		info["ip"] = ipAddr
		info["gateway_slowlog"] = 1
		info["slowlog"] = slowlog.SlowLog
		samples = append(samples, alarm.NewSample("", 0, 0, info))
	}

	log.Info("slowlog alarm do send")
	if err := m.alarmClient.SimpleAlarm(m.clusterId, "slowlog alarm", "", samples); err != nil {
		log.Error("slowlog alarm do failed: %v", err)
	}
}

func (m *Metric) Run() {
	log.Warn("metric report run")
	timer := time.NewTicker(time.Minute * 10)
	for {
		select {
		case <-timer.C:
		// slowlog
			log.Warn("report slowlog metric...")
		func() {
			m.lock.Lock()
			slowLogger := m.slowLogger
			m.slowLogger = &SlowLog{
				maxSlowLogNum: m.maxSlowLogNum,
				slowLog:       make([]*statspb.SlowLog, 0, m.maxSlowLogNum),
			}
			m.lock.Unlock()
			stats := &statspb.SlowLogStats{}
			if len(slowLogger.slowLog) == 0 {
				log.Info("no slow log in queue")
				return
			}
			stats.SlowLogs = slowLogger.slowLog
			values := url.Values{}
			values.Set("clusterId", fmt.Sprintf("%d", m.clusterId))
			values.Set("namespace", "GS")
			values.Set("subsystem", m.host)
			_url := fmt.Sprintf(`http://%s/metric/slowlog?%s`, m.metricAddr, values.Encode())
			err := m.SendMetric(_url, stats)
			if err != nil {
				log.Warn("send metric server failed, err[%v]", err)
			}

			// slowlog alarm
			m.gatewaySlowlogAlarm(fmt.Sprint(m.clusterId), m.host, slowLogger.slowLog)
		}()

		// error log
			log.Warn("report errorlog metric...")
		func() {
			m.errorLogLock.Lock()
			errorLogger := m.errorLogger
			m.errorLogger = &ErrorLog {
				maxErrorLogNum: m.maxSlowLogNum,
				errorLog: make([]string, 0, m.maxSlowLogNum),
			}
			m.errorLogLock.Unlock()
			if len(errorLogger.errorLog) == 0 {
				log.Warn("no error log in queue")
				return
			}
			// errorlog alarm
			m.gatewayErrorlogAlarm(fmt.Sprint(m.clusterId), m.host, errorLogger.errorLog)
		}()
		}
	}
}

func (m *Metric) ProxyApiMetric(method string, ack bool, delay time.Duration) {
	if m == nil {
		return
	}
	m.proxyMeter.AddApiWithDelay(method, ack, delay)
}

func (m *Metric) StoreApiMetric(method string, ack bool, delay time.Duration) {
	if m == nil {
		return
	}
	m.storeMeter.AddApiWithDelay(method, ack, delay)
}

func (m *Metric) ErrorLogMetric(errorLog string) {
	if m == nil {
		return
	}
	m.errorLogLock.Lock()
	defer m.errorLogLock.Unlock()

	errorLogger := m.errorLogger
	index := errorLogger.currentIndex
	if uint64(len(errorLogger.errorLog)) < errorLogger.maxErrorLogNum {
		errorLogger.errorLog = append(errorLogger.errorLog, errorLog)
	} else {
		errorLogger.errorLog[index % errorLogger.maxErrorLogNum] = errorLog
	}

	errorLogger.currentIndex++
}

func (m *Metric) SlowLogMetric(slowLog string, delay time.Duration) {
	if m == nil {
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	slowLogger := m.slowLogger
	index := slowLogger.currentIndex
	if uint64(len(slowLogger.slowLog)) < slowLogger.maxSlowLogNum {
		slowLogger.slowLog = append(slowLogger.slowLog, &statspb.SlowLog{
			SlowLog: slowLog,
			Lats:    delay.Nanoseconds() / 1000000,
		})
	} else {
		slowLogger.slowLog[index%slowLogger.maxSlowLogNum] = &statspb.SlowLog{
			SlowLog: slowLog,
			Lats:    delay.Nanoseconds() / 1000000,
		}
	}

	slowLogger.currentIndex++
}

func (m *Metric) AddConnectCount(delta int64) {
	if m == nil {
		return
	}
	atomic.AddInt64(&m.connectCount, delta)
}

func (m *Metric) SendMetric(url string, message proto.Message) error {
	if m == nil {
		return nil
	}
	if m.cli == nil {
		return nil
	}
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	request, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	response, err := m.cli.Do(request)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return errors.New("response not ok!!!!")
	}
	io.Copy(ioutil.Discard, response.Body)
	response.Body.Close()
	return nil
}

func (m *Metric) Output(output interface{}) {
	if m == nil {
		return
	}
	if apiMetric, ok := output.(*metrics.OpenFalconCustomData); ok {
		log.Debug("custom metric %+v", apiMetric)
	} else if tps, ok := output.(*metrics.TpsStats); ok {
		log.Debug("TpsStats metric %+v", tps)
		// report to metric
		stats := &statspb.ProcessStats{}
		// cpu
		cpu_rate, err := cpuRate()
		if err != nil {
			log.Warn("get cpu rate failed, err %v", err)
			return
		}
		stats.CpuProcRate = cpu_rate
		// mem
		total, used, err := memInfo()
		if err != nil {
			log.Warn("get mem info failed, err %v", err)
			return
		}

		// fd
		fdNum, err := fdInfo()
		if err != nil {
			log.Warn("get fd num failed, err %v", err)
			return
		}
		stats.HandleNum = fdNum

		stats.MemoryTotal = total
		stats.MemoryUsed = used
		count := atomic.LoadInt64(&m.connectCount)
		if count < 0 {
			count = 0
			log.Warn("connect count invalid!!!!, must bug!!!!!!")
		}
		stats.ConnectCount = uint64(count)
		stats.ThreadNum = uint32(runtime.NumGoroutine())
		// tps
		stats.TpStats = &statspb.TpStats{
			Tps: tps.Tps,
			// min　latency ms
			Min: tps.Min,
			// max　latency ms
			Max: tps.Max,
			// avg　latency ms
			Avg:         tps.Avg,
			Tp_50:       tps.Tp_50,
			Tp_90:       tps.Tp_90,
			Tp_99:       tps.Tp_99,
			Tp_999:      tps.Tp_999,
			TotalNumber: tps.TotalNumber,
			ErrNumber:   tps.ErrNumber,
		}
		values := url.Values{}
		values.Set("clusterId", fmt.Sprintf("%d", m.clusterId))
		values.Set("namespace", "GS")
		values.Set("subsystem", m.host)
		_url := fmt.Sprintf(`http://%s/metric/process?%s`, m.metricAddr, values.Encode())
		err = m.SendMetric(_url, stats)
		if err != nil {
			log.Warn("send metric server failed, err[%v]", err)
		}
	}
}

func cpuRate() (float64, error) {
	var rate float64

	c := `ps aux`
	cmd := exec.Command("sh", "-c", c)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return rate, err
	}
	pid := os.Getpid()
	find := false
	for {
		line, err := out.ReadString('\n')
		if err != nil {
			break
		}
		tokens := strings.Split(line, " ")
		ft := make([]string, 0)
		for _, t := range tokens {
			if t != "" && t != "\t" {
				ft = append(ft, t)
			}
		}
		_pid, err := strconv.Atoi(ft[1])
		if err != nil {
			continue
		}
		if pid == _pid {
			rate, err = strconv.ParseFloat(ft[2], 64)
			if err != nil {
				return rate, err
			}
			find = true
			break
		}
	}
	if !find {
		return rate, errors.New("not found process!!!!")
	}
	return rate, nil
}

func fdInfo() (num uint32, err error) {
	var files []os.FileInfo
	files, err = ioutil.ReadDir(fmt.Sprintf(`/proc/%d`, os.Getpid()))
	if err != nil {
		return
	}
	num = uint32(len(files))
	return
}
