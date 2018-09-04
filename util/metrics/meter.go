package metrics

import (
	//"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type ApiMetric struct {
	name                   string
	numberOfRequest        int64
	numberOfErrResponse    int64
	summaryDelayOfResponse int64
	maxDelayOfResponse     time.Duration
	minDelayOfResponse     time.Duration
}

const (
	RUNNING = int32(1)
	STOPPED = int32(0)
)

type MetricMeter struct {
	// host or other name that is not repeated
	name  string
	run   int32
	mutex *sync.RWMutex

	metrics   map[string]*ApiMetric
	interval  time.Duration
	timestamp time.Time
	avgTotal  float64
	lats      []float64

	output     Output
}

func NewMetricMeter(name string, interval time.Duration, output Output) *MetricMeter {
	if output == nil {
		output = &DefaultOutput{}
	}
	meter := &MetricMeter{
		name:      name,
		run:       STOPPED,
		mutex:     new(sync.RWMutex),
		metrics:   make(map[string]*ApiMetric),
		timestamp: time.Now(),
		lats:      make([]float64, 0, 100000),
		output:    output,
		interval:  interval,
	}
	go meter.Run()
	return meter
}

func (this *MetricMeter) Run() {
	var interval, waitTime time.Duration
	if atomic.LoadInt32(&this.run) == STOPPED {
		atomic.StoreInt32(&this.run, RUNNING)

		this.timestamp = time.Now()
		interval = this.interval
		waitTime = interval
		for atomic.LoadInt32(&this.run) == RUNNING {
			time.Sleep(waitTime)
			tag := time.Now()
			this.reportAndReset()
			waitTime = interval - (time.Now().Sub(tag))
			if waitTime < 0 {
				waitTime = 0
			}
		}
	}
}

func (this *MetricMeter) Stop() {
	if this == nil {
		return
	}
	atomic.StoreInt32(&this.run, STOPPED)
}

func (this *MetricMeter) findMetric(method string) (*ApiMetric, bool) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	if metric, ok := this.metrics[method]; ok {
		return metric, true
	}
	return nil, false
}

func (this *MetricMeter) AddApi(reqMethod string, ack bool) {
	if this == nil {
		return
	}
	method := reqMethod
	metric, ok := this.findMetric(method)
	if !ok {
		this.mutex.Lock()
		metric, ok = this.metrics[method]
		if !ok {
			metric = new(ApiMetric)
			metric.name = method
			this.metrics[method] = metric
		}
		this.mutex.Unlock()
	}

	atomic.AddInt64(&metric.numberOfRequest, 1)
	if !ack {
		atomic.AddInt64(&metric.numberOfErrResponse, 1)
	}
}

func (this *MetricMeter) AddApiWithDelay(reqMethod string, ack bool, delay time.Duration) {
	if this == nil {
		return
	}
	method := reqMethod
	metric, ok := this.findMetric(method)
	if !ok {
		this.mutex.Lock()
		metric, ok = this.metrics[method]
		if !ok {
			metric = new(ApiMetric)
			metric.name = method
			metric.maxDelayOfResponse = delay
			metric.minDelayOfResponse = delay
			this.metrics[method] = metric
		}
		this.mutex.Unlock()
	}
	atomic.AddInt64(&metric.numberOfRequest, 1)
	if !ack {
		atomic.AddInt64(&metric.numberOfErrResponse, 1)
	}
	atomic.AddInt64(&metric.summaryDelayOfResponse, int64(delay))
	this.mutex.Lock()
	defer this.mutex.Unlock()
	if metric.maxDelayOfResponse < delay {
		metric.maxDelayOfResponse = delay
	}
	if metric.minDelayOfResponse > delay {
		metric.minDelayOfResponse = delay
	}
	this.lats = append(this.lats, delay.Seconds())
	this.avgTotal += delay.Seconds()
}

type OpenFalconCustomData struct {
	Name        string  `json:"name,omitempty"`
	Metric      string  `json:"metric,omitempty"`
	TotalNumber uint64 `json:"number_of_request,omitempty"`
	ErrNumber uint64 `json:"number_of_err_response,omitempty"`
	Avg float64 `json:"average_delay_of_response,omitempty"`
	Max float64 `json:"max_delay_of_response,omitempty"`
	Min float64 `json:"min_delay_of_response,omitempty"`
}

type TpsStats struct {
	Name        string  `json:"name,omitempty"`
	TotalNumber uint64 `json:"total_number,omitempty"`
	ErrNumber uint64 `json:"err_number,omitempty"`
	Tps uint64 `json:"tps,omitempty"`
	// min　latency ms
	Min float64 `json:"min,omitempty"`
	// max　latency ms
	Max float64 `json:"max,omitempty"`
	// avg　latency ms
	Avg    float64 `json:"avg,omitempty"`
	Tp_10  float64 `json:"tp_10,omitempty"`
	Tp_25  float64 `json:"tp_25,omitempty"`
	Tp_50  float64 `json:"tp_50,omitempty"`
	Tp_75  float64 `json:"tp_75,omitempty"`
	Tp_90  float64 `json:"tp_90,omitempty"`
	Tp_95  float64 `json:"tp_95,omitempty"`
	Tp_99  float64 `json:"tp_99,omitempty"`
	Tp_999 float64 `json:"tp_999,omitempty"`
}

func (this *MetricMeter) report2OpenFalcon(total time.Duration, lats []float64, avgTotal float64, metrics map[string]*ApiMetric) {
	sort.Float64s(lats)
    	var max, min time.Duration
	var totalNumber, errNumber uint64
	for method, metric := range metrics {
		stats := &OpenFalconCustomData{}

		numberOfResponse := metric.numberOfRequest
		averageDelayOfResponse := time.Duration(0)
		if numberOfResponse > 0 {
			averageDelayOfResponse = time.Duration(int64(metric.summaryDelayOfResponse) / numberOfResponse)
		}
		totalNumber += uint64(metric.numberOfRequest)
		errNumber += uint64(metric.numberOfErrResponse)
		if metric.maxDelayOfResponse > max {
			max = metric.maxDelayOfResponse
		}
		if min == 0 || metric.minDelayOfResponse < min {
			min = metric.minDelayOfResponse
		}
		stats.Name = this.name
		stats.Metric = method
		stats.TotalNumber = uint64(metric.numberOfRequest)
		stats.ErrNumber = uint64(metric.numberOfErrResponse)
		stats.Avg = averageDelayOfResponse.Seconds()
		stats.Max = metric.maxDelayOfResponse.Seconds()
		stats.Min = metric.minDelayOfResponse.Seconds()
		this.output.Output(stats)
	}
	if len(lats) > 0 {
		tps := float64(len(lats)) / total.Seconds()
		average := avgTotal / float64(len(lats))

		stats := &TpsStats{}
		stats.Name = this.name
		stats.TotalNumber = totalNumber
		stats.ErrNumber = errNumber
		stats.Tps = uint64(tps)
		stats.Avg = average
		stats.Max = max.Seconds()
		stats.Min = min.Seconds()
		pctls := []int{100, 250, 500, 750, 900, 950, 990, 999}
		data := make([]float64, len(pctls))
		j := 0
		for i := 0; i < len(lats) && j < len(pctls); i++ {
			current := (i * 1000) / len(lats)
			if current >= pctls[j] {
				data[j] = lats[i]
				j++
			}
		}
		for i := 0; i < len(pctls); i++ {
			switch i {
			case 0:
				stats.Tp_10 = data[i]
			case 1:
				stats.Tp_25 = data[i]
			case 2:
				stats.Tp_50 = data[i]
			case 3:
				stats.Tp_75 = data[i]
			case 4:
				stats.Tp_90 = data[i]
			case 5:
				stats.Tp_95 = data[i]
			case 6:
				stats.Tp_99 = data[i]
			case 7:
				stats.Tp_999 = data[i]
			}
		}
		this.output.Output(stats)
	}
}

func (this *MetricMeter) reportAndReset() {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				fmt.Printf("Error: %s.\n", x)
			case error:
				fmt.Printf("Error: %s.\n", x.Error())
			default:
				fmt.Printf("Unknown panic error.")
			}
		}
	}()

	newMetric := make(map[string]*ApiMetric)
	newLat :=  make([]float64, 0, 100000)

	this.mutex.Lock()
	metrics := this.metrics
	total := time.Now().Sub(this.timestamp)
	avgTotal := this.avgTotal
	lats := this.lats
	this.metrics = newMetric
	this.timestamp = time.Now()
	this.lats = newLat
	this.avgTotal = 0
	this.mutex.Unlock()

	if len(metrics) == 0 {
		return
	}
	this.report2OpenFalcon(total, lats, avgTotal, metrics)
}

type BufferWriter struct {
	lock   sync.Mutex
	offset int
	buff   []byte
}

func NewBufferWriter() *BufferWriter {
	return &BufferWriter{buff: make([]byte, 10240)}
}

func (b *BufferWriter) Write(p []byte) (n int, err error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	if len(b.buff[b.offset:]) < len(p) {
		return 0, errors.New("buffer size limit")
	}
	copy(b.buff[b.offset:], p)
	b.offset += len(p)
	return len(p), nil
}

func (b *BufferWriter) Get() []byte {
	b.lock.Lock()
	defer b.lock.Unlock()
	buff := make([]byte, len(b.buff[:b.offset]))
	copy(buff, b.buff[:b.offset])
	return buff
}

func (b *BufferWriter) Read(p []byte) (n int, err error) {
	//b.lock.Lock()
	//defer b.lock.Unlock()
	//if len(b.buff)
	return 0, nil
}
