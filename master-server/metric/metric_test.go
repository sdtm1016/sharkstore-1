package metric

import (
	"testing"
	"net/http"
	"net/url"
	"io/ioutil"
	"strings"
	"errors"
	"fmt"
	"time"

	"model/pkg/statspb"
	"model/pkg/mspb"
	"encoding/json"
)

var (
	clusterId = uint64(1)
	user = "root"
	pw = "123456"
	host = "localhost:3306"
	db = "fbase"
	table = "metric"
)

func send(cli *http.Client, request *http.Request) error {
	response, err := cli.Do(request)
	if err != nil {
		return err
	}
	if response.Body != nil {
		defer response.Body.Close()
        data, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return err
		}
		fmt.Println("response ", string(data))
	}
	if response.StatusCode != http.StatusOK {
		return errors.New("request failed, not ok")
	}

	return nil
}

func metricCluster(cli *http.Client) error {
	stats := &statspb.ClusterStats{
		CapacityTotal: 10000,
		SizeUsed: 1000,
		RangeNum: 2000,
		DbNum: 1,
		TableNum: 5,
	}
	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://localhost:8080/metric/cluster?clusterId=%d", clusterId)
	request, err := http.NewRequest("POST", url, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	return send(cli, request)
}

func metricMac(cli *http.Client, _type, ip string) error {
	stats := &statspb.MacStats{
		CpuProcRate: 0.5,
		CpuCount:    32,
		Load1:       0.1,
		Load5:       0.5,
		Load15:      1.5,
		MemStats:    &statspb.MemStats{
		MemoryTotal:          100000,
		MemoryUsedRss:        1000,
		MemoryUsed:           1000,
		MemoryFree:            9000,
		MemoryUsedPercent:     0.1,
		SwapMemoryTotal:       100000,
		SwapMemoryUsed:        1000,
		SwapMemoryFree:        9000,
		SwapMemoryUsedPercent: 0.2,
		},
		NetStats:    &statspb.NetStats{
			NetIoInBytePerSec:             1000,
			NetIoOutBytePerSec:            1000,
			NetIoInPackagePerSec:          2000,
			NetIoOutPackagePerSec:         1000,
			NetTcpConnections:             1000,
			NetTcpActiveOpensPerSec:       1000,
			NetIpRecvPackagePerSec:        1000,
			NetIpSendPackagePerSec:        1000,
			NetIpDropPackagePerSec:        1000,
			NetTcpRecvPackagePerSec:       1000,
			NetTcpSendPackagePerSec:       1000,
			NetTcpErrPackagePerSec:        1000,
			NetTcpRetransferPackagePerSec: 10,
		},
		// 多个磁盘的统计情况
		DiskStats: []*statspb.DiskStats{&statspb.DiskStats{
			DiskPath:             "/export/Data1",
			DiskTotal:            10000,
			DiskUsed:             1000,
			DiskFree:             9000,
			DiskProcRate:         0.5,
			DiskReadBytePerSec:   100,
			DiskWriteBytePerSec:  100,
			DiskReadCountPerSec:  100,
			DiskWriteCountPerSec: 100,
		},&statspb.DiskStats{
			DiskPath:             "/export/Data2",
			DiskTotal:            10000,
			DiskUsed:             1000,
			DiskFree:             9000,
			DiskProcRate:         0.5,
			DiskReadBytePerSec:   100,
			DiskWriteBytePerSec:  100,
			DiskReadCountPerSec:  100,
			DiskWriteCountPerSec: 100,
		},},
		// 进程数
		ProcessNum: 10,
		// 线程数
		ThreadNum: 10,
		// 句柄数
		HandleNum: 1000,
	}
	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	val := url.Values{}
	val.Set("clusterId", fmt.Sprintf("%d", clusterId))
	val.Set("namespace", _type)
	val.Set("subsystem", ip)
	url := fmt.Sprintf("http://localhost:8080/metric/mac?%s",
		val.Encode())
	request, err := http.NewRequest("POST", url, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	return send(cli, request)
}

func metricProcess(cli *http.Client, _type, addr string) error {
	var stats *statspb.ProcessStats
	if _type == "DS" {
		stats = &statspb.ProcessStats{
			CpuProcRate: 0.5,
			MemoryTotal:          100000,
			MemoryUsed:           1000,
			DiskStats:    &statspb.DiskStats{
				DiskPath:             "/export/Data",
				DiskTotal:            10000,
				DiskUsed:             1000,
				DiskFree:             9000,
				DiskProcRate:         0.5,
				DiskReadBytePerSec:   100,
				DiskWriteBytePerSec:  100,
				DiskReadCountPerSec:  100,
				DiskWriteCountPerSec: 100,
			},
			TpStats:      &statspb.TpStats{
				Tps: 10000,
				// min　latency ms
				Min: 0.1,
				// max　latency ms
				Max: 20.0,
				// avg　latency ms
				Avg:    2,
				Tp_50:  1,
				Tp_90:  2,
				Tp_99:  5,
				Tp_999: 10,
			},
			// 只有ds process需要统计这部分信息
			DsInfo:  &statspb.DsInfo{
				RangeCount: 1000,
				// Current range split count.
				RangeSplitCount: 10,
				// Current sending snapshot count.
				SendingSnapCount: 20,
				// Current receiving snapshot count.
				ReceivingSnapCount: 10,
				// How many range is applying snapshot.
				ApplyingSnapCount: 10,
				RangeLeaderCount:  5,
				// ds version for update
				Version: "v1",
			},
			// 线程数
			ThreadNum: 10,
			// 句柄数
			HandleNum: 1024,
			// When the data server is started (unix timestamp in seconds).
			StartTime: time.Now().Unix(),
		}
	} else {
		stats = &statspb.ProcessStats{
			CpuProcRate: 0.5,
			MemoryTotal:          100000,
			MemoryUsed:           1000,
			DiskStats:    &statspb.DiskStats{
				DiskPath:             "/export/Data",
				DiskTotal:            10000,
				DiskUsed:             1000,
				DiskFree:             9000,
				DiskProcRate:         0.5,
				DiskReadBytePerSec:   100,
				DiskWriteBytePerSec:  100,
				DiskReadCountPerSec:  100,
				DiskWriteCountPerSec: 100,
			},
			TpStats:      &statspb.TpStats{
				Tps: 10000,
				// min　latency ms
				Min: 0.1,
				// max　latency ms
				Max: 20.0,
				// avg　latency ms
				Avg:    2,
				Tp_50:  1,
				Tp_90:  2,
				Tp_99:  5,
				Tp_999: 10,
			},

			// 线程数
			ThreadNum: 10,
			// 句柄数
			HandleNum: 1024,
			// When the data server is started (unix timestamp in seconds).
			StartTime: time.Now().Unix(),
		}
	}

	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	val := url.Values{}
	val.Set("clusterId", fmt.Sprintf("%d", clusterId))
	val.Set("namespace", _type)
	val.Set("subsystem", addr)
	url := fmt.Sprintf("http://localhost:8080/metric/process?%s",
		val.Encode())
	request, err := http.NewRequest("POST", url, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	return send(cli, request)
}

func metricDb(cli *http.Client, name string) error {
	stats := &statspb.DatabaseStats{
		TableNum: 5,
	}
	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://localhost:8080/metric/db?clusterId=%d&namespace=%s", clusterId, db)
	request, err := http.NewRequest("POST", url, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	return send(cli, request)
}

func metricTable(cli *http.Client, dbName, tableName string) error {
	stats := &statspb.TableStats{
		RangeNum: 2000,
		Size_: 10000,
	}
	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://localhost:8080/metric/table?clusterId=%d&namespace=%s&subsystem=%s", clusterId, db, table)
	request, err := http.NewRequest("POST", url, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	return send(cli, request)
}

func metricNode(cli *http.Client) error {
	stats := &mspb.NodeStats{
		RangeCount: 10,
		RangeSplitCount:4,
		SendingSnapCount:4,
		ReceivingSnapCount:3,
		ApplyingSnapCount:2,
		RangeLeaderCount:6,
		Capacity:10,
		UsedSize:5,
		Available:5,
		BytesWritten: 10000,
		BytesRead: 1000,
		KeysWritten: 2000,
		KeysRead: 1302,
		IsBusy: true,
	}
	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://localhost:8080/metric/node?clusterId=%d&namespace=%s&subsystem=%s",
		clusterId, "1", "192.168.108.113:6680")
	request, err := http.NewRequest("POST", url, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	return send(cli, request)
}

func metricRanges(cli *http.Client) error {
	rngInfos := make([]*statspb.RangeInfo, 0)
	rngInfo := &statspb.RangeInfo{
		RangeId:   12,
		LeaderId:  13,
		NodeAdder: "192.168.108.113:11100",
		Stats: &mspb.RangeStats{
			BytesWritten: 10,
			BytesRead:4,
			KeysWritten:4,
			KeysRead:3,
			ApproximateSize:100,
		},
	}
	rngInfos = append(rngInfos, rngInfo)
	data, err := json.Marshal(rngInfos)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://localhost:8080/metric/range?clusterId=%d", clusterId)
	request, err := http.NewRequest("POST", url, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	return send(cli, request)
}

func TestMetric(t *testing.T) {
	dns := fmt.Sprintf("%s:%s@tcp(%s)/%s?readTimeout=5s&writeTimeout=5s&timeout=10s",
		user, pw, host, db)
	store := NewSqlStore(dns, 2)
	err := store.Open()
	if err != nil {
		t.Errorf("test failed, err %v", err)
		time.Sleep(time.Second)
		return
	}
	metricStart(t, store)
}

func TestEsMetric(t *testing.T){
	urls  := []string{"http://192.168.182.11:20001"}
	store := NewEsStore(urls, 2)
	err := store.Open()
	if err != nil {
		t.Errorf("test failed, err %v", err)
		time.Sleep(time.Second)
		return
	}
	metricStart(t, store)
}

func metricStart(t *testing.T, store Store)  {
	metric := NewRawMetric("localhost", 8080, store)
	go metric.Start()
	time.Sleep(time.Second)
	cli := &http.Client{}
	err := metricCluster(cli)
	if err != nil {
		t.Errorf("test failed, err %v", err)
		time.Sleep(time.Second)
		return
	}
	err = metricMac(cli, "DS", "127.0.0.1")
	if err != nil {
		t.Errorf("test failed, err %v", err)
		time.Sleep(time.Second)
		return
	}
	err = metricProcess(cli, "DS", "127.0.0.1:6000")
	if err != nil {
		t.Errorf("test failed, err %v", err)
		time.Sleep(time.Second)
		return
	}
	err = metricProcess(cli, "GS", "127.0.0.1:7000")
	if err != nil {
		t.Errorf("test failed, err %v", err)
		time.Sleep(time.Second)
		return
	}
	err = metricDb(cli, "fbase")
	if err != nil {
		t.Errorf("test failed, err %v", err)
		time.Sleep(time.Second)
		return
	}
	err = metricTable(cli, "fbase", "metric")
	if err != nil {
		t.Errorf("test failed, err %v", err)
		time.Sleep(time.Second)
		return
	}
	time.Sleep(time.Second)
	err = metricCluster(cli)
	if err != nil {
		t.Errorf("test failed, err %v", err)
		time.Sleep(time.Second)
		return
	}
	time.Sleep(time.Second * 5)
	err = metricNode(cli)
	if err != nil {
		t.Errorf("test failed, err %v", err)
		time.Sleep(time.Second)
		return
	}
	err = metricRanges(cli)
	if err != nil {
		t.Errorf("test failed, err %v", err)
		time.Sleep(time.Second)
		return
	}
	time.Sleep(time.Second * 5)
}
