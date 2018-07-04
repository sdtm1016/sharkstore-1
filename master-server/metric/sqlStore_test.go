package metric

import (
	"testing"
	"time"
	"fmt"
	"model/pkg/statspb"
	"model/pkg/mspb"
	"util/assert"
)

func TestPrepare(t *testing.T) {
	net := make(map[string]interface{})
	net["cluster_id"] = 1
	net["tps"] = 11
	net["update_time"] = time.Now().Unix()
	net["min_tp"] = float32(1.2)
	net["max_tp"] = float32(3.2)
	net["avg_tp"] = float32(2)
	net["tp50"] = float32(0.1)
	net["tp90"] = float32(0.11)
	net["tp99"] = float32(0.111)
	net["tp999"] = float32(0.11)
	net["net_in_per_sec"] = int8(3)
	net["net_out_per_sec"] = int16(4)
	net["clients_count"] = int32(5)
	net["open_clients_per_sec"] = int64(6)


	format, args, err := prepare(METRIC_CLUSTER_NET, net)
	if err != nil {
		t.Errorf("test error %v", err)
		return
	}
	t.Logf("%s, %v", format, args)
}

func Test_SqlStore(t *testing.T)  {
	net := make(map[string]interface{})
	net["cluster_id"] = 1
	net["tps"] = 11
	net["update_time"] = time.Now().Unix()
	net["min_tp"] = float32(1.2)
	net["max_tp"] = float32(3.2)
	net["avg_tp"] = float32(2)
	net["tp50"] = float32(0.1)
	net["tp90"] = float32(0.11)
	net["tp99"] = float32(0.111)
	net["tp999"] = float32(0.11)
	net["net_in_per_sec"] = int8(3)
	net["net_out_per_sec"] = int16(4)
	net["clients_count"] = int32(5)
	net["open_clients_per_sec"] = int64(6)

	dns := fmt.Sprintf("%s:%s@tcp(%s)/%s?readTimeout=5s&writeTimeout=5s&timeout=10s",
		user, pw, host, db)
	store := NewSqlStore(dns, 2)
	err := store.Open()
	if err != nil {
		t.Errorf("test failed, err %v", err)
		time.Sleep(time.Second)
		return
	}
	err = store.Put(&Message{
		ClusterId: 1,
		Namespace: NAMESPACE_CLUSTER,
		Subsystem: METRIC_CLUSTER_NET,
		Items: net,
	})
	if err != nil {
		t.Errorf("test failed, err %v", err)
		return
	}
	time.Sleep(30 * time.Second)
}


func Test_BatchSqlStore(t *testing.T)  {
	//mock send to sql
	dns := fmt.Sprintf("%s:%s@tcp(%s)/%s?readTimeout=5s&writeTimeout=5s&timeout=10s",
		user, pw, host, db)
	store := NewSqlStore(dns, 2)
	err := store.Open()
	if err != nil {
		t.Errorf("test failed, err %v", err)
		time.Sleep(time.Second)
		return
	}
	MockRangeStats(store, t)
	time.Sleep(30 * time.Second)
}

func MockRangeStats(store Store, t *testing.T)  {
	rngLength := 2
	var rangeStats []*statspb.RangeInfo
	for i := 0; i < rngLength; i++ {
		rngInfo := &statspb.RangeInfo{
			RangeId:   uint64(4 + i),
			LeaderId:  13,
			NodeAdder: "192.168.111.111:9899",
			Stats: &mspb.RangeStats{
				BytesWritten:    10,
				BytesRead:       4,
				KeysWritten:     4,
				KeysRead:        3,
				ApproximateSize: 100,
			},
		}
		rangeStats = append(rangeStats, rngInfo)
	}
	assert.Equal(t, len(rangeStats), rngLength, "ok")
	var metaList []interface{}
	for _, rngStat := range rangeStats {
		if rngStat.GetStats() == nil {
			continue
		}
		meta := make(map[string]interface{})
		meta["cluster_id"] = clusterId
		meta["range_id"] = rngStat.GetRangeId()
		meta["addr"] = rngStat.GetNodeAdder()
		meta["bytes_written"] = rngStat.GetStats().GetBytesWritten()
		meta["bytes_read"] = rngStat.GetStats().GetBytesRead()
		meta["keys_written"] = rngStat.GetStats().GetKeysWritten()
		meta["keys_read"] = rngStat.GetStats().GetKeysRead()
		meta["approximate_size"] = rngStat.GetStats().GetApproximateSize()
		meta["update_time"] = time.Now().Unix()
		metaList = append(metaList, meta)
	}
	if len(metaList) > 0 {
		store.Put(&Message{
			ClusterId: clusterId,
			Namespace: NAMESPACE_RANGE,
			Subsystem: METRIC_RANGE_STATS,
			Items:     metaList,
		})
	}
}