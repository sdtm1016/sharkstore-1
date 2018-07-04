package server

import (
	"testing"
	"time"
	"model/pkg/metapb"
	"util/assert"
	"util"
)

func TestMetricTask(t *testing.T) {
	mockCluster := MockCluster(t)
	defer closeCluster(mockCluster)
	metric := NewMetric(mockCluster, "192.168.108.111:8887", 30 * time.Second)
	metric.Run()
	event := NewAddPeerEvent(12, 1, &metapb.Peer{Id: 2, NodeId: 1}, "test")
	metric.CollectEvent(event)
	event = NewAddPeerEvent(22, 1, &metapb.Peer{Id: 3, NodeId: 1}, "test2")
	metric.CollectEvent(event)
	complexEvent := NewChangePeerEvent(32, &Range{Range: &metapb.Range{Id: 1}}, &metapb.Peer{Id: 3, NodeId: 1}, &metapb.Peer{Id: 4, NodeId: 2},
		"test3")
	if len(metric.eventList) != 2 {
		t.Fatal("test failed")
		return
	}
	if metric.eventList[0].GetRangeID() != 1 {
		t.Fatal("test failed")
		return
	}
	if metric.eventList[1].GetRangeID() != 1 {
		t.Fatal("test failed")
		return
	}
	metric.CollectEvent(complexEvent)
	t.Logf("%v", metric.eventList)
	wg.Add(1)
	wg.Wait()
}

func TestMetricScheduler(t *testing.T) {
	metric := NewMetric(nil, "127.0.0.1:8887", time.Second)
	metric.CollectScheduleCounter("a", "AA")
	metric.CollectScheduleCounter("a", "AA")
	metric.CollectScheduleCounter("b", "BB")
	if metric.scheduleCounter["a"]["AA"] != 2 {
		t.Fatal("test failed")
		return
	}
	if metric.scheduleCounter["b"]["BB"] !=1 {
		t.Fatal("test failed")
		return
	}
}

func TestMetricStore(t *testing.T)  {
	mockCluster := MockCluster(t)
	defer closeCluster(mockCluster)
	assert.Nil(t, mockCluster.metric)
	metricInterval := util.NewDuration(time.Duration(20) * time.Second)
	metricConfig := &MetricConfig{Interval: metricInterval, Address: "192.168.108.111:8887"}
	err := mockCluster.StoreMetricConfig(metricConfig)
	assert.Nil(t, err)
	getConfig, err := mockCluster.loadMetricConfig()
	assert.Nil(t, err)
	t.Logf("%v", getConfig)
	interval := metricConfig.Interval
	address := metricConfig.Address
	t.Logf("metric config : %v, %v", interval, address)

}

func TestMetric_Update(t *testing.T)  {
	mockCluster := MockCluster(t)
	defer closeCluster(mockCluster)
	assert.Nil(t, mockCluster.metric)
	//mock config by config.toml
	mockCluster.opt.MetricInterval = time.Duration(10) * time.Second
	mockCluster.opt.MetricAddr = "127.0.0.1:8887"

	//mock http handle, set metric send config
	metricInterval := util.NewDuration(time.Duration(30) * time.Second)
	metricConfig := &MetricConfig{Interval: metricInterval, Address: "127.0.0.2:8887"}
	err := mockCluster.StoreMetricConfig(metricConfig)
	assert.Nil(t, err)

	mockMetric := initMetricSender(mockCluster, mockCluster.opt)
	assert.NotNil(t, mockMetric)
	mockMetric.Run()
	t.Logf("current metric config is: %v", mockCluster.opt)
	mockCluster.metric = mockMetric

	for i:=0; i< 1000; i++ {
		event := NewAddPeerEvent(uint64(i), uint64(i), &metapb.Peer{Id: uint64(i), NodeId: uint64(i)}, "test")
		mockMetric.CollectEvent(event)
	}

	assert.Equal(t, len(mockMetric.eventList), 1000, "ok")

	err = UpdateMetric(mockCluster, "192.168.108.111:8887", metricInterval.Duration)
	assert.Nil(t, err)

	nowConfig, err := mockCluster.loadMetricConfig()
	t.Logf("new metric config is: %v", nowConfig)
	t.Logf("current event list is: %v", len(mockCluster.metric.eventList))

	assert.Nil(t, err)
	assert.NotNil(t, mockCluster.metric)
	time.Sleep(time.Duration(2) * time.Minute)

}