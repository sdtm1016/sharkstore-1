package metric

import (
	"testing"
	"util/assert"
)

func TestUpdateMetric(t *testing.T) {
	metricAddress := "192.168.108.111:8887"
	GsMetric = NewMetric(1, "192.168.108.122:3360", metricAddress, 1000, "")
	assert.Equal(t, GsMetric.GetMetricAddress(), metricAddress, "error")
	newAddress := "127.0.0.1:9998"
	err := UpdateMetric(newAddress)
	if err != nil {
		t.Logf("error %v", err)
		return
	}
	assert.Equal(t, GsMetric.GetMetricAddress(), newAddress, "error")

}