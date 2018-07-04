package metric

import (
	"testing"
	"time"
	"strings"
	"github.com/olivere/elastic"
	"encoding/json"
	"golang.org/x/net/context"
	"fmt"
	"util/assert"
)

var (
	es_url = "http://192.168.182.11:20001"
)

/**
	Post cluster_meta/_search
	{
	  "stored_fields": [
		"total_capacity"
	  ],
	  "query": {
		"bool": {
		  "filter": [
			{
			  "term": {
				"cluster_id":0
			  }
			}
		  ]
		}
	  }
	}
 */

func TestClusterMetaMetricToEsStore(t *testing.T) {
	urls := []string{es_url}
	store := NewEsStore(urls, 2)
	err := store.Open()
	if err != nil {
		t.Errorf("test failed, err %v", err)
		time.Sleep(time.Second)
		return
	}
	for i := 0; i < 1; i++ {
		meta := make(map[string]interface{})
		meta["cluster_id"] = i
		meta["total_capacity"] = 11
		meta["used_capacity"] = 1
		meta["range_count"] = 2
		meta["db_count"] = 2
		meta["table_count"] = 2
		meta["ds_count"] = 2
		meta["gs_count"] = 3
		fault_List := []string{"192.12.12.111", "121.121.121.1"}
		meta["fault_list"] = strings.Join(fault_List, ";")
		meta["update_time"] = 1516121212

		err = store.Put(&Message{
			ClusterId: clusterId,
			Namespace: NAMESPACE_CLUSTER,
			Subsystem: METRIC_CLUSTER_META,
			Items:     meta,
		})

		if err != nil {
			t.Errorf("test failed, err %v", err)
			time.Sleep(time.Second)
			return
		}
	}

	time.Sleep(20 * time.Second)

}

/**
	Post range_stats/_search
	{
	  "stored_fields":["range_id","addr"],
	  "query": {
		"bool": {
		  "filter": [
			{
			  "term": {
				"addr": "192.168.111.111:9898"
			  }
			}
		  ]
		}
	  }
	}
 */
func TestBachInsertToEs(t *testing.T) {
	//mock send to es
	urls := []string{es_url}
	store := NewEsStore(urls, 2)
	err := store.Open()
	if err != nil {
		t.Errorf("test failed, err %v", err)
		return
	}
	MockRangeStats(store, t)
	time.Sleep(time.Duration(3) * time.Minute)
}

func Test_EsBulk(t *testing.T) {
	nodes := []string{es_url}
	client, err := elastic.NewClient(elastic.SetURL(nodes...), elastic.SetSniff(false))
	if err != nil {
		t.Errorf("elasticsearch client[%s] open failed, err[%v]", nodes, err)
		return
	}
	bulkRequest := client.Bulk()
	var metaList []map[string]interface{}
	for i := 3; i < 4; i++ {
		meta := make(map[string]interface{})
		meta["cluster_id"] = 1 + i
		meta["range_id"] = i + 1
		meta["addr"] = "maggie4"
		meta["bytes_written"] = 3
		meta["bytes_read"] = 4
		meta["keys_written"] = 5
		meta["keys_read"] = 6
		meta["approximate_size"] = 7
		meta["update_time"] = time.Now().Unix()
		metaList = append(metaList, meta)
	}

	for _, item := range metaList {
		r := elastic.NewBulkIndexRequest().Index(METRIC_RANGE_STATS).Type(METRIC_RANGE_STATS).Doc(item)
		body, err := json.Marshal(item)
		if err != nil {
			t.Errorf("error %v", err)
		}
		result, _ := r.Source()
		t.Logf("body is %v", string(body))
		t.Logf("source is %v", result)
		bulkRequest = bulkRequest.Add(r)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*50)
	defer cancel()
	resp, err := bulkRequest.Do(ctx)
	t.Logf("error 2 %v", err)
	t.Logf("resp  %v", resp)
}

func TestGetIndexRule(t *testing.T) {
	index := "cluster_net"
	if format := GetIndexRule(index); len(format) != 0 {
		index = fmt.Sprintf("%s-%s", index, time.Now().Format(format))
	}
	assert.Equal(t, index, "cluster_net", "ok" )

	index = "range_stats"
	if format := GetIndexRule(index); len(format) != 0 {
		index = fmt.Sprintf("%s-%s", index, time.Now().Format(format))
	}
	assert.Equal(t, index, "range_stats-201806", "ok" )
}