package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"model/pkg/metapb"
	"net/http"
	"os"

	"crypto/md5"
	"database/sql"
	"net/url"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Host struct {
	ip    string
	ports []string
}

var baseURL string // http://{masterAddr}
var masterAddr = flag.String("maddr", "127.0.0.1:8887", "master address. eg 127.0.0.1:8887")

const clusterId = 1

const token = "test"

var nodesAddrs []*Host


var (
	GatewayMonitor = []*metapb.Column{
		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "address", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "command", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "calls", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "total_usec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "parse_usec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "call_usec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "hits", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "misses", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "slowlogs", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "trips", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "ttl", DataType: metapb.DataType_BigInt},
	}

	FbaseCluster = []*metapb.Column{
		&metapb.Column{Name: "id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "cluster_name", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "cluster_url", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "gateway_http", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "gateway_sql", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "cluster_sign", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "auto_transfer", DataType: metapb.DataType_Tinyint},
		&metapb.Column{Name: "auto_failover", DataType: metapb.DataType_Tinyint},
		&metapb.Column{Name: "auto_split", DataType: metapb.DataType_Tinyint},
		&metapb.Column{Name: "create_time", DataType: metapb.DataType_Varchar},
	}

	TestTable = []*metapb.Column{
		&metapb.Column{Name: "id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "name", DataType: metapb.DataType_Varchar},
	}
	RangeMonitor = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		//&metapb.Column{Name: "size", DataType: metapb.DataType_BigInt, },

		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "range_size", DataType: metapb.DataType_BigInt},

		&metapb.Column{Name: "ops", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "bytes_in_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "bytes_out_per_sec", DataType: metapb.DataType_BigInt},

		&metapb.Column{Name: "ttl", DataType: metapb.DataType_BigInt},
	}

	NodeProcessMonitor = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "nodeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, Unsigned: true},
		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "host", DataType: metapb.DataType_Varchar},

		&metapb.Column{Name: "cpu_percent", DataType: metapb.DataType_Double},
		&metapb.Column{Name: "io_read_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "io_write_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "io_read_bytes", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "io_write_bytes", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "memory_rss", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "memory_vms", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "memory_swap", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "memory_percent", DataType: metapb.DataType_Double},
		&metapb.Column{Name: "net_connection_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_byte_sent", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_byte_recv", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_packet_sent", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_packet_recv", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_err_in", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_err_out", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_drop_in", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_drop_out", DataType: metapb.DataType_BigInt},

		&metapb.Column{Name: "range_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "leader_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "ttl", DataType: metapb.DataType_BigInt},
	}

	NodeMonitor = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "nodeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, Unsigned: true},

		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},

		&metapb.Column{Name: "host", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "cpu_proc_rate", DataType: metapb.DataType_Double},
		&metapb.Column{Name: "total_memory", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "used_memory", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "total_swap_memory", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "used_swap_memory", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "total_disk", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "used_disk", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "connected_clients", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "range_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "bytes_in_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "bytes_out_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "load", DataType: metapb.DataType_Double},
		&metapb.Column{Name: "ttl", DataType: metapb.DataType_BigInt},
	}
	DbMonitor = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "dbid", DataType: metapb.DataType_BigInt, PrimaryKey: 1},

		//&metapb.Column{Name: "size", DataType: metapb.DataType_BigInt, },
		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "db_size", DataType: metapb.DataType_BigInt},

		&metapb.Column{Name: "ops", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "bytes_in_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "bytes_out_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "range_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "ttl", DataType: metapb.DataType_BigInt},
	}
	TableMonitor = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "tableid", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		//&metapb.Column{Name: "size", DataType: metapb.DataType_BigInt, },

		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "table_size", DataType: metapb.DataType_BigInt},

		&metapb.Column{Name: "ops", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "bytes_in_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "bytes_out_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "range_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "ttl", DataType: metapb.DataType_BigInt},
	}
	ClusterMonitor = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},

		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},

		&metapb.Column{Name: "total_capacity", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "used_capacity", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "ops", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "bytes_in_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "bytes_out_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "range_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "connected_clients", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "ttl", DataType: metapb.DataType_BigInt},
	}

	ClusterTask = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "taskid", DataType: metapb.DataType_BigInt, PrimaryKey: 1},

		&metapb.Column{Name: "finish_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},

		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "used_time", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "state", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "detail", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "ttl", DataType: metapb.DataType_BigInt},
	}

	EventStatistics = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "db_name", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "table_name", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "nodeid", DataType: metapb.DataType_BigInt, Unsigned: true},
		&metapb.Column{Name: "start_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "end_time", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "statistics_type", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "ttl", DataType: metapb.DataType_BigInt},
	}

	JimDbCluster = []*metapb.Column{
		&metapb.Column{Name: "net_io_out_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "used_memory_rss", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "_total_connections_received_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "connected_clients", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_io_in_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "used_memory", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "_expired_keys_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "instantaneous_ops_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "spaceId", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "time1", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
	}

	JimDbInstance = []*metapb.Column{
		&metapb.Column{Name: "used_memory", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "used_memory_rss", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_io_in_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_io_out_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "instantaneous_ops_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "connected_clients", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "_cpu_usage_perc", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "_cpu_usage_children_perc", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "_expired_keys_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "_evicted_keys_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "_total_connections_received_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "ip_port", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "time1", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
	}
)

func init() {
	nodesAddrs = make([]*Host, 4)

	nodesAddrs[0] = &Host{
		ip:    "192.168.31.1",
		ports: []string{"6061", "1234", "1235"},
	}
	nodesAddrs[1] = &Host{
		ip:    "192.168.31.2",
		ports: []string{"6062", "1236", "1237"},
	}
	nodesAddrs[2] = &Host{
		ip:    "192.168.31.3",
		ports: []string{"6063", "1238", "1239"},
	}


}

type httpReply struct {
	Code    int         `json:"code"`
	Message string      `json:"message`
	Data    interface{} `json:"data"`
}

func CreateNodes() {
	for _, host := range nodesAddrs {
		url := baseURL + "/node/create?d=1491381933007&s=b771b8ebcd515b2a7427c0902d02a04e&"
		url += fmt.Sprintf(`server_ip=%s&server_port=%s&raft_heartbeat_port=%s&raft_replication_port=%s&dataserver_version=1.0.1`,
			host.ip, host.ports[0], host.ports[1], host.ports[2])
		resp, err := http.Get(url)
		if err != nil {
			fmt.Println("http error: ", err)
			os.Exit(1)
		}
		if resp.Body != nil {
			data, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				fmt.Println("read resp failed, err ", err)
				os.Exit(1)
			}
			reply := &httpReply{}
			err = json.Unmarshal(data, reply)
			if err != nil {
				fmt.Println("json unmarshal failed, err ", err)
				os.Exit(1)
			}
			if reply.Code != 0 {
				fmt.Println("create nodes failed!!!!!!!!")
				//os.Exit(1)
			}
		}
	}
	fmt.Println("create nodes success")
}

func CreateNodes1() {
	for i, host := range nodesAddrs {
		if i+1 < len(nodesAddrs) {
			continue
		}
		url := baseURL + "/node/create?d=1491381933007&s=b771b8ebcd515b2a7427c0902d02a04e&"
		url += fmt.Sprintf(`server_ip=%s&server_port=%s&raft_heartbeat_port=%s&raft_replication_port=%s&dataserver_version=1.0.1`,
			host.ip, host.ports[0], host.ports[1], host.ports[2])
		resp, err := http.Get(url)
		if err != nil {
			fmt.Println("http error: ", err)
			os.Exit(1)
		}
		if resp.Body != nil {
			data, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				fmt.Println("read resp failed, err ", err)
				os.Exit(1)
			}
			reply := &httpReply{}
			err = json.Unmarshal(data, reply)
			if err != nil {
				fmt.Println("json unmarshal failed, err ", err)
				os.Exit(1)
			}
			if reply.Code != 0 {
				fmt.Println("create nodes failed!!!!!!!!")
				os.Exit(1)
			}
		}
	}
	fmt.Println("create nodes success")
}

func CreateDatabase(name string) {
	var info = url.Values{}
	timeout := time.Now().Unix()
	s := sign(clusterId, timeout, []byte(token))
	info.Set("name", name)
	info.Set("properties", "test")
	info.Set("d", fmt.Sprintf("%d", timeout))
	info.Set("s", fmt.Sprintf("%x", s))
	uu := info.Encode()
	url := baseURL + fmt.Sprintf("/manage/database/create?%s", uu)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("http error: ", err)
		os.Exit(1)
	}
	if resp.Body != nil {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("read resp failed, err ", err)
			os.Exit(1)
		}
		reply := &httpReply{}
		err = json.Unmarshal(data, reply)
		if err != nil {
			fmt.Printf("json unmarshal failed, data[%s] err [%v]\n", string(data), err)
			os.Exit(1)
		}
		if reply.Code != 0 {
			fmt.Println("create database failed!!!!!!!!")
			os.Exit(1)
		}
	}
	fmt.Println("create database success")
}

func initData() {

	var info = url.Values{}
	timeout := time.Now().Unix()
	s := sign(clusterId, timeout, []byte(token))
	info.Set("d", fmt.Sprintf("%d", timeout))
	info.Set("s", fmt.Sprintf("%x", s))
	uu := info.Encode()
	url := baseURL + fmt.Sprintf("/manage/cluster/init?%s", uu)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("http error: ", err)
		os.Exit(1)
	}
	if resp.Body != nil {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("read resp failed, err ", err)
			os.Exit(1)
		}
		reply := &httpReply{}
		err = json.Unmarshal(data, reply)
		if err != nil {
			fmt.Printf("json unmarshal failed, data[%s] err [%v]\n", string(data), err)
			os.Exit(1)
		}
		if reply.Code != 0 {
			fmt.Println("initData failed!!!!!!!!")
			os.Exit(1)
		}
	}
	fmt.Println("initData success\n\n\n")
}

func getInitTable() {

	var info = url.Values{}
	timeout := time.Now().Unix()
	s := sign(clusterId, timeout, []byte(token))
	info.Set("dbName", "fbase")
	info.Set("d", fmt.Sprintf("%d", timeout))
	info.Set("s", fmt.Sprintf("%x", s))
	uu := info.Encode()
	url := baseURL + fmt.Sprintf("/manage/table/getall?%s", uu)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("http error: ", err)
		os.Exit(1)
	}
	if resp.Body != nil {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("read resp failed, err ", err)
			os.Exit(1)
		}
		reply := &httpReply{}
		err = json.Unmarshal(data, reply)
		if err != nil {
			fmt.Printf("json unmarshal failed, data[%s] err [%v]\n", string(data), err)
			os.Exit(1)
		}
		if reply.Code != 0 {
			fmt.Println("getInitTable failed!!!!!!!!")
			os.Exit(1)
		}
		fmt.Printf("tables %v \n\n", reply)
	}
	fmt.Println("getInitTable success\n\n\n\n")
}

func getNodes() {

	var info = url.Values{}
	timeout := time.Now().Unix()
	s := sign(clusterId, timeout, []byte(token))
	info.Set("d", fmt.Sprintf("%d", timeout))
	info.Set("s", fmt.Sprintf("%x", s))
	uu := info.Encode()
	url := baseURL + fmt.Sprintf("/manage/node/getall?%s", uu)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("http error: ", err)
		os.Exit(1)
	}
	if resp.Body != nil {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("read resp failed, err ", err)
			os.Exit(1)
		}
		reply := &httpReply{}
		err = json.Unmarshal(data, reply)
		if err != nil {
			fmt.Printf("json unmarshal failed, data[%s] err [%v]\n", string(data), err)
			os.Exit(1)
		}
		if reply.Code != 0 {
			fmt.Println("getNodes failed!!!!!!!!")
			os.Exit(1)
		}
		fmt.Printf("reply:%v\n\n", reply)
	}
	fmt.Println("getNodes success")
}

type TableProperty struct {
	Columns []*metapb.Column `json:"columns"`
}

func CreateTable(name string) {
	size := 10 * 1024 * 1024
	url := baseURL + "/table/create?d=1491381933007&s=b771b8ebcd515b2a7427c0902d02a04e&"
	url += fmt.Sprintf(`dbName=test&tableName=%s&rangenumber=1&peernumber=3&memdbsize=%d&`, name, size)
	property := &TableProperty{}
	columns := make([]*metapb.Column, 3)
	column := &metapb.Column{
		DataType:   metapb.DataType_Varchar,
		Index:      true,
		Name:       "user_name",
		PrimaryKey: 1,
		Nullable:   false,
	}
	columns[0] = column

	column = &metapb.Column{
		DataType:   metapb.DataType_Varchar,
		Index:      true,
		Name:       "pass_word",
		PrimaryKey: 0,
		Nullable:   false,
	}
	columns[1] = column

	column = &metapb.Column{
		DataType:   metapb.DataType_Varchar,
		Index:      true,
		Name:       "real_name",
		PrimaryKey: 0,
		Nullable:   false,
	}
	columns[2] = column

	property.Columns = columns
	data, err := json.Marshal(property)
	if err != nil {
		fmt.Println("json marshal failed")
		os.Exit(1)
	}
	url += fmt.Sprintf(`properties=%s`, string(data))
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("http error: ", err)
		os.Exit(1)
	}
	if resp.Body != nil {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("read resp failed, err ", err)
			os.Exit(1)
		}
		reply := &httpReply{}
		err = json.Unmarshal(data, reply)
		if err != nil {
			fmt.Println("json unmarshal failed, err ", err)
			os.Exit(1)
		}
		if reply.Code != 0 {
			fmt.Println("create table failed!!!!!!!!")
			os.Exit(1)
		}
	}
	fmt.Println("create table success")
}

func CreateTable1() {
	size := 10 * 1024 * 1024
	url := baseURL + "/table/create?d=1491381933007&s=b771b8ebcd515b2a7427c0902d02a04e&"
	url += fmt.Sprintf(`dbName=test&tableName=split&rangenumber=1&peernumber=3&memdbsize=%d&`, size)
	property := &TableProperty{}
	columns := make([]*metapb.Column, 2)
	column := &metapb.Column{
		DataType:   metapb.DataType_Varchar,
		Index:      true,
		Name:       "id",
		PrimaryKey: 1,
		Nullable:   false,
	}
	columns[0] = column

	column = &metapb.Column{
		DataType:   metapb.DataType_Varchar,
		Index:      true,
		Name:       "name",
		PrimaryKey: 1,
		Nullable:   false,
	}
	columns[1] = column

	property.Columns = columns
	data, err := json.Marshal(property)
	if err != nil {
		fmt.Println("json marshal failed")
		os.Exit(1)
	}
	url += fmt.Sprintf(`properties=%s`, string(data))
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("http error: ", err)
		os.Exit(1)
	}
	if resp.Body != nil {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("read resp failed, err ", err)
			os.Exit(1)
		}
		reply := &httpReply{}
		err = json.Unmarshal(data, reply)
		if err != nil {
			fmt.Println("json unmarshal failed, err ", err)
			os.Exit(1)
		}
		if reply.Code != 0 {
			fmt.Println("create table failed!!!!!!!!")
			os.Exit(1)
		}
	}
	fmt.Println("create table success")
}

func CreateTable2() {
	size := 10 * 1024 * 1024
	url := baseURL + "/table/create?d=1491381933007&s=b771b8ebcd515b2a7427c0902d02a04e&"
	//url += fmt.Sprintf(`dbname=test&tablename=split2&rangenumber=4&peernumber=3&pkdupcheck=false&memdbsize=%d&`, size)
	url += fmt.Sprintf(`dbName=test&tableName=split1&rangenumber=4&peernumber=3&memdbsize=%d&`, size)
	property := &TableProperty{}
	columns := make([]*metapb.Column, 3)
	column := &metapb.Column{
		DataType:   metapb.DataType_Varchar,
		Index:      true,
		Name:       "user_name",
		PrimaryKey: 1,
		Nullable:   false,
	}
	columns[0] = column

	column = &metapb.Column{
		DataType:   metapb.DataType_Varchar,
		Index:      true,
		Name:       "pass_word",
		PrimaryKey: 0,
		Nullable:   false,
	}
	columns[1] = column

	column = &metapb.Column{
		DataType:   metapb.DataType_Varchar,
		Index:      true,
		Name:       "real_name",
		PrimaryKey: 0,
		Nullable:   false,
	}
	columns[2] = column

	property.Columns = columns
	data, err := json.Marshal(property)
	if err != nil {
		fmt.Println("json marshal failed")
		os.Exit(1)
	}
	url += fmt.Sprintf(`properties=%s`, string(data))
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("http error: ", err)
		os.Exit(1)
	}
	if resp.Body != nil {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("read resp failed, err ", err)
			os.Exit(1)
		}
		reply := &httpReply{}
		err = json.Unmarshal(data, reply)
		if err != nil {
			fmt.Println("json unmarshal failed, err ", err)
			os.Exit(1)
		}
		if reply.Code != 0 {
			fmt.Println("create table failed!!!!!!!!")
			os.Exit(1)
		}
	}
	fmt.Println("create table success")
}

func CreateTable3() {
	size := 10 * 1024 * 1024
	url := baseURL + "/table/create?d=1491381933007&s=b771b8ebcd515b2a7427c0902d02a04e&"
	//url += fmt.Sprintf(`dbname=test&tablename=split2&rangenumber=4&peernumber=3&pkdupcheck=false&memdbsize=%d&`, size)
	url += fmt.Sprintf(`dbName=test&tableName=lltest&rangenumber=4&peernumber=3&memdbsize=%d&rangekeys=10&`, size)
	property := &TableProperty{}
	columns := make([]*metapb.Column, 3)
	column := &metapb.Column{
		DataType:   metapb.DataType_Varchar,
		Index:      true,
		Name:       "user_name",
		PrimaryKey: 1,
		Nullable:   false,
	}
	columns[0] = column

	column = &metapb.Column{
		DataType:   metapb.DataType_Varchar,
		Index:      true,
		Name:       "pass_word",
		PrimaryKey: 0,
		Nullable:   false,
	}
	columns[1] = column

	column = &metapb.Column{
		DataType:   metapb.DataType_Varchar,
		Index:      true,
		Name:       "real_name",
		PrimaryKey: 0,
		Nullable:   false,
	}
	columns[2] = column

	property.Columns = columns
	data, err := json.Marshal(property)
	if err != nil {
		fmt.Println("json marshal failed")
		os.Exit(1)
	}
	url += fmt.Sprintf(`properties=%s`, string(data))
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("http error: ", err)
		os.Exit(1)
	}
	if resp.Body != nil {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("read resp failed, err ", err)
			os.Exit(1)
		}
		reply := &httpReply{}
		err = json.Unmarshal(data, reply)
		if err != nil {
			fmt.Println("json unmarshal failed, err ", err)
			os.Exit(1)
		}
		if reply.Code != 0 {
			fmt.Println("create table failed!!!!!!!!")
			os.Exit(1)
		}
	}
	fmt.Println("create table success")
}

func sign(clusterId, timeout int64, token []byte) []byte {
	h := md5.New()
	h.Write([]byte(fmt.Sprintf("%d", clusterId)))
	h.Write([]byte(fmt.Sprintf("%d", timeout)))
	h.Write(token)
	return h.Sum(nil)
}

func CreateTable4(dbname, tablename string) {
	var info = url.Values{}
	property := &TableProperty{}
	columns := make([]*metapb.Column, 3)
	column := &metapb.Column{
		DataType:   metapb.DataType_Varchar,
		Index:      true,
		Name:       "user_name",
		PrimaryKey: 1,
		Nullable:   false,
	}
	columns[0] = column

	column = &metapb.Column{
		DataType:   metapb.DataType_Varchar,
		Index:      true,
		Name:       "pass_word",
		PrimaryKey: 0,
		Nullable:   false,
	}
	columns[1] = column

	column = &metapb.Column{
		DataType:   metapb.DataType_Varchar,
		Index:      true,
		Name:       "real_name",
		PrimaryKey: 0,
		Nullable:   false,
	}
	columns[2] = column

	//column := &metapb.Column{
	//	DataType:   metapb.DataType_Varchar,
	//	Index:      true,
	//	Name:       "id",
	//	PrimaryKey: 1,
	//	Nullable:   false,
	//}
	//columns[0] = column

	//column = &metapb.Column{
	//	DataType:   metapb.DataType_Varchar,
	//	Index:      true,
	//	Name:       "name",
	//	PrimaryKey: 1,
	//	Nullable:   false,
	//}
	//columns[1] = column

	property.Columns = columns
	data, err := json.Marshal(property)
	if err != nil {
		fmt.Println("json marshal failed")
		os.Exit(1)
	}
	timeout := time.Now().Unix()
	s := sign(clusterId, timeout, []byte(token))
	info.Set("dbName", dbname)
	info.Set("tableName", tablename)
	info.Set("properties", string(data))
	info.Set("d", fmt.Sprintf("%d", timeout))
	info.Set("s", fmt.Sprintf("%x", s))
	uu := info.Encode()
	url := baseURL + fmt.Sprintf("/manage/table/create?%s", uu)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("http error: ", err)
		os.Exit(1)
	}
	if resp.Body != nil {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("read resp failed, err ", err)
			os.Exit(1)
		}
		reply := &httpReply{}
		err = json.Unmarshal(data, reply)
		if err != nil {
			fmt.Println("json unmarshal failed, err ", err)
			os.Exit(1)
		}
		if reply.Code != 0 {
			fmt.Println("create table failed!!!!!!!!")
			os.Exit(1)
		}
	}
	fmt.Println("create table success")
}

func CreateTable5(dbname, tablename string) {
	var info = url.Values{}
	property := &TableProperty{}
	columns := make([]*metapb.Column, 4)
	column := &metapb.Column{
		DataType:   metapb.DataType_Varchar,
		Index:      true,
		Name:       "h",
		PrimaryKey: 1,
		Nullable:   false,
	}
	columns[0] = column

	column = &metapb.Column{
		DataType:   metapb.DataType_Varchar,
		Index:      true,
		Name:       "user_name",
		PrimaryKey: 1,
		Nullable:   false,
	}
	columns[1] = column

	column = &metapb.Column{
		DataType:   metapb.DataType_Varchar,
		Index:      true,
		Name:       "pass_word",
		PrimaryKey: 0,
		Nullable:   false,
	}
	columns[2] = column

	column = &metapb.Column{
		DataType:   metapb.DataType_Varchar,
		Index:      true,
		Name:       "real_name",
		PrimaryKey: 0,
		Nullable:   false,
	}
	columns[3] = column

	//column := &metapb.Column{
	//	DataType:   metapb.DataType_Varchar,
	//	Index:      true,
	//	Name:       "id",
	//	PrimaryKey: 1,
	//	Nullable:   false,
	//}
	//columns[0] = column

	//column = &metapb.Column{
	//	DataType:   metapb.DataType_Varchar,
	//	Index:      true,
	//	Name:       "name",
	//	PrimaryKey: 1,
	//	Nullable:   false,
	//}
	//columns[1] = column

	property.Columns = columns
	data, err := json.Marshal(property)
	if err != nil {
		fmt.Println("json marshal failed")
		os.Exit(1)
	}
	timeout := time.Now().Unix()
	s := sign(clusterId, timeout, []byte(token))
	info.Set("dbName", dbname)
	info.Set("tableName", tablename)
	info.Set("properties", string(data))
	info.Set("d", fmt.Sprintf("%d", timeout))
	info.Set("s", fmt.Sprintf("%x", s))
	uu := info.Encode()
	url := baseURL + fmt.Sprintf("/manage/table/create?%s", uu)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("http error: ", err)
		os.Exit(1)
	}
	if resp.Body != nil {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("read resp failed, err ", err)
			os.Exit(1)
		}
		reply := &httpReply{}
		err = json.Unmarshal(data, reply)
		if err != nil {
			fmt.Println("json unmarshal failed, err ", err)
			os.Exit(1)
		}
		if reply.Code != 0 {
			fmt.Println("create table failed!!!!!!!!")
			os.Exit(1)
		}
	}
	fmt.Println("create table success")
}

func CreateTableNew(dbName, tableName string, columns []*metapb.Column) {
	size := 10 * 1024 * 1024
	url := baseURL + "/table/create?d=1491381933007&s=b771b8ebcd515b2a7427c0902d02a04e&"
	url += fmt.Sprintf(`dbName=%s&tableName=%s&rangenumber=1&peernumber=3&memdbsize=%d&`, dbName, tableName, size)
	property := &TableProperty{}
	property.Columns = columns
	data, err := json.Marshal(property)
	if err != nil {
		fmt.Println("json marshal failed")
		os.Exit(1)
	}
	url += fmt.Sprintf(`properties=%s`, string(data))
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("http error: ", err)
		os.Exit(1)
	}
	if resp.Body != nil {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("read resp failed, err ", err)
			os.Exit(1)
		}
		reply := &httpReply{}
		err = json.Unmarshal(data, reply)
		if err != nil {
			fmt.Println("json unmarshal failed, err ", err)
			os.Exit(1)
		}
		if reply.Code != 0 {
			fmt.Println("create table failed!!!!!!!! ", tableName)
			os.Exit(1)
		}
	}
	fmt.Println("create table success")
}

func CreateMonitor() error {
	CreateDatabase("fbase")
	CreateTableNew("fbase", "range_monitor", RangeMonitor)
	CreateTableNew("fbase", "node_monitor", NodeMonitor)
	CreateTableNew("fbase", "node_process_monitor", NodeProcessMonitor)
	CreateTableNew("fbase", "db_monitor", DbMonitor)
	CreateTableNew("fbase", "table_monitor", TableMonitor)
	CreateTableNew("fbase", "cluster_monitor", ClusterMonitor)
	CreateTableNew("fbase", "cluster_task", ClusterTask)
	CreateTableNew("fbase", "fbase_cluster", FbaseCluster)
	CreateTableNew("fbase", "event_statistics", EventStatistics)
	return nil
}

func CreateTest() {
	CreateDatabase("db0")
	CreateTableNew("db0", "t0", TestTable)
}

func CreateJimDb() error {
	CreateDatabase("jimdb")
	CreateTableNew("jimdb", "jimdb_cluster", JimDbCluster)
	CreateTableNew("jimdb", "jimdb_instance", JimDbInstance)
	return nil
}

func CreateJimDb1() error {
	CreateDatabase("jimdb")
	for i := 1; i <= 20; i++ {
		jimdb_cluster := fmt.Sprintf("jimdb_cluster%d", i)
		jimdb_instance := fmt.Sprintf("jimdb_instance%d", i)
		CreateTableNew("jimdb", jimdb_cluster, JimDbCluster)
		CreateTableNew("jimdb", jimdb_instance, JimDbInstance)
	}

	return nil
}

func insert20() error {
	user := "test"
	password := "123456"
	host := "127.0.0.1:6060"
	database := "test"
	table := "lltest"
	dns := fmt.Sprintf("%s:%s@tcp(%s)/%s", user, password, host, database)
	db, err := sql.Open("mysql", dns)
	if err != nil {
		fmt.Println(err)
		return err
	}
	for i := 0; i < 20; i++ {
		u := fmt.Sprintf("%d", i+1)
		p := fmt.Sprintf("%d", time.Now().UnixNano())
		r := fmt.Sprintf("%d", time.Now().UnixNano())
		SQL := fmt.Sprintf(`INSERT INTO %s(user_name, pass_word, real_name) VALUES("%s","%s","%s")`, table, u, p, r)
		_, err := db.Exec(SQL)
		if err != nil {
			fmt.Println("insert failed, ", err)
			return err
		}
	}
	fmt.Println("insert success!!!")
	return nil
}

func selectLimit() error {
	user := "test"
	password := "123456"
	host := "127.0.0.1:6060"
	database := "test"
	table := "lltest"
	dns := fmt.Sprintf("%s:%s@tcp(%s)/%s", user, password, host, database)
	db, err := sql.Open("mysql", dns)
	if err != nil {
		fmt.Println(err)
		return err
	}
	SQL := fmt.Sprintf(`select * from %s limit 0,2`, table)
	var user_name, pass_word, real_name string
	row, err := db.Query(SQL)
	if err != nil {
		fmt.Println("select failed ", err)
		return err
	}
	defer row.Close()
	for row.Next() {
		row.Scan(&user_name, &pass_word, &real_name)
		fmt.Println(user_name, pass_word, real_name)
	}
	return nil
}

func main() {
	flag.Parse()
	baseURL = "http://" + *masterAddr

	getNodes()

	initData()

	getInitTable()
	//CreateNodes()
	CreateDatabase("test")
	CreateTable4("test", "sharkstore")
	CreateTable5("test", "sharkstore1")

}
