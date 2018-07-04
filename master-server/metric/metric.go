package metric

import (
	"fmt"
	"strings"
	"util/server"
	"sync"

	"golang.org/x/net/context"
	"sync/atomic"
	"util/log"
	"model/pkg/statspb"
	"model/pkg/mspb"
	"time"
	"util/alarm"
)

const (
	TYPE_SQL           = "sql"
	TYPE_ELASTICSEARCH = "elasticsearch"

	NAMESPACE_CLUSTER  = "cluster"
	NAMESPACE_MAC      = "mac"
	NAMESPACE_PROCESS  = "process"
	NAMESPACE_DB       = "db"
	NAMESPACE_TABLE    = "table"
	NAMESPACE_TASK     = "task"
	NAMESPACE_SCHEDULE = "schedule"
	NAMESPACE_HOTSPOT  = "hotspot"
	NAMESPACE_NODE     = "node"
	NAMESPACE_RANGE    = "range"

	METRIC_TASK_STATS       = "task_stats"
	METRIC_CLUSTER_META     = "cluster_meta"
	METRIC_CLUSTER_NET      = "cluster_net"
	METRIC_CLUSTER_SLOWLOG  = "cluster_slowlog"
	METRIC_MAC_META         = "mac_meta"
	METRIC_MAC_NET          = "mac_net"
	METRIC_MAC_MEM          = "mac_mem"
	METRIC_MAC_DISK         = "mac_disk"
	METRIC_PROCESS_META     = "process_meta"
	METRIC_PROCESS_DISK     = "process_disk"
	METRIC_PROCESS_NET      = "process_net"
	METRIC_PROCESS_DS       = "process_ds"
	METRIC_DB_META          = "db_meta"
	METRIC_TABLE_META       = "table_meta"
	METRIC_SCHEDULE_COUNTER = "schedule_counter"
	METRIC_HOTSPOT          = "hotspot_stats"
	METRIC_NODE_STATS       = "node_stats"
	METRIC_RANGE_STATS      = "range_stats"
)
type NodeThreshold struct {
	CapacityUsedRate uint64 `toml:"capacity-used-rate" json:"capacity-used-rate"` // capacity/used_city in node stats
	WriteBps uint64 	`toml:"write-bps" json:"write-bps"`
	WriteOps uint64 	`toml:"write-ops" json:"write-ops"`
	ReadBps uint64 		`toml:"read-bps" json:"read-bps"`
	ReadOps uint64 		`toml:"read-ops" json:"read-ops"`
}

type RangeThreshold struct {
	WriteBps uint64 	`toml:"write-bps" json:"write-bps"`
	WriteOps uint64 	`toml:"write-ops" json:"write-ops"`
	ReadBps uint64 		`toml:"read-bps" json:"read-bps"`
	ReadOps uint64 		`toml:"read-ops" json:"read-ops"`
}

type ThresholdConfig struct {
	Node NodeThreshold `toml:"node-threshold" json:"node-threshold"`
	Range RangeThreshold `toml:"range-threshold" json:"range-threshold"`
}


type Metric struct {
	ip     string
	port   uint16
	server *server.Server

	AlarmCli *alarm.Client
	Threshold ThresholdConfig

	store  Store
	ctx    context.Context
	cancel context.CancelFunc

	items *ClusterCache

	wg sync.WaitGroup

	connsLock sync.Mutex
	conns     map[*conn]struct{}

	// TODO alarm filter
	index uint64
}

func NewRawMetric(ip string, port uint16, store Store) *Metric {
	ctx, cancel := context.WithCancel(context.Background())
	return &Metric{
		ip:     ip,
		port:   port,
		store:  store,
		ctx:    ctx,
		cancel: cancel,
		items:  NewClusterCache(),
		conns:  make(map[*conn]struct{}),
	}
}

func NewMetric(svr *server.Server, store Store, threshold ThresholdConfig) *Metric {
	ctx, cancel := context.WithCancel(context.Background())
	m := &Metric{
		Threshold: threshold,
	}
	m.server = svr

	m.store = store
	m.ctx = ctx
	m.cancel = cancel
	m.items = NewClusterCache()
	m.conns = make(map[*conn]struct{})

	m.initHttpHandle()
	return m
}

func (m *Metric) initHttpHandle() {
	svr := m.server
	svr.Handle("/metric/mac", m.handleMacMetric)
	svr.Handle("/metric/process", m.handleProcessMetric)
	svr.Handle("/metric/slowlog", m.handleSlowLogMetric)
	svr.Handle("/metric/db", m.handleDbMetric)
	svr.Handle("/metric/table", m.handleTableMetric)
	svr.Handle("/metric/cluster", m.handleClusterMetric)
	svr.Handle("/metric/node", m.handleNodeMetric)
	svr.Handle("/metric/range", m.handleRangeMetric)
	svr.Handle("/metric/event", m.handleEventMetric)
	svr.Handle("/metric/schedule", m.handleScheduleMetric)
	svr.Handle("/metric/hotspot", m.handleHotspotMetric)
	svr.Handle("/metric/get/mac", m.handleGetMacMetric)
	svr.Handle("/metric/get/process", m.handleGetProcessMetric)
	svr.Handle("/metric/get/db", m.handleGetDbMetric)
	svr.Handle("/metric/get/table", m.handleGetTableMetric)
	svr.Handle("/metric/get/cluster", m.handleGetClusterMetric)
	svr.Handle("/metric/tcp/process", m.handleTcpProcessMetric)
	svr.Handle("/metric/tcp/mac", m.handleTcpMacMetric)
	svr.Handle("/metric/tcp/cluster", m.handleTcpClusterMetric)
	svr.Handle("/metric/tcp/db", m.handleTcpDbMetric)
	svr.Handle("/metric/tcp/table", m.handleTcpTableMetric)
}

func (m *Metric) Start() {
	s := server.NewServer()
	s.Init("metric", &server.ServerConfig{
		Ip:      m.ip,
		Port:    fmt.Sprintf("%d", m.port),
		Version: "v1",
	})
	m.server = s
	m.initHttpHandle()
	//go m.collect()
	s.Run()
}

func (m *Metric) Open() error {
	return m.store.Open()
}

func (m *Metric) Stop() {
	if m.server != nil {
		m.server.Close()
	}
}

func (m *Metric) pushCluster(clusterId uint64, cluster *Cluster) {
	var tps, gsNum, totalNumber, errNumber uint64
	var max, min, avg, tp50, tp90, tp99, tp999 float64
	var count int = 0
	for _, proc := range cluster.ProcessCache.GetAll() {
		if proc.Type == "GS" {
			gsNum++
			if count == 0 {
				max = proc.Item.TpStats.Max
				min = proc.Item.TpStats.Min
				tp50 = proc.Item.TpStats.Tp_50
				tp90 = proc.Item.TpStats.Tp_90
				tp99 = proc.Item.TpStats.Tp_99
				tp999 = proc.Item.TpStats.Tp_999
			} else {
				if proc.Item.TpStats.Max > max {
					max = proc.Item.TpStats.Max
				}
				if proc.Item.TpStats.Min < min {
					min = proc.Item.TpStats.Min
				}
				if proc.Item.TpStats.Tp_50 > tp50 {
					tp50 = proc.Item.TpStats.Tp_50
				}
				if proc.Item.TpStats.Tp_90 > tp90 {
					tp90 = proc.Item.TpStats.Tp_90
				}
				if proc.Item.TpStats.Tp_99 > tp99 {
					tp99 = proc.Item.TpStats.Tp_99
				}
				if proc.Item.TpStats.Tp_999 > tp999 {
					tp999 = proc.Item.TpStats.Tp_999
				}
			}
			tps += proc.Item.TpStats.Tps
			avg += proc.Item.TpStats.Avg
			totalNumber += proc.Item.TpStats.TotalNumber
			errNumber += proc.Item.TpStats.ErrNumber
			count++
		} else if proc.Type == "DS" {
			//totalSize += proc.Item.GetDiskStats().GetDiskTotal()
			//usedSize += proc.Item.GetDiskStats().GetDiskUsed()
		}
	}
	if count != 0 {
		avg = avg / float64(count)
	}
	cluster.Item.GsNum = gsNum

	cluster.Item.Tps = tps
	cluster.Item.Max = max
	cluster.Item.Min = min
	cluster.Item.Avg = avg
	cluster.Item.TP50 = tp50
	cluster.Item.TP90 = tp90
	cluster.Item.TP99 = tp99
	cluster.Item.TP999 = tp999
	cluster.Item.TotalNumber = totalNumber
	cluster.Item.ErrNumber = errNumber
	//todo NetTcpConnections、NetTcpActiveOpensPerSec、NetIoInBytePerSec、NetIoOutBytePerSec

	item := cluster.Item
	// cluster meta
	meta := make(map[string]interface{})
	/*
	cluster_id, total_capacity, used_capacity, range_count, db_count, table_count, ds_count, task_count, gs_count,
		fault_list, update_time
	*/
	meta["cluster_id"] = clusterId
	meta["total_capacity"] = item.CapacityTotal
	meta["used_capacity"] = item.SizeUsed
	meta["range_count"] = item.RangeNum
	meta["db_count"] = item.DbNum
	meta["table_count"] = item.TableNum
	meta["node_down_count"] = item.NodeDownCount
	meta["node_up_count"] = item.NodeUpCount
	meta["node_offline_count"] = item.NodeOfflineCount
	meta["node_tombstone_count"] = item.NodeTombstoneCount
	meta["leader_balance_ratio"] = item.LeaderBalanceRatio
	meta["region_balance_ratio"] = item.RegionBalanceRatio
	meta["gs_count"] = item.GsNum
	meta["update_time"] = item.UpdateTime

	log.Debug("push cluster[%d] metric to store service", clusterId)

	m.store.Put(&Message{
		ClusterId: clusterId,
		Namespace: NAMESPACE_CLUSTER,
		Subsystem: METRIC_CLUSTER_META,
		Items:     meta,
	})

	// cluster net
	/*
	cluster_id, tps, min_tp, max_tp, avg_tp, tp50, tp90, tp99, tp999, net_in_per_sec, net_out_per_sec, clients_count, open_clients_per_sec, create_time
	*/
	net := make(map[string]interface{})
	net["cluster_id"] = clusterId
	net["tps"] = item.Tps
	net["min_tp"] = item.Min
	net["max_tp"] = item.Max
	net["avg_tp"] = item.Avg
	net["tp50"] = item.TP50
	net["tp90"] = item.TP90
	net["tp99"] = item.TP99
	net["tp999"] = item.TP999
	net["total_number"] = item.TotalNumber
	net["err_number"] = item.ErrNumber
	net["net_in_per_sec"] = item.NetIoInBytePerSec
	net["net_out_per_sec"] = item.NetIoOutBytePerSec
	net["clients_count"] = item.NetTcpConnections
	net["open_clients_per_sec"] = item.NetTcpActiveOpensPerSec
	net["update_time"] = item.UpdateTime

	m.store.Put(&Message{
		ClusterId: clusterId,
		Namespace: NAMESPACE_CLUSTER,
		Subsystem: METRIC_CLUSTER_NET,
		Items:     net,
	})
}

func (m *Metric) pushMac(clusterId uint64, item *MacItem) {
	/*
	mac_meta: cluster_id, type, ip, cpu_rate, load1, load5, load15, process_num, thread_num, handle_num, create_time
	mac_net: cluster_id, type, ip, net_io_in_byte_per_sec, net_io_out_byte_per_sec, net_io_in_package_per_sec, net_io_out_package_per_sec,
			 net_tcp_connections, net_tcp_active_opens_per_sec, net_ip_recv_package_per_sec, net_ip_send_package_per_sec,net_ip_drop_package_per_sec,
			 net_tcp_recv_package_per_sec, net_tcp_send_package_per_sec, net_tcp_err_package_per_sec, net_tcp_retransfer_package_per_sec, create_time
	mac_mem: cluster_id, type, ip, memory_total, memory_used_rss, memory_used, memory_free, memory_used_percent, swap_memory_total, swap_memory_used, swap_memory_free,
			 swap_memory_used_percent, create_time
	max_disk: cluster_id, type, ip, disk_path, disk_total, disk_used, disk_free, disk_proc_rate, disk_read_byte_per_sec, disk_write_byte_per_sec,
			  disk_read_count_per_sec, disk_write_count_per_sec, create_time
	*/
	// mac meta
	meta := make(map[string]interface{})
	meta["cluster_id"] = clusterId
	meta["type"] = item.Type
	meta["ip"] = item.Ip
	meta["cpu_rate"] = item.Item.CpuProcRate
	meta["load1"] = item.Item.Load1
	meta["load5"] = item.Item.Load5
	meta["load15"] = item.Item.Load15
	meta["process_num"] = item.Item.ProcessNum
	meta["thread_num"] = item.Item.ThreadNum
	meta["handle_num"] = item.Item.HandleNum
	meta["update_time"] = item.UpdateTime

	m.store.Put(&Message{
		ClusterId: clusterId,
		Namespace: NAMESPACE_MAC,
		Subsystem: METRIC_MAC_META,
		Items:     meta,
	})

	// mac net
	net := make(map[string]interface{})
	net["cluster_id"] = clusterId
	net["type"] = item.Type
	net["ip"] = item.Ip
	net["net_io_in_byte_per_sec"] = item.Item.NetStats.NetIoInBytePerSec
	net["net_io_out_byte_per_sec"] = item.Item.NetStats.NetIoOutBytePerSec
	net["net_io_in_package_per_sec"] = item.Item.NetStats.NetIoInPackagePerSec
	net["net_io_out_package_per_sec"] = item.Item.NetStats.NetIoOutPackagePerSec
	net["net_tcp_connections"] = item.Item.NetStats.NetTcpConnections
	net["net_tcp_active_opens_per_sec"] = item.Item.NetStats.NetTcpActiveOpensPerSec
	net["net_ip_recv_package_per_sec"] = item.Item.NetStats.NetIpRecvPackagePerSec
	net["net_ip_send_package_per_sec"] = item.Item.NetStats.NetIpSendPackagePerSec
	net["net_ip_drop_package_per_sec"] = item.Item.NetStats.NetIpDropPackagePerSec
	net["net_tcp_recv_package_per_sec"] = item.Item.NetStats.NetTcpRecvPackagePerSec
	net["net_tcp_send_package_per_sec"] = item.Item.NetStats.NetTcpSendPackagePerSec
	net["net_tcp_err_package_per_sec"] = item.Item.NetStats.NetTcpErrPackagePerSec
	net["net_tcp_retransfer_package_per_sec"] = item.Item.NetStats.NetTcpRetransferPackagePerSec
	net["update_time"] = item.UpdateTime

	m.store.Put(&Message{
		ClusterId: clusterId,
		Namespace: NAMESPACE_MAC,
		Subsystem: METRIC_MAC_NET,
		Items:     net,
	})

	// mac mem
	mem := make(map[string]interface{})
	mem["cluster_id"] = clusterId
	mem["type"] = item.Type
	mem["ip"] = item.Ip
	mem["memory_total"] = item.Item.MemStats.MemoryTotal
	mem["memory_used_rss"] = item.Item.MemStats.MemoryUsed
	mem["memory_used"] = item.Item.MemStats.MemoryUsed
	mem["memory_free"] = item.Item.MemStats.MemoryFree
	mem["memory_used_percent"] = item.Item.MemStats.MemoryUsedPercent
	mem["swap_memory_total"] = item.Item.MemStats.SwapMemoryTotal
	mem["swap_memory_used"] = item.Item.MemStats.SwapMemoryUsed
	mem["swap_memory_free"] = item.Item.MemStats.SwapMemoryFree
	mem["swap_memory_used_percent"] = item.Item.MemStats.SwapMemoryUsedPercent
	mem["update_time"] = item.UpdateTime

	m.store.Put(&Message{
		ClusterId: clusterId,
		Namespace: NAMESPACE_MAC,
		Subsystem: METRIC_MAC_MEM,
		Items:     mem,
	})

	for _, diskStats := range item.Item.DiskStats {
		/*
		max_disk: cluster_id, type, ip, disk_path, disk_total, disk_used, disk_free, disk_proc_rate, disk_read_byte_per_sec, disk_write_byte_per_sec,
              disk_read_count_per_sec, disk_write_count_per_sec, create_time
		*/
		disk := make(map[string]interface{})
		disk["cluster_id"] = clusterId
		disk["type"] = item.Type
		disk["ip"] = item.Ip
		disk["disk_path"] = diskStats.DiskPath
		disk["disk_total"] = diskStats.DiskTotal
		disk["disk_used"] = diskStats.DiskUsed
		disk["disk_free"] = diskStats.DiskFree
		disk["disk_proc_rate"] = diskStats.DiskProcRate
		disk["disk_read_byte_per_sec"] = diskStats.DiskReadBytePerSec
		disk["disk_write_byte_per_sec"] = diskStats.DiskWriteBytePerSec
		disk["disk_read_count_per_sec"] = diskStats.DiskReadCountPerSec
		disk["disk_write_count_per_sec"] = diskStats.DiskWriteCountPerSec
		disk["update_time"] = item.UpdateTime

		m.store.Put(&Message{
			ClusterId: clusterId,
			Namespace: NAMESPACE_MAC,
			Subsystem: METRIC_MAC_DISK,
			Items:     disk,
		})
	}

}

func (m *Metric) pushProcess(clusterId uint64, item *ProcessItem) {
	/*
	process_meta: cluster_id, type, addr, id, cpu_rate, load1, load5, load15, thread_num, handle_num, memory_used, start_time, create_time
	process_disk: cluster_id, type, addr, id, disk_path, disk_total, disk_used, disk_free, disk_proc_rate, disk_read_byte_per_sec, disk_write_byte_per_sec,
			  disk_read_count_per_sec, disk_write_count_per_sec, create_time
	process_net: cluster_id, type, addr, id, tps, min_tp, max_tp, avg_tp, tp50, tp90, tp99, tp999, net_io_in_byte_per_sec, net_io_out_byte_per_sec, net_io_in_package_per_sec,
				 net_io_out_package_per_sec, net_tcp_connections, net_tcp_active_opens_per_sec, create_time
	process_ds: cluster_id, type, addr, id, range_count, range_split_count, sending_snap_count, receiving_snap_count,
				applying_snap_count, range_leader_count, version, create_time
	*/
	var typeUpper = strings.ToUpper(item.Type)
	// process meta
	meta := make(map[string]interface{})
	meta["cluster_id"] = clusterId
	meta["type"] = typeUpper
	meta["addr"] = item.Addr
	meta["cpu_rate"] = item.Item.CpuProcRate
	meta["thread_num"] = item.Item.ThreadNum
	meta["handle_num"] = item.Item.HandleNum
	// TODO memory used
	meta["memory_total"] = item.Item.MemoryTotal
	meta["memory_used"] = item.Item.MemoryUsed
	meta["start_time"] = item.Item.StartTime
	meta["update_time"] = item.UpdateTime

	m.store.Put(&Message{
		ClusterId: clusterId,
		Namespace: NAMESPACE_PROCESS,
		Subsystem: METRIC_PROCESS_META,
		Items:     meta,
	})

	// process disk
	/*
	process_disk: cluster_id, type, addr, id, disk_path, disk_total, disk_used, disk_free, disk_proc_rate, disk_read_byte_per_sec, disk_write_byte_per_sec,
              disk_read_count_per_sec, disk_write_count_per_sec, create_time
	*/
	if item.Item.DiskStats != nil {
		disk := make(map[string]interface{})
		disk["cluster_id"] = clusterId
		disk["type"] = typeUpper
		disk["addr"] = item.Addr
		disk["disk_path"] = item.Item.DiskStats.DiskPath
		disk["disk_total"] = item.Item.DiskStats.DiskTotal
		disk["disk_used"] = item.Item.DiskStats.DiskUsed
		disk["disk_free"] = item.Item.DiskStats.DiskFree
		disk["disk_proc_rate"] = item.Item.DiskStats.DiskProcRate
		disk["disk_read_byte_per_sec"] = item.Item.DiskStats.DiskReadBytePerSec
		// TODO memory used
		disk["disk_write_byte_per_sec"] = item.Item.DiskStats.DiskWriteBytePerSec
		disk["disk_read_count_per_sec"] = item.Item.DiskStats.DiskReadCountPerSec
		disk["disk_write_count_per_sec"] = item.Item.DiskStats.DiskWriteCountPerSec
		disk["update_time"] = item.UpdateTime

		m.store.Put(&Message{
			ClusterId: clusterId,
			Namespace: NAMESPACE_PROCESS,
			Subsystem: METRIC_PROCESS_DISK,
			Items:     disk,
		})
	}

	// process net
	/*
	 process_net: cluster_id, type, addr, id, tps, min_tp, max_tp, avg_tp, tp50, tp90, tp99, tp999, net_io_in_byte_per_sec, net_io_out_byte_per_sec, net_io_in_package_per_sec,
                 net_io_out_package_per_sec, net_tcp_connections, net_tcp_active_opens_per_sec, create_time
	*/
	net := make(map[string]interface{})
	net["cluster_id"] = clusterId
	net["type"] = typeUpper
	net["addr"] = item.Addr
	net["tps"] = item.Item.TpStats.Tps
	net["min_tp"] = item.Item.TpStats.Min
	net["max_tp"] = item.Item.TpStats.Max
	net["avg_tp"] = item.Item.TpStats.Avg
	net["tp50"] = item.Item.TpStats.Tp_50
	net["tp90"] = item.Item.TpStats.Tp_90
	net["tp99"] = item.Item.TpStats.Tp_99
	net["tp999"] = item.Item.TpStats.Tp_999
	net["total_number"] = item.Item.TpStats.TotalNumber
	net["err_number"] = item.Item.TpStats.ErrNumber
	net["connect_count"] = item.Item.ConnectCount
	net["update_time"] = item.UpdateTime

	m.store.Put(&Message{
		ClusterId: clusterId,
		Namespace: NAMESPACE_PROCESS,
		Subsystem: METRIC_PROCESS_NET,
		Items:     net,
	})

	if item.Item.DsInfo != nil {
		/*
		process_ds: cluster_id, type, addr, id, range_count, range_split_count, sending_snap_count, receiving_snap_count,
                applying_snap_count, range_leader_count, version, create_time
		*/
		ds := make(map[string]interface{})
		ds["cluster_id"] = clusterId
		ds["type"] = typeUpper
		ds["addr"] = item.Addr
		ds["range_count"] = item.Item.DsInfo.RangeCount
		ds["range_split_count"] = item.Item.DsInfo.RangeSplitCount
		ds["sending_snap_count"] = item.Item.DsInfo.SendingSnapCount
		ds["receiving_snap_count"] = item.Item.DsInfo.ReceivingSnapCount
		ds["applying_snap_count"] = item.Item.DsInfo.ApplyingSnapCount
		ds["range_leader_count"] = item.Item.DsInfo.RangeLeaderCount
		ds["version"] = item.Item.DsInfo.Version
		ds["update_time"] = item.UpdateTime

		m.store.Put(&Message{
			ClusterId: clusterId,
			Namespace: NAMESPACE_PROCESS,
			Subsystem: METRIC_PROCESS_DS,
			Items:     ds,
		})
	}
}

func (m *Metric) pushSlowLog(clusterId uint64, item *SlowLogItem) {
	/*
	CREATE TABLE IF NOT EXISTS `cluster_slowlog` (
  `cluster_id` bigint(20) NOT NULL,
  `update_time` bigint(20) NOT NULL,
  `su` bigint(20) NOT NULL,
  `type`  varchar(32) NOT NULL,
  `addr`  varchar(32) NOT NULL,
  `lats` bigint(20) NOT NULL,
  `slowlog` varchar(2048) NOT NULL,
  PRIMARY KEY (`cluster_id`, `update_time`, `su`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
	*/
	for _, slowLog := range item.Item.GetSlowLogs() {
		meta := make(map[string]interface{})
		meta["cluster_id"] = clusterId
		meta["type"] = item.Type
		meta["addr"] = item.Addr
		meta["su"] = atomic.AddUint64(&m.index, 1)
		meta["lats"] = slowLog.Lats
		meta["slowlog"] = slowLog.SlowLog
		meta["update_time"] = item.UpdateTime

		m.store.Put(&Message{
			ClusterId: clusterId,
			Namespace: NAMESPACE_CLUSTER,
			Subsystem: METRIC_CLUSTER_SLOWLOG,
			Items:     meta,
		})
	}
}

func (m *Metric) pushDb(clusterId uint64, item *DbItem) {
	var size uint64 = 0
	// table
	for _, table := range item.TableCache.GetAll() {
		size += table.Size
	}
	item.Size = size

	/*db_meta: cluster_id, db_name, table_num, range_size, create_time*/
	// process meta
	meta := make(map[string]interface{})
	meta["cluster_id"] = clusterId
	meta["db_name"] = item.DbName
	meta["table_num"] = item.TableNum
	meta["range_size"] = item.Size
	meta["update_time"] = item.UpdateTime

	m.store.Put(&Message{
		ClusterId: clusterId,
		Namespace: NAMESPACE_DB,
		Subsystem: METRIC_DB_META,
		Items:     meta,
	})
}

func (m *Metric) pushTable(clusterId uint64, item *TableItem) {
	/*table_meta: cluster_id, db_name, table_name, range_count, range_size, create_time*/
	// process meta
	meta := make(map[string]interface{})
	meta["cluster_id"] = clusterId
	meta["db_name"] = item.DbName
	meta["table_name"] = item.TableName
	meta["range_count"] = item.RangeNum
	meta["range_size"] = item.Size
	meta["update_time"] = item.UpdateTime

	m.store.Put(&Message{
		ClusterId: clusterId,
		Namespace: NAMESPACE_TABLE,
		Subsystem: METRIC_TABLE_META,
		Items:     meta,
	})
}

func (m *Metric) pushTask(clusterId uint64, item *statspb.TaskInfo) {
	// process meta
	meta := make(map[string]interface{})
	meta["cluster_id"] = clusterId
	meta["task_id"] = item.GetTaskId()
	meta["range_id"] = item.GetRangeId()
	meta["kind"] = item.GetKind()
	meta["name"] = item.GetName()
	meta["state"] = item.GetState()
	meta["start"] = item.GetStart()
	meta["end"] = item.GetEnd()
	meta["used_time"] = item.GetUsedTime()
	meta["describe"] = item.GetDescribe()
	meta["update_time"] = time.Now().Unix()

	m.store.Put(&Message{
		ClusterId: clusterId,
		Namespace: NAMESPACE_TASK,
		Subsystem: METRIC_TASK_STATS,
		Items:     meta,
	})
}

func (m *Metric) pushSchedule(clusterId uint64, item *statspb.ScheduleCount) {
	// process meta
	meta := make(map[string]interface{})
	meta["cluster_id"] = clusterId
	meta["name"] = item.GetName()
	meta["label"] = item.GetLabel()
	meta["count"] = item.GetCount()
	meta["update_time"] = time.Now().Unix()

	m.store.Put(&Message{
		ClusterId: clusterId,
		Namespace: NAMESPACE_SCHEDULE,
		Subsystem: METRIC_SCHEDULE_COUNTER,
		Items:     meta,
	})
}

func (m *Metric) pushHotspot(clusterId uint64, hotspot *statspb.HotSpotStats) {
	meta := make(map[string]interface{})
	meta["cluster_id"] = clusterId
	meta["node_id"] = hotspot.GetNodeId()
	meta["node_addr"] = hotspot.GetNodeAddr()
	meta["total_written_bytes_as_peer"] = hotspot.GetTotalWrittenBytesAsPeer()
	meta["hot_write_region_as_peer"] = hotspot.GetHotWriteRegionAsPeer()
	meta["total_written_bytes_as_leader"] = hotspot.GetTotalWrittenBytesAsLeader()
	meta["hot_write_region_as_leader"] = hotspot.GetHotWriteRegionAsLeader()
	meta["update_time"] = time.Now().Unix()

	m.store.Put(&Message{
		ClusterId: clusterId,
		Namespace: NAMESPACE_HOTSPOT,
		Subsystem: METRIC_HOTSPOT,
		Items:     meta,
	})
}

func (m *Metric) pushNodeStats(clusterId, nodeId uint64, nodeAddr string, nodeStats *mspb.NodeStats) {
	meta := make(map[string]interface{})
	meta["cluster_id"] = clusterId
	meta["node_id"] = nodeId
	meta["addr"] = nodeAddr
	meta["range_count"] = nodeStats.GetRangeCount()
	meta["range_split_count"] = nodeStats.GetRangeSplitCount()
	meta["sending_snap_count"] = nodeStats.GetSendingSnapCount()
	meta["receiving_snap_count"] = nodeStats.GetReceivingSnapCount()
	meta["applying_snap_count"] = nodeStats.GetApplyingSnapCount()
	meta["range_leader_count"] = nodeStats.GetRangeLeaderCount()
	meta["capacity"] = nodeStats.GetCapacity()
	meta["used_size"] = nodeStats.GetUsedSize()
	meta["available_size"] = nodeStats.GetAvailable()
	meta["bytes_written"] = nodeStats.GetBytesWritten()
	meta["keys_written"] = nodeStats.GetKeysWritten()
	meta["bytes_read"] = nodeStats.GetBytesRead()
	meta["keys_read"] = nodeStats.GetKeysRead()
	meta["isbusy"] = nodeStats.GetIsBusy()
	meta["update_time"] = time.Now().Unix()

	m.store.Put(&Message{
		ClusterId: clusterId,
		Namespace: NAMESPACE_NODE,
		Subsystem: METRIC_NODE_STATS,
		Items:     meta,
	})
}

func (m *Metric) pushRangeStats(clusterId uint64, rangeStats []*statspb.RangeInfo) {
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
		m.store.Put(&Message{
			ClusterId: clusterId,
			Namespace: NAMESPACE_RANGE,
			Subsystem: METRIC_RANGE_STATS,
			Items:     metaList,
		})
	}
}

func (m *Metric) push(queue chan *Message) {
	for {
		select {
		case <-m.ctx.Done():
			return
		case msg, ok := <-queue:
			if !ok {
				continue
			}
			m.store.Put(msg)
		}
	}
}

func (m *Metric) getCluster(clusterId uint64) *Cluster {
	return m.items.Get(clusterId)
}

func (m *Metric) setCluster(cluster *Cluster) {
	m.items.Set(cluster)
}
