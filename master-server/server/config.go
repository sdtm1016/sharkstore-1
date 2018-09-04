package server

import (
	"time"
	"util"
	"strings"
	"fmt"

	"util/log"
	"github.com/BurntSushi/toml"
	"master-server/metric"
)

const (
	defaultServerName                = "ms"
	defaultMaxReplicas               = 3
	defaultMaxSnapshotCount          = 3
	defaultMaxNodeDownTime           = time.Hour
	defaultLeaderScheduleLimit       = 64
	defaultRegionScheduleLimit       = 12
	defaultReplicaScheduleLimit      = 16
	defaultRaftHbInterval            = time.Millisecond * 500
	defaultRaftRetainLogsCount       = 100
	defaultMaxTaskWaitTime           = 5 * time.Minute
	defaultMaxRangeDownTime          = 10 * time.Minute
	defaultNodeRangeBalanceTime      = 2 * time.Minute
	defaultStorageAvailableThreshold = 20
	defaultWriteByteOpsThreshold     = 30 * 1024 * 1024
)
const DefaultFactor = 0.75

const DefaultConfig = `
# MS Configuration.

name = "ms"
node-id = 1
# process role is master or metric
role = "master"
version = "v1"
# secret key for master, leaves it empty will ignore http request signature verification
secret-key = ""

# cluster meta data store path
data-dir = "/tmp/sharkstore/data"

[cluster]
cluster-id = 1

[[cluster.peer]]
id = 1
host = "127.0.0.1"
http-port = 8887
rpc-port = 18887
raft-ports = [8877,8867]

[raft]
heartbeat-interval = "500ms"
retain-logs-count = 100

[log]
dir = "/tmp/sharkstore/log"
module = "master"
# log level debug, info, warn, error
level = "info"

[metric]
# metric client push interval, set "0s" to disable metric.
interval = "15s"
# metric address, leaves it empty will disable metric.
address = ""

[metric.server]
address = "127.0.0.1:8887"
queue-num = 100
store-type = "elasticsearch"
store-url = ["http://192.168.182.11:20001","http://192.168.182.12:20001","http://192.168.182.13:20001"]

[schedule]
max-snapshot-count = 3
max-node-down-time = "1h"
leader-schedule-limit = 64
region-schedule-limit = 16
replica-schedule-limit = 24
max-task-timeout = "300s"
# 12 times of region heartbeat time
max-range-down-time = "600s"
node-range-balance-time = "120s"
storage-available-threshold = 20
writeByte-ops-threshold = 31457280

[replication]
# The number of replicas for each region.
max-replicas = 3
# The label keys specified the location of a node.
# The placement priorities is implied by the order of label keys.
# For example, ["zone", "rack"] means that we should place replicas to
# different zones first, then to different racks if we don't have enough zones.
location-labels = []
`

type Config struct {
	Name              string `toml:"name,omitempty" json:"name"`
	NodeId            uint64 `toml:"node-id,omitempty" json:"node-id"`
	raftHeartbeatAddr string
	raftReplicaAddr   string
	webManageAddr     string
	rpcServerAddr     string
	Role              string `toml:"role,omitempty" json:"role"`
	Version           string `toml:"version,omitempty" json:"version"`
	SecretKey         string `toml:"secret-key,omitempty" json:"secret-key"`
	DataPath          string `toml:"data-dir,omitempty" json:"data-dir"`

	Cluster     ClusterConfig     `toml:"cluster,omitempty" json:"cluster"`
	Raft        RaftConfig        `toml:"raft,omitempty" json:"raft"`
	Schedule    ScheduleConfig    `toml:"schedule,omitempty" json:"schedule"`
	Replication ReplicationConfig `toml:"replication,omitempty" json:"replication"`

	Log         LogConfig         `toml:"log,omitempty" json:"log"`
	Metric      MetricConfig      `toml:"metric,omitempty" json:"metric"`

	Threshold metric.ThresholdConfig `toml:"threshold,omitempty" json:"threshold"`
	Alarm AlarmConfig`toml:"alarm,omitempty" json:"alarm"`

}

func NewDefaultConfig() *Config {
	c := &Config{}
	if _, err := toml.Decode(DefaultConfig, c); err != nil {
		log.Panic("decode toml failed, err %v", err)
	}
	if err := c.adjust(); err != nil {
		log.Panic("validate config failed, err %v", err)
	}
	return c
}

func (c *Config) LoadFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	if err != nil {
		return err
	}
	return c.adjust()
}

func (c *Config) adjust() error {
	adjustString(&c.Role, "master")
	adjustString(&c.Version, "v1")
	if strings.Compare(c.Role, "master") != 0 && strings.Compare(c.Role, "metric") != 0 {
		return fmt.Errorf("invalid role %s", c.Role)
	}

	var err error
	err = c.Log.adjust()
	if err != nil {
		return err
	}
	err = c.Metric.adjust(c.Role)
	if err != nil {
		return err
	}
	switch c.Role {
	case "master":
		if c.DataPath == "" {
			return fmt.Errorf("invalid data path")
		}
		err = c.Cluster.adjust()
		if err != nil {
			return err
		}
		if c.NodeId == 0 {
		LocalIPs:
			for _, ip := range util.GetLocalIps() {
				for _, peer := range c.Cluster.Peers {
					if peer.Host == ip {
						c.NodeId = peer.ID
						break LocalIPs
					}
				}
			}
		}
		for _, peer := range c.Cluster.Peers {
			if peer.ID == c.NodeId {
				c.webManageAddr = fmt.Sprintf("%s:%d", peer.Host, peer.HttpPort)
				c.rpcServerAddr = fmt.Sprintf("%s:%d", peer.Host, peer.RpcPort)
				c.raftHeartbeatAddr = fmt.Sprintf("%s:%d", peer.Host, peer.RaftPorts[0])
				c.raftReplicaAddr = fmt.Sprintf("%s:%d", peer.Host, peer.RaftPorts[1])
				break
			}
		}
		if c.NodeId == 0 || c.rpcServerAddr == "" {
			return fmt.Errorf("invalid node ID %d", c.NodeId)
		}
		err = c.Raft.adjust()
		if err != nil {
			return err
		}
		c.Schedule.adjust()
		c.Replication.adjust()
	}
	return nil
}

type ClusterPeer struct {
	ID        uint64 `toml:"id,omitempty" json:"id"`
	Host      string `toml:"host,omitempty" json:"host"`
	HttpPort  int    `toml:"http-port,omitempty" json:"http-port"`
	RpcPort   int    `toml:"rpc-port,omitempty" json:"rpc-port"`
	RaftPorts []int  `toml:"raft-ports,omitempty" json:"raft-ports"`
}

type ClusterConfig struct {
	ClusterID uint64         `toml:"cluster-id,omitempty" json:"cluster-id"`
	Peers     []*ClusterPeer `toml:"peer,omitempty" json:"peer"`
}

type AlarmReceiver struct {
	Mail string `toml:"mail,omitempty" json:"mail"`
	Sms  string `toml:"sms,omitempty" json:"sms"`
}



type AlarmConfig struct {
	ServerAddress string  		`toml:"server-address" json:"server-address"`
	ServerPort int   		`toml:"server-port,omitempty" json:"port"`
	RemoteAlarmServerAddress string	`toml:"remote-alarm-server-address,omitempty" json:"remote-alarm-server-address"`
	MysqlArgs string		`toml:"mysql-args,omitempty" json:"mysql-args"`
	//MessageGatewayAddress string	`toml:"message-gateway-address,omitempty" json:"message-gateway-address"`
	//Receivers []*AlarmReceiver	`toml:"receivers,omitempty" json:"receivers"`
	JimUrl string 			`toml:"jim-url,omitempty" json:"jim-url"`
	JimApAddr string		`toml:"jim-ap-addr,omitempty" json:"jim-ap-addr"`
}

func (c *ClusterConfig) adjust() error {
	if c.ClusterID <= 0 {
		return fmt.Errorf("invalid cluster ID %d", c.ClusterID)
	}
	if len(c.Peers) == 0 {
		return fmt.Errorf("invalid cluster peers")
	}
	for _, peer := range c.Peers {
		if peer.ID <= 0 {
			return fmt.Errorf("invalid cluster peer ID %d", peer.ID)
		}
		if peer.Host == "" {
			return fmt.Errorf("invalid cluster peer host")
		}
		if peer.HttpPort <= 1024 || peer.HttpPort > 65535 {
			return fmt.Errorf("invalid cluster peer http port %d", peer.HttpPort)
		}
		if peer.RpcPort <= 1024 || peer.RpcPort > 65535 {
			return fmt.Errorf("invalid cluster peer rpc port %d", peer.RpcPort)
		}
		if len(peer.RaftPorts) != 2 {
			return fmt.Errorf("invalid cluster peer raft ports %v", peer.RaftPorts)
		}
		for _, port := range peer.RaftPorts {
			if port <= 1024 || port > 65535 {
				return fmt.Errorf("invalid cluster peer raft port %d", port)
			}
		}
	}
	return nil
}

type RaftConfig struct {
	HeartbeatInterval util.Duration `toml:"heartbeat-interval,omitempty" json:"heartbeat-interval"`
	RetainLogsCount   uint64        `toml:"retain-logs-count,omitempty" json:"retain-logs-count"`
}

func (c *RaftConfig) adjust() error {
	adjustDuration(&c.HeartbeatInterval, defaultRaftHbInterval)
	adjustUint64(&c.RetainLogsCount, defaultRaftRetainLogsCount)
	return nil
}

type LogConfig struct {
	Dir    string `toml:"dir,omitempty" json:"dir"`
	Module string `toml:"module,omitempty" json:"module"`
	Level  string `toml:"level,omitempty" json:"level"`
}

func (c *LogConfig) adjust() error {
	if c.Dir == "" {
		return fmt.Errorf("invalid log dir")
	}
	adjustString(&c.Module, defaultServerName)
	adjustString(&c.Level, "debug")
	switch c.Level {
	case "TRACE", "trace", "Trace":
	case "debug", "Debug", "DEBUG":
	case "info", "Info", "INFO":
	case "warn", "Warn", "WARN":
	case "error", "Error", "ERROR":
	default:
		c.Level = "debug"
	}
	return nil
}

type MetricServer struct {
	Address   string   `toml:"address,omitempty" json:"address"`
	QueueNum  uint64   `toml:"queue-num,omitempty" json:"queue-num"`
	StoreType string   `toml:"store-type,omitempty" json:"store-type"`
	StoreUrl  []string `toml:"store-url,omitempty" json:"store-url"`
}

type MetricConfig struct {
	Interval util.Duration `toml:"interval,omitempty" json:"interval"`
	Address  string        `toml:"address,omitempty" json:"address"`

	Server   MetricServer  `toml:"server,omitempty" json:"server"`
}

func (c *MetricConfig) adjust(role string) error {
	if role == "metric" {
		if c.Server.Address == "" {
			return fmt.Errorf("invalid metric server address")
		}
		adjustUint64(&c.Server.QueueNum, 100)
		if len(c.Server.StoreUrl) == 0 {
			return fmt.Errorf("invalid metric server db url")
		}
	}
	return nil
}

// ScheduleConfig is the schedule configuration.
type ScheduleConfig struct {
	// If the snapshot count of one store is greater than this value,
	// it will never be used as a source or target store.
	MaxSnapshotCount uint64 `toml:"max-snapshot-count,omitempty" json:"max-snapshot-count"`
	// MaxStoreDownTime is the max duration after which
	// a store will be considered to be down if it hasn't reported heartbeats.
	MaxNodeDownTime util.Duration `toml:"max-node-down-time,omitempty" json:"max-node-down-time"`
	// LeaderScheduleLimit is the max coexist leader schedules.
	LeaderScheduleLimit uint64 `toml:"leader-schedule-limit,omitempty" json:"leader-schedule-limit"`
	// RegionScheduleLimit is the max coexist region schedules.
	RegionScheduleLimit uint64 `toml:"region-schedule-limit,omitempty" json:"region-schedule-limit"`
	// ReplicaScheduleLimit is the max coexist replica schedules.
	ReplicaScheduleLimit      uint64        `toml:"replica-schedule-limit,omitempty" json:"replica-schedule-limit"`
	MaxTaskTimeout            util.Duration `toml:"max-task-timeout,omitempty" json:"max-task-timeout"`
	MaxRangeDownTime          util.Duration `toml:"max-range-down-time,omitempty" json:"max-range-down-time"`
	NodeRangeBalanceTime      util.Duration `toml:"node-range-balance-time,omitempty" json:"node-range-balance-time"`
	StorageAvailableThreshold uint64        `toml:"storage-available-threshold,omitempty" json:"storage-available-threshold"`
	WriteByteOpsThreshold     uint64        `toml:"writeByte-ops-threshold,omitempty" json:"writeByte-ops-threshold"`
}

func (c *ScheduleConfig) adjust() {
	adjustUint64(&c.MaxSnapshotCount, defaultMaxSnapshotCount)
	adjustDuration(&c.MaxNodeDownTime, defaultMaxNodeDownTime)
	adjustUint64(&c.LeaderScheduleLimit, defaultLeaderScheduleLimit)
	adjustUint64(&c.RegionScheduleLimit, defaultRegionScheduleLimit)
	adjustUint64(&c.ReplicaScheduleLimit, defaultReplicaScheduleLimit)
	adjustDuration(&c.MaxTaskTimeout, defaultMaxTaskWaitTime)
	adjustDuration(&c.MaxRangeDownTime, defaultMaxRangeDownTime)
	adjustDuration(&c.NodeRangeBalanceTime, defaultNodeRangeBalanceTime)
	adjustUint64(&c.StorageAvailableThreshold, defaultStorageAvailableThreshold)
	adjustUint64(&c.WriteByteOpsThreshold, defaultWriteByteOpsThreshold)

}

// ReplicationConfig is the replication configuration.
type ReplicationConfig struct {
	// MaxReplicas is the number of replicas for each region.
	MaxReplicas uint64 `toml:"max-replicas,omitempty" json:"max-replicas"`

	// The label keys specified the location of a store.
	// The placement priorities is implied by the order of label keys.
	// For example, ["zone", "rack"] means that we should place replicas to
	// different zones first, then to different racks if we don't have enough zones.
	LocationLabels util.StringSlice `toml:"location-labels,omitempty" json:"location-labels"`
}

func (c *ReplicationConfig) clone() *ReplicationConfig {
	locationLabels := make(util.StringSlice, 0, len(c.LocationLabels))
	copy(locationLabels, c.LocationLabels)
	return &ReplicationConfig{
		MaxReplicas:    c.MaxReplicas,
		LocationLabels: locationLabels,
	}
}

func (c *ReplicationConfig) adjust() {
	adjustUint64(&c.MaxReplicas, defaultMaxReplicas)
}

// scheduleOption is a wrapper to access the configuration safely.
type scheduleOption struct {
	MaxSnapshotCount          uint64
	MaxNodeDownTime           time.Duration
	MaxRangeDownTime          time.Duration
	MaxReplicaDownTime        time.Duration
	MaxTaskTimeout            time.Duration
	NodeRangeBalanceTime      time.Duration
	LeaderScheduleLimit       uint64
	RegionScheduleLimit       uint64
	ReplicaScheduleLimit      uint64
	StorageAvailableThreshold uint64
	WriteByteOpsThreshold     uint64
	//rep *Replication
	MaxReplicas uint64
	MetricAddr  string
	MetricInterval time.Duration
}

func newScheduleOption(cfg *Config) *scheduleOption {
	o := &scheduleOption{
		MaxSnapshotCount:          cfg.Schedule.MaxSnapshotCount,
		MaxNodeDownTime:           cfg.Schedule.MaxNodeDownTime.Duration,
		MaxRangeDownTime:          cfg.Schedule.MaxRangeDownTime.Duration,
		LeaderScheduleLimit:       cfg.Schedule.LeaderScheduleLimit,
		RegionScheduleLimit:       cfg.Schedule.RegionScheduleLimit,
		ReplicaScheduleLimit:      cfg.Schedule.ReplicaScheduleLimit,
		MaxTaskTimeout:            cfg.Schedule.MaxTaskTimeout.Duration,
		NodeRangeBalanceTime:      cfg.Schedule.NodeRangeBalanceTime.Duration,
		StorageAvailableThreshold: cfg.Schedule.StorageAvailableThreshold,
		WriteByteOpsThreshold:cfg.Schedule.WriteByteOpsThreshold,
		MetricAddr: cfg.Metric.Address,
		MetricInterval: cfg.Metric.Interval.Duration,
		MaxReplicas: cfg.Replication.MaxReplicas,
	}

	//o.rep = newReplication(&cfg.Replication)
	return o
}
/**
func (o *scheduleOption) GetReplication() *Replication {
	return o.rep
}
*/
func (o *scheduleOption) GetMaxReplicas() int {
	return int(o.MaxReplicas)
}

func (o *scheduleOption) SetMaxReplicas(replicas int) {
	o.MaxReplicas = uint64(replicas)
}

func (o *scheduleOption) GetMaxSnapshotCount() uint64 {
	return o.MaxSnapshotCount
}

func (o *scheduleOption) GetMaxNodeDownTime() time.Duration {
	return o.MaxNodeDownTime
}

func (o *scheduleOption) GetMaxRangeDownTime() time.Duration {
	return o.MaxRangeDownTime
}

func (o *scheduleOption) GetMaxTaskTimeout() time.Duration {
	return o.MaxTaskTimeout
}

func (o *scheduleOption) GetNodeRangeBalanceTime() time.Duration {
	return o.NodeRangeBalanceTime
}

func (o *scheduleOption) GetStorageAvailableThreshold() uint64 {
	return o.StorageAvailableThreshold
}

func (o *scheduleOption) GetWriteByteOpsThreshold() uint64 {
	return o.WriteByteOpsThreshold
}

func (o *scheduleOption) GetLeaderScheduleLimit() uint64 {
	return o.LeaderScheduleLimit
}

func (o *scheduleOption) GetRegionScheduleLimit() uint64 {
	return o.RegionScheduleLimit
}

func (o *scheduleOption) GetReplicaScheduleLimit() uint64 {
	return o.ReplicaScheduleLimit
}

func (o *scheduleOption) GetMetricAddress() string {
	return o.MetricAddr
}

func (o *scheduleOption) GetMetricInterval() time.Duration {
	return o.MetricInterval
}

func adjustString(v *string, defValue string) {
	if len(*v) == 0 {
		*v = defValue
	}
}

func adjustUint64(v *uint64, defValue uint64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustInt64(v *int64, defValue int64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustFloat64(v *float64, defValue float64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustDuration(v *util.Duration, defValue time.Duration) {
	if v.Duration == 0 {
		v.Duration = defValue
	}
}
