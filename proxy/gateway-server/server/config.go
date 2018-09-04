package server

import (
	"util"
	"util/log"
	"github.com/BurntSushi/toml"

	"time"
	"fmt"
	"bufio"
	"strings"
	"io"
	"io/ioutil"
	"os"
)

const (
	DefaultHttpPort    = 8080
	DefaultLockRpcPort = 8090
	DefaultSqlPort     = 6060

	DefaultServerName = "gateway"

	DefaultInsertSlowLog = 20 * time.Millisecond
	DefaultSelectSlowLog = 100 * time.Millisecond
	DefaultDeleteSlowLog = 20 * time.Millisecond

	DefaultMaxWorkNum      = 100
	DefaultMaxTaskQueueLen = 10000
	DefaultGrpcPoolSize    = 3
	DefaultGrpcInitWinSize = 64 * 1024
	DefaultMaxSlowLogLen   = 10

	DefaultMaxRawCount = 10000
)

type Config struct {
	HttpPort    int `toml:"http-port,omitempty" json:"http-port"`
	LockRpcPort int `toml:"lock-port,omitempty" json:"lock-port"`
	SqlPort     int `toml:"mysql-port,omitempty" json:"mysql-port"`

	MaxClients int    `toml:"max-clients,omitempty" json:"max-clients"`
	MaxLimit   uint64 `toml:"max-record-limit,omitempty" json:"max-record-limit"`

	User     string `toml:"user,omitempty" json:"user"`
	Password string `toml:"password,omitempty" json:"password"`
	Charset  string `toml:"charset,omitempty" json:"charset"`

	Alarm AlarmConfig `toml:"alarm,omitempty" json:"alarm"`
	Performance PerformConfig `toml:"performance,omitempty" json:"performance"`
	Cluster     ClusterConfig `toml:"cluster,omitempty" json:"cluster"`
	Log         LogConfig     `toml:"log,omitempty" json:"log"`
	Metric      MetricConfig  `toml:"metric,omitempty" json:"metric"`

	BenchConfig BenchMarkConfig `toml:"benchmark,omitempty" json:"benchmark"`
}

const DefaultConfig = `
# GS Configuration.

#http port
http-port = 8080

#distributed lock grpc service port
lock-port = 8090

#mysql port
mysql-port = 6060

#max client connection number allowed for sql port
max-clients = 10000
#max limit number for row record
max-record-limit = 10000
#mysql login user
user = "test"
password = "123456"
#mysql connect property
charset = "utf8"


[performance]
#proxy concurrency number
max-work-num = 100
#task queue size
max-task-queue-len = 10000
#keep connect size for each ds
grpc-pool-size = 10
# 128 KB
grpc-win-size = 131072
#be identified to 'slow command' when time consuming is greater than the value, unit: millisecond
slow-insert = "20ms"
slow-select = "100ms"
slow-delete = "20ms"


[cluster]
id = 1
address = ["127.0.165.52:8887"]
token = "xxxx"


[log]
dir = "./log"
module = "gateway"
# log level debug, info, warn, error
level = "debug"


[metric]
# metric client push interval, set "0s" to disable metric.
interval = "15s"
# receive metric address, leaves it empty will disable metric.
address = ""
`

var configFileN *string

func (c *Config) LoadConfig(configFileName *string) {
	if *configFileName != "" {
		configFileN = configFileName
		_, err := toml.DecodeFile(*configFileName, c)
		if err != nil {
			log.Panic("load config file failed, err %v", err)
		}
	} else {
		//load default config
		if _, err := toml.Decode(DefaultConfig, c); err != nil {
			log.Panic("decode default config failed, err %v", err)
		}
	}
	if err := c.adjust(); err != nil {
		log.Panic("validate config failed, err %v", err)
	}
}

type AlarmConfig struct {
	Address string `toml:"address,omitempty" json:"address"`
}

type PerformConfig struct {
	MaxWorkNum      uint64 `toml:"max-work-num,omitempty" json:"max-work-num"`
	MaxTaskQueueLen uint64 `toml:"max-task-queue-len,omitempty" json:"max-task-queue-len"`
	GrpcPoolSize    int    `toml:"grpc-pool-size,omitempty" json:"grpc-pool-size"`
	GrpcInitWinSize int    `toml:"grpc-win-size,omitempty" json:"grpc-win-size"`

	InsertSlowLog util.Duration `toml:"slow-insert,omitempty" json:"slow-insert"`
	SelectSlowLog util.Duration `toml:"slow-select,omitempty" json:"slow-select"`
	DeleteSlowLog util.Duration `toml:"slow-delete,omitempty" json:"slow-delete"`
	SlowLogMaxLen uint64        `toml:"slow-log-max-len,omitempty" json:"slow-log-max-len"`
	//HeartbeatIntervalSec util.Duration `toml:"hb-interval,omitempty" json:"hb-interval"`
}

func (p *PerformConfig) adjust() error {
	adjustUint64(&p.MaxWorkNum, DefaultMaxWorkNum)
	adjustUint64(&p.MaxTaskQueueLen, DefaultMaxTaskQueueLen)
	adjustInt(&p.GrpcPoolSize, DefaultGrpcPoolSize)
	adjustInt(&p.GrpcInitWinSize, DefaultGrpcInitWinSize)

	adjustDuration(&p.InsertSlowLog, DefaultInsertSlowLog)
	adjustDuration(&p.SelectSlowLog, DefaultSelectSlowLog)
	adjustDuration(&p.DeleteSlowLog, DefaultDeleteSlowLog)
	adjustUint64(&p.SlowLogMaxLen, DefaultMaxSlowLogLen)

	return nil
}

type ClusterConfig struct {
	ID         uint64   `toml:"id,omitempty" json:"id"`
	ServerAddr []string `toml:"address,omitempty" json:"address"`
	Token      string   `toml:"token,omitempty" json:"token"`
}

func (c *ClusterConfig) adjust() error {
	if c.ID == uint64(0) {
		return fmt.Errorf("invalid cluster.id config")
	}
	if len(c.ServerAddr) == 0 {
		return fmt.Errorf("invalid cluster.address config")
	}
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
	adjustString(&c.Module, DefaultServerName)
	adjustString(&c.Level, "info")
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

type MetricConfig struct {
	Interval util.Duration `toml:"interval,omitempty" json:"interval"`
	Address  string        `toml:"address,omitempty" json:"address"`
}

func (c *Config) adjust() error {
	if c.HttpPort == 0 {
		c.HttpPort = DefaultHttpPort
	}
	if c.LockRpcPort == 0 {
		c.LockRpcPort = DefaultLockRpcPort
	}
	if c.SqlPort == 0 {
		c.SqlPort = DefaultSqlPort
	}

	if c.MaxClients == 0 {
		return fmt.Errorf("max-clients not specified")
	}

	if c.User == "" {
		return fmt.Errorf("mysql user not specified")
	}

	if c.Password == "" {
		return fmt.Errorf("mysql password not specified")
	}

	var err error
	err = c.Performance.adjust()
	if err != nil {
		return err
	}

	err = c.Cluster.adjust()
	if err != nil {
		return err
	}

	err = c.Log.adjust()
	if err != nil {
		return err
	}

	return nil
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

func adjustInt(v *int, defValue int) {
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

type BenchMarkConfig struct {
	Type    int    `toml:"type,omitempty" json:"type"`
	DataLen int    `toml:"data-len,omitempty" json:"data-len"`
	SendNum int    `toml:"send-num,omitempty" json:"send-num"`
	Threads int    `toml:"threads,omitempty" json:"threads"`
	DB      string `toml:"db,omitempty" json:"db"`
	Table   string `toml:"table,omitempty" json:"table"`
	Batch   int    `toml:"batch,omitempty" json:"batch"`
	Scope   int    `toml:"scope,omitempty" json:"scope"`
}

func UpdateConfig(addr string) error{
	if *configFileN == "" {
		return nil
	}
	content, err := readAndReplaceConfig(*configFileN, addr)
	if err != nil {
		return  err
	}
	if content == "" {
		return nil
	}
	err = writeConfig(*configFileN, content)
	return err
}

func readAndReplaceConfig(fileName string, addr string) (string, error)   {
	f, err := os.Open(fileName)
	if err != nil {
		return "", err
	}
	defer f.Close()

	rd := bufio.NewReader(f)
	blockFlag := false
	var lines []string
	for {
		line, err := rd.ReadString('\n')
		if err != nil && err != io.EOF {
			return "", err
		}
		if strings.Contains(line, "[metric]") {
			blockFlag = true
		}
		if blockFlag && strings.Contains(line, "address")  && strings.Contains(line, "=") {
			line = "address = \"" + addr + "\"\n"
			blockFlag = false
		}
		lines = append(lines, line)
		if err != nil && io.EOF == err {
			break
		}
	}
	content := strings.Join(lines, "")
	return content, nil
}
func writeConfig(fileName string, content string) error {
	data := []byte(content)
	err := ioutil.WriteFile(fileName, data, 0666)
	if err != nil {
		return err
	}
	return nil
}