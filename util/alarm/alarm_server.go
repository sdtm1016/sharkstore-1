package alarm

import (
	"fmt"
	"sync"
	"strings"
	"strconv"
	"errors"
	"net"
	"net/http"
	"time"
	"util/log"
	"util/deepcopy"
	"model/pkg/alarmpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"golang.org/x/net/context"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gomodule/redigo/redis"
)

const (
	APP_NAME_GATEWAY = "gateway"
	APP_NAME_MASTER = "master"
	APP_NAME_METRIC = "metric"
	APP_KEY_JOIN_LETTER = "_"
)

type AlarmServer interface {
	NodeRangeAlarm(context.Context, *alarmpb.NodeRangeAlarmRequest) (*alarmpb.NodeRangeAlarmResponse, error)
	SimpleAlarm(ctx context.Context, req *alarmpb.SimpleRequest) (*alarmpb.SimpleResponse, error)
}

type Server struct {
	gateway *MessageGateway

	aliveCheckingAppKeys []string
	aliveCheckingLock sync.RWMutex

	sqlArgs string

	jimUrl string
	jimApAddr string
	jimConnTimeoutSec time.Duration
	jimWriteTimeoutSec time.Duration
	jimReadTimeoutSec time.Duration
	jimClientPool *redis.Pool
}

func (s *Server) jimDial() (redis.Conn, error) {
	if (len(s.jimUrl) == 0 || len(s.jimApAddr) == 0) {
		return nil, errors.New("no jim url or ap addr")
	}

	var dialOpts []redis.DialOption
	dialOpts = append(dialOpts, redis.DialPassword(s.jimUrl))

	if s.jimConnTimeoutSec > 0 {
		dialOpts = append(dialOpts, redis.DialConnectTimeout(s.jimConnTimeoutSec*time.Second))
	}
	if s.jimWriteTimeoutSec > 0 {
		dialOpts = append(dialOpts, redis.DialWriteTimeout(s.jimWriteTimeoutSec * time.Second))
	}
	if s.jimReadTimeoutSec > 0 {
		dialOpts = append(dialOpts, redis.DialReadTimeout(s.jimReadTimeoutSec*time.Second))
	}

	return redis.Dial("tcp", s.jimApAddr, dialOpts...)
}

func (s *Server) jimSendCommand(commandName string, args ...interface{}) (interface{}, error) {
	conn := s.jimClientPool.Get()
	return conn.Do(commandName, args...)
}

func newServer(ctx context.Context, alarmServerAddr string, sqlArgs, jimUrl, jimApAddr string) *Server {
	gateway := NewMessageGateway(ctx, alarmServerAddr)
	if gateway == nil {
		return nil
	}

	s := new(Server)
	s.gateway = gateway

	s.sqlArgs = sqlArgs
	s.jimUrl = jimUrl
	s.jimApAddr = jimApAddr
	s.jimClientPool = &redis.Pool{
		MaxIdle: 3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return s.jimDial()
		},
	}

	go s.aliveCheckingAlarm()
	return s
}

func (s *Server) splitAliveAppKey(appKey string) (appName, clusterId, appAddr string) {
	strs := strings.Split(appKey, APP_KEY_JOIN_LETTER)
	appName = strs[1]
	clusterId = strs[2]
	appAddr = strs[3]
	return
}

// ex. alive_gateway_1_127.0.0.1
func (s *Server) genAliveAppKey(appName, clusterId, appAddr string) string {
	name := strings.ToLower(appName)
	switch {
	case strings.HasPrefix(name, APP_NAME_GATEWAY):
		log.Info("in gen app key: treat [%v] as [%v]", appName, APP_NAME_GATEWAY)
		return fmt.Sprintf("alive%s%v%s%v%s%v",
			APP_KEY_JOIN_LETTER, APP_NAME_GATEWAY,
			APP_KEY_JOIN_LETTER, clusterId,
			APP_KEY_JOIN_LETTER, appAddr)
	case strings.HasPrefix(name, APP_NAME_MASTER):
		log.Info("in gen app key: treat [%v] as [%v]", appName, APP_NAME_MASTER)
		return fmt.Sprintf("alive%s%v%s%v%s%v",
			APP_KEY_JOIN_LETTER, APP_NAME_MASTER,
			APP_KEY_JOIN_LETTER, clusterId,
			APP_KEY_JOIN_LETTER, appAddr)
	case strings.HasPrefix(name, APP_NAME_METRIC):
		log.Info("in gen app key: treat [%v] as [%v]", appName, APP_NAME_METRIC)
		return fmt.Sprintf("alive%s%v%s%v%s%v",
			APP_KEY_JOIN_LETTER, APP_NAME_METRIC,
			APP_KEY_JOIN_LETTER, clusterId,
			APP_KEY_JOIN_LETTER, appAddr)
	default:
		return ""
	}
}

func (s *Server) addAliveCheckingAppAddr(appName, clusterId, appAddr string) {
	appKey := s.genAliveAppKey(appName, clusterId, appAddr)
	if len(appKey) == 0 {
		log.Warn("app name [%v] can not be generated", appName)
		return
	}

	s.aliveCheckingLock.Lock()
	defer s.aliveCheckingLock.Unlock()

	s.aliveCheckingAppKeys = append(s.aliveCheckingAppKeys, appKey)
}

func (s *Server) copyAliveCheckingAppKeys() (ret []string) {
	s.aliveCheckingLock.Lock()
	defer s.aliveCheckingLock.Unlock()

	if (len(s.aliveCheckingAppKeys) == 0) {
		return
	}

	ret = deepcopy.Iface(s.aliveCheckingAppKeys).([]string)
	s.aliveCheckingAppKeys = nil
	return
}

func NewAlarmServer(ctx context.Context, port int, remoteAlarmServerAddr string, sqlArgs, jimUrl, jimApAddr string) (*Server, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		return nil, err
	}
	s := grpc.NewServer()
	server := newServer(ctx, remoteAlarmServerAddr, sqlArgs, jimUrl, jimApAddr)
	if server == nil {
		return nil, errors.New("server is nil")
	}
	// register alarm server
	alarmpb.RegisterAlarmServer(s, server)
	reflection.Register(s)
	go s.Serve(lis)

	return server, nil
}

func (s *Server) parseHost(host string) ([]string, error) {
	gwHostInfo := strings.SplitN(host, ":", 2)
	if len(gwHostInfo) != 2 {
		return nil, errors.New("splitN failed != input arg N")
	}
	gwHost := gwHostInfo[0]
	_, err := strconv.ParseInt(gwHostInfo[1], 10, 64)
	//gwPort, err := strconv.ParseInt(gwHostInfo[1], 10, 64)
	if err != nil {
		return nil, err
	}

	addrs, err := net.LookupHost(gwHost)
	if err != nil {
		return nil, err
	}

	return addrs, nil
}

func (s *Server) loadAliveCheckingAppAddrs() error {
	log.Debug("load cluster info from sharkstore cluster table ")
	clusterInfos, err := s.getClusterInfo()
	if err != nil {
		log.Error("alive checking get cluster info failed: %v", err)
		return err
	}
	for _, info := range clusterInfos {
		log.Debug("info row: %v", info)
		if len(info.remark) == 0 || strings.Compare(info.remark, "NULL") == 0 {
			// do with gw
			log.Debug("parse gateway host: %v", info.appGwHost)
			gwAddrs, err := s.parseHost(info.appGwHost)
			if err != nil {
				log.Error("parse gateway domain failed: %v", err)
				continue
			}

			log.Debug("parsed gateway host result: %v", gwAddrs)
			for _, gwAddr := range gwAddrs {
				s.addAliveCheckingAppAddr(APP_NAME_GATEWAY, fmt.Sprint(info.id), gwAddr)
			}
		} else {
			log.Debug("len info remark[%v] != 0", info.remark)
		}
	}

	return nil
}

type ClusterInfo struct {
	id uint64
	appName string
	appGwHost string
	appMsHost string
	remark string
}

func (s *Server) getClusterInfo() (clusterInfos []ClusterInfo, err error) {
	db, err := sql.Open("mysql", s.sqlArgs)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var (
		clusterId uint64
		gwHost string
		remark string
	)
	rows, err := db.Query("select id, gateway_sql, remark from fbase_cluster")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&clusterId, &gwHost, &remark)
		if err != nil {
			return nil, err
		}
		clusterInfos = append(clusterInfos, ClusterInfo{
			id: clusterId,
			appName: APP_NAME_GATEWAY,
			appGwHost: gwHost,
			remark: remark,
		})
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return
}

func (s *Server) NodeRangeAlarm(ctx context.Context, req *alarmpb.NodeRangeAlarmRequest) (*alarmpb.NodeRangeAlarmResponse, error) {
	log.Info("receive node/range alarm message: %v", req.String())
	var err error
	resp := new(alarmpb.NodeRangeAlarmResponse)
	clusterId := req.GetHead().GetClusterId()
	// todo
	switch req.GetType() {
	case alarmpb.NodeRangeAlarmType_RANGE_NO_HEARTBEAT:
	case alarmpb.NodeRangeAlarmType_NODE_NO_HEARTBEAT:
	case alarmpb.NodeRangeAlarmType_NODE_DISK_SIZE:
	case alarmpb.NodeRangeAlarmType_NODE_LEADER_COUNT:
	}

	if err := s.gateway.notify(Message{
		ClusterId: clusterId,
		Title: "node/range alarm",
		Content: req.GetDescribe(),
		samples: req.SampleJson,
	}, time.Second); err != nil {
		log.Error("node/range alarm notify timeout")
	}

	return resp, err
}

func (s *Server) HandleAppPing(w http.ResponseWriter, r *http.Request) {
	log.Debug("handle app ping")

	resp := "ok"
	defer w.Write([]byte(resp))

	appName := r.FormValue("app_name") // app argv[0]
	appClusterId := r.FormValue("cluster_id")
	ipAddrs := r.FormValue("ip_addrs") // string ip join with ',': ip1,ip2,...
	ttl, err := strconv.ParseUint(r.FormValue("ping_interval"), 10, 64)
	if err != nil {
		resp = fmt.Sprintf("ping_interval parseuint failed: %v", err)
		log.Error(resp)
		return
	}
	ipAddrsArr := strings.Split(ipAddrs, ",")
	if len(ipAddrsArr) == 0 {
		resp = fmt.Sprintf("ip_addrs can not be split with letter ','")
		log.Error(resp)
		return
	}

	for _, appAddr := range ipAddrsArr {
		appKey := s.genAliveAppKey(appName, appClusterId, appAddr)
		log.Info("gen app key: %v", appKey)
		if len(appKey) == 0 {
			resp = fmt.Sprintf("app name [%v] can not be generated", appName)
			log.Error(resp)
			return
		}

		// setex key with ttl ping_interval to jimdb
		reply, err := s.jimSendCommand("setex",  appKey, ttl*2, "")
		replyStr, err := redis.String(reply, err)
		if err != nil {
			resp = fmt.Sprintf("jim command setex reply type is not int: %v", err)
			log.Error(resp)
			return
		}
		if (strings.Compare(replyStr, "ok") != 0) {
			resp = fmt.Sprintf("jim command setex reply is not ok")
			log.Error(resp)
			return
		}
	}
}

func (s *Server) aliveCheckingAlarm() {
	log.Info("alive checking alarm processing...")

	loadTicker := time.NewTicker(30 *time.Second)
	checkTicker := time.NewTicker(10 *time.Second)
	for {
		select {
		case <-loadTicker.C:
			if err := s.loadAliveCheckingAppAddrs(); err != nil {
				log.Error("loadAliveCheckingAppAddrs failed: %v", err)
			}

		case <-checkTicker.C:
			appKeys := s.copyAliveCheckingAppKeys()
			for _, key := range appKeys {
				log.Debug("to check alive app key: %v", key)
				// get app key from jimdb, if it is not in, then alarm
				// exists return interger
				reply, err := s.jimSendCommand("exists", key)
				if err != nil {
					log.Error("jim send command error: %v", err)
					continue
				}
				replyInt, err := redis.Int(reply, err)
				if err != nil {
					log.Error("jim command setex reply type is not int: %v", err)
					continue
				}

				if replyInt != 0 { // app key exists
					log.Info("alive app key exists: %v", key)
					continue
				}

				var samples []*Sample
				appName, clusterId, appAddr := s.splitAliveAppKey(key)
				info := make(map[string]interface{})
				info["spaceId"] = clusterId
				info["ip"] = appAddr
				info["app_is_not_alive"] = 1
				info["app_name"] = appName

				samples = append(samples, NewSample("", 0, 0, info))
				samplesJson := SamplesToJson(samples)

				log.Debug("alive alarm samples json: %v", samplesJson)
				if err := s.gateway.notify(Message{
					ClusterId: func() int64 {
						ret, _ :=strconv.ParseInt(clusterId, 10, 64)
						return ret
					}(),
					Title: "alive alarm",
					Content: "",
					samples: samplesJson,
				}, time.Second); err != nil {
					log.Error("alive alarm notify timeout")
				}
			}
		}
	}
}

func (s *Server) SimpleAlarm(ctx context.Context, req *alarmpb.SimpleRequest) (*alarmpb.SimpleResponse, error) {
	log.Info("receive simple alarm message: %v", req.String())
	var err error
	resp := new(alarmpb.SimpleResponse)

	clusterId := req.GetHead().GetClusterId()

	if err := s.gateway.notify(Message{
		ClusterId: clusterId,
		Title: req.GetTitle(),
		Content: req.GetContent(),
		samples: req.SampleJson,
	}, time.Second); err != nil {
		log.Error("simple alarm notify timeout")
	}

	return resp, err
}
