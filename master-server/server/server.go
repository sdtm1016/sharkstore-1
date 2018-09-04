package server

import (
	"fmt"
	"net"
	"runtime"
	"sync"

	"util/log"
	"util/server"
	"util/alarm"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"model/pkg/mspb"
	"master-server/raft"
	"golang.org/x/net/context"
	"master-server/metric"
)

type Server struct {
	raftPeers map[uint64]*Peer
	conf      *Config
	opt       *scheduleOption

	cluster *Cluster
	store   Store

	server    *server.Server
	rpcServer *grpc.Server

	metricServer *metric.Metric
	alarmServer  *alarm.Server
	alarmClient  *alarm.Client

	leaderChangeNotify chan uint64
	wg                 sync.WaitGroup
	ctx                context.Context
	cancel             context.CancelFunc
}

func (service *Server) ParseClusterInfo() []*Peer {
	var peers []*Peer
	raftPeers := make(map[uint64]*Peer)
	for _, peer := range service.conf.Cluster.Peers {
		node := &Peer{}
		node.ID = peer.ID
		node.WebManageAddr = fmt.Sprintf("%s:%d", peer.Host, peer.HttpPort)
		node.RpcServerAddr = fmt.Sprintf("%s:%d", peer.Host, peer.RpcPort)
		node.RaftHeartbeatAddr = fmt.Sprintf("%s:%d", peer.Host, peer.RaftPorts[0])
		node.RaftReplicateAddr = fmt.Sprintf("%s:%d", peer.Host, peer.RaftPorts[1])
		peers = append(peers, node)
		raftPeers[node.ID] = node
	}

	service.raftPeers = raftPeers
	return peers
}

func (service *Server) initHttpHandler() () {
	s := service.server
	s.Handle("/manage/database/create", NewHandler(service.validRequest, service.handleDatabaseCreate))
	s.Handle("/manage/database/delete", NewHandler(service.validRequest, service.handleDatabaseDelete))
	s.Handle("/manage/table/create", NewHandler(service.validRequest, service.handleTableCreate))
	s.Handle("/manage/sql/table/create", NewHandler(service.validRequest, service.handleSqlTableCreate))

	s.Handle("/manage/table/cancel", NewHandler(service.validRequest, service.handleTableCancel))
	s.Handle("/manage/table/edit", NewHandler(service.validRequest, service.handleTableEdit))
	s.Handle("/manage/table/editPolicy", NewHandler(service.validRequest, service.handleTableEditPolicy))
	s.Handle("/manage/table/delete", NewHandler(service.validRequest, service.handleTableDelete))
	s.Handle("/manage/table/delete/fast", NewHandler(service.validRequest, service.handleTableFastDelete))
	s.Handle("/manage/node/login", NewHandler(service.validRequest, service.handleHttpNodeLogin))
	s.Handle("/manage/node/logout", NewHandler(service.validRequest, service.handleHttpNodeLogout))
	s.Handle("/manage/node/delete", NewHandler(service.validRequest, service.handleHttpNodeDelete))
	s.Handle("/manage/node/upgrade", NewHandler(service.validRequest, service.handleNodeUpgrade))
	s.Handle("/manage/node/setLogLevel", NewHandler(service.validRequest, service.handleNodeSetLogLevel))
	s.Handle("/manage/node/getRangeTopo", NewHandler(service.validRequest, service.handleNodeGetRangeTopo))
	s.Handle("/manage/node/updateIsolationLabel", NewHandler(service.validRequest, service.handleNodeUpdateIsolationLabel))
	s.Handle("/manage/node/getConfigOfNode", NewHandler(service.validRequest, service.handleNodeGetConfig))
	s.Handle("/manage/node/setConfigOfNode", NewHandler(service.validRequest, service.handleNodeSetConfig))
	s.Handle("/manage/node/getDsInfoOfNode", NewHandler(service.validRequest, service.handleNodeGetDsInfo))
	s.Handle("/manage/node/clearQueueOfNode", NewHandler(service.validRequest, service.handleNodeClearQueue))
	s.Handle("/manage/node/getPendingQueuesOfNode", NewHandler(service.validRequest, service.handleNodeGetPendingQueues))
	s.Handle("/manage/node/flushDBOfNode", NewHandler(service.validRequest, service.handleNodeFlushDB))

	s.Handle("/manage/scheduler/getall", NewHandler(service.validRequest, service.handleSchedulerGetAll))
	s.Handle("/manage/scheduler/add", NewHandler(service.validRequest, service.handleAddScheduler))
	s.Handle("/manage/scheduler/remove", NewHandler(service.validRequest, service.handleRemoveScheduler))
	s.Handle("/manage/scheduler/detail", NewHandler(service.validRequest, service.handleQuerySchedulerDetail))

	s.Handle("/manage/database/getall", NewHandler(service.validRequest, service.handleDBGetAll))
	s.Handle("/manage/table/getall", NewHandler(service.validRequest, service.handleTableGetAll))
	s.Handle("/manage/get/table", NewHandler(service.validRequest, service.handleTableGet))
	s.Handle("/manage/node/getall", NewHandler(service.validRequest, service.handleNodeGetAll))
	s.Handle("/manage/master/getleader", NewHandler(service.validRequest, service.handleMasterGetLeader))
	s.Handle("/manage/master/getall", NewHandler(service.validRequest, service.handleMasterGetAll))
	//s.Handle("/manage/range/getleader", NewHandler(service.verifier, service.handleRangeGetLeader))
	//s.Handle("/manage/range/getpeerinfo", NewHandler(service.verifier, service.handleRangeGetPeerInfo))
	s.Handle("/manage/task/getTypeAll", NewHandler(service.validRequest, service.handleTaskTypeGetAll))

	s.Handle("/manage/getAutoScheduleInfo", NewHandler(service.validRequest, service.handleManageGetAutoScheduleInfo))
	s.Handle("/manage/setAutoScheduleInfo", NewHandler(service.validRequest, service.handleManageSetAutoScheduleInfo))

	s.Handle("/manage/cluster/init", NewHandler(service.validRequest, service.handleManageClusterInit))

	s.Handle("/manage/table/route/get", NewHandler(service.validRequest, service.handleTableGetRoute))
	s.Handle("/manage/range/delete", NewHandler(service.validRequest, service.handleRangeDelete))
	s.Handle("/manage/range/getRangeTopo", NewHandler(service.validRequest, service.handleRangeGetRangeTopo))
	s.Handle("/manage/range/add/peer", NewHandler(service.validRequest, service.handleRangeAddPeer))
	s.Handle("/manage/range/del/peer", NewHandler(service.validRequest, service.handleRangeDelPeer))
	s.Handle("/manage/range/leader/change", NewHandler(service.validRequest, service.handleRangeLeaderChange))
	s.Handle("/manage/range/task/query", NewHandler(service.validRequest, service.handleRangeTaskQuery))
	s.Handle("/manage/range/unhealthy/recover", NewHandler(service.validRequest, service.handleUnhealthyRangeRecover))
	s.Handle("/manage/range/rebuildRange", NewHandler(service.validRequest, service.handleRangeRecreate))
	s.Handle("/manage/range/replaceRange", NewHandler(service.validRequest, service.handleRangeRecreate))
	s.Handle("/manage/range/unhealthy/query", NewHandler(service.validRequest, service.handleUnhealthyRangeQuery))
	s.Handle("/manage/range/unstable/query", NewHandler(service.validRequest, service.handleUnstableRangeQuery))
	s.Handle("/manage/range/getPeerInfo", NewHandler(service.validRequest, service.handlePeerInfoQuery))
	s.Handle("/manage/range/updateRange", NewHandler(service.validRequest, service.handleUnhealthyRangeUpdate))
	s.Handle("/manage/range/updateEpoch", NewHandler(service.validRequest, service.handleUpdateRangeEpoch))
	s.Handle("/manage/range/offlineRange", NewHandler(service.validRequest, service.handleRangeOffline))
	s.Handle("/manage/range/transfer", NewHandler(service.validRequest, service.handleRangeTransfer))
	s.Handle("/manage/range/getOpsTopN", NewHandler(service.validRequest, service.handleRangeTopNQuery))
	s.Handle("/manage/range/forceSplitRange", NewHandler(service.validRequest, service.handleRangeForceSplit))
	s.Handle("/manage/range/forceCompactRange", NewHandler(service.validRequest, service.handleRangeForceCompact))

	s.Handle("/manage/task/getall", NewHandler(service.validRequest, service.handleGetAllTask))
	s.Handle("/manage/task/delete", NewHandler(service.validRequest, service.handleDeleteTask))

	s.Handle("/manage/range/leader/query", NewHandler(service.validRequest, service.handleRangeLeaderQuery))
	s.Handle("/manage/topology/query", NewHandler(service.validRequest, service.handleTopologyQuery))
	s.Handle("/manage/table/topology/query", NewHandler(service.validRequest, service.handleTableTopologyQuery))
	s.Handle("/manage/table/topology/missing", NewHandler(service.validRequest, service.handleTableTopologyMissing))
	s.Handle("/manage/table/topology/create", NewHandler(service.validRequest, service.handleTableTopologyCreate))
	s.Handle("/manage/table/topology/batchCreate", NewHandler(service.validRequest, service.handleTableTopologyBatchCreate))
	s.Handle("/manage/table/range/duplicate", NewHandler(service.validRequest, service.handleTableRangeDuplicate))
	s.Handle("/debug/node/info", NewHandler(service.validRequest, service.handleDebugNodeInfo))
	s.Handle("/debug/range/info", NewHandler(service.validRequest, service.handleDebugRangeInfo))
	//s.Handle("/debug/range/trace", NewHandler(service.verifier, service.handleDebugRangeTrace))
	//s.Handle("/debug/node/trace", NewHandler(service.verifier, service.handleDebugNodeTrace))
	s.Handle("/debug/log/setlevel", NewHandler(service.validRequest, service.handleDebugLogSetLevel))
	s.Handle("/debug/range/getall", NewHandler(service.validRequest, service.handleTableGetRanges))
	s.Handle("/debug/table/topology/check", NewHandler(service.validRequest, service.handleTopologyCheck))
	s.Handle("/debug/range/search", NewHandler(service.validRequest, service.handleSearchRange))

	s.Handle("/peer/delete_force", NewHandler(service.validRequest, service.handlePeerDeleteForce))
	s.Handle("/range/locate", NewHandler(service.validRequest, service.handleRangeLocate))

	s.Handle("/metric/config/set", NewHandler(service.validRequest, service.handleMetricConfigSet))
	s.Handle("/metric/config/get", NewHandler(service.validRequest, service.handleMetricConfigGet))
	s.Handle("/test/alarm", NewHandler(nil, service.handleTestAlarm))

	return
}

func (service *Server) InitMasterServer(conf *Config) {
	service.leaderChangeNotify = make(chan uint64, 5)
	peers := service.ParseClusterInfo()
	if len(peers) == 0 {
		log.Fatal("init server failed")
		return
	}
	cnf := &StoreConfig{
		RaftRetainLogs:        int64(conf.Raft.RetainLogsCount),
		RaftHeartbeatInterval: conf.Raft.HeartbeatInterval.Duration,
		RaftHeartbeatAddr:     conf.raftHeartbeatAddr,
		RaftReplicateAddr:     conf.raftReplicaAddr,
		RaftPeers:             peers,

		NodeID:   conf.NodeId,
		DataPath: service.conf.DataPath,

		LeaderChangeHandler: func(leader uint64) {
			service.leaderChangeNotify <- leader
		},
		FatalHandler: func(err *raft.FatalError) {
			// TODO event
			log.Error("raft fatal: id[%d], err[%v]", err.ID, err.Err)
		},
	}
	saveStore, err := NewRaftStore(cnf)
	if err != nil {
		log.Fatal("create raft store failed, err[%v]", err)
		return
	}
	service.store = saveStore
	opt := newScheduleOption(conf)
	service.opt = opt
	service.cluster = NewCluster(uint64(conf.Cluster.ClusterID), uint64(conf.NodeId), saveStore, opt)
	if service.server == nil {
		s := server.NewServer()
		s.Init("master", &server.ServerConfig{
			Sock:    service.conf.webManageAddr,
			Version: "v1",
		}, )
		service.server = s
	}
	service.initHttpHandler()
	if len(conf.Alarm.ServerAddress) != 0 {
		service.alarmClient, err = alarm.NewAlarmClient(conf.Alarm.ServerAddress)
		if err != nil {
			log.Fatal("create alarm client failed, err[%v]", err)
		}
		service.cluster.alarmCli = service.alarmClient
	}
}

func (service *Server) InitAlarmServer(conf AlarmConfig) (err error) {
	//var alarmReceivers []*alarm.User
	//for _, r := range conf.Receivers {
	//	alarmReceivers = append(alarmReceivers, &alarm.User{
	//		Mail: r.Mail,
	//		Sms:  r.Sms,
	//	})
	//}
	log.Info("alarm server config: \n" +
		"	server port: %v\n" +
		"	remote alarm server addr: %v\n" +
		"	mysql args: %v\n" +
		"	jim url: %v\n" +
		"	jim app addr: %v\n", conf.ServerPort,
		conf.RemoteAlarmServerAddress, conf.MysqlArgs, conf.JimUrl, conf.JimApAddr)

	service.alarmServer, err = alarm.NewAlarmServer(service.ctx, conf.ServerPort,
		conf.RemoteAlarmServerAddress, conf.MysqlArgs, conf.JimUrl, conf.JimApAddr)
	if err != nil {
		log.Error("alarm.NewAlarmServer failed, err: [%v]", err)
		return nil
	}

	// app ping url, for alive alarm
	service.server.Handle("/app/ping", service.alarmServer.HandleAppPing)
	return nil
}

func (service *Server) InitMetricServer(conf *Config) {
	if service.server == nil {
		s := server.NewServer()
		s.Init("metric", &server.ServerConfig{
			Sock:    conf.Metric.Server.Address,
			Version: "v1",
		})
		service.server = s
	}
	var store metric.Store
	if conf.Metric.Server.StoreType == metric.TYPE_SQL {
		dns := fmt.Sprintf("%s/%s?readTimeout=5s&writeTimeout=5s&timeout=10s",
			conf.Metric.Server.StoreUrl[0], "fbase")
		store = metric.NewSqlStore(dns, int(conf.Metric.Server.QueueNum))
	} else { //默认为es存储
		store = metric.NewEsStore(conf.Metric.Server.StoreUrl, int(conf.Metric.Server.QueueNum))
	}
	service.metricServer = metric.NewMetric(service.server, store, conf.Threshold)

	if len(conf.Alarm.ServerAddress) != 0 {
		var err error
		service.alarmClient, err = alarm.NewAlarmClient(conf.Alarm.ServerAddress)
		if err != nil {
			log.Fatal("create alarm client failed, err[%v]", err)
		}
		service.metricServer.AlarmCli = service.alarmClient
	}
}

func (service *Server) InitServer(conf *Config) {
	service.conf = conf
	ctx, cancel := context.WithCancel(context.Background())
	service.ctx = ctx
	service.cancel = cancel

	switch conf.Role {
	case "master":
		service.InitMasterServer(conf)
	case "metric":
		service.InitMetricServer(conf)
		service.InitAlarmServer(conf.Alarm)
	}
}

func (service *Server) MasterStart() {
	// 最后加载raft
	err := service.store.Open()
	if err != nil {
		log.Fatal("raft load failed, err[%v]", err)
	}
	// rpc
	lis, err := net.Listen("tcp", service.conf.rpcServerAddr)
	if err != nil {
		log.Fatal("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	mspb.RegisterMsServerServer(s, service)
	// Register reflection service on gRPC server.
	reflection.Register(s)
	go func() {
		if err = s.Serve(lis); err != nil {
			log.Fatal("failed to serve: %v", err)
		}
	}()
	service.rpcServer = s
	service.background()
}

func (service *Server) MetricStart() {
	err := service.metricServer.Open()
	if err != nil {
		log.Fatal("open metric store failed, err[%v]", err)
	}
}

func (service *Server) Start() error {
	defer func() {
		if x := recover(); x != nil {
			buf := make([]byte, 1<<20)
			runtime.Stack(buf, true)
			log.Error("\n--------------------\n%s", buf)
			panic(buf)
		}
	}()
	switch service.conf.Role {
	case "master":
		service.MasterStart()
	case "metric":
		service.MetricStart()
	}
	service.server.Run()
	return nil
}

func (service *Server) GetLeader() *Peer {
	return service.cluster.GetLeader()
}

func (service *Server) IsLeader() bool {
	return service.cluster.IsLeader()
}

func (service *Server) getRaftMembers() []*Peer {
	var raftMembers []*Peer
	for _, p := range service.raftPeers {
		raftMembers = append(raftMembers, p)
	}
	return raftMembers

}

func (service *Server) RaftLeaderChange(leaderId uint64) {
	log.Info("leader change[%d]", leaderId)
	// leader 没有变更
	if service.cluster.GetLeader().GetId() == leaderId {
		log.Info("leader not changed")
		return
	}
	// 本节点当选为leader

	raftLeader := service.raftPeers[leaderId]
	if service.conf.NodeId == leaderId {
		log.Info("be elected leader")
		cluster := NewCluster(uint64(service.conf.Cluster.ClusterID), uint64(service.conf.NodeId), service.store, service.opt)
		err := cluster.LoadCache()
		if err != nil {
			log.Fatal("master server load disk to mem failed, err[%v]", err)
			return
		}
		_cluster := service.cluster
		_cluster.Close()
		cluster.UpdateLeader(raftLeader)
		cluster.Start()
		service.cluster = cluster
		service.cluster.alarmCli = service.alarmClient
		return
	}

	service.cluster.UpdateLeader(raftLeader)
	service.cluster.Close()
}

func (service *Server) watchLeader() {
	defer service.wg.Done()
	for {
		select {
		case <-service.ctx.Done():
			return
		case leaderId := <-service.leaderChangeNotify:
			service.RaftLeaderChange(leaderId)
		}
	}
}

func (service *Server) background() {
	service.wg.Add(1)
	go service.watchLeader()
}

// Quit 保存退出
func (service *Server) Quit() {
	// TODO: safe quit
}
