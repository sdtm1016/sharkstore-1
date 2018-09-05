package server

import (
	"bytes"
	"container/heap"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"master-server/http_reply"
	"model/pkg/ds_admin"
	"model/pkg/metapb"
	"model/pkg/taskpb"
	"util"
	"util/deepcopy"
	"util/log"
	"util/server"
)

var (
	http_error                       string = "bad request"
	http_error_parameter_not_enough  string = "parameter is not enough"
	http_error_invalid_parameter     string = "invalid param"
	http_error_database_find         string = "database is not existed"
	http_error_table_find            string = "table is not existed"
	http_error_table_deleted         string = "table is deleted"
	http_error_range_find            string = "range is not existed"
	http_error_peer_find             string = "range peer is not existed"
	http_error_node_find             string = "node is not existed"
	http_error_range_split           string = "range is spliting"
	http_error_range_create          string = "range create error"
	http_error_cluster_has_no_leader string = "raft cluster has no leader"
	http_error_master_is_not_leader  string = "this master server is not leader node"
	http_error_wrong_sign            string = "sign is wrong"
	http_error_sign_timeout          string = "sign timeout."
	http_error_invalid_signtime      string = "invalid sign timestamp."
	http_error_database_exist        string = "database is existed"
	http_error_wrong_cluster         string = "cluster id is wrong"
	http_error_range_busy            string = "range is busy"
)

const (
	HTTP_OK = iota
	HTTP_ERROR
	HTTP_ERROR_PARAMETER_NOT_ENOUGH
	HTTP_ERROR_INVALID_PARAM
	HTTP_ERROR_DATABASE_FIND
	HTTP_ERROR_TABLE_FIND
	HTTP_ERROR_TABLE_DELETED
	HTTP_ERROR_RANGE_CREATE
	HTTP_ERROR_CLUSTER_HAS_NO_LEADER
	HTTP_ERROR_MASTER_IS_NOT_LEADER
	HTTP_ERROR_WRONG_SIGN
	HTTP_ERROR_SIGN_TIMEOUT
	HTTP_ERROR_INVALID_SIGNTIME
	HTTP_ERROR_RANGE_FIND
	HTTP_ERROR_RANGE_SPLIT
	HTTP_ERROR_DATABASE_EXISTED
	HTTP_ERROR_TASK_FIND
	HTTP_ERROR_CLUSTERID
	HTTP_ERROR_NODE_FIND
	HTTP_ERROR_RANGE_BUSY
	HTTP_ERROR_PEER_FIND
)

const (
	HTTP_DB_NAME                    = "dbName"
	HTTP_DB_ID                      = "dbId"
	HTTP_TABLE_NAME                 = "tableName"
	HTTP_TABLE_ID                   = "tableId"
	HTTP_CLUSTER_ID                 = "clusterId"
	HTTP_RANGE_ID                   = "rangeId"
	HTTP_NODE_ID                    = "nodeId"
	HTTP_NODE_IDS                   = "nodeIds"
	HTTP_PEER_ID                    = "peerId"
	HTTP_NAME                       = "name"
	HTTP_PROPERTIES                 = "properties"
	HTTP_PKDUPCHECK                 = "pkDupCheck"
	HTTP_RANGEKEYS_NUM              = "rangeKeysNum"
	HTTP_RANGEKEYS_START            = "rangeKeysStart"
	HTTP_RANGEKEYS_END              = "rangeKeysEnd"
	HTTP_RANGEKEYS                  = "rangeKeys"
	HTTP_POLICY                     = "policy"
	HTTP_D                          = "d"
	HTTP_S                          = "s"
	HTTP_TOKEN                      = "token"
	HTTP_SQL                        = "sql"
	HTTP_SERVER_PORT                = "serverPort"
	HTTP_RAFT_HEARTBEAT_PORT        = "raftHeartbeatPort"
	HTTP_RAFT_REPLICA_PORT          = "raftReplicaPort"
	HTTP_TASK_ID                    = "taskId"
	HTTP_TASK_IDS                   = "taskIds"
	HTTP_MACHINES                   = "machines"
	HTTP_CLUSTER_AUTO_SCHEDULE_INFO = "clusterAutoScheduleInfo"
	HTTP_AUTO_TRANSFER_UNABLE       = "autoTransferUnable"
	HTTP_AUTO_FAILOVER_UNABLE       = "autoFailoverUnable"
	HTTP_AUTO_SPLIT_UNABLE          = "autoSplitUnable"
	HTTP_TABLE_AUTO_INFO            = "tableAutoInfo"
	HTTP_FAST                       = "fast"
	HTTP_STARTKEY                   = "startKey"
	HTTP_ENDKEY                     = "endKey"
	HTTP_ISOLATION_LABEL 			= "isolationLabel"
)

const (
	ROUTE_SUBSCRIBE = "route_subscribe"
)

type Proxy struct {
	targetHost string
}

func (p *Proxy) proxy(w http.ResponseWriter, r *http.Request) {
	director := func(req *http.Request) {
		req.URL.Scheme = "http"
		req.URL.Host = p.targetHost
	}
	proxy := &httputil.ReverseProxy{Director: director}
	proxy.ServeHTTP(w, r)
}

type HttpReply httpReply

type httpReply struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

// 签名计算 clusterId + time + token
func (service *Server) verifier(w http.ResponseWriter, r *http.Request) bool {
	if service.conf.SecretKey == "" {
		return true
	}
	d := r.FormValue(HTTP_D)
	sign := r.FormValue(HTTP_S)

	if len(d) == 0 || len(sign) == 0 {
		log.Warn("d/s len = 0")
		sendReply(w, &httpReply{
			Code:    HTTP_ERROR_PARAMETER_NOT_ENOUGH,
			Message: http_error_parameter_not_enough + " no d/s",
		})
		return false
	}
	// TODO timeout check
	sec, err := strconv.ParseUint(d, 10, 64)
	if err != nil {
		log.Warn("invalid time %s", d)
		sendReply(w, &httpReply{
			Code:    HTTP_ERROR_INVALID_SIGNTIME,
			Message: http_error_invalid_signtime,
		})
		return false
	}
	// 5分钟超时时间
	if time.Unix(int64(sec), 0).Add(time.Minute * 5).Before(time.Now()) {
		log.Warn("sign timeout")
		sendReply(w, &httpReply{
			Code:    HTTP_ERROR_SIGN_TIMEOUT,
			Message: http_error_sign_timeout,
		})
		return false
	}
	h := md5.New()
	h.Write([]byte(fmt.Sprintf("%d", service.conf.Cluster.ClusterID)))
	h.Write([]byte(d))
	h.Write([]byte(service.conf.SecretKey))
	if sign != fmt.Sprintf("%x", h.Sum(nil)) {
		log.Warn("wrong sign")
		sendReply(w, &httpReply{
			Code:    HTTP_ERROR_WRONG_SIGN,
			Message: http_error_wrong_sign,
		})
		return false
	}
	return true
}

func (service *Server) validRequest(w http.ResponseWriter, r *http.Request) bool {
	reply := &httpReply{}
	if !service.IsLeader() {
		log.Debug("service not leader node %d leader %v", service.cluster.nodeId, service.cluster.leader)
		if point := service.GetLeader(); point == nil {
			reply.Code = HTTP_ERROR_MASTER_IS_NOT_LEADER
			reply.Message = http_error_cluster_has_no_leader
			sendReply(w, reply)
		} else {
			proxy := &Proxy{targetHost: point.WebManageAddr}
			proxy.proxy(w, r)
		}
		return false
	}
	return service.verifier(w, r)
}

func sendReply(w http.ResponseWriter, httpreply *httpReply) {
	reply, err := json.Marshal(httpreply)
	if err != nil {
		log.Error("http reply marshal error: %s", err)
		w.WriteHeader(500)
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.Error("http reply[%s] len[%d] write error: %v", string(reply), len(reply), err)
	}
}

func (service *Server) handleDatabaseCreate(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	dbName := r.FormValue(HTTP_NAME)
	dbProperties := r.FormValue(HTTP_PROPERTIES)

	if dbName == "" {
		log.Error("http create database: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}

	database, err := service.cluster.CreateDatabase(dbName, dbProperties)
	if err != nil {
		if err == ErrDupDatabase {
			log.Warn("create database[%s] repeat", dbName)
		} else {
			log.Error("http create database: %v", err)
			reply.Code = HTTP_ERROR
			reply.Message = err.Error()
			return
		}
	}
	log.Info("create database[%s] success", dbName)
	reply.Data = deepcopy.Iface(database.DataBase).(*metapb.DataBase)
	return
}

func (service *Server) handleTableCreate(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	properties := r.FormValue(HTTP_PROPERTIES)
	pkDupCheck := r.FormValue(HTTP_PKDUPCHECK)
	isolationLabel := r.FormValue(HTTP_ISOLATION_LABEL)
	policy, _ := strconv.Atoi(r.FormValue(HTTP_POLICY))
	if dbName == "" || tName == "" || properties == "" {
		log.Error("http create table: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	if strings.IndexByte(tName, '-') != -1 {
		reply.Code = HTTP_ERROR
		reply.Message = "table name has letter '-'"
		return
	}
	rangeKeysNumStr := r.FormValue(HTTP_RANGEKEYS_NUM)
	rangeKeysStart := r.FormValue(HTTP_RANGEKEYS_START)
	rangeKeysEnd := r.FormValue(HTTP_RANGEKEYS_END)
	rangeKeys := r.FormValue(HTTP_RANGEKEYS)
	var rangeKeysNum uint64
	if "" != rangeKeysNumStr {
		var err error
		rangeKeysNum, err = strconv.ParseUint(rangeKeysNumStr, 10, 64)
		if err != nil {
			log.Error("http create table: %s", http_error_database_find)
			reply.Code = HTTP_ERROR
			reply.Message = fmt.Errorf("rangeKeysNum is not int: %v", err).Error()
			return
		}
		if "" == rangeKeysStart || "" == rangeKeysEnd {
			log.Error("http create table: %s", http_error_database_find)
			reply.Code = HTTP_ERROR
			reply.Message = "rangeKeysStart or rangeKeysEnd is empty"
			return
		}
	}
	if _, ok := service.cluster.FindDatabase(dbName); !ok {
		log.Error("http create table: %s", http_error_database_find)
		reply.Code = HTTP_ERROR_DATABASE_FIND
		reply.Message = http_error_database_find
		return
	}
	if len(service.cluster.GetAllActiveNode()) == 0 {
		log.Error("http create table: %s", http_error)
		reply.Code = HTTP_ERROR
		reply.Message = "cluster has no node"
		return
	}

	// 默认不检查
	if len(pkDupCheck) == 0 {
		pkDupCheck = "false"
	}
	columns, regxs, err := ParseProperties(properties)
	if err != nil {
		log.Error("parse cols: %s failed, err: %v", properties, err)
		reply.Code = HTTP_ERROR_INVALID_PARAM
		reply.Message = err.Error()
		return
	}
	var sliceKeys [][]byte
	if len(rangeKeys) != 0 {
		if sliceKeys, err = rangeKeysSplit(rangeKeys, ","); err != nil {
			log.Error("invalid rangekeys %s, err[%v]", rangeKeys, err)
			reply.Code = HTTP_ERROR_INVALID_PARAM
			reply.Message = err.Error()
			return
		}
	} else if len(rangeKeysStart) != 0 && len(rangeKeysEnd) != 0 && rangeKeysNum != 0 {
		sliceKeys, err = ScopeSplit([]byte(rangeKeysStart), []byte(rangeKeysEnd), rangeKeysNum, nil)
		if err != nil {
			log.Error("create table error: scope split %v", err)
			reply.Code = HTTP_ERROR_INVALID_PARAM
			reply.Message = err.Error()
			return
		}
	}
	table, err := service.cluster.CreateTable(dbName, tName, isolationLabel, int32(policy), columns, regxs, pkDupCheck != "false", sliceKeys)
	if err != nil {
		if err == ErrDupTable {
			log.Warn("http create table repeat %s", tName)
		} else {
			log.Error("http create table: %v", err)
			reply.Code = HTTP_ERROR
			reply.Message = err.Error()
			return
		}
	}
	log.Info("create table[%s:%s]", dbName, tName)
	reply.Data = deepcopy.Iface(table.Table).(*metapb.Table)
	return
}

func (service *Server) handleSqlTableCreate(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	dbName := r.FormValue(HTTP_DB_NAME)
	command := r.FormValue(HTTP_SQL)
	log.Debug("sql create table: %v", command)

	if len(dbName) == 0 || len(command) == 0 {
		log.Error("sql http create table: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	if _, ok := service.cluster.FindDatabase(dbName); !ok {
		log.Error("sql http create table: %s", http_error_database_find)
		reply.Code = HTTP_ERROR_DATABASE_FIND
		reply.Message = http_error_database_find
		return
	}

	var table *metapb.Table
	if table = parseCreateTableSql(command); table == nil {
		reply.Code = HTTP_ERROR
		reply.Message = "table is nil, check log for detail"
		return
	}
	if strings.IndexByte(table.GetName(), '-') != -1 {
		reply.Code = HTTP_ERROR
		reply.Message = "table name has letter '-'"
		return
	}

	_, err := service.cluster.CreateTable(dbName, table.GetName(), "", 0, table.GetColumns(), nil, false, nil)
	if err != nil {
		log.Error("http create table: %v", err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	log.Info("sql table create [%s:%s] success", dbName, table.GetName())
	return
}

func (service *Server) handleNodeUpgrade(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	var id uint64
	var err error
	if id, err = strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64); err != nil {
		log.Error("http upgrade node: %v", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	if err := service.cluster.NodeUpgrade(id); err != nil {
		log.Error("http upgrade node failed. error:[%v]", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	return
}

func (service *Server) handleNodeSetLogLevel(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	var id uint64
	var err error
	if id, err = strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64); err != nil {
		log.Error("http set log level of node: %v", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}

	logLevel := r.FormValue("logLevel")
	if len(logLevel) == 0 {
		log.Error("http set node log level: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}

	if err := service.cluster.setNodeLogLevelRemote(id, logLevel); err != nil {
		log.Error("http set node log level failed. error:[%v]", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	return
}

func (service *Server) handleNodeSetConfig(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	var nodeId uint64
	var err error
	if nodeId, err = strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64); err != nil {
		log.Error("http set config of node: %v", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}

	configString := r.FormValue("setConfig")
	if configString == "" {
		log.Error("setConfigs cannot be empty.")
		reply.Code = -1
		reply.Message = "setConfigs cannot be empty."
		return
	}

	var configArray []ds_adminpb.ConfigItem
	if err = json.Unmarshal([]byte(configString), &configArray); err != nil {
		log.Error("configs json parse error: %v", err.Error())
		reply.Code = -1
		reply.Message = "configs json parse error: " + err.Error()
		return
	}

	var configs []*ds_adminpb.ConfigItem
	for _, configItem := range configArray {
		item := deepcopy.Iface(configItem).(ds_adminpb.ConfigItem)
		configs = append(configs, &item)
	}
	if err = service.cluster.setConfigRemote(nodeId, configs); err != nil {
		log.Error("http set node config failed. error:[%v]", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	return
}

func (service *Server) handleNodeGetConfig(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	var nodeId uint64
	var err error
	if nodeId, err = strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64); err != nil {
		log.Error("http get config of node: %v", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}

	keyString := r.FormValue("getConfigKey")
	if keyString == "" {
		log.Error("config key cannot be empty.")
		reply.Code = -1
		reply.Message = "config key cannot be empty."
		return
	}

	var keyArray []ds_adminpb.ConfigKey
	if err = json.Unmarshal([]byte(keyString), &keyArray); err != nil {
		log.Error("config key json parse error: %v", err.Error())
		reply.Code = -1
		reply.Message = "config key json parse error: " + err.Error()
		return
	}

	var keys []*ds_adminpb.ConfigKey
	for _, configKey := range keyArray {
		key := deepcopy.Iface(configKey).(ds_adminpb.ConfigKey)
		keys = append(keys, &key)
	}
	resp, err := service.cluster.getConfigRemote(nodeId, keys)
	if err != nil {
		log.Error("http get node config failed. error:[%v]", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	//configBytes, err := json.Marshal(resp.Configs)
	//if err != nil {
	//	log.Error("http get node config failed. error:[%v]", err.Error())
	//	reply.Code = -1
	//	reply.Message = err.Error()
	//	return
	//}
	//reply.Data = string(configBytes)
	reply.Data = resp.Configs
	return
}

func (service *Server) handleNodeGetDsInfo(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	var nodeId uint64
	var err error
	if nodeId, err = strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64); err != nil {
		log.Error("http get ds_info of node: %v", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}

	path := r.FormValue("dsInfoPath")
	if path == "" {
		log.Error("http get ds_info of node error: path is empty")
		reply.Code = -1
		reply.Message = "http get ds_info of node error: path is empty"
		return
	}

	resp, err := service.cluster.getDsInfoRemote(nodeId, path)
	if err != nil {
		log.Error("http get ds_info failed. error:[%v]", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	reply.Data = resp.Data
	return
}

func (service *Server) handleRangeForceSplit(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	var err error
	rangeId, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("http range force split error: %s", http_error_invalid_parameter)
		reply.Code = HTTP_ERROR_INVALID_PARAM
		reply.Message = http_error_invalid_parameter
		return
	}
	rng := service.cluster.FindRange(rangeId)
	if rng == nil {
		log.Error("http range force split error: %s", http_error_range_find)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}

	node := service.cluster.FindNodeById(rng.Leader.NodeId)
	if node == nil {
		log.Error("http range force split error: %s", http_error_node_find)
		reply.Code = HTTP_ERROR_NODE_FIND
		reply.Message = http_error_node_find
		return
	}

	err = service.cluster.ForceSplitRemote(node.GetAdminAddr(), rangeId, rng.GetRangeEpoch().GetVersion())
	if err != nil {
		log.Error("http range force split failed. error:[%v]", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	return
}

func (service *Server) handleRangeForceCompact(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	var err error
	rangeId, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("http range force compact error: %s", http_error_invalid_parameter)
		reply.Code = HTTP_ERROR_INVALID_PARAM
		reply.Message = http_error_invalid_parameter
		return
	}
	rng := service.cluster.FindRange(rangeId)
	if rng == nil {
		log.Error("http range force compact error: %s", http_error_range_find)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}

	node := service.cluster.FindNodeById(rng.Leader.NodeId)
	if node == nil {
		log.Error("http range force compact error: %s", http_error_node_find)
		reply.Code = HTTP_ERROR_NODE_FIND
		reply.Message = http_error_node_find
		return
	}

	_, err = service.cluster.ForceCompactRemote(node.GetAdminAddr(), rangeId)
	if err != nil {
		log.Error("http range force compact failed. error:[%v]", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	// we haven't use the resp now
	//reply.Data = resp.EndKey
	return
}

func (service *Server) handleNodeClearQueue(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	var nodeId uint64
	var err error
	if nodeId, err = strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64); err != nil {
		log.Error("http node clear queue: %v", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}

	var queueType ds_adminpb.ClearQueueRequest_QueueType
	queueTypeInt, err := strconv.ParseInt(r.FormValue("queueType"), 10, 64)
	if err != nil {
		log.Error("http node clear queue: %v", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	switch queueTypeInt {
	case 2:
		queueType = ds_adminpb.ClearQueueRequest_SLOW_WORKER
	case 1:
		queueType = ds_adminpb.ClearQueueRequest_FAST_WORKER
	case 0:
	default:
		queueType = ds_adminpb.ClearQueueRequest_ALL
	}

	resp, err := service.cluster.clearQueueRemote(nodeId, queueType)
	if err != nil {
		log.Error("http node clear queue[type=%v] failed. error:[%v]", queueType, err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	reply.Data = resp.Cleared
	return
}

func (service *Server) handleNodeGetPendingQueues(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	var pendingType ds_adminpb.GetPendingsRequest_PendingType
	var nodeId, count, pendingTypeInt uint64
	var err error
	if nodeId, err = strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64); err != nil {
		log.Error("http node clear queue: %v", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}

	if count, err = strconv.ParseUint(r.FormValue("count"), 10, 64); err != nil {
		log.Error("http node clear queue: %v", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}

	if pendingTypeInt, err = strconv.ParseUint(r.FormValue("pendingType"), 10, 64); err != nil {
		log.Error("http node clear queue: %v", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}

	switch pendingTypeInt {
	case 4:
		pendingType = ds_adminpb.GetPendingsRequest_RANGE_SELECT
	case 3:
		pendingType = ds_adminpb.GetPendingsRequest_PONIT_SELECT
	case 2:
		pendingType = ds_adminpb.GetPendingsRequest_SELECT
	case 1:
		pendingType = ds_adminpb.GetPendingsRequest_INSERT
	case 0:
	default:
		pendingType = ds_adminpb.GetPendingsRequest_ALL
	}

	resp, err := service.cluster.getPendingQueuesRemote(nodeId, pendingType, count)
	if err != nil {
		log.Error("http node get pending queue[type=%v, count=%d] failed. error:[%v]", pendingType, count, err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	reply.Data = resp.Desc
	return
}

func (service *Server) handleNodeFlushDB(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	var nodeId uint64
	var err error
	if nodeId, err = strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64); err != nil {
		log.Error("http node flush db: %v", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}

	wait, err := strconv.ParseBool(r.FormValue("wait"))
	if err != nil {
		log.Error("http node flush db: %v", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}

	err = service.cluster.flushDBRemote(nodeId, wait)
	if err != nil {
		log.Error("http node flush db failed. error:[%v]", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	return
}

func (service *Server) handleSchedulerGetAll(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	log.Debug("get executed schedule all")
	reply.Data = service.cluster.GetAllWorker()
	return
}

func (service *Server) handleQuerySchedulerDetail(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	name := r.FormValue("name")
	log.Debug("get schedule %v info ", name)
	reply.Data = service.cluster.GetWorkerInfo(name)
	return
}

func (service *Server) handleAddScheduler(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	cluster := service.cluster
	name := r.FormValue("name")
	switch name {
	case failoverWorkerName:
		cluster.AddFailoverWorker()
	case deleteTableWorkerName:
		cluster.AddDeleteTableWorker()
	case trashReplicaGcWorkerName:
		cluster.AddTrashReplicaGCWorker()
	case createTableWorkerName:
		cluster.AddCreateTableWorker()
	case rangeHbCheckWorkerName:
		cluster.AddRangeHbCheckWorker()
	case balanceRangeWorkerName:
		cluster.AddBalanceRangeWorker()
	case balanceLeaderWorkerName:
		cluster.AddBalanceLeaderWorker()
	case balanceNodeOpsWorkerName:
		cluster.AddBalanceNodeOpsWorker()

	default:
		log.Warn("unknown worker %s", name)
		reply.Code = -1
		reply.Message = "unknown worker"
	}
	return
}

func (service *Server) handleRemoveScheduler(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	cluster := service.cluster
	name := r.FormValue("name")
	err := cluster.RemoveWorker(name)
	if err != nil {
		reply.Code = -1
		reply.Message = err.Error()
	}
	return
}

func (service *Server) handleDBGetAll(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	reply.Data = service.cluster.GetAllDatabase()
	return
}

func (service *Server) handleTableGetAll(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	dbName := r.FormValue(HTTP_DB_NAME)

	if dbName == "" {
		log.Error("http table getall: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	var db *Database
	var ok bool
	if db, ok = service.cluster.FindDatabase(dbName); !ok {
		log.Error("http table getall: db [%s] not found", dbName)
		reply.Code = HTTP_ERROR_DATABASE_FIND
		reply.Message = http_error_database_find
		return
	}
	reply.Data = db.GetAllTable()
	return
}

func (service *Server) handleTableGet(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)

	if dbName == "" || tName == "" {
		log.Error("http get table: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	var db *Database
	var ok bool
	if db, ok = service.cluster.FindDatabase(dbName); !ok {
		log.Error("http get table: db [%s] not found", dbName)
		reply.Code = HTTP_ERROR_DATABASE_FIND
		reply.Message = http_error_database_find
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		log.Error("http get table: db [%s] not found", dbName)
		reply.Code = HTTP_ERROR_TABLE_FIND
		reply.Message = http_error_table_find
		return
	}
	reply.Data = deepcopy.Iface(table.Table).(*metapb.Table)
	return
}

func (service *Server) handleNodeGetAll(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	nodes := service.cluster.GetAllNode()
	var _nodes []*metapb.Node
	for _, n := range nodes {
		nodeCopy := deepcopy.Iface(n.Node).(*metapb.Node)
		nodeCopy.Version = fmt.Sprintf("%d", n.opsStat.GetMax())
		_nodes = append(_nodes, nodeCopy)
	}
	reply.Data = _nodes
	return
}

func (service *Server) handleMasterGetLeader(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	point := service.GetLeader()
	if point == nil {
		log.Error("http get master leader: no leader")
		reply.Code = HTTP_ERROR_CLUSTER_HAS_NO_LEADER
		reply.Message = http_error_cluster_has_no_leader
		return
	}
	reply.Data = deepcopy.Iface(point).(*Peer)
}

func (service *Server) handleMasterGetAll(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	type MsMember struct {
		LeaderId uint64  `json:"leader_id,omitempty"`
		Node     []*Peer `json:"node,omitempty"`
	}

	point := service.GetLeader()
	if point == nil {
		log.Error("http get master all members: no leader")
		reply.Code = HTTP_ERROR_CLUSTER_HAS_NO_LEADER
		reply.Message = http_error_cluster_has_no_leader
		return
	}

	reply.Data = &MsMember{
		LeaderId: point.GetId(),
		Node:     service.getRaftMembers()}
}

func (service *Server) handleRangeGetLeader(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	rangeid, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)

	if err != nil {
		log.Error("http get range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	cluster := service.cluster
	range_ := cluster.FindRange(rangeid)
	if range_ == nil {
		log.Error("http get range leader: range [%d] is not existed", rangeid)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	leader := range_.Leader
	if leader == nil {
		log.Error("http get range leader: no leader")
		reply.Code = HTTP_ERROR_CLUSTER_HAS_NO_LEADER
		reply.Message = http_error_cluster_has_no_leader
		return
	}
	if node := cluster.FindNodeById(leader.GetNodeId()); node != nil {
		reply.Data = node.GetServerAddr()
	}
	return
}

func (service *Server) handleRangeGetRangeTopo(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	rangeId, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("http get range id error: %s", http_error_invalid_parameter)
		reply.Code = HTTP_ERROR_INVALID_PARAM
		reply.Message = http_error_invalid_parameter
		return
	}

	cluster := service.cluster
	myRange := cluster.FindRange(rangeId)
	if myRange == nil {
		log.Error("http get range[rangeId=%d] info maybe not exists", rangeId)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}

	opsDescription := fmt.Sprintf(" \nBytesWritten=[%d], BytesRead=[%d], KeysWritten=[%d], KeysRead=[%d], OpsMax=[%d]",
		myRange.BytesWritten, myRange.BytesRead, myRange.KeysWritten, myRange.KeysRead, myRange.opsStat.GetMax())

	type Peer struct {
		Id   uint64       `json:"id,omitempty"`
		Node *metapb.Node `json:"node,omitempty"`
	}
	type Range struct {
		Id uint64 `json:"id,omitempty"`
		// Range key range [start_key, end_key).
		StartKey   []byte             `json:"start_key,omitempty"`
		EndKey     []byte             `json:"end_key,omitempty"`
		RangeEpoch *metapb.RangeEpoch `json:"range_epoch,omitempty"`
		Peers      []*Peer            `json:"peers,omitempty"`
		// Range state
		State      int32  `json:"state,omitempty"`
		DbName     string `json:"db_name,omitempty"`
		TableName  string `json:"table_name,omitempty"`
		TableId    uint64 `json:"table_id,omitempty"`
		CreateTime int64  `json:"create_time,omitempty"`
		LastHbTime string `json:"last_hb_time,omitempty"`
	}
	type Route struct {
		Range  *Range `json:"range,omitempty"`
		Leader *Peer  `json:"leader,omitempty"`
	}

	var _peers []*Peer
	for _, p := range myRange.Peers {
		node := cluster.FindNodeById(p.NodeId)
		peer := &Peer{
			p.Id,
			deepcopy.Iface(node.Node).(*metapb.Node),
		}
		if myRange.GetLeader().GetId() == p.GetId() {
			continue
		}
		_peers = append(_peers, peer)
	}
	_range := &Range{
		Id:         myRange.GetId(),
		StartKey:   myRange.StartKey,
		EndKey:     myRange.EndKey,
		RangeEpoch: myRange.RangeEpoch,
		Peers:      _peers,
		State:      int32(myRange.State),
		LastHbTime: myRange.LastHbTimeTS.Format("2006-01-02 15:04:05") + opsDescription,
		TableId:    myRange.GetTableId(),
	}
	_route := &Route{
		Range: _range,
		Leader: &Peer{
			myRange.GetLeader().GetId(),
			deepcopy.Iface(cluster.FindNodeById(myRange.GetLeader().GetNodeId()).Node).(*metapb.Node),
		},
	}

	var _routes []*Route
	_routes = append(_routes, _route)
	reply.Data = _routes
	log.Info("get peers of range[rangeId=%d] succeed", rangeId)
}

func (service *Server) handleNodeGetRangeTopo(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	cluster := service.cluster

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)

	var table *Table
	var tbFind bool
	if dbName != "" {
		db, dbFind := cluster.FindDatabase(dbName)
		if !dbFind {
			reply.Code = HTTP_ERROR
			reply.Message = ErrNotExistDatabase.Error()
			return
		}

		if tName != "" {
			table, tbFind = db.FindTable(tName)
			if !tbFind {
				reply.Code = HTTP_ERROR
				reply.Message = ErrNotExistTable.Error()
				return
			}
			if metapb.TableStatus_TableRunning != table.Status {
				reply.Code = HTTP_ERROR
				reply.Message = fmt.Sprintf("table [%s] is not running.", table.GetName())
				return
			}
		}
	}

	nodeId, err := strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64)
	if err != nil {
		log.Error("http get node id error: %s", http_error_invalid_parameter)
		reply.Code = HTTP_ERROR_INVALID_PARAM
		reply.Message = http_error_invalid_parameter
		return
	}

	node := cluster.FindNodeById(nodeId)
	if node == nil {
		log.Error("node[nodeId=%d] not found", nodeId)
		reply.Code = HTTP_ERROR_NODE_FIND
		reply.Message = http_error_node_find
		return
	}

	type Peer struct {
		Id   uint64       `json:"id,omitempty"`
		Node *metapb.Node `json:"node,omitempty"`
	}
	type Range struct {
		Id uint64 `json:"id,omitempty"`
		// Range key range [start_key, end_key).
		StartKey   []byte             `json:"start_key,omitempty"`
		EndKey     []byte             `json:"end_key,omitempty"`
		RangeEpoch *metapb.RangeEpoch `json:"range_epoch,omitempty"`
		Peers      []*Peer            `json:"peers,omitempty"`
		// Range state
		State      int32  `json:"state,omitempty"`
		DbName     string `json:"db_name,omitempty"`
		TableName  string `json:"table_name,omitempty"`
		TableId    uint64 `json:"table_id,omitempty"`
		CreateTime int64  `json:"create_time,omitempty"`
		LastHbTime string `json:"last_hb_time,omitempty"`
	}
	type Route struct {
		Range  *Range `json:"range,omitempty"`
		Leader *Peer  `json:"leader,omitempty"`
	}

	var _routes []*Route
	for _, r := range node.ranges.GetAllRange() {
		var peers []*Peer
		var leader *Peer

		for _, p := range r.Peers {
			nd := cluster.FindNodeById(p.GetNodeId())
			peer := &Peer{
				p.Id,
				deepcopy.Iface(nd.Node).(*metapb.Node),
			}
			if r.GetLeader().GetId() == p.GetId() {
				leader = peer
				continue
			}
			peers = append(peers, peer)
		}

		opsDescription := fmt.Sprintf(" \nBytesWritten=[%d], BytesRead=[%d], KeysWritten=[%d], KeysRead=[%d], OpsMax=[%d]",
			r.BytesWritten, r.BytesRead, r.KeysWritten, r.KeysRead, r.opsStat.GetMax())

		_range := &Range{
			Id:         r.GetId(),
			StartKey:   r.GetStartKey(),
			EndKey:     r.GetEndKey(),
			RangeEpoch: r.GetRangeEpoch(),
			Peers:      peers,
			State:      int32(r.State),
			LastHbTime: r.LastHbTimeTS.Format("2006-01-02 15:04:05") + opsDescription,
		}
		if table != nil && tbFind {
			_range.DbName = table.GetDbName()
			_range.TableName = table.GetName()
			_range.TableId = table.GetId()
		}

		_route := &Route{
			_range,
			leader,
		}

		_routes = append(_routes, _route)
	}

	reply.Data = _routes
	log.Info("get peers of node[nodeId=%d] succeed", nodeId)
}

func (service *Server) handleNodeUpdateIsolationLabel(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	cluster := service.cluster
	nodeId, err := strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64)
	if err != nil {
		log.Error("http get node id error: %s", http_error_invalid_parameter)
		reply.Code = HTTP_ERROR_INVALID_PARAM
		reply.Message = http_error_invalid_parameter
		return
	}

	isolationLabel := r.FormValue(HTTP_ISOLATION_LABEL)
	if isolationLabel == "" {
		log.Warn("updating node isolation label to empty: nodeId=%d", nodeId)
		//reply.Code = HTTP_ERROR_INVALID_PARAM
		//reply.Message = http_error_invalid_parameter
		//return
	}

	node := cluster.FindNodeById(nodeId)
	if node == nil {
		log.Error("node[nodeId=%d] not found", nodeId)
		reply.Code = HTTP_ERROR_NODE_FIND
		reply.Message = http_error_node_find
		return
	}

	node.Node.IsolationLabel = isolationLabel
	cluster.UpdateNode(node)

	log.Info("updated node isolation label=[%s]: nodeId[=%v]", isolationLabel, nodeId)
	return
}

func (service *Server) handleRangeGetPeerInfo(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	rangeId, err1 := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	nodeId, err2 := strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64)

	if err1 != nil || err2 != nil {
		log.Error("http get range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}

	cluster := service.cluster
	rang := cluster.FindRange(rangeId)
	if rang == nil {
		log.Error("http get range peer info: range [%d] is not existed", rangeId)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	type Peer struct {
		Id   uint64       `json:"id,omitempty"`
		Node *metapb.Node `json:"node,omitempty"`
	}
	type PeerInfo struct {
		Id uint64 `json:"id,omitempty"`
		// Range key range [start_key, end_key).
		StartKey   []byte             `json:"start_key,omitempty"`
		EndKey     []byte             `json:"end_key,omitempty"`
		RangeEpoch *metapb.RangeEpoch `json:"range_epoch,omitempty"`
		Peer       *Peer              `json:"peers,omitempty"`
		// Range state
		State string `json:"state,omitempty"`
	}
	node := service.cluster.FindNodeById(nodeId)
	if node == nil {
		log.Error("node[%d] not found", nodeId)
		reply.Code = HTTP_ERROR_NODE_FIND
		reply.Message = http_error_node_find
		return
	}
	p := rang.GetNodePeer(nodeId)
	if p == nil {
		log.Error("node[%d] not found", nodeId)
		reply.Code = HTTP_ERROR_PEER_FIND
		reply.Message = http_error_peer_find
		return
	}
	peer := &Peer{
		Id:   p.GetId(),
		Node: deepcopy.Iface(node.Node).(*metapb.Node),
	}
	info := &PeerInfo{
		Id:         rang.GetId(),
		StartKey:   rang.GetStartKey(),
		EndKey:     rang.GetEndKey(),
		RangeEpoch: rang.GetRangeEpoch(),
		Peer:       peer,
		State:      rang.State.String(),
	}
	reply.Data = info
	log.Info("get range[%s] peer[%s] info success", rang.SString(), rang.GetId(), peer.Node.GetServerAddr())
}

func (service *Server) handleTaskTypeGetAll(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	var taskName []string
	for _, taskType := range taskpb.TaskType_name {
		taskName = append(taskName, taskType)
	}
	reply.Data = taskName
	return
}

func (service *Server) handleRangeDelete(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	rangeId, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("http range add peer: range id is not int: %v", err)
		reply.Code = HTTP_ERROR
		reply.Message = "range is not int"
		return
	}

	cluster := service.cluster
	region := cluster.FindRange(rangeId)
	if region == nil {
		log.Error("http range add peer: range [%d] is not existed", rangeId)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}

	rngCopy := deepcopy.Iface(region.Range).(*metapb.Range)
	if err := cluster.storeDeleteRange(rngCopy); err != nil {
		log.Error("http delete range range [%d] failed!", rangeId)
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("store delete range: %v", err)
		return
	}
	cluster.memDeleteRange(rngCopy)
	log.Info("delete range [%d] is success", rangeId)
}

func (service *Server) handleRangeAddPeer(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	rangeId, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("http range add peer: range id is not int: %v", err)
		reply.Code = HTTP_ERROR
		reply.Message = "range is not int"
		return
	}

	cluster := service.cluster
	rng := cluster.FindRange(rangeId)
	if rng == nil {
		log.Error("http range add peer: range [%d] is not existed", rangeId)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	newPeer, err := cluster.allocPeerAndSelectNode(rng, true)
	if newPeer == nil || err != nil {
		reply.Code = -1
		reply.Message = "can not find best node to add peer"
		return
	}
	id, err := cluster.GenId()
	if err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	tc := NewTaskChain(id, rng.GetId(), "console-add-peer", NewAddPeerTask())
	// TOOD: check return
	cluster.taskManager.Add(tc)
	log.Info("add range<%v> peer create task success", rangeId)
}

func (service *Server) handleRangeDelPeer(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	rangeId, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("http range del peer: range id is not int: %v", err)
		reply.Code = HTTP_ERROR
		reply.Message = "range is not int"
		return
	}
	peerId, err := strconv.ParseUint(r.FormValue(HTTP_PEER_ID), 10, 64)
	if err != nil {
		log.Error("http range del peer: peer id is not int: %v", err)
		reply.Code = HTTP_ERROR
		reply.Message = "range is not int"
		return
	}
	cluster := service.cluster
	rng := cluster.FindRange(rangeId)
	if r == nil {
		log.Error("http get range info: range [%d] is not existed", rangeId)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	peer := rng.GetPeer(peerId)
	if peer == nil {
		log.Error("http get range info: range [%d] peer [%d] is not existed", rangeId, peerId)
		reply.Code = HTTP_ERROR_PEER_FIND
		reply.Message = http_error_peer_find
		return
	}
	id, err := cluster.GenId()
	if err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	tc := cluster.hbManager.createDelPeerTask(id, rng, peer, "console-del-peer")
	// TODO: check return
	cluster.taskManager.Add(tc)
	log.Info("del range<%v> peer<%v> create task success", rangeId, peerId)
}

func (service *Server) handleManageGetAutoScheduleInfo(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	type ClusterAutoScheduleInfo struct {
		// 是否支持数据迁移
		AutoTransferUnable bool `json:"autoTransferUnable"`
		// 是否支持failOver
		AutoFailoverUnable bool `json:"autoFailoverUnable"`
		AutoSplitUnable    bool `json:"autoSplitUnable"`
	}
	info := ClusterAutoScheduleInfo{
		AutoTransferUnable: service.cluster.autoTransferUnable,
		AutoFailoverUnable: service.cluster.autoFailoverUnable,
		AutoSplitUnable:    service.cluster.autoSplitUnable,
	}
	reply.Data = info
	log.Info("get cluster auto schedule info success!!!")
	return
}

func (service *Server) handleManageSetAutoScheduleInfo(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	_autoTransferUnable := r.FormValue(HTTP_AUTO_TRANSFER_UNABLE)
	_autoFailoverUnable := r.FormValue(HTTP_AUTO_FAILOVER_UNABLE)
	_autoSplitUnable := r.FormValue(HTTP_AUTO_SPLIT_UNABLE)
	var autoTransferUnable, autoFailoverUnable, autoSplitUnable bool
	autoTransferUnable, err := strconv.ParseBool(_autoTransferUnable)
	if err != nil {
		reply.Code = HTTP_ERROR_INVALID_PARAM
		reply.Message = http_error_invalid_parameter
		return
	}
	autoFailoverUnable, err = strconv.ParseBool(_autoFailoverUnable)
	if err != nil {
		reply.Code = HTTP_ERROR_INVALID_PARAM
		reply.Message = http_error_invalid_parameter
		return
	}
	autoSplitUnable, err = strconv.ParseBool(_autoSplitUnable)
	if err != nil {
		reply.Code = HTTP_ERROR_INVALID_PARAM
		reply.Message = http_error_invalid_parameter
		return
	}

	if err := service.cluster.UpdateAutoScheduleInfo(autoFailoverUnable, autoTransferUnable, autoSplitUnable); err != nil {
		log.Error("update cluster auto schedule info failed, err[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = http_error_parameter_not_enough
		return
	}

	log.Info("set cluster auto schedule info success!!!")
	return
}

var (
	fbase = "fbase"
	//CREATE TABLE IF NOT EXISTS `cluster_meta` (
	//`cluster_id` bigint(20) NOT NULL,
	//`update_time` bigint(20) NOT NULL,
	//`total_capacity` bigint(20) NOT NULL,
	//`used_capacity` bigint(20) NOT NULL,
	//`range_count` bigint(20) NOT NULL,
	//`db_count` bigint(20) NOT NULL,
	//`table_count` bigint(20) NOT NULL,
	//`ds_count` bigint(20) NOT NULL,
	//`gs_count` bigint(20) NOT NULL,
	//`fault_list`varchar(2048) NOT NULL,
	//PRIMARY KEY (`cluster_id`, `update_time`)
	//);

	cluster_meta = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "update_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "total_capacity", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "used_capacity", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "range_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "db_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "table_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "ds_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "gs_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "fault_list", DataType: metapb.DataType_Varchar}}

	//CREATE TABLE IF NOT EXISTS `cluster_net` (
	//`cluster_id` bigint(20) NOT NULL,
	//`update_time` bigint(20) NOT NULL,
	//`tps` bigint(20) NOT NULL,
	//`min_tp` float NOT NULL,
	//`max_tp` float NOT NULL,
	//`avg_tp` float NOT NULL,
	//`tp50` float NOT NULL,
	//`tp90` float NOT NULL,
	//`tp99` float NOT NULL,
	//`tp999` float NOT NULL,
	//`total_number` bigint(20) NOT NULL,
	//`err_number` bigint(20) NOT NULL,
	//`net_in_per_sec` bigint(20) NOT NULL,
	//`net_out_per_sec` bigint(20) NOT NULL,
	//`clients_count` bigint(20) NOT NULL,
	//`open_clients_per_sec` bigint(20) NOT NULL,
	//PRIMARY KEY (`cluster_id`, `update_time`)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8;

	cluster_net = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "update_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "tps", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "min_tp", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "max_tp", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "avg_tp", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "tp50", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "tp90", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "tp99", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "tp999", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "total_number", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "err_number", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_in_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_out_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "clients_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "open_clients_per_sec", DataType: metapb.DataType_BigInt}}

	//CREATE TABLE IF NOT EXISTS `cluster_slowlog` (
	//`cluster_id` bigint(20) NOT NULL,
	//`update_time` bigint(20) NOT NULL,
	//`su` bigint(20) NOT NULL,
	//`type`  varchar(32) NOT NULL,
	//`addr`  varchar(32) NOT NULL,
	//`lats` bigint(20) NOT NULL,
	//`slowlog` varchar(2048) NOT NULL,
	//PRIMARY KEY (`cluster_id`, `update_time`, `su`)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8;

	cluster_slowlog = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "update_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "su", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "type", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "addr", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "lats", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "slowlog", DataType: metapb.DataType_Varchar}}

	//CREATE TABLE IF NOT EXISTS `mac_meta` (
	//`cluster_id` bigint(20) NOT NULL,
	//`type`  varchar(32) NOT NULL,
	//`ip`  varchar(32) NOT NULL,
	//`update_time` bigint(20) NOT NULL,
	//`cpu_rate` float NOT NULL,
	//`load1` float NOT NULL,
	//`load5` float NOT NULL,
	//`load15` float NOT NULL,
	//`process_num` bigint(20) NOT NULL,
	//`thread_num` bigint(20) NOT NULL,
	//`handle_num` bigint(20) NOT NULL,
	//PRIMARY KEY (`cluster_id`, `type`, `ip`, `update_time`)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8;

	mac_meta = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "type", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "ip", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "update_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "cpu_rate", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "load1", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "load5", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "load15", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "process_num", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "thread_num", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "handle_num", DataType: metapb.DataType_BigInt}}

	//CREATE TABLE IF NOT EXISTS `mac_net` (
	//`cluster_id` bigint(20) NOT NULL,
	//`type`  varchar(32) NOT NULL,
	//`ip`  varchar(32) NOT NULL,
	//`update_time` bigint(20) NOT NULL,
	//`net_io_in_byte_per_sec` bigint(20) NOT NULL,
	//`net_io_out_byte_per_sec` bigint(20) NOT NULL,
	//`net_io_in_package_per_sec` bigint(20) NOT NULL,
	//`net_io_out_package_per_sec` bigint(20) NOT NULL,
	//`net_tcp_connections` bigint(20) NOT NULL,
	//`net_tcp_active_opens_per_sec` bigint(20) NOT NULL,
	//`net_ip_recv_package_per_sec` bigint(20) NOT NULL,
	//`net_ip_send_package_per_sec` bigint(20) NOT NULL,
	//`net_ip_drop_package_per_sec` bigint(20) NOT NULL,
	//`net_tcp_recv_package_per_sec` bigint(20) NOT NULL,
	//`net_tcp_send_package_per_sec` bigint(20) NOT NULL,
	//`net_tcp_err_package_per_sec` bigint(20) NOT NULL,
	//`net_tcp_retransfer_package_per_sec` bigint(20) NOT NULL,
	//PRIMARY KEY (`cluster_id`, `type`, `ip`, `update_time`)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8;

	mac_net = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "type", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "ip", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "update_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "net_io_in_byte_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_io_out_byte_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_io_in_package_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_io_out_package_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_tcp_connections", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_tcp_active_opens_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_ip_recv_package_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_ip_send_package_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_ip_drop_package_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_tcp_recv_package_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_tcp_send_package_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_tcp_err_package_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "net_tcp_retransfer_package_per_sec", DataType: metapb.DataType_BigInt}}

	//CREATE TABLE IF NOT EXISTS `mac_mem` (
	//`cluster_id` bigint(20) NOT NULL,
	//`type`  varchar(32) NOT NULL,
	//`ip`  varchar(32) NOT NULL,
	//`update_time` bigint(20) NOT NULL,
	//`memory_total` bigint(20) NOT NULL,
	//`memory_used_rss` bigint(20) NOT NULL,
	//`memory_used` bigint(20) NOT NULL,
	//`memory_free` bigint(20) NOT NULL,
	//`memory_used_percent` float NOT NULL,
	//`swap_memory_total` bigint(20) NOT NULL,
	//`swap_memory_used` bigint(20) NOT NULL,
	//`swap_memory_free` bigint(20) NOT NULL,
	//`swap_memory_used_percent` float NOT NULL,
	//PRIMARY KEY (`cluster_id`, `type`, `ip`, `update_time`)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8;

	mac_mem = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "type", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "ip", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "update_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "memory_total", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "memory_used_rss", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "memory_used", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "memory_free", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "memory_used_percent", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "swap_memory_total", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "swap_memory_used", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "swap_memory_free", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "swap_memory_used_percent", DataType: metapb.DataType_BigInt}}

	//CREATE TABLE IF NOT EXISTS `mac_disk` (
	//`cluster_id` bigint(20) NOT NULL,
	//`type`  varchar(32) NOT NULL,
	//`ip`  varchar(32) NOT NULL,
	//`update_time` bigint(20) NOT NULL,
	//`disk_path` varchar(128) NOT NULL,
	//`disk_total` bigint(20) NOT NULL,
	//`disk_used` bigint(20) NOT NULL,
	//`disk_free` bigint(20) NOT NULL,
	//`disk_proc_rate` float NOT NULL,
	//`disk_read_byte_per_sec` bigint(20) NOT NULL,
	//`disk_write_byte_per_sec` bigint(20) NOT NULL,
	//`disk_read_count_per_sec` bigint(20) NOT NULL,
	//`disk_write_count_per_sec` bigint(20) NOT NULL,
	//PRIMARY KEY (`cluster_id`, `type`, `ip`, `disk_path`, `update_time`)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8;

	mac_disk = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "type", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "ip", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "disk_path", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "update_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "disk_total", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "disk_used", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "disk_free", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "disk_proc_rate", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "disk_read_byte_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "disk_write_byte_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "disk_read_count_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "disk_write_count_per_sec", DataType: metapb.DataType_BigInt}}

	//CREATE TABLE IF NOT EXISTS `process_meta` (
	//`cluster_id` bigint(20) NOT NULL,
	//`type`  varchar(32) NOT NULL,
	//`addr`  varchar(32) NOT NULL,
	//`update_time` bigint(20) NOT NULL,
	//`cpu_rate` float NOT NULL,
	//`thread_num` bigint(20) NOT NULL,
	//`handle_num` bigint(20) NOT NULL,
	//`memory_used` bigint(20) NOT NULL,
	//`memory_total` bigint(20) NOT NULL,
	//`start_time` bigint(20) NOT NULL,
	//PRIMARY KEY (`cluster_id`, `type`, `addr`, `update_time`)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8;

	process_meta = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "type", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "addr", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "update_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "cpu_rate", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "thread_num", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "handle_num", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "memory_used", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "memory_total", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "start_time", DataType: metapb.DataType_BigInt}}

	//CREATE TABLE IF NOT EXISTS `process_disk` (
	//`cluster_id` bigint(20) NOT NULL,
	//`type`  varchar(32) NOT NULL,
	//`addr`  varchar(32) NOT NULL,
	//`update_time` bigint(20) NOT NULL,
	//`disk_path` varchar(128) NOT NULL,
	//`disk_total` bigint(20) NOT NULL,
	//`disk_used` bigint(20) NOT NULL,
	//`disk_free` bigint(20) NOT NULL,
	//`disk_proc_rate` float NOT NULL,
	//`disk_read_byte_per_sec` bigint(20) NOT NULL,
	//`disk_write_byte_per_sec` bigint(20) NOT NULL,
	//`disk_read_count_per_sec` bigint(20) NOT NULL,
	//`disk_write_count_per_sec` bigint(20) NOT NULL,
	//PRIMARY KEY (`cluster_id`, `type`, `addr`, `update_time`)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8;

	process_disk = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "type", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "addr", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "update_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "disk_path", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "disk_total", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "disk_used", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "disk_free", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "disk_proc_rate", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "disk_read_byte_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "disk_write_byte_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "disk_read_count_per_sec", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "disk_write_count_per_sec", DataType: metapb.DataType_BigInt}}

	//CREATE TABLE IF NOT EXISTS `process_net` (
	//`cluster_id` bigint(20) NOT NULL,
	//`type`  varchar(32) NOT NULL,
	//`addr`  varchar(32) NOT NULL,
	//`update_time` bigint(20) NOT NULL,
	//`tps` bigint(20) NOT NULL,
	//`min_tp` float NOT NULL,
	//`max_tp` float NOT NULL,
	//`avg_tp` float NOT NULL,
	//`tp50` float NOT NULL,
	//`tp90` float NOT NULL,
	//`tp99` float NOT NULL,
	//`tp999` float NOT NULL,
	//`total_number` bigint(20) NOT NULL,
	//`err_number` bigint(20) NOT NULL,
	//`connect_count` bigint(20) NOT NULL,
	//PRIMARY KEY (`cluster_id`, `type`, `addr`, `update_time`)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8;

	process_net = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "type", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "addr", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "update_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "tps", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "min_tp", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "max_tp", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "avg_tp", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "tp50", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "tp90", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "tp99", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "tp999", DataType: metapb.DataType_Float},
		&metapb.Column{Name: "total_number", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "err_number", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "connect_count", DataType: metapb.DataType_BigInt}}

	//CREATE TABLE IF NOT EXISTS `process_ds` (
	//`cluster_id` bigint(20) NOT NULL,
	//`type`  varchar(32) NOT NULL,
	//`addr`  varchar(32) NOT NULL,
	//`update_time` bigint(20) NOT NULL,
	//`range_count` bigint(20) NOT NULL,
	//`range_split_count` bigint(20) NOT NULL,
	//`sending_snap_count` bigint(20) NOT NULL,
	//`receiving_snap_count` bigint(20) NOT NULL,
	//`applying_snap_count` bigint(20) NOT NULL,
	//`range_leader_count` bigint(20) NOT NULL,
	//`version` varchar(32) DEFAULT NULL,
	//PRIMARY KEY (`cluster_id`, `type`, `addr`, `update_time`)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8;

	process_ds = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "type", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "addr", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "update_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "range_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "range_split_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "sending_snap_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "receiving_snap_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "applying_snap_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "range_leader_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "version", DataType: metapb.DataType_Varchar}}

	//CREATE TABLE IF NOT EXISTS `db_meta` (
	//`cluster_id` bigint(20) NOT NULL,
	//`db_name`  varchar(32) NOT NULL,
	//`update_time` bigint(20) NOT NULL,
	//`table_num` bigint(20) NOT NULL,
	//`range_size` bigint(20) NOT NULL,
	//PRIMARY KEY (`cluster_id`, `db_name`, `update_time`)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8;

	db_meta = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "db_name", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "update_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "table_num", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "range_size", DataType: metapb.DataType_BigInt}}

	//CREATE TABLE IF NOT EXISTS `table_meta` (
	//`cluster_id` bigint(20) NOT NULL,
	//`db_name`  varchar(32) NOT NULL,
	//`table_name`  varchar(32) NOT NULL,
	//`update_time` bigint(20) NOT NULL,
	//`range_count` bigint(20) NOT NULL,
	//`range_size` bigint(20) NOT NULL,
	//PRIMARY KEY (`cluster_id`, `db_name`, `table_name`, `update_time`)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8;

	table_meta = []*metapb.Column{
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "db_name", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "table_name", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "update_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "range_count", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "range_size", DataType: metapb.DataType_BigInt}}

	fbase_cluster = []*metapb.Column{
		&metapb.Column{Name: "id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "cluster_name", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "cluster_url", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "gateway_http", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "gateway_sql", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "cluster_sign", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "auto_transfer", DataType: metapb.DataType_Tinyint},
		&metapb.Column{Name: "auto_failover", DataType: metapb.DataType_Tinyint},
		&metapb.Column{Name: "auto_split", DataType: metapb.DataType_Tinyint},
		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt}}

	fbase_role = []*metapb.Column{
		&metapb.Column{Name: "role_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "role_name", DataType: metapb.DataType_Varchar}}

	fbase_privilege = []*metapb.Column{
		&metapb.Column{Name: "user_name", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "privilege", DataType: metapb.DataType_BigInt},
	}

	fbase_user = []*metapb.Column{
		&metapb.Column{Name: "id", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
		&metapb.Column{Name: "erp", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "mail", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "tel", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "user_name", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "real_name", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "superior_name", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "department1", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "department2", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "organization_name", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "create_time", DataType: metapb.DataType_TimeStamp},
		&metapb.Column{Name: "update_time", DataType: metapb.DataType_TimeStamp}}

	fbase_sql_apply = []*metapb.Column{
		&metapb.Column{Name: "id", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "db_name", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "table_name", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "sentence", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "status", DataType: metapb.DataType_Tinyint},
		&metapb.Column{Name: "applyer", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "auditor", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "create_time", DataType: metapb.DataType_TimeStamp},
		&metapb.Column{Name: "remark", DataType: metapb.DataType_Varchar}}

	fbase_lock_nsp = []*metapb.Column{
		&metapb.Column{Name: "id", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "db_name", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "table_name", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "db_id", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "table_id", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "status", DataType: metapb.DataType_Tinyint},
		&metapb.Column{Name: "applyer", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "auditor", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "create_time", DataType: metapb.DataType_TimeStamp}}

	fbase_configure_nsp = []*metapb.Column{
		&metapb.Column{Name: "id", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
		&metapb.Column{Name: "db_name", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "table_name", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "cluster_id", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "db_id", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "table_id", DataType: metapb.DataType_BigInt},
		&metapb.Column{Name: "status", DataType: metapb.DataType_Tinyint},
		&metapb.Column{Name: "applyer", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "auditor", DataType: metapb.DataType_Varchar},
		&metapb.Column{Name: "create_time", DataType: metapb.DataType_TimeStamp}}

	metric_server = []*metapb.Column{
		&metapb.Column{Name: "addr", DataType: metapb.DataType_Varchar, PrimaryKey: 1},
	}
)

func (service *Server) handleManageClusterInit(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	cluster := service.cluster
	_, err := cluster.CreateDatabase(fbase, "")
	if err != nil {
		log.Warn("create database %s failed, err %v", fbase, err)
	}
	isolationLabel := r.FormValue(HTTP_ISOLATION_LABEL)
	//// cluster_meta
	//parseColumn(cluster_meta)
	//_, err = cluster.CreateTable(fbase, "cluster_meta", cluster_meta, nil, false, nil)
	//if err != nil {
	//	log.Warn("create table %s %s failed, err %v", fbase, "cluster_meta", err)
	//}
	//// cluster_net
	//parseColumn(cluster_net)
	//_, err = cluster.CreateTable(fbase, "cluster_net", cluster_net, nil, false, nil)
	//if err != nil {
	//	log.Warn("create table %s %s failed, err %v", fbase, "cluster_net", err)
	//}
	//// cluster_slowlog
	//parseColumn(cluster_slowlog)
	//_, err = cluster.CreateTable(fbase, "cluster_slowlog", cluster_slowlog, nil, false, nil)
	//if err != nil {
	//	log.Warn("create table %s %s failed, err %v", fbase, "cluster_slowlog", err)
	//}
	//// mac_meta
	//parseColumn(mac_meta)
	//_, err = cluster.CreateTable(fbase, "mac_meta", mac_meta, nil, false, nil)
	//if err != nil {
	//	log.Warn("create table %s %s failed, err %v", fbase, "mac_meta", err)
	//}
	//// mac_disk
	//parseColumn(mac_disk)
	//_, err = cluster.CreateTable(fbase, "mac_disk", mac_disk, nil, false, nil)
	//if err != nil {
	//	log.Warn("create table %s %s failed, err %v", fbase, "mac_disk", err)
	//}
	//// mac_mem
	//parseColumn(mac_mem)
	//_, err = cluster.CreateTable(fbase, "mac_mem", mac_mem, nil, false, nil)
	//if err != nil {
	//	log.Warn("create table %s %s failed, err %v", fbase, "mac_mem", err)
	//}
	//// mac_net
	//parseColumn(mac_net)
	//_, err = cluster.CreateTable(fbase, "mac_net", mac_net, nil, false, nil)
	//if err != nil {
	//	log.Warn("create table %s %s failed, err %v", fbase, "mac_net", err)
	//}
	//// process_meta
	//parseColumn(process_meta)
	//_, err = cluster.CreateTable(fbase, "process_meta", process_meta, nil, false, nil)
	//if err != nil {
	//	log.Warn("create table %s %s failed, err %v", fbase, "process_meta", err)
	//}
	//// process_disk
	//parseColumn(process_disk)
	//_, err = cluster.CreateTable(fbase, "process_disk", process_disk, nil, false, nil)
	//if err != nil {
	//	log.Warn("create table %s %s failed, err %v", fbase, "process_disk", err)
	//}
	//// process_ds
	//parseColumn(process_ds)
	//_, err = cluster.CreateTable(fbase, "process_ds", process_ds, nil, false, nil)
	//if err != nil {
	//	log.Warn("create table %s %s failed, err %v", fbase, "process_ds", err)
	//}
	//// process_net
	//parseColumn(process_net)
	//_, err = cluster.CreateTable(fbase, "process_net", process_net, nil, false, nil)
	//if err != nil {
	//	log.Warn("create table %s %s failed, err %v", fbase, "process_net", err)
	//}
	//// db_meta
	//parseColumn(db_meta)
	//_, err = cluster.CreateTable(fbase, "db_meta", db_meta, nil, false, nil)
	//if err != nil {
	//	log.Warn("create table %s %s failed, err %v", fbase, "db_meta", err)
	//}
	//// table_meta
	//parseColumn(table_meta)
	//_, err = cluster.CreateTable(fbase, "table_meta", table_meta, nil, false, nil)
	//if err != nil {
	//	log.Warn("create table %s %s failed, err %v", fbase, "table_meta", err)
	//}
	// fbase_cluster
	parseColumn(fbase_cluster)
	_, err = cluster.CreateTable(fbase, "fbase_cluster", isolationLabel, 0, fbase_cluster, nil, false, nil)
	if err != nil {
		log.Warn("create table %s %s failed, err %v", fbase, "fbase_cluster", err)
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Errorf("createTable process_meta err: %v", err).Error()
		return
	}

	// fbase_role
	parseColumn(fbase_role)
	_, err = cluster.CreateTable(fbase, "fbase_role", isolationLabel, 0, fbase_role, nil, false, nil)
	if err != nil {
		log.Warn("create table %s %s failed, err %v", fbase, "fbase_role", err)
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Errorf("createTable process_meta err: %v", err).Error()
		return
	}
	// fbase_user
	parseColumn(fbase_user)
	_, err = cluster.CreateTable(fbase, "fbase_user", isolationLabel, 0, fbase_user, nil, false, nil)
	if err != nil {
		log.Warn("create table %s %s failed, err %v", fbase, "fbase_user", err)
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Errorf("createTable process_meta err: %v", err).Error()
		return
	}
	parseColumn(fbase_privilege)
	_, err = cluster.CreateTable(fbase, "fbase_privilege", isolationLabel, 0, fbase_privilege, nil, false, nil)
	if err != nil {
		log.Warn("create table %s %s failed, err %v", fbase, "fbase_privilege", err)
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Errorf("createTable fbase_privilege err: %v", err).Error()
		return
	}
	parseColumn(fbase_sql_apply)
	_, err = cluster.CreateTable(fbase, "fbase_sql_apply", isolationLabel, 0, fbase_sql_apply,nil, false, nil)
	if err != nil {
		log.Warn("create table %s %s failed, err %v", fbase, "fbase_sql_apply", err)
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Errorf("createTable fbase_sql_apply err: %v", err).Error()
		return
	}
	//lock namespace
	parseColumn(fbase_lock_nsp)
	_, err = cluster.CreateTable(fbase, "fbase_lock_nsp", isolationLabel, 0, fbase_lock_nsp, nil, false, nil)
	if err != nil {
		log.Warn("create table %s %s failed, err %v", fbase, "fbase_lock_nsp", err)
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Errorf("createTable fbase_lock_nsp err: %v", err).Error()
		return
	}
	//configure namespace
	parseColumn(fbase_configure_nsp)
	_, err = cluster.CreateTable(fbase, "fbase_configure_nsp", isolationLabel, 0, fbase_configure_nsp, nil, false, nil)
	if err != nil {
		log.Warn("create table %s %s failed, err %v", fbase, "fbase_configure_nsp", err)
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Errorf("createTable fbase_configure_nsp err: %v", err).Error()
		return
	}

	//metric_server
	parseColumn(metric_server)
	_, err = cluster.CreateTable(fbase, "metric_server", isolationLabel, 0, metric_server, nil, false, nil)
	if err != nil {
		log.Warn("create table %s %s failed, err %v", fbase, "fbase_lock_nsp", err)
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Errorf("createTable metric_server err: %v", err).Error()
		return
	}

	log.Info("cluster init success!!!")
	reply.Message = "cluster init success"
	return
}

func (service *Server) handleDatabaseDelete(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	dbName := r.FormValue(HTTP_DB_NAME)

	if dbName == "" {
		log.Error("http delete database: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}

	if err := service.cluster.DeleteDatabase(dbName); err != nil {
		log.Error("http create database: %v", err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	log.Info("create database[%s] success", dbName)
	return
}

func (service *Server) handleTableDelete(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)

	if dbName == "" || tName == "" {
		log.Error("http delete table: %s", http_error_parameter_not_enough)
		reply.Code = -1
		reply.Message = http_error_parameter_not_enough
		return
	}

	if _, err := service.cluster.DeleteTable(dbName, tName, false); err != nil {
		log.Error("http delete table: %v", err)
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	log.Info("delete table[%s %s] success", dbName, tName)
}

func (service *Server) handleTableFastDelete(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	var fast bool
	if r.FormValue(HTTP_FAST) != "" {
		fast = true
	}

	if dbName == "" || tName == "" {
		log.Error("http delete table: %s", http_error_parameter_not_enough)
		reply.Code = -1
		reply.Message = http_error_parameter_not_enough
		return
	}

	if _, err := service.cluster.DeleteTable(dbName, tName, fast); err != nil {
		log.Error("http delete table: %v", err)
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	log.Info("fast delete table[%s %s] success", dbName, tName)
}

func (service *Server) handleTableCancel(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	if dbName == "" || tName == "" {
		log.Error("http edit table: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	cluster := service.cluster
	err := cluster.CancelTable(dbName, tName)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}

	log.Info("cancel table[%s:%s] success", dbName, tName)
}

func (service *Server) handleTableEdit(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	properties := r.FormValue(HTTP_PROPERTIES)
	if dbName == "" || tName == "" || properties == "" {
		log.Error("http edit table: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	cluster := service.cluster
	db, find := cluster.FindDatabase(dbName)
	if !find {
		log.Warn("db[%s] not exist", dbName)
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		log.Warn("table[%s:%s] not exist", dbName, tName)
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	if err := service.cluster.EditTable(table, properties); err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		log.Warn("edit table[%s:%s] failed, properties[%s]", dbName, tName, properties)
		return
	}
	log.Info("edit table[%s:%s] success", dbName, tName)
}

func (service *Server) handleTableEditPolicy(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	policy, _ := strconv.Atoi(r.FormValue(HTTP_POLICY))
	if dbName == "" || tName == "" || policy < 0 {
		log.Error("http edit table policy: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	cluster := service.cluster
	db, find := cluster.FindDatabase(dbName)
	if !find {
		log.Warn("db[%s] not exist", dbName)
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		log.Warn("table[%s:%s] not exist", dbName, tName)
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}

	//table.Table.ReadFromNode = cluster.int2ReadFromNode(int32(policy))
	newTable := deepcopy.Iface(table.Table).(*metapb.Table)
	newTable.ReadFromNode = cluster.int2ReadFromNode(int32(policy))
	err := cluster.storeTable(newTable)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		log.Error("store table failed, err[%v]", err)
		return
	}
	table.Table = newTable

	log.Info("edit db:table[%s:%s] to policy[%v] success", dbName, tName, table.Table.ReadFromNode)
}

func (service *Server) handleNodeDelete(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	var nodeId uint64
	var err error
	if nodeId, err = strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64); err != nil {
		log.Warn("http delete node: %v", err.Error())
		reply.Code = HTTP_ERROR_INVALID_PARAM
		reply.Message = err.Error()
		return
	}
	if err = service.cluster.DeleteNodeById(nodeId); err != nil {
		log.Warn("http delete node: %v", err.Error())
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	log.Info("node[%d] delete success", nodeId)
}

func (s *Server) handleHttpNodeLogin(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	var id uint64
	var err error
	if id, err = strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64); err != nil {
		log.Error("Parse login node id failed. error:[%v]", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	var force bool
	if len(r.FormValue("force")) == 0 {
		force = false
	} else {
		force, err = strconv.ParseBool(r.FormValue("force"))
		if err != nil {
			reply.Code = -1
			reply.Message = err.Error()
			return
		}
	}

	if err := s.cluster.LoginNode(id, force); err != nil {
		log.Error("Http login node failed. error:[%v]", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	return
}

func (service *Server) handleHttpNodeLogout(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	var id uint64
	var err error
	if id, err = strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64); err != nil {
		log.Error("http delete node: %v", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	if err := service.cluster.LogoutNode(id); err != nil {
		log.Error("http logout node failed. error:[%v]", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	return
}

func (service *Server) handleHttpNodeDelete(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	var nodeIds []uint64
	if err := json.Unmarshal([]byte(r.FormValue(HTTP_NODE_IDS)), &nodeIds); err != nil {
		log.Error("http delete node: %v", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
	}

	var errNodeIds []uint64
	for _, id := range nodeIds {
		node := service.cluster.FindNodeById(id)
		if node != nil && node.IsLogout() {
			if err := service.cluster.DeleteNodeById(id); err == nil {
				continue
			}
		}
		errNodeIds = append(errNodeIds, id)
	}

	if len(errNodeIds) > 0 {
		log.Warn("nodes %v not exist or not allow delete", errNodeIds)
		reply.Code = -1
		reply.Message = fmt.Sprintf("%v %s", errNodeIds, ErrNotAllowDelete.Error())
		return
	}

	log.Info("http node delete %v success", nodeIds)
	return
}

func (service *Server) handleRangeLeaderChange(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	rangeId, err1 := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	peerId, err2 := strconv.ParseUint(r.FormValue(HTTP_PEER_ID), 10, 64)

	if err1 != nil || err2 != nil {
		log.Error("http range leader change: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}

	cluster := service.cluster
	rng := cluster.FindRange(rangeId)
	if rng == nil {
		log.Error("http range leader change: range [%d] is not existed", rangeId)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	newLeader := rng.GetPeer(peerId)
	if newLeader == nil {
		log.Error("http range leader change: peerId [%d] is not in the range[%v] replica", peerId, rng)
		reply.Code = -1
		reply.Message = fmt.Sprintf("peer [%d] is not in the range[%v] replica", peerId, rng)
		return
	}

	node := service.cluster.FindNodeById(newLeader.GetNodeId())
	if node == nil {
		log.Error("http range leader change: node [%d] is not existed", newLeader.GetNodeId())
		reply.Code = -1
		reply.Message = ErrNotExistNode.Error()
		return
	}

	if !node.require() {
		log.Error("http range leader change: node [%d] cannot be schedule, status: %v", node.GetId(), node.GetState())
		reply.Code = -1
		reply.Message = fmt.Sprintf("node [%d] cannot be schedule", node.GetId())
		return
	}

	if !rng.require(cluster) {
		log.Debug("http range leader change: range [%d] cannot be schedule, status: %v", rng.GetId(), rng.State)
		reply.Code = -1
		reply.Message = fmt.Sprintf("range [%d] cannot be schedule", rng.GetId())
		return
	}

	taskID, err := cluster.GenId()
	if err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	tc := NewTaskChain(taskID, rng.GetId(), "console-change-leader",
		NewChangeLeaderTask(rng.GetLeader().GetNodeId(), newLeader.GetNodeId()))
	// TODO: check return
	cluster.taskManager.Add(tc)
	log.Info("to change leader range[%s] success", rng.SString())
	return
}

func (service *Server) handleRangeTransfer(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	rangeId, err1 := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	peerId, err2 := strconv.ParseUint(r.FormValue(HTTP_PEER_ID), 10, 64)

	if err1 != nil || err2 != nil {
		log.Error("http range peer transfer: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}

	cluster := service.cluster
	rng := cluster.FindRange(rangeId)
	if rng == nil {
		log.Error("http range peer transfer: range [%d] is not existed", rangeId)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	oldPeer := rng.GetPeer(peerId)
	if oldPeer == nil {
		log.Error("http range peer transfer: peer [%d] is not the region[%v] replica", peerId, rng.GetId())
		reply.Code = -1
		reply.Message = fmt.Sprintf("peer [%d] is not the region[%v] replica", peerId, rng.GetId())
		return
	}

	node := service.cluster.FindNodeById(oldPeer.GetNodeId())
	if node == nil {
		log.Error("http range peer transfer: node [%d] is not existed", oldPeer.GetNodeId())
		reply.Code = -1
		reply.Message = ErrNotExistNode.Error()
		return
	}

	if !node.require() {
		log.Error("http range peer transfer: node [%d] cannot be schedule, status: %v", node.GetId(), node.GetState())
		reply.Code = -1
		reply.Message = fmt.Sprintf("node [%d] cannot be schedule", node.GetId())
		return
	}

	if !rng.require(cluster) {
		log.Debug("http range peer transfer: range [%d] cannot be schedule, status: %v", rng.GetId(), rng.State)
		reply.Code = -1
		reply.Message = fmt.Sprintf("range [%d] cannot be schedule", rng.GetId())
		return
	}

	taskID, err := cluster.GenId()
	if err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	tc := NewTransferPeerTasks(taskID, rng, "console-transfer-peer", oldPeer)
	// TODO: check tc
	cluster.taskManager.Add(tc)
	log.Info("to transfer range[%s] peer success", rng.SString())
	return
}

func (service *Server) handleRangeTopNQuery(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	topN, err := strconv.Atoi(r.FormValue("topN"))
	if err != nil || topN == 0 {
		log.Error("http range topn query: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	cluster := service.cluster
	h := &http_reply.RangeStatHeap{}
	for _, r := range cluster.GetAllRanges() {
		if r.opsStat.GetMax() == 0 {
			continue
		}

		leader := r.GetLeader()
		if leader == nil {
			continue
		}
		var nodeAddr string
		node := cluster.FindNodeById(leader.GetNodeId())
		if node != nil {
			nodeAddr = node.GetServerAddr()
		}
		statInfo := http_reply.RangeStatsInfo{
			RangeId:      r.GetId(),
			LeaderId:     leader.GetId(),
			NodeAddr:     nodeAddr,
			TableId:      r.GetTableId(),
			BytesWritten: r.BytesWritten,
			BytesRead:    r.BytesRead,
			KeysWritten:  r.KeysWritten,
			KeysRead:     r.KeysRead,
			WriteOps:     r.opsStat.GetMax(),
		}
		heap.Push(h, statInfo)
	}

	if topN > h.Len() {
		topN = h.Len()
	}

	var result []http_reply.RangeStatsInfo
	for i := 0; i < topN; i++ {
		result = append(result, heap.Pop(h).(http_reply.RangeStatsInfo))
	}
	reply.Data = result
	log.Debug("query cluster range topN %v success", topN)
	return
}

func (service *Server) handleDeleteTask(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	rangeId, err1 := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	taskId, err2 := strconv.ParseUint(r.FormValue(HTTP_TASK_ID), 10, 64)
	if err1 != nil || err2 != nil {
		log.Error("http delete task: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	cluster := service.cluster
	task := cluster.taskManager.Find(rangeId)
	if task.GetID() == taskId {
		cluster.taskManager.Remove(task, cluster)
	}
	log.Info("delete range %v task[%s] success", rangeId, task.String())

	return
}

func (service *Server) handleGetAllTask(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	var resp http_reply.TaskResponse
	for _, e := range service.cluster.GetAllTasks() {
		resp = append(resp, &http_reply.Task{
			Id:       e.GetID(),
			Type:     e.GetName(),
			RangeId:  e.GetRangeID(),
			Describe: e.String(),
			// TODO:
		})
	}
	reply.Data = resp
}

func (service *Server) handleRangeTaskQuery(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	rangeId, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("query range task: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	cluster := service.cluster
	rng := cluster.FindRange(rangeId)
	if rng == nil {
		log.Error("query range task: range [%d] is not existed", rangeId)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	t := cluster.taskManager.Find(rangeId)
	reply.Data = t
	return
}

func (service *Server) unhealthyRangeQuery(dbName, tableName string) []*http_reply.RangeBrief {
	vals := make(url.Values)
	vals.Set(HTTP_DB_NAME, dbName)
	vals.Set(HTTP_TABLE_NAME, tableName)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("get", "http://whatever?"+vals.Encode(), nil)
	service.handleUnhealthyRangeQuery(w, r)
	data, err := ioutil.ReadAll(w.Body)
	if err != nil {
		log.Error("unhealthyRangeQuery read body: %v", err)
		return nil
	}
	log.Info("handleUnhealthyRangeQuery return: %v", string(data))
	reply := new(http_reply.RangeBriefReply)
	if err := json.Unmarshal(data, &reply); err != nil {
		log.Error("unhealthyRangeQuery unmarshal: %v", err)
		return nil
	}
	if reply.Code != HTTP_OK {
		log.Error("unhealthyRangeQuery reply: %v", reply.Message)
		return nil
	}
	return reply.Data
}

func (service *Server) peerInfoQuery(dbName, tableName string, rangeId uint64) []*http_reply.PeerBrief {
	vals := make(url.Values)
	vals.Set(HTTP_DB_NAME, dbName)
	vals.Set(HTTP_TABLE_NAME, tableName)
	vals.Set(HTTP_RANGE_ID, fmt.Sprint(rangeId))
	w := httptest.NewRecorder()
	r := httptest.NewRequest("get", "http://whatever?"+vals.Encode(), nil)
	service.handlePeerInfoQuery(w, r)
	data, err := ioutil.ReadAll(w.Body)
	if err != nil {
		log.Warn("getPeerInfoQuery read body: %v", err)
		return nil
	}
	log.Info("handlePeerInfoQuery return: %v", string(data))
	reply := new(http_reply.PeerBriefReply)
	if err := json.Unmarshal(data, &reply); err != nil {
		log.Warn("peerInfoQuery unmarshal: %v", err)
		return nil
	}
	if reply.Code != HTTP_OK {
		log.Error("peerInfoQuery reply: %v", reply.Message)
		return nil
	}
	return reply.Data
}

func (service *Server) unhealthyRangeRecreate(dbName, tableName string, rangeId, peerId uint64) {
	log.Info("unhealthyRangeRecreate do: dbname[%v] tablename[%v] rangeid[%v]", dbName, tableName, rangeId)
	vals := make(url.Values)
	vals.Set(HTTP_DB_NAME, dbName)
	vals.Set(HTTP_TABLE_NAME, tableName)
	vals.Set(HTTP_RANGE_ID, fmt.Sprint(rangeId))
	vals.Set(HTTP_PEER_ID, fmt.Sprint(peerId))
	w := httptest.NewRecorder()
	r := httptest.NewRequest("get", "http://whatever?"+vals.Encode(), nil)
	service.handleRangeRecreate(w, r)
	data, err := ioutil.ReadAll(w.Body)
	if err != nil {
		log.Warn("unhealthyRangeUpdate read body: %v", err)
		return
	}
	log.Info("handleUnhealthyRangeUpdate return: %v", string(data))
	reply := new(http_reply.Reply)
	if err := json.Unmarshal(data, &reply); err != nil {
		log.Warn("unhealthyRangeUpdate unmarshal: %v", err)
		return
	}
	if reply.Code != HTTP_OK {
		log.Error("unhealthyRangeUpdate reply: %v", reply.Message)
		return
	}
	log.Info("unhealthyRangeUpdate ok: dbname[%v] tablename[%v] rangeid[%v]", dbName, tableName, rangeId)
}

func (service *Server) unhealthyRangeUpdate(dbName, tableName string, rangeId, peerId uint64) {
	log.Info("unhealthyRangeUpdate do: dbname[%v] tablename[%v] rangeid[%v]", dbName, tableName, rangeId)
	vals := make(url.Values)
	vals.Set(HTTP_DB_NAME, dbName)
	vals.Set(HTTP_TABLE_NAME, tableName)
	vals.Set(HTTP_RANGE_ID, fmt.Sprint(rangeId))
	vals.Set(HTTP_PEER_ID, fmt.Sprint(peerId))
	w := httptest.NewRecorder()
	r := httptest.NewRequest("get", "http://whatever?"+vals.Encode(), nil)
	service.handleUnhealthyRangeUpdate(w, r)
	data, err := ioutil.ReadAll(w.Body)
	if err != nil {
		log.Warn("unhealthyRangeUpdate read body: %v", err)
		return
	}
	log.Info("handleUnhealthyRangeUpdate return: %v", string(data))
	reply := new(http_reply.Reply)
	if err := json.Unmarshal(data, &reply); err != nil {
		log.Warn("unhealthyRangeUpdate unmarshal: %v", err)
		return
	}
	if reply.Code != HTTP_OK {
		log.Error("unhealthyRangeUpdate reply: %v", reply.Message)
		return
	}
	log.Info("unhealthyRangeUpdate ok: dbname[%v] tablename[%v] rangeid[%v]", dbName, tableName, rangeId)
}

func (service *Server) handleRangeRecreate(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	rangeId, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = "rangeId is not int"
		return
	}
	peerId, err := strconv.ParseUint(r.FormValue(HTTP_PEER_ID), 10, 64)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = "peerId is not int"
		return
	}
	rng := service.cluster.FindRange(rangeId)
	if rng == nil {
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("range[%v] not found", rangeId)
		return
	}

	if err := service.cluster.rangeRecreate(rng, peerId); err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("range[%v] recreate failed", rangeId)
		return
	}
	service.cluster.unhealthyRanges.Delete(rangeId)
}

var recoverLock sync.Mutex

func (service *Server) handleUnhealthyRangeRecover(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	unhealthyRangeList := service.unhealthyRangeQuery(r.FormValue(HTTP_DB_NAME), r.FormValue(HTTP_TABLE_NAME))
	if len(unhealthyRangeList) == 0 {
		reply.Code = HTTP_ERROR
		reply.Message = "unhealthy range list is empty"
		return
	}

	go func() {
		recoverLock.Lock()
		defer recoverLock.Unlock()
		for _, r := range unhealthyRangeList {
			rng := service.cluster.FindRange(r.Id)
			if rng == nil {
				continue
			}
			table, found := service.cluster.FindTableById(rng.GetTableId())
			if !found {
				continue
			}
			ps := service.peerInfoQuery(table.GetDbName(), table.GetName(), r.Id)
			pId := func() uint64 {
				var maxIndex uint64
				var id uint64
				for _, p := range ps {
					if p.Index > maxIndex {
						maxIndex = p.Index
						id = p.Id
					}
				}
				return id
			}()
			service.unhealthyRangeRecreate(table.GetDbName(), table.GetName(), r.Id, pId)
		}
	}()
}

func (service *Server) handleUnhealthyRangeQuery(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	rId := r.FormValue(HTTP_RANGE_ID)

	log.Info("handleUnhealthyRangeQuery: dbname[%v] tablename[%v] rangeId:%v", dbName, tName, rId)
	cluster := service.cluster
	db, find := cluster.FindDatabase(dbName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	if table.Status != metapb.TableStatus_TableRunning {
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("table <%v> is not running", table.GetName())
		return
	}

	var result []*http_reply.RangeBrief
	for _, r := range cluster.GetAllUnhealthyRanges() {
		if r.GetTableId() == table.GetId() && r.State == metapb.RangeState_R_Abnormal {
			rCopy := deepcopy.Iface(r.Range).(*metapb.Range)

			rng := &http_reply.RangeBrief{
				Id:         rCopy.GetId(),
				StartKey:   fmt.Sprintf("%v", rCopy.StartKey),
				EndKey:     fmt.Sprintf("%v", rCopy.EndKey),
				State:      int32(r.State),
				LastHbTime: r.LastHbTimeTS.Format("2006-01-02 15:04:05"),
			}

			var peers []uint64
			for _, peer := range r.GetPeers() {
				peers = append(peers, peer.GetId())
			}
			rng.Peers = peers

			if len(r.GetDownPeers()) != 0 {
				var downPeers []uint64
				for _, downPeer := range r.GetDownPeers() {
					downPeers = append(downPeers, downPeer.Peer.GetId())
				}
				rng.DownPeers = downPeers
			}

			if r.Leader != nil {
				rng.Leader = r.Leader.GetId()
			}

			result = append(result, rng)
		}
	}
	for {
		rangeId, _ := strconv.ParseInt(rId, 10, 0)
		if rangeId <= 0 {
			break
		}
		r := cluster.FindRange(uint64(rangeId))
		if r != nil && r.GetTableId() == table.GetId() {
			rCopy := deepcopy.Iface(r.Range).(*metapb.Range)

			rng := &http_reply.RangeBrief{
				Id:         rCopy.GetId(),
				StartKey:   fmt.Sprintf("%v", rCopy.StartKey),
				EndKey:     fmt.Sprintf("%v", rCopy.EndKey),
				State:      int32(r.State),
				LastHbTime: r.LastHbTimeTS.Format("2006-01-02 15:04:05"),
			}

			var peers []uint64
			for _, peer := range r.GetPeers() {
				peers = append(peers, peer.GetId())
			}
			rng.Peers = peers

			if len(r.GetDownPeers()) != 0 {
				var downPeers []uint64
				for _, downPeer := range r.GetDownPeers() {
					downPeers = append(downPeers, downPeer.Peer.GetId())
				}
				rng.DownPeers = downPeers
			}

			if r.Leader != nil {
				rng.Leader = r.Leader.GetId()
			}

			result = append(result, rng)
		}

		break

	}

	reply.Data = result
	return
}

func (service *Server) handleUnstableRangeQuery(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)

	log.Info("handleUnstableRangeQuery: dbname[%v] tablename[%v] ", dbName, tName)
	cluster := service.cluster
	db, find := cluster.FindDatabase(dbName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	if table.Status != metapb.TableStatus_TableRunning {
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("table <%v> is not running", table.GetName())
		return
	}

	var result []*http_reply.RangeBrief
	for _, r := range cluster.GetAllUnstableRanges() {
		if r.GetTableId() == table.GetId() {
			rCopy := deepcopy.Iface(r.Range).(*metapb.Range)

			rng := &http_reply.RangeBrief{
				Id:         rCopy.GetId(),
				StartKey:   fmt.Sprintf("%v", rCopy.StartKey),
				EndKey:     fmt.Sprintf("%v", rCopy.EndKey),
				State:      int32(r.State),
				LastHbTime: r.LastHbTimeTS.Format("2006-01-02 15:04:05"),
			}

			var peers []uint64
			for _, peer := range r.GetPeers() {
				peers = append(peers, peer.GetId())
			}
			rng.Peers = peers

			if len(r.GetDownPeers()) != 0 {
				var downPeers []uint64
				for _, downPeer := range r.GetDownPeers() {
					downPeers = append(downPeers, downPeer.Peer.GetId())
				}
				rng.DownPeers = downPeers
			}

			if r.Leader != nil {
				rng.Leader = r.Leader.GetId()
			}

			result = append(result, rng)
		}
	}
	reply.Data = result
	return
}

func (service *Server) handlePeerInfoQuery(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)

	rangeId, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("http get peer info: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	cluster := service.cluster
	range_ := cluster.FindRange(rangeId)
	if range_ == nil {
		log.Error("http get peer info: range [%d] is not existed", rangeId)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}

	db, find := cluster.FindDatabase(dbName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	if table.Status != metapb.TableStatus_TableRunning {
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("table <%v> is not running", table.GetName())
		return
	}

	if range_.GetTableId() != table.GetId() {
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("range<%v> not in table<%v>", rangeId, table.GetName())
		return
	}

	reply.Data = cluster.queryPeerRemote(range_.Range)
	return
}

func (service *Server) handleUnhealthyRangeUpdate(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)

	rangeId, err1 := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	peerId, err2 := strconv.ParseUint(r.FormValue(HTTP_PEER_ID), 10, 64)

	if err1 != nil || err2 != nil {
		log.Error("http get peer info: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	cluster := service.cluster
	range_ := cluster.FindRange(rangeId)
	if range_ == nil {
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}

	if range_.State != metapb.RangeState_R_Abnormal {
		reply.Code = HTTP_ERROR
		reply.Message = "range has been recovered"
		return
	}

	db, find := cluster.FindDatabase(dbName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	if table.Status != metapb.TableStatus_TableRunning {
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("table <%v> is not running", table.GetName())
		return
	}

	if range_.GetTableId() != table.GetId() {
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("range<%v> not in table<%v>", rangeId, table.GetName())
		return
	}

	if err := cluster.updateRangePeerRemote(range_, peerId); err != nil {
		log.Error("update remote range meta failed, err:[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("update remote range meta error.")
		return
	}
	return
}

func (service *Server) handleUpdateRangeEpoch(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	rangeId, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("rangeid parse: %v", err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	confVer, err1 := strconv.ParseUint(r.FormValue("epochConfVer"), 10, 64)
	version, err2 := strconv.ParseUint(r.FormValue("epochVersion"), 10, 64)
	if err1 != nil || err2 != nil {
		errStr := err1.Error() + err2.Error()
		log.Error("epoch pasrse: %v", errStr)
		reply.Code = HTTP_ERROR
		reply.Message = errStr
		return
	}
	cluster := service.cluster
	range_ := cluster.FindRange(rangeId)
	if range_ == nil {
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}

	if range_.State != metapb.RangeState_R_Abnormal {
		reply.Code = HTTP_ERROR
		reply.Message = "range has been recovered"
		return
	}

	db, find := cluster.FindDatabase(dbName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	if table.Status != metapb.TableStatus_TableRunning {
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("table <%v> is not running", table.GetName())
		return
	}

	if range_.GetTableId() != table.GetId() {
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("range<%v> not in table<%v>", rangeId, table.GetName())
		return
	}

	if err := cluster.UpdateRangeEpochRemote(range_, &metapb.RangeEpoch{
		ConfVer: confVer,
		Version: version,
	}); err != nil {
		log.Error("update remote range meta failed, err:[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("update remote range meta error.")
		return
	}
	return
}

func (service *Server) handleRangeOffline(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)

	rangeId, err1 := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	peerId, err2 := strconv.ParseUint(r.FormValue(HTTP_PEER_ID), 10, 64)

	if err1 != nil || err2 != nil {
		log.Error("http get peer info: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	cluster := service.cluster
	range_ := cluster.FindRange(rangeId)
	if range_ == nil {
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}

	db, find := cluster.FindDatabase(dbName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	if table.Status != metapb.TableStatus_TableRunning {
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("table <%v> is not running", table.GetName())
		return
	}

	if range_.GetTableId() != table.GetId() {
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("range<%v> not in table<%v>", rangeId, table.GetName())
		return
	}

	log.Debug("handle range offline, rangeId:[%v], peerId:[%v]", rangeId, peerId)
	if err := cluster.offlineRangeRemote(range_, peerId); err != nil {
		log.Error("offline remote range failed, err:[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("offline remote range error.")
		return
	}
	return
}

func (service *Server) handleRangeLeaderQuery(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	rangeId, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("query range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	cluster := service.cluster
	range_ := cluster.FindRange(rangeId)
	if range_ == nil {
		log.Error("query range leader: range [%d] is not existed", rangeId)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	reply.Data = range_.GetLeader()
	return
}

type ValidHandler func(w http.ResponseWriter, r *http.Request) bool

type routeInfo struct {
	*metapb.Route
	downPeer []uint64
	pendPeer []uint64
}
type routeInfoByStartKey []*routeInfo

func (ri routeInfoByStartKey) Len() int {
	return len(ri)
}
func (ri routeInfoByStartKey) Swap(i, j int) {
	ri[i], ri[j] = ri[j], ri[i]
}
func (ri routeInfoByStartKey) Less(i, j int) bool {
	iStart := ri[i].GetRange().GetStartKey()
	jStart := ri[j].GetRange().GetStartKey()
	if len(iStart) == 0 {
		return true
	}
	if bytes.Compare(iStart, jStart) == 1 {
		return false
	}
	return true
}

func (service *Server) handleTestAlarm(w http.ResponseWriter, r *http.Request) {
	err := service.cluster.alarmCli.RangeNoHeartbeatAlarm(int64(service.cluster.GetClusterId()), nil, "test alarm", nil)
	if err != nil {
		w.Write([]byte(err.Error()))
	} else {
		w.Write([]byte("ok"))
	}
}

func (service *Server) handleRangeLocate(w http.ResponseWriter, r *http.Request) {
	reply := &http_reply.Reply{}
	defer http_reply.SendReply(w, reply)

	req, err := http_reply.GetRangeLocateRequest(r)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("handle rangelocate request: %v", err)
		return
	}
	log.Debug("handle route locate: dbName: %v, tableName: %v", req.DbName, req.TableName)

	dbName := req.DbName
	tName := req.TableName
	cluster := service.cluster
	db, find := cluster.FindDatabase(dbName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}

	if table.Status != metapb.TableStatus_TableRunning {
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("table <%v> is not running", table.GetName())
		return
	}

	ranges := cluster.GetTableAllRanges(table.GetId())
	routes := make([]*routeInfo, 0, len(ranges))
	for _, rng := range ranges {
		routes = append(routes, &routeInfo{
			Route: &metapb.Route{
				Range:  deepcopy.Iface(rng.Range).(*metapb.Range),
				Leader: rng.GetLeader(),
			},
			downPeer: func() (downs []uint64) {
				for _, down := range rng.GetDownPeers() {
					downs = append(downs, down.Peer.GetId())
				}
				return
			}(),
			pendPeer: func() (pends []uint64) {
				for _, pend := range rng.GetPendingPeers() {
					pends = append(pends, deepcopy.Iface(pend.GetId()).(uint64))
				}
				return
			}(),
		})
	}
	// sort by start/end key
	sort.Sort(routeInfoByStartKey(routes))

	var _routes []*http_reply.Route
	for _, r := range routes {
		var peers []*http_reply.Peer
		var leader *http_reply.Peer
		rng := r.GetRange()
		for _, p := range rng.GetPeers() {
			node := service.cluster.FindNodeById(p.GetNodeId())
			if node == nil {
				log.Error("node[%d] not found", p.GetNodeId())
				break
			}
			peer := &http_reply.Peer{
				Id: p.GetId(),
				Node: &metapb.Node{
					Id:         node.GetId(),
					ServerAddr: node.GetServerAddr(),
					RaftAddr:   node.GetRaftAddr(),
					State:      node.GetState(),
					Version:    node.GetVersion(),
				},
			}
			if r.GetLeader().GetId() == p.GetId() {
				leader = peer
				continue
			}
			peers = append(peers, peer)
		}
		_range := &http_reply.Range{
			Id:         rng.GetId(),
			StartKey:   rng.GetStartKey(),
			EndKey:     rng.GetEndKey(),
			RangeEpoch: &metapb.RangeEpoch{ConfVer: rng.GetRangeEpoch().GetConfVer(), Version: rng.GetRangeEpoch().GetVersion()},
			Peers:      peers,
		}
		route := &http_reply.Route{
			Range:  _range,
			Leader: leader,
			Downs:  r.downPeer,
			Pends:  r.pendPeer,
		}
		_routes = append(_routes, route)
	}

	data, err := json.Marshal(&http_reply.RangeLocateResponse{Routes: _routes})
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = "result marshal error" + err.Error()
		return
	}
	reply.Data = string(data)
	return
}

func (service *Server) handleTableGetRoute(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	cluster := service.cluster
	db, find := cluster.FindDatabase(dbName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	if table.Status != metapb.TableStatus_TableRunning {
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("table <%v> is not running", table.GetName())
		return
	}

	type Peer struct {
		Id   uint64       `json:"id,omitempty"`
		Node *metapb.Node `json:"node,omitempty"`
	}
	type Range struct {
		Id uint64 `json:"id,omitempty"`
		// Range key range [start_key, end_key).
		StartKey   []byte             `json:"start_key,omitempty"`
		EndKey     []byte             `json:"end_key,omitempty"`
		RangeEpoch *metapb.RangeEpoch `json:"range_epoch,omitempty"`
		Peers      []*Peer            `json:"peers,omitempty"`
		// Range state
		State      int32  `json:"state,omitempty"`
		DbName     string `json:"db_name,omitempty"`
		TableName  string `json:"table_name,omitempty"`
		TableId    uint64 `json:"table_id,omitempty"`
		CreateTime int64  `json:"create_time,omitempty"`
		LastHbTime string `json:"last_hb_time,omitempty"`
	}
	type Route struct {
		Range  *Range `json:"range,omitempty"`
		Leader *Peer  `json:"leader,omitempty"`
	}

	ranges := cluster.GetTableAllRanges(table.GetId())
	var _routes []*Route
	for _, rng := range ranges {
		rngCopy := deepcopy.Iface(rng.Range).(*metapb.Range)
		var peers []*Peer
		var leader *Peer
		for _, p := range rngCopy.GetPeers() {
			node := service.cluster.FindNodeById(p.GetNodeId())
			peer := &Peer{
				Id: p.GetId(),
				Node: &metapb.Node{
					Id:         node.GetId(),
					ServerAddr: node.GetServerAddr(),
					RaftAddr:   node.GetRaftAddr(),
					State:      node.GetState(),
					Version:    node.GetVersion(),
				},
			}
			if rng.GetLeader().GetId() == p.GetId() {
				leader = peer
				continue
			}
			peers = append(peers, peer)
		}
		_range := &Range{
			Id: rngCopy.GetId(),
			// Range key range [start_key, end_key).
			StartKey:   rngCopy.GetStartKey(),
			EndKey:     rngCopy.GetEndKey(),
			RangeEpoch: &metapb.RangeEpoch{ConfVer: rngCopy.GetRangeEpoch().GetConfVer(), Version: rngCopy.GetRangeEpoch().GetVersion()},
			Peers:      peers,
			// Range state
			State:      int32(rng.State),
			DbName:     table.GetDbName(),
			TableName:  table.GetName(),
			TableId:    table.GetId(),
			LastHbTime: rng.LastHbTimeTS.Format("2006-01-02 15:04:05"),
		}
		route := &Route{
			Range:  _range,
			Leader: leader,
		}
		_routes = append(_routes, route)
	}
	reply.Data = _routes
}

func (service *Server) handleTableGetRanges(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	cluster := service.cluster
	db, find := cluster.FindDatabase(dbName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	ranges := cluster.GetTableAllRanges(table.GetId())
	var rngs []*metapb.Range
	for _, r := range ranges {
		rngs = append(rngs, r.Range)
	}
	reply.Data = rngs

	return
}

func (service *Server) handlePeerDeleteForce(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tableName := r.FormValue(HTTP_TABLE_NAME)
	if len(dbName) == 0 || len(tableName) == 0 {
		log.Error("http range del peer: db [%s] is not existed", dbName)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	rangeId, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("http range del peer: range id is not int: %v", err)
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("rangeid parse uint: %v", err)
		return
	}
	peerId, err := strconv.ParseUint(r.FormValue(HTTP_PEER_ID), 10, 64)
	if err != nil {
		log.Error("http range del peer: peer id is not int: %v", err)
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("peerid parse uint: %v", err)
		return
	}
	log.Debug("handle peer delete force: dbname: %v, tableName: %v, rangeId: %v, peerId: %v",
		dbName, tableName, rangeId, peerId)

	rang := service.cluster.FindRange(rangeId)
	if rang == nil {
		log.Error("http range del peer: table [%s] is not existed", tableName)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	peer := rang.GetPeer(peerId)
	if peer == nil {
		log.Error("http range del peer: table [%s] is not existed", tableName)
		reply.Code = HTTP_ERROR_PEER_FIND
		reply.Message = http_error_peer_find
		return
	}
	node := service.cluster.FindNodeById(peer.GetNodeId())
	if node == nil {
		log.Error("http range del peer: node[%d] not found", peer.GetNodeId())
		reply.Code = HTTP_ERROR_NODE_FIND
		reply.Message = http_error_node_find
		return
	}

	if err := service.cluster.cli.DeleteRange(node.GetServerAddr(), rangeId, peerId); err != nil {
		log.Error("http range del peer node %v: do delete error: %v", peer.GetNodeId(), err)
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("delete range %v peer %v (%v) error: %v", rangeId, peerId, node.GetServerAddr(), err)
		return
	}
	log.Info("delete range %v peer %v (%v) ok", rangeId, peerId, node.GetServerAddr())
}

func (service *Server) handleRangeSetEpoch(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	rangeId, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("rangeId parse uint error: %v", err)
		return
	}
	ConfVer, err := strconv.ParseUint(r.FormValue("epochConfVer"), 10, 64)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("epoch ConfVer parse uint error: %v", err)
		return
	}
	Version, err := strconv.ParseUint(r.FormValue("epochVersion"), 10, 64)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("epoch Version parse uint error: %v", err)
		return
	}

	rang := service.cluster.FindRange(rangeId)
	if rang == nil {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistRange.Error()
		return
	}
	rang.lock.Lock()
	defer rang.lock.Unlock()
	rang.RangeEpoch.ConfVer = ConfVer
	rang.RangeEpoch.Version = Version
}

func (service *Server) handleSearchRange(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	rawKey := r.FormValue("key")
	if len(rawKey) == 0 {
		reply.Code = HTTP_ERROR_INVALID_PARAM
		reply.Message = http_error_invalid_parameter
		return
	}
	cluster := service.cluster
	db, find := cluster.FindDatabase(dbName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	// 按照主键编码
	pkCols := table.GetPkColumns()
	if pkCols == nil {
		reply.Code = HTTP_ERROR
		reply.Message = ErrPkMustNotNull.Error()
		return
	}
	var key []byte
	var err error
	key, err = util.EncodePrimaryKey(key, pkCols[0], []byte(rawKey))
	if pkCols == nil {
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	key = append(util.EncodeStorePrefix(util.Store_Prefix_KV, table.GetId()), key...)
	rng := cluster.SearchRange(key)
	if rng == nil {
		reply.Code = HTTP_ERROR
		reply.Message = "not find range"
		return
	}
	reply.Data = rng.Range

	return
}

/**
integrality check
*/
func (service *Server) handleTopologyCheck(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	cluster := service.cluster
	db, find := cluster.FindDatabase(dbName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}

	var start, end []byte
	start = util.EncodeStorePrefix(util.Store_Prefix_KV, table.GetId())
	_, end = bytesPrefix(start)

	var rang *Range
	var searchKey []byte
	flag := true
	for flag {
		if searchKey == nil {
			searchKey = start
		} else {
			searchKey = rang.GetEndKey()
			if bytes.Compare(searchKey, end) == 0 {
				flag = false
				break
			}
		}
		rang = cluster.SearchRange(searchKey)
		if rang == nil {
			reply.Code = HTTP_ERROR
			reply.Message = "range scope is not found: " + string(searchKey)
			return
		}
	}

	return
}

/**
topology miss scope
*/
func (service *Server) handleTableTopologyMissing(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	cluster := service.cluster
	db, find := cluster.FindDatabase(dbName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}

	reply.Data = cluster.ranges.GetTableTopologyMissing(table.GetId())
	return
}

func (service *Server) handleTableTopologyCreate(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	startKey := r.FormValue(HTTP_STARTKEY)
	endKey := r.FormValue(HTTP_ENDKEY)
	cluster := service.cluster
	db, find := cluster.FindDatabase(dbName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}

	start, err1 := base64.StdEncoding.DecodeString(startKey)
	end, err2 := base64.StdEncoding.DecodeString(endKey)
	if err1 != nil || err2 != nil {
		reply.Code = HTTP_ERROR
		reply.Message = "decode error"
		return
	}

	if bytes.Compare(start, end) > -1 {
		reply.Code = HTTP_ERROR
		reply.Message = "startKey is equal or greater than endKey"
		return
	}

	if _, find := cluster.ranges.SearchRange(start); find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrExistsRange.Error()
		return
	}

	var startKeyI, endKeyI []byte
	startKeyI = util.EncodeStorePrefix(util.Store_Prefix_KV, table.GetId())
	_, endKeyI = bytesPrefix(startKeyI)

	if bytes.Compare(start, startKeyI) == -1 || bytes.Compare(end, endKeyI) == 1 {
		reply.Code = HTTP_ERROR
		reply.Message = "startKey or endKey is over limit table topology scope"
		return
	}

	if err := cluster.createRangeByScope(start, end, table); err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	log.Info("create range by scope: start %v, end [%v] success", start, end)
	return
}

func (service *Server) handleTableTopologyBatchCreate(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	cluster := service.cluster
	db, find := cluster.FindDatabase(dbName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}

	go func() {
		rangesMissing := cluster.ranges.GetTableTopologyMissing(table.GetId())
		for i, r := range rangesMissing {
			if err := cluster.createRangeByScope(r.StartKey, r.EndKey, table); err != nil {
				log.Warn("batch create range failed, startKey:%v, endKey:%v", r.StartKey, r.EndKey)
			}
			if i%10 == 0 {
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	log.Info("batch create range success")
	return
}

func (service *Server) handleTableRangeDuplicate(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	cluster := service.cluster
	db, find := cluster.FindDatabase(dbName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	reply.Data = cluster.ranges.GetTableRangeDuplicate(table.Id)
	return
}

func (service *Server) handleTopologyQuery(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	cluster := service.cluster
	reply.Data = cluster.ranges.GetAllRangeFromTopology()
	return
}

func (service *Server) handleTableTopologyQuery(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	cluster := service.cluster
	db, find := cluster.FindDatabase(dbName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	table, find := db.FindTable(tName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	reply.Data = cluster.ranges.GetTableAllRangesFromTopology(table.GetId())
	return
}

//set metric send config
func (service *Server) handleMetricConfigSet(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	interval := r.FormValue("interval")
	addr := r.FormValue("address")

	var metricInterval util.Duration
	metricInterval.UnmarshalJSON([]byte(interval))

	if err := UpdateMetric(service.cluster, addr, metricInterval.Duration); err != nil {
		log.Warn("set metric send config err, %v", err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	log.Info("set metric send config success, interval:%v, addr:%v", metricInterval, addr)
	return
}

//get metric send config
func (service *Server) handleMetricConfigGet(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	cluster := service.cluster
	metricConfig := &MetricConfig{
		Address:  cluster.metric.GetMetricAddr(),
		Interval: util.NewDuration(cluster.metric.GetMetricInterval()),
	}
	reply.Data = metricConfig
	log.Info("get metric send config success, %v", metricConfig)
	return
}

type SignHandler func(w http.ResponseWriter, r *http.Request) bool
type HttpHandler func(w http.ResponseWriter, r *http.Request)

func NewHandler(valid ValidHandler, handler HttpHandler) server.ServiceHttpHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if valid != nil {
			if !valid(w, r) {
				return
			}
		}
		if handler != nil {
			handler(w, r)
		}
	}
}
