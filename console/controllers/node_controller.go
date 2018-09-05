package controllers

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"console/common"
	"console/service"
	"util/log"
	//"strings"
	"fmt"
)

const (
	NODE_NODEINFOGETALL = "/node/nodeInfoGetAll"
	NODE_NODESTATUSUPDATE = "/node/nodeStatusUpdate"
	NODE_NODELOGLEVELUPDATE = "/node/nodeLogLevelUpdate"
	NODE_DELETENODE = "/node/deleteNode"
	NODE_GET_RANGE_TOPOLOGY = "/node/getRangeTopoByNode"
	NODE_UPDATE_ISOLATION_LABEL = "/node/updateIsolationLabel"
	NODE_GET_CONFIG = "/node/getConfigOfNode"
	NODE_SET_CONFIG = "/node/setConfigOfNode"
	NODE_GET_DS_INFO = "/node/getDsInfoOfNode"
	NODE_CLEAR_QUEUE = "/node/clearQueueOfNode"
	NODE_GET_PENDING_QUEUES = "/node/getPendingQueuesOfNode"
	NODE_FLUSH_DB = "/node/flushDBOfNode"
)

type NodeViewInfo struct {
}

func NewNodeViewInfo () *NodeViewInfo{
	return &NodeViewInfo{}
}
func (ctrl *NodeViewInfo) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	log.Debug("query cluster node info. cid:[%v]", cId)
	return service.NewService().GetNodeViewInfo(cId)
}

type NodeStatusUpdate struct {
}

func NewNodeStatusUpdate () *NodeStatusUpdate{
	return &NodeStatusUpdate{}
}
func (ctrl *NodeStatusUpdate) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	nIdStr := c.PostForm("nodeId")
	sStr := c.PostForm("status")
	if cIdStr == "" || nIdStr == "" || sStr == ""{
		return nil, common.PARSE_PARAM_ERROR
	}
	clusterId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	nodeId, err := strconv.Atoi(nIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	status, err := strconv.Atoi(sStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	log.Debug("set node status logout. clusterId:[%v], nodeId:[%v]", clusterId, nodeId)
	switch status {
	case 1:
		err = service.NewService().SetNodeLogIn(clusterId, nodeId)
	case 2:
		err = service.NewService().SetNodeLogOut(clusterId, nodeId)
	case 3:
		err = service.NewService().SetNodeUpgrade(clusterId, nodeId)
	default:
		return nil, fmt.Errorf("not support")
	}
	if err != nil {
		return nil, err
	}
	return nil, nil
}

type NodeLogLevelUpdate struct {
}

func NewNodeLogLevelUpdate () *NodeLogLevelUpdate{
	return &NodeLogLevelUpdate{}
}
func (ctrl *NodeLogLevelUpdate) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	nIdStr := c.PostForm("nodeId")
	logLevel := c.PostForm("logLevel")
	if cIdStr == "" || nIdStr == "" || logLevel == ""{
		return nil, common.PARSE_PARAM_ERROR
	}
	clusterId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	nodeId, err := strconv.Atoi(nIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	log.Debug("set node log level. clusterId:[%v], nodeId:[%v], logLevel:[%v]", clusterId, nodeId, logLevel)
	err = service.NewService().SetNodeLogLevel(clusterId, nodeId, logLevel)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

type NodeDelete struct {
}

func NewNodeDelete () *NodeDelete{
	return &NodeDelete{}
}
func (ctrl *NodeDelete) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	nIdsStr := c.PostForm("nodeIds")
	if cIdStr == "" || nIdsStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	clusterId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	//var nodeIds []int
	//if err := json.Unmarshal([]byte(nIdsStr), &nodeIds); err != nil {
	//	log.Info("error3 %v %v", cIdStr, nodeIds)
	//	return nil, common.PARSE_PARAM_ERROR
	//}

	log.Debug("delete cluster node. clusterId:[%v], nodeIds:[%v]", clusterId, nIdsStr)
	err = service.NewService().DeleteNodes(clusterId, nIdsStr)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

type NodeRangeTopo struct {
}

func NewNodeRangeTopo() *NodeRangeTopo {
	return &NodeRangeTopo{}
}

func (ctrl *NodeRangeTopo) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.Query("clusterId")
	nIdStr := c.Query("nodeId")
	if len(cIdStr) == 0 || len(nIdStr) == 0 {
		return nil, common.PARSE_PARAM_ERROR
	}
	clusterId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	nodeId, err := strconv.Atoi(nIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	log.Debug("getting range topology of node. clusterId:[%v], nodeId:[%v]", clusterId, nodeId)
	data, err := service.NewService().GetRangeTopoByNodeId(clusterId, nodeId)
	if err != nil {
		return nil, err
	}
	return data, nil
}

type UpdateIsolationLabel struct {
}

func NewUpdateIsolationLabel() *UpdateIsolationLabel {
	return &UpdateIsolationLabel{}
}

func (ctrl *UpdateIsolationLabel) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	nIdStr := c.PostForm("nodeId")
	isolationLabel := c.PostForm("isolationLabel")
	if len(cIdStr) == 0 || len(nIdStr) == 0 {
		return nil, common.PARSE_PARAM_ERROR
	}
	clusterId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	nodeId, err := strconv.Atoi(nIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	log.Debug("updating isolation label=[%s] of node. clusterId:[%v], nodeId:[%v]", isolationLabel, clusterId, nodeId)
	if err := service.NewService().UpdateNodeIsolationLabel(clusterId, nodeId, isolationLabel); err != nil {
		return nil, err
	}
	return nil, nil
}

type NodeGetConfig struct {
}

func NewNodeGetConfig() *NodeGetConfig {
	return &NodeGetConfig{}
}

func (ctrl *NodeGetConfig) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	nIdStr := c.PostForm("nodeId")
	getConfigKey := c.PostForm("getConfigKey")
	if cIdStr == "" || nIdStr == "" || getConfigKey == ""{
		return nil, common.PARSE_PARAM_ERROR
	}
	clusterId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	nodeId, err := strconv.Atoi(nIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	log.Debug("node get config. clusterId:[%v], nodeId:[%v], getConfigKey:[%v]", clusterId, nodeId, getConfigKey)

	return service.NewService().GetConfigOfNode(clusterId, nodeId, getConfigKey)
}

type NodeSetConfig struct {
}

func NewNodeSetConfig() *NodeSetConfig {
	return &NodeSetConfig{}
}

func (ctrl *NodeSetConfig) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	nIdStr := c.PostForm("nodeId")
	setConfig := c.PostForm("setConfig")
	if cIdStr == "" || nIdStr == "" || setConfig == ""{
		return nil, common.PARSE_PARAM_ERROR
	}
	clusterId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	nodeId, err := strconv.Atoi(nIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	log.Debug("node set config. clusterId:[%v], nodeId:[%v], setConfig:[%v]", clusterId, nodeId, setConfig)

	return service.NewService().SetConfigOfNode(clusterId, nodeId, setConfig)
}

type NodeGetDsInfo struct {
}

func NewNodeGetDsInfo() *NodeGetDsInfo {
	return &NodeGetDsInfo{}
}

func (ctrl *NodeGetDsInfo) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	nIdStr := c.PostForm("nodeId")
	path := c.PostForm("dsInfoPath")

	if cIdStr == "" || nIdStr == "" || path == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	clusterId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	nodeId, err := strconv.Atoi(nIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	log.Debug("node get ds_info. clusterId:[%v], nodeId:[%v], path=[%s]", clusterId, nodeId, path)

	return service.NewService().GetDsInfoOfNode(clusterId, nodeId, path)
}

type NodeClearQueue struct {
}

func NewNodeClearQueue() *NodeClearQueue {
	return &NodeClearQueue{}
}

func (ctrl *NodeClearQueue) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	nIdStr := c.PostForm("nodeId")
	queueType := c.PostForm("queueType")

	if cIdStr == "" || nIdStr == "" || queueType == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	clusterId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	nodeId, err := strconv.Atoi(nIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	log.Debug("node clear queue. clusterId:[%v], nodeId:[%v], queueType=[%s]", clusterId, nodeId, queueType)

	return service.NewService().ClearQueueOfNode(clusterId, nodeId, queueType)
}

type NodeGetPendingQueues struct {
}

func NewNodeGetPendingQueues() *NodeGetPendingQueues {
	return &NodeGetPendingQueues{}
}

func (ctrl *NodeGetPendingQueues) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	nIdStr := c.PostForm("nodeId")
	pendingType := c.PostForm("pendingType")
	count := c.PostForm("count")

	if cIdStr == "" || nIdStr == "" || pendingType == "" || count == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	clusterId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	nodeId, err := strconv.Atoi(nIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	log.Debug("node get pendings. clusterId:[%v], nodeId:[%v], pendingType=[%s], count=[%s]", clusterId, nodeId, pendingType, count)

	return service.NewService().GetPendingQueuesOfNode(clusterId, nodeId, pendingType, count)
}

type NodeFlushDB struct {
}

func NewNodeFlushDB() *NodeFlushDB {
	return &NodeFlushDB{}
}

func (ctrl *NodeFlushDB) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	nIdStr := c.PostForm("nodeId")
	wait := c.PostForm("wait")
	var waitBool bool
	var err error

	if cIdStr == "" || nIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	if wait == "" {
		waitBool = false
	}
	if waitBool, err = strconv.ParseBool(wait); err != nil {
		waitBool = false
	}

	clusterId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	nodeId, err := strconv.Atoi(nIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	log.Debug("flush db of node. clusterId:[%v], nodeId:[%v], wait:[%t]", clusterId, nodeId, waitBool)

	return service.NewService().FlushDBOfNode(clusterId, nodeId, waitBool)
}