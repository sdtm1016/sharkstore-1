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
	cIdStr := c.Query("clusterId")
	nIdStr := c.Query("nodeId")
	isolationLabel := c.Query("isolationLabel")
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