package controllers

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"console/service"
	"console/common"
	"util/log"
)

const (
	REQURI_MASTER_All             = "/master/queryAll"
	REQURI_MASTER_LEADER          = "/master/queryLeader"
	REQURI_MASTER_LOGLEVEL_UPDATE = "/master/logLevelUpdate"
)

/**
 * 集群master all
 */
type MasterAllAction struct {
}

func NewMasterAllAction() *MasterAllAction {
	return &MasterAllAction{
	}
}
func (ctrl *MasterAllAction) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	log.Debug("query cluster master leader. cid:[%v]", cId)

	cToken := common.BuildNewClusterToken(cId, "", "")
	return service.NewService().GetMasterAll(cId, cToken)
}

/**
 * 集群master
 */
type MasterLeaderAction struct {
}

func NewMasterLeaderAction() *MasterLeaderAction {
	return &MasterLeaderAction{
	}
}
func (ctrl *MasterLeaderAction) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	log.Debug("query cluster master leader. cid:[%v]", cId)

	cToken := common.BuildNewClusterToken(cId, "", "")
	return service.NewService().GetMasterLeader(cId, cToken)
}

type MasterLogLevelUpdate struct {
}

func NewMasterLogLevelUpdate() *MasterLogLevelUpdate {
	return &MasterLogLevelUpdate{}
}
func (ctrl *MasterLogLevelUpdate) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	logLevel := c.PostForm("logLevel")
	if cIdStr == "" || logLevel == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	clusterId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	log.Debug("set master log level. clusterId:[%v], logLevel:[%v]", clusterId, logLevel)
	err = service.NewService().SetMasterLogLevel(clusterId, logLevel)
	if err != nil {
		return nil, err
	}
	return nil, nil
}
