package controllers

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"console/service"
	"console/common"
	"util/log"
	"github.com/gin-contrib/sessions"
	"console/right"
	"fmt"
)

const (
	REQURI_CLUSTER_GETALL = "/cluster/queryClusters"
	REQURI_CLUSTER_GETBYID = "/cluster/getById"
	REQURI_CLUSTER_CREATE = "/cluster/createCluster"
	REQURI_CLUSTER_INIT = "/cluster/initCluster"
	REQURI_CLUSTER_TOGGLEAUTO= "/cluster/toggleAuto"
)

/**
 * 查询集群列表
 */
type ClusterGetAllAction struct {
}
func NewClusterGetAllAction() *ClusterGetAllAction {
	return &ClusterGetAllAction{
	}
}
func (ctrl *ClusterGetAllAction)Execute(c *gin.Context) (interface{}, error) {
	userName := sessions.Default(c).Get("user_name").(string)
	user := right.GetCacheUser(userName)
	if user == nil {
		return nil, fmt.Errorf("no user cached %v", userName)
	}
	var clusterIds []int64
	for id, r := range user.Right {
		if id == 0 &&  r == 1{
			log.Debug("admin [%v] get cluster: %v", userName, clusterIds)
			return service.NewService().GetAllClusters()
		}
		clusterIds = append(clusterIds, id)
	}
	log.Debug("user [%v] get cluster list: %v", userName, clusterIds)
	return service.NewService().GetClusterById(clusterIds...)
}


/**
 * 查询集群详情
 */
type ClusterGetByIdAction struct {
}
func NewClusterGetByIdAction() *ClusterGetByIdAction {
	return &ClusterGetByIdAction{
	}
}
func (ctrl *ClusterGetByIdAction)Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.ParseInt(cIdStr, 10, 64)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	log.Debug("query cluster: %v info", cId)
	clusters, err :=  service.NewService().GetClusterById(cId)
	if err != nil {
		return nil, err
	}
	return  clusters[0], nil
}

/**
 * 创建集群
 */
type ClusterCreateAction struct {
}
func NewClusterCreateAction() *ClusterCreateAction {
	return &ClusterCreateAction {
	}
}
func (ctrl *ClusterCreateAction)Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	cName := c.PostForm("clusterName")
	if cName == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	masterUrl := c.PostForm("masterUrl")
	if masterUrl == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	gatewayHttpUrl := c.PostForm("gatewayHttpUrl")
	if gatewayHttpUrl == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	gatewaySqlUrl := c.PostForm("gatewaySqlUrl")
	if gatewaySqlUrl == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	log.Debug("create new cluster. cid:[%v]", cId)

	cToken := common.BuildNewClusterToken(cId, cName, "")
	err = service.NewService().CreateCluster(cId, cName, masterUrl, gatewayHttpUrl, gatewaySqlUrl, cToken, time.Now().Unix())
	if err != nil {
		return nil, err
	}

	return nil, nil
}

/**
 * 集群初始化
 */
type ClusterInitAction struct {
}
func NewClusterInitAction() *ClusterInitAction {
	return &ClusterInitAction{
	}
}
func (ctrl *ClusterInitAction)Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	masterUrl := c.PostForm("masterUrl")
	if masterUrl == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	log.Debug("init cluster. cid:[%v]", cId)
	isolationLabel := c.PostForm("isolationLabel")

	cToken := common.BuildNewClusterToken(cId, "", "")
	err = service.NewService().InitCluster(cId, masterUrl, isolationLabel, cToken)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

/**
 * 集群开关
 */
type ClusterToggleAction struct {
}
func NewClusterToggleAction() *ClusterToggleAction {
	return &ClusterToggleAction{
	}
}
func (ctrl *ClusterToggleAction)Execute(c *gin.Context) (interface{}, error) {
	log.Debug("set cluster toggle")
	cIdStr := c.Query("clusterId")
	if cIdStr == "" {
		log.Error("set cluster toggle error")
		return nil, common.PARSE_PARAM_ERROR
	}

	autoTransfer := c.PostForm("autoTransferUnable")
	autoFailover := c.PostForm("autoFailoverUnable")

	log.Debug("autoTransfer" + autoTransfer + ", autoFailover" + autoFailover)

	if autoFailover == "" && autoTransfer == "" {
		return nil, common.PARSE_PARAM_ERROR
	}

	clusterId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	err = service.NewService().SetClusterToggle(clusterId, autoTransfer, autoFailover)
	if err != nil {
		return nil, err
	}
	return nil, nil
}