package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/gin-contrib/sessions"

	"console/service"
	"console/common"
	"util/log"

	"strconv"
	"time"
	"encoding/json"
)

const (
	REQURI_CONFIGURE_NAMESPACE_GETALL = "/configure/namespace/queryList"
	REQURI_CONFIGURE_NAMESPACE_APPLY  = "/configure/namespace/apply"
	REQURI_CONFIGURE_NAMESPACE_AUDIT  = "/configure/namespace/audit"
	REQURI_CONFIGURE_CLUSTER_LIST     = "/configure/cluster/getList"
	REQURI_CONFIGURE_CLUSTER_INFO     = "/configure/cluster/getInfo"
	REQURI_CONFIGURE_NAMESPACE_UPDATE = "/configure/namespace/update"
	REQURI_CONFIGURE_NAMESPACE_DELETE = "/configure/namespace/delete"
	REQURI_CONFIGURE_GETALL           = "/configure/queryList"
)

/**
* 查询configure namespace 列表
*/
type ConfigureGetAllNspAction struct {
}

func NewConfigureGetAllNspAction() *ConfigureGetAllNspAction {
	return &ConfigureGetAllNspAction{
	}
}
func (ctrl *ConfigureGetAllNspAction) Execute(c *gin.Context) (interface{}, error) {
	userName := sessions.Default(c).Get("user_name").(string)
	isAdmin, _ := service.NewService().IsAdmin(userName)
	log.Debug("user [%v] get configure namespace apply list, isAdmin: %v", userName, isAdmin)
	pageInfo, err := common.GetPagerInfo(c)
	if err != nil {
		return nil, err
	}
	totalRecord, namespaceList, err := service.NewService().GetAllConfigureNsp(userName, isAdmin, pageInfo)
	if err != nil {
		log.Warn("get configure namespace list error, %v", err)
		return nil, err
	}

	pageData := new(PageData)
	pageData.Total = totalRecord
	pageData.Data = namespaceList
	return pageData, nil
}

/**
* 申请configure namespace
*/
type ConfigureNspApplyAction struct {
}

func NewConfigureNspApplyAction() *ConfigureNspApplyAction {
	return &ConfigureNspApplyAction{
	}
}
func (ctrl *ConfigureNspApplyAction) Execute(c *gin.Context) (interface{}, error) {
	userName := sessions.Default(c).Get("user_name").(string)
	if len(userName) == 0 {
		return nil, common.NO_USER
	}
	cIdStr := c.PostForm("clusterId")
	dbName := c.PostForm("dbName")
	tableName := c.PostForm("tableName")
	if cIdStr == "" || dbName == "" || tableName == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	log.Debug("apply configure dbName:%v, tableName: %v, applyer:%v, cluserId:%v.", dbName, tableName, userName, cId)
	err = service.NewService().ApplyConfigureNsp(cId, dbName, tableName, userName, time.Now().Unix())
	if err != nil {
		return nil, err
	}

	return nil, nil
}

/**
* 审批configure namespace
*/
type ConfigureNspAuditAction struct {
}

func NewConfigureNspAuditAction() *ConfigureNspAuditAction {
	return &ConfigureNspAuditAction{
	}
}
func (ctrl *ConfigureNspAuditAction) Execute(c *gin.Context) (interface{}, error) {
	ids := c.PostForm("ids")
	status := c.PostForm("status")
	if ids == "" || status == "" {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("audit configure apply, ids:%v, status:%v.", ids, status)
	var idArray []string
	if err := json.Unmarshal([]byte(ids), &idArray); err != nil {
		return nil, common.PARSE_PARAM_ERROR
	}
	statusI, err := strconv.Atoi(status)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	userName := sessions.Default(c).Get("user_name").(string)
	isAdmin, err := service.NewService().IsAdmin(userName)
	if err != nil {
		return nil, common.NO_USER
	}

	if !isAdmin {
		return nil, common.NO_RIGHT
	}

	err = service.NewService().AuditConfigureNsp(idArray, statusI, userName)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

/**
* configure namespace修改
*/
type ConfigureNspUpdateAction struct {
}

func NewConfigureNspUpdateAction() *ConfigureNspUpdateAction {
	return &ConfigureNspUpdateAction{
	}
}

func (ctrl *ConfigureNspUpdateAction) Execute(c *gin.Context) (interface{}, error) {
	applyId := c.PostForm("id")
	applyer := c.PostForm("applyer")
	if "" == applyId || "" == applyer {
		return nil, common.PARSE_PARAM_ERROR
	}
	log.Debug("update configure applyer %v of id: %v ", applyer, applyId)
	err := service.NewService().UpdateConfigureNsp(applyId, applyer)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

/**
* Configure namespace 删除
*/
type ConfigureNspDeleteAction struct {
}

func NewConfigureNspDeleteAction() *ConfigureNspDeleteAction {
	return &ConfigureNspDeleteAction{
	}
}

func (ctrl *ConfigureNspDeleteAction) Execute(c *gin.Context) (interface{}, error) {
	ids := c.PostForm("ids")
	if ids == "" {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("delete configure apply record, ids:%v.", ids)
	var idArray []string
	if err := json.Unmarshal([]byte(ids), &idArray); err != nil {
		return nil, common.PARSE_PARAM_ERROR
	}

	return nil, service.NewService().DeleteConfigureNsp(idArray)
}

/**
 * configure cluster 刚上线的时候，锁集群是通过配置的，后期改成获取对应权限的集群列表
 */
type ConfigureClusterListGetAction struct {
}

func NewConfigureClusterListGetAction() *ConfigureClusterListGetAction {
	return &ConfigureClusterListGetAction{
	}
}
func (ctrl *ConfigureClusterListGetAction) Execute(c *gin.Context) (interface{}, error) {
	log.Debug("get configure cluster list")
	return service.NewService().GetConfigureClusterList()
}


/**
* 查询configure center 一级 列表
*/
type ConfigureGetAllAction struct {
}

func NewConfigureGetAllAction() *ConfigureGetAllAction {
	return &ConfigureGetAllAction{
	}
}
func (ctrl *ConfigureGetAllAction) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.Query("clusterId")
	dbName := c.Query("dbName")
	tableName := c.Query("tableName")
	if "" == cIdStr || "" == dbName || "" == tableName {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	log.Debug("get configure detail list: clusterId %v, dbName %v, tableName %v", cId, dbName, tableName)

	pageInfo, err := common.GetPagerInfo(c)
	if err != nil {
		return nil, err
	}
	confList, err := service.NewService().GetAllConfigure(cId, dbName, tableName, pageInfo)
	if err != nil {
		log.Warn("get configure detail list error, %v", err)
		return nil, err
	}

	pageData := new(PageData)
	pageData.Total = len(confList) //由于http方式不支持count，所以，只能迭代取
	pageData.Data = confList
	pageData.PageIndex = pageInfo.PageIndex
	pageData.PageSize = pageInfo.PageSize
	return pageData, nil
}


type ConfigureClusterInfoGetAction struct {
}

func NewConfigureClusterInfoGetAction() *ConfigureClusterInfoGetAction {
	return &ConfigureClusterInfoGetAction{
	}
}
func (ctrl *ConfigureClusterInfoGetAction) Execute(c *gin.Context) (interface{}, error) {
	log.Debug("get configure cluster info")
	cIdStr := c.PostForm("clusterId")
	if "" == cIdStr {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	return service.NewService().GetClusterInfo(cId)
}