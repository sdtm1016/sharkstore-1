package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/gin-contrib/sessions"

	"console/common"
	"console/service"
	"util/log"

	"strconv"
	"time"
	"encoding/json"
)

const (
	REQURI_LOCK_NAMESPACE_GETALL = "/lock/namespace/queryList"
	REQURI_LOCK_NAMESPACE_APPLY  = "/lock/namespace/apply"
	REQURI_LOCK_NAMESPACE_AUDIT  = "/lock/namespace/audit"
	REQURI_LOCK_CLUSTER_LIST     = "/lock/cluster/getList"
	REQURI_LOCK_CLUSTER_INFO     = "/lock/cluster/getInfo"
	REQURI_LOCK_NAMESPACE_UPDATE = "/lock/namespace/update"
	REQURI_LOCK_NAMESPACE_DELETE = "/lock/namespace/delete"
	REQURI_LOCK_LOCK_GETALL      = "/lock/lock/queryList"
	REQURI_LOCK_LOCK_FORCEUNLOCK = "/lock/lock/forceUnLock"
	REQURI_LOCK_CLIENT_TOKEN     = "/lock/client/getToken"
)

/**
 * 查询lock namespace 列表
 */
type LockGetAllNspAction struct {
}

func NewLockGetAllNspAction() *LockGetAllNspAction {
	return &LockGetAllNspAction{
	}
}
func (ctrl *LockGetAllNspAction) Execute(c *gin.Context) (interface{}, error) {
	userName := sessions.Default(c).Get("user_name").(string)
	isAdmin, _ := service.NewService().IsAdmin(userName)
	log.Debug("user [%v] get lock namespace apply list, isAdmin: %v", userName, isAdmin)
	pageInfo, err := common.GetPagerInfo(c)
	if err != nil {
		return nil, err
	}
	totalRecord, namespaceList, err := service.NewService().GetAllLockNsp(userName, isAdmin, pageInfo)
	if err != nil {
		log.Warn("get lock namespace list error, %v", err)
		return nil, err
	}

	pageData := new(PageData)
	pageData.Total = totalRecord
	pageData.Data = namespaceList
	return pageData, nil
}

/**
 * 申请lock namespace
 */
type LockNspApplyAction struct {
}

func NewLockNspApplyAction() *LockNspApplyAction {
	return &LockNspApplyAction{
	}
}
func (ctrl *LockNspApplyAction) Execute(c *gin.Context) (interface{}, error) {
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

	log.Debug("apply lock dbName:%v, tableName: %v, applyer:%v, cluserId:%v.", dbName, tableName, userName, cId)
	err = service.NewService().ApplyLockNsp(cId, dbName, tableName, userName, time.Now().Unix())
	if err != nil {
		return nil, err
	}

	return nil, nil
}

/**
 * 审批lock namespace
 */
type LockNspAuditAction struct {
}

func NewLockNspAuditAction() *LockNspAuditAction {
	return &LockNspAuditAction{
	}
}
func (ctrl *LockNspAuditAction) Execute(c *gin.Context) (interface{}, error) {
	ids := c.PostForm("ids")
	status := c.PostForm("status")
	if ids == "" || status == "" {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("audit lock apply, ids:%v, status:%v.", ids, status)
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

	return nil, service.NewService().AuditLockNsp(idArray, statusI, userName)
}

/**
 * lock namespace修改
 */
type LockNspUpdateAction struct {
}

func NewLockNspUpdateAction() *LockNspUpdateAction {
	return &LockNspUpdateAction{
	}
}

func (ctrl *LockNspUpdateAction) Execute(c *gin.Context) (interface{}, error) {
	applyId := c.PostForm("id")
	applyer := c.PostForm("applyer")
	if "" == applyId || "" == applyer {
		return nil, common.PARSE_PARAM_ERROR
	}
	log.Debug("update lock applyer %v of id: %v ", applyer, applyId)
	err := service.NewService().UpdateLockNsp(applyId, applyer)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

/**
 * lock namespace 删除
 */
type LockNspDeleteAction struct {
}

func NewLockNspDeleteAction() *LockNspDeleteAction {
	return &LockNspDeleteAction{
	}
}

func (ctrl *LockNspDeleteAction) Execute(c *gin.Context) (interface{}, error) {
	ids := c.PostForm("ids")
	if ids == "" {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("delete lock apply record, ids:%v.", ids)
	var idArray []string
	if err := json.Unmarshal([]byte(ids), &idArray); err != nil {
		return nil, common.PARSE_PARAM_ERROR
	}

	return nil, service.NewService().DeleteLockNsp(idArray)
}

/**
 * lock cluster 刚上线的时候，锁集群是通过配置的，后期改成获取对应权限的集群列表
 */
type LockClusterListGetAction struct {
}

func NewLockClusterListGetAction() *LockClusterListGetAction {
	return &LockClusterListGetAction{
	}
}
func (ctrl *LockClusterListGetAction) Execute(c *gin.Context) (interface{}, error) {
	log.Debug("get lock cluster list")
	return service.NewService().GetLockClusterList()
}

/**
 * 查询lock 详情 列表
 */
type LockGetAllAction struct {
}

func NewLockGetAllAction() *LockGetAllAction {
	return &LockGetAllAction{
	}
}
func (ctrl *LockGetAllAction) Execute(c *gin.Context) (interface{}, error) {
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
	log.Debug("get lock detail list: clusterId %v, dbName %v, tableName %v", cId, dbName, tableName)

	pageInfo, err := common.GetPagerInfo(c)
	if err != nil {
		return nil, err
	}
	lockList, err := service.NewService().GetAllLock(cId, dbName, tableName, pageInfo)
	if err != nil {
		log.Warn("get lock detail list error, %v", err)
		return nil, err
	}

	pageData := new(PageData)
	pageData.Total = len(lockList) //由于http方式不支持count，所以，只能迭代取
	pageData.Data = lockList
	pageData.PageIndex = pageInfo.PageIndex
	pageData.PageSize = pageInfo.PageSize
	return pageData, nil
}

/**
 * lock 强制解锁
 */
type LockForceUnLockAction struct {
}

func NewLockForceUnLockAction() *LockForceUnLockAction {
	return &LockForceUnLockAction{
	}
}
func (ctrl *LockForceUnLockAction) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	dbName := c.PostForm("dbName")
	tableName := c.PostForm("tableName")
	key := c.PostForm("key")
	if "" == cIdStr || "" == dbName || "" == tableName || "" == key {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	log.Debug("force unlock: clusterId %v, dbName %v, tableName %v, key %v", cIdStr, dbName, tableName, key)
	return nil, service.NewService().ForceUnLock(cId, dbName, tableName, key)
}

/**
 * lock 生成token [配置中心一样的算法]
 */
type LockClientGetTokenAction struct {
}

func NewLockClientGetTokenAction() *LockClientGetTokenAction {
	return &LockClientGetTokenAction{
	}
}
func (ctrl *LockClientGetTokenAction) Execute(c *gin.Context) (interface{}, error) {
	dbIdStr := c.PostForm("dbId")
	tableIdStr := c.PostForm("tableId")
	if "" == dbIdStr || "" == tableIdStr {
		return nil, common.PARSE_PARAM_ERROR
	}
	dbId, err1 := strconv.Atoi(dbIdStr)
	tableId, err2 := strconv.Atoi(tableIdStr)
	if err1 != nil || err2 != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	log.Debug("compute token: dbId %v, tableId %v", dbId, tableId)
	return service.NewService().ComputeClientToken(dbId, tableId), nil
}

type LockClusterInfoGetAction struct {
}

func NewLockClusterInfoGetAction() *LockClusterInfoGetAction {
	return &LockClusterInfoGetAction{
	}
}
func (ctrl *LockClusterInfoGetAction) Execute(c *gin.Context) (interface{}, error) {
	log.Debug("get lock cluster info")
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
