package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/gin-contrib/sessions"

	"console/common"
	"console/service"
	"util/log"

	"fmt"
	"time"
	"encoding/json"
	"strconv"
)

const (
	REQURI_SQL_GETALL       = "/sql/queryApplyList"
	REQURI_SQL_APPLY        = "/sql/apply"
	REQURI_SQL_APPLY_DETAIL = "/sql/apply/detail"
	REQURI_SQL_APPLY_AUDIT  = "/sql/audit"
)

/**
 * 查询sql apply列表
 */
type SqlGetAllAction struct {
}

func NewSqlGetAllAction() *SqlGetAllAction {
	return &SqlGetAllAction{
	}
}
func (ctrl *SqlGetAllAction) Execute(c *gin.Context) (interface{}, error) {
	userName := sessions.Default(c).Get("user_name").(string)
	isAdmin, _ := service.NewService().IsAdmin(userName)
	log.Debug("user [%v] get sql apply list, isAdmin: %v", userName, isAdmin)
	pageInfo, err := common.GetPagerInfo(c)
	if err != nil {
		return nil, err
	}
	log.Info("page %v", pageInfo)
	totalRecord, sqlApplyList, err := service.NewService().GetAllSqlApply(userName, isAdmin, pageInfo)
	if err != nil {
		log.Warn("get sql apply list error, %v", err)
		return nil, err
	}
	pageData := new(PageData)
	pageData.Total = totalRecord
	pageData.Data = sqlApplyList
	log.Info("get sql apply list, %v", pageData)
	return pageData, nil
}

/**
 * 申请sql
 */
type SqlApplyAction struct {
}

func NewSqlApplyAction() *SqlApplyAction {
	return &SqlApplyAction{
	}
}
func (ctrl *SqlApplyAction) Execute(c *gin.Context) (interface{}, error) {
	userName := sessions.Default(c).Get("user_name").(string)
	if len(userName) == 0 {
		return nil, common.NO_USER
	}
	dbName := c.PostForm("dbName")
	tableName := c.PostForm("tableName")
	sentence := c.PostForm("sentence")
	remark := c.PostForm("remark")

	if dbName == "" || tableName == "" || sentence == "" {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("apply sql, dbName:%v, tableName:%v, remark:%v,  applyer:%v.", dbName, tableName, remark, userName)
	err := service.NewService().ApplySql(dbName, tableName, sentence, userName, remark, time.Now().Unix())
	if err != nil {
		return nil, err
	}

	return nil, nil
}

/**
 * apply detail info
 */
type SqlApplyGetAction struct {
}

func NewSqlApplyGetAction() *SqlApplyGetAction {
	return &SqlApplyGetAction{
	}
}
func (ctrl *SqlApplyGetAction) Execute(c *gin.Context) (interface{}, error) {
	id := c.Query("id")
	if id == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	log.Debug("get sql apply %v  info", id)

	return service.NewService().GetSqlApplyInfo(id)
}

/**
 * 审批sql
 */
type SqlAuditAction struct {
}

func NewSqlAuditAction() *SqlAuditAction {
	return &SqlAuditAction{
	}
}
func (ctrl *SqlAuditAction) Execute(c *gin.Context) (interface{}, error) {
	ids := c.PostForm("ids")
	status := c.PostForm("status")
	if ids == "" || status == "" {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("audit sql apply, ids:%v, status:%v.", ids, status)
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
		return nil, fmt.Errorf("query user right failed %v", userName)
	}

	if !isAdmin {
		return nil, fmt.Errorf("only admin can audit")
	}

	err = service.NewService().AuditSql(idArray, statusI, userName)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
