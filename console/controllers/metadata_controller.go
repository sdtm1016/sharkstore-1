package controllers

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"console/common"
	"console/service"
	"util/log"
	//"strings"
	"encoding/json"
)

const (
	REQURL_META_CREATEDB = "/metadata/createDb"
	REQURL_META_GETALLDB = "/metadata/dbDataGetAll"
	REQURL_META_GETALLDBVIEW = "/metadata/dbDataGetAllViewPage"
	REQURL_META_CREATETABLE = "/metadata/createTable"
	REQURL_META_GETALLTABLE = "/metadata/dbTablesDataGetByDbName"
	REQURL_META_DELTABLE  = "/metadata/delTable"
	REQURL_META_EDITTABLE = "/metadata/editTable"
	REQURL_META_GETTABLECOLUMNS = "/metadata/getTableColumns"

	RANGE_GETRANGEBYDBTABLE = "/range/getRangeByBbTable"
)

/**
 * 创建db
 */
type CreateDbAction struct {
}
func NewCreateDbAction() *CreateDbAction {
	return &CreateDbAction{
	}
}
func (ctrl *CreateDbAction)Execute(c *gin.Context) (interface{}, error) {
	dbName := c.PostForm("dbName")
	if dbName == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	return nil, service.NewService().CreateDb(cId, dbName)
}

/**
 * 获取集群全部db
 */
type GetAllDbAction struct {
}
func NewGetAllDbAction() *GetAllDbAction {
	return &GetAllDbAction{
	}
}
func (ctrl *GetAllDbAction)Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	return service.NewService().GetAllDb(cId)
}

/**
 * 获取集群全部db,View版本
 */
type GetAllDbViewAction struct {
}
func NewGetAllDbViewAction() *GetAllDbViewAction {
	return &GetAllDbViewAction{
	}
}
func (ctrl *GetAllDbViewAction)Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.Query("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	infos, err := service.NewService().GetAllDb(cId)
	if err != nil {
		return nil, err
	}
	type GetAllDbViewResp struct {
		Page    int        `json:"page"`
		Records int            `json:"records"`
		Rows    interface{}        `json:"rows"`
		Total   int                `json:"total"`
	}
	return &GetAllDbViewResp{
		Page:    1,
		Records: len(*infos),
		Rows:    infos,
		Total:   len(*infos),
	}, nil
}

/**
 * 创建表
 */
type CreateTableAction struct {
}
func NewCreateTableAction() *CreateTableAction {
	return &CreateTableAction{}
}
func (ctrl *CreateTableAction)Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	log.Debug("clusterId:[%v]", cId)

	dbName := c.PostForm("dbName")
	if dbName == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	tableName := c.PostForm("name")
	if tableName == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	log.Debug("dbName:[%v], name:[%v]", dbName, tableName)
	isolationLabel := c.PostForm("isolationLabel")
	if isolationLabel == "" {
		log.Warn("isolation label not set when creating table=%s.", isolationLabel, tableName)
	} else {
		log.Debug("setting isolation label=%s when creating table=%s.", isolationLabel, tableName)
	}

	policy := c.PostForm("policy")
	if policy == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	rangeKeys := c.PostForm("rangeKeys")
	log.Debug("policy:[%v], rangeKeys:[%v]", policy, rangeKeys)

	columns := c.PostForm("columns")
	if columns == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	regxs := c.PostForm("regxs")
	if regxs == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	log.Debug("columns:%v, regxs:%v", columns, regxs)

	type columnJsonType struct {
		DataType int `json:"data_type"`
		Name     string  `json:"name"`
		PrimaryKey int `json:"primary_key"`
		DefaultValue string `json:"default_value"`
		Unsigned bool `json:"unsigned"`
		Index  bool `json:"index"`
		Regxs  bool `json:"regxs"`
	}
	var columnJsonArray []columnJsonType
	if err := json.Unmarshal([]byte(columns), &columnJsonArray); err != nil {
		log.Error("parse columns failed. err:[%v]", err)
		return nil, common.PARAM_FORMAT_ERROR
	}
	//log.Debug("columnJsonArray:%v", columnJsonArray)
	type regxsJsonType struct{
	}
	var regxsJsonArray []regxsJsonType
	if err := json.Unmarshal([]byte(regxs), &regxsJsonArray); err != nil {
		log.Error("parse regxs failed. err:[%v]", err)
		return nil, common.PARAM_FORMAT_ERROR
	}
	//log.Debug("regxsJsonArray:%v", regxsJsonArray)

	return nil, service.NewService().CreateTable(cId, dbName, tableName, policy, rangeKeys, isolationLabel, &columnJsonArray, &regxsJsonArray)
}

/**
 * 获取全部表
 */
type GetAllTableAction struct {
}
func NewGetAllTableAction() *GetAllTableAction {
	return &GetAllTableAction{}
}
func (ctrl *GetAllTableAction)Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.Query("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	dbName := c.Query("dbName")
	if dbName == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	dbId := c.Query("dbId")
	if dbId == "" {
		return nil, common.PARSE_PARAM_ERROR
	}

	return service.NewService().GetAllTables(cId, dbId, dbName)
}

/**
 * 删除表
 */
type DeleteTableAction struct {
}
func NewDeleteTableAction() *DeleteTableAction {
	return &DeleteTableAction{}
}
func (ctrl *DeleteTableAction)Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	dbName := c.PostForm("dbName")
	if dbName == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	tableName := c.PostForm("tableName")
	if tableName == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	flag := c.PostForm("flag")
	if flag == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	return nil, service.NewService().DeleteTable(cId, dbName, tableName, flag)
}

/**
 * 修改表
 */
type EditTableAction struct {
}
func NewEditTableAction() *EditTableAction {
	return &EditTableAction{}
}
func (ctrl *EditTableAction)Execute(c *gin.Context) (interface{}, error) {

	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	log.Debug("-----------edist clusterid:[%v]", cIdStr)
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	dbName := c.PostForm("dbName")
	if dbName == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	log.Debug("-----------edist dbname:[%v]", dbName)
	tableName := c.PostForm("name")
	if tableName == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	log.Debug("-----------edist name:[%v]", tableName)
	rangeKeys := c.PostForm("rangeKeys")
	log.Debug("-----------edist rangeKeys:[%v]", rangeKeys)
	columns := c.PostForm("columns")
	if columns == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	regxs := c.PostForm("regxs")
	if regxs == "" {
		return nil, common.PARSE_PARAM_ERROR
	}

	type columnJsonType struct {
		Id uint64 `json:"id"`
		DataType int `json:"data_type"`
		Name     string  `json:"name"`
		PrimaryKey int `json:"primary_key"`
		DefaultValue string `json:"default_value"`
		Unsigned bool `json:"unsigned"`
		Index  bool `json:"index"`
		Regxs  bool `json:"regxs"`
	}
	var columnJsonArray []columnJsonType
	if err := json.Unmarshal([]byte(columns), &columnJsonArray); err != nil {
		log.Error("parse columns failed. err:[%v]", err)
		return nil, common.PARAM_FORMAT_ERROR
	}
	log.Debug("columnJsonArray:%v", columnJsonArray)
	type regxsJsonType struct{
	}
	var regxsJsonArray []regxsJsonType
	if err := json.Unmarshal([]byte(regxs), &regxsJsonArray); err != nil {
		log.Error("parse regxs failed. err:[%v]", err)
		return nil, common.PARAM_FORMAT_ERROR
	}
	log.Debug("regxsJsonArray:%v", regxsJsonArray)

	return nil, service.NewService().EditTable(cId, dbName, tableName, rangeKeys, &columnJsonArray, &regxsJsonArray)
}

type RangeViewInfo struct {

}
func NewRangeViewInfo () *RangeViewInfo{
	return &RangeViewInfo{}
}
func (ctrl *RangeViewInfo) Execute(c *gin.Context) (interface{}, error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("RangeViewInfo execute panic: %v", string(r.([]byte)))
		}
	}()
	// db_name name clusterId
	dbName := c.Query("dbName")
	if len(dbName) == 0 {
		return nil, common.PARSE_PARAM_ERROR
	}
	tName := c.Query("tableName")
	if len(tName) == 0 {
		return nil, common.PARSE_PARAM_ERROR
	}
	clusterId, err := strconv.ParseUint(c.Query("clusterId"), 10, 64)
	if err != nil {
		return nil, common.PARSE_PARAM_ERROR
	}
	return service.NewService().GetRangeViewInfo(dbName, tName, clusterId)
}

/**
 * 获取某表详情
 */
type GetTableColumns struct {
}
func NewGetTableColumns() *GetTableColumns {
	return &GetTableColumns{}
}
func (ctrl *GetTableColumns)Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	dbName := c.PostForm("dbName")
	if dbName == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	tableName := c.PostForm("name")
	if tableName == "" {
		return nil, common.PARSE_PARAM_ERROR
	}

	return service.NewService().GetTableColumns(cId, tableName, dbName)
}