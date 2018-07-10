/**
 * 数据模型的存储逻辑
 */
package service

import (
	"database/sql"
	"fmt"
	"net/http"
	"time"
	"strings"
	"io/ioutil"
	"encoding/json"
	"bytes"
)
import (
	_ "github.com/go-sql-driver/mysql"
)
import (
	"console/models"
	"console/common"
	"console/config"
	"util/log"
	"util/ttlcache"
	"strconv"
	"errors"
	"sync"
)

const (
	//DB_NAME = "fbase_mock_console"
	DB_NAME     = "fbase"
	LOCK_DBNAME = "lock"
	LOCK_COLUMN = "lock_col"

	TABLE_NAME_USER      = "fbase_user"
	TABLE_NAME_CLUSTER   = "fbase_cluster"
	TABLE_NAME_ROLE      = "fbase_role"
	TABLE_NAME_PRIVILEGE = "fbase_privilege"
	TABLE_NAME_LOCK_NSP  = "fbase_lock_nsp"
	TABLE_NAME_METRIC_SERVER   = "metric_server"
)

var serviceInstance *Service = nil

type Service struct {
	config     *config.Config
	db         *sql.DB
	adminCache *ttlcache.TTLCache
}

func NewService() *Service {
	if serviceInstance == nil {
		log.Error("Firstly invoke initService before get service instance.")
		return nil
	}

	return serviceInstance
}

func (s *Service) GetDb() *sql.DB {
	return s.db
}

func (s *Service) GetUserInfoByErp(erp string) (*models.UserInfo, error) {
	rows, err := s.db.Query(fmt.Sprintf(`SELECT * FROM %s WHERE erp="%s"`, TABLE_NAME_USER, erp))
	if err != nil {
		log.Error("db select is failed. err:[%v]", err)
		return nil, common.DB_ERROR
	}
	if !rows.Next() {
		return nil, nil
	}

	info := models.NewUserInfo()
	if err := rows.Scan(&(info.Id), &(info.Erp), &(info.Mail), &(info.Tel), &(info.UserName), &(info.RealName),
		&(info.SuperiorName), &(info.Department1), &(info.Department2), &(info.OrganizationName),
		&(info.CreateTime), &(info.ModifyDate)); err != nil {
		log.Error("db scan is failed. err:[%v]", err)
		return nil, common.DB_ERROR
	}

	return info, nil
}

func (s *Service) GetClusterById(ids ...int64) ([]*models.ClusterInfo, error) {
	result := make([]*models.ClusterInfo, 0, 10) // TODO: 分页
	for _, id := range ids {
		rows, err := s.db.Query(fmt.Sprintf(`SELECT id, cluster_name, cluster_url, gateway_http, gateway_sql, cluster_sign,
		auto_transfer, auto_failover, auto_split, create_time FROM %s where id=%d`, TABLE_NAME_CLUSTER, id))
		if err != nil {
			log.Error("db select is failed. err:[%v]", err)
			return nil, common.DB_ERROR
		}

		for rows.Next() {
			info := models.NewClusterInfo()
			if err := rows.Scan(&(info.Id), &(info.Name), &(info.MasterUrl), &(info.GatewayHttpUrl), &(info.GatewaySqlUrl),
				&(info.ClusterToken), &(info.AutoTransferUnable), &(info.AutoFailoverUnable), &(info.AutoSplitUnable), &(info.CreateTime)); err != nil {
				log.Error("db scan is failed. err:[%v]", err)
				return nil, common.DB_ERROR
			}
			log.Debug("selected cluster:%v", info)
			result = append(result, info)
		}
	}
	return result, nil
}

func (s *Service) GetAllClusters() ([]*models.ClusterInfo, error) {
	rows, err := s.db.Query(fmt.Sprintf(`SELECT id, cluster_name, cluster_url, gateway_http, gateway_sql, cluster_sign,
		auto_transfer, auto_failover, auto_split, create_time FROM %s`, TABLE_NAME_CLUSTER))
	if err != nil {
		log.Error("db select is failed. err:[%v]", err)
		return nil, common.DB_ERROR
	}

	result := make([]*models.ClusterInfo, 0, 10) // TODO: 分页
	for rows.Next() {
		info := models.NewClusterInfo()
		if err := rows.Scan(&(info.Id), &(info.Name), &(info.MasterUrl), &(info.GatewayHttpUrl), &(info.GatewaySqlUrl),
			&(info.ClusterToken), &(info.AutoTransferUnable), &(info.AutoFailoverUnable), &(info.AutoSplitUnable), &(info.CreateTime)); err != nil {
			log.Error("db scan is failed. err:[%v]", err)
			return nil, common.DB_ERROR
		}
		log.Debug("selected cluster:%v", info)
		result = append(result, info)
	}

	return result, nil
}

func (s *Service) CreateCluster(cId int, cName, masterUrl, gateHttpUrl, gateSqlUrl, cToken string, cTime int64) error {
	result, err := s.db.Exec(fmt.Sprintf(`INSERT INTO %s (id, cluster_name, cluster_url, gateway_http, gateway_sql, cluster_sign,
		auto_failover, auto_transfer, auto_split, create_time) values (%d, "%s", "%s", "%s", "%s", "%s", 0, 0, 0, %d)`, TABLE_NAME_CLUSTER, cId, cName, masterUrl,
		gateHttpUrl, gateSqlUrl, cToken, cTime))
	if err != nil {
		log.Error("db exec is failed. err:[%v]", err)
		return common.DB_ERROR
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Error("db rowsaffected is failed. err:[%v]", err)
		return common.DB_ERROR
	}
	if rowsAffected != 1 {
		return common.CLUSTER_DUPCREATE_ERROR
	}

	return nil
}

func (s *Service) CreateDb(cId int, dbName string) error {
	info, err := s.selectClusterById(cId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(info.Id, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["name"] = dbName

	var createDbResp = struct {
		Code int         `json:"code"`
		Msg  string      `json:"message"`
		Data interface{} `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/database/create", reqParams, &createDbResp); err != nil {
		return err
	}
	if createDbResp.Code != 0 {
		log.Error("master createdb is failed. err:[%v]", createDbResp)
		return common.INTERNAL_ERROR
	}

	return nil
}

func (s *Service) GetAllDb(cId int) (*[]models.DbInfo, error) {
	info, err := s.selectClusterById(cId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(info.Id, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign

	var getAllDbResp = struct {
		Code int             `json:"code"`
		Msg  string          `json:"message"`
		Data []models.DbInfo `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/database/getall", reqParams, &getAllDbResp); err != nil {
		return nil, err
	}
	if getAllDbResp.Code != 0 {
		log.Error("master getalldb is failed. err:[%v]", getAllDbResp)
		return nil, common.INTERNAL_ERROR
	}
	log.Debug("getalldb:%v", getAllDbResp)

	return &(getAllDbResp.Data), nil
}

func (s *Service) CreateTable(cId int, dbName, tableName, policy, rangeKeys, isolationLabel string, columnJsonArray, regxsJsonArray interface{}) error {
	info, err := s.selectClusterById(cId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(info.Id, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["rangeKeys"] = rangeKeys
	reqParams["tableName"] = tableName
	reqParams["policy"] = policy
	reqParams["isolationLabel"] = isolationLabel
	var propJson = struct {
		Columns interface{} `json:"columns"`
		Regxs   interface{} `json:"regxs"`
	}{}
	propJson.Columns = columnJsonArray
	propJson.Regxs = regxsJsonArray
	p, _ := json.Marshal(propJson)
	r, _ := json.Marshal(string(p))

	//log.Debug("string(r):%v", string(r))
	r1 := strings.TrimPrefix(string(r), "\"")
	//log.Debug("trim prefix string(r):%v", r1)
	r2 := strings.TrimSuffix(r1, "\"")
	//log.Debug("trim suffix string(r):%v", r2)
	r3 := strings.Replace(r2, "\\", "", -1)
	reqParams["properties"] = r3

	var createTableResp = struct {
		Code int         `json:"code"`
		Msg  string      `json:"message"`
		Data interface{} `json:"data"`
	}{}
	if err := sendPostReqStrBody(info.MasterUrl, "/manage/table/create", reqParams, &createTableResp); err != nil {
		return err
	}
	if createTableResp.Code != 0 {
		log.Error("master createTable is failed. err:[%v]", createTableResp)
		return common.INTERNAL_ERROR
	}

	return nil
}

func (s *Service) GetAllTables(cId int, dbId, dbName string) (*[]models.TableInfo, error) {
	info, err := s.selectClusterById(cId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(info.Id, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName

	var getAllTables = struct {
		Code int                `json:"code"`
		Msg  string             `json:"message"`
		Data []models.TableInfo `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/table/getall", reqParams, &getAllTables); err != nil {
		return nil, err
	}
	if getAllTables.Code != 0 {
		log.Error("master getalltable is failed. err:[%v]", getAllTables)
		return nil, common.INTERNAL_ERROR
	}

	return &getAllTables.Data, nil
}

func (s *Service) DeleteTable(cId int, dbName, tableName, flag string) error {
	info, err := s.selectClusterById(cId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(info.Id, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["tableName"] = tableName

	var url string
	if "true" == flag {
		reqParams["fast"] = flag
		url = "/manage/table/delete/fast"
	} else {
		url = "/manage/table/delete"
	}
	var deleteTableResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, url, reqParams, &deleteTableResp); err != nil {
		return err
	}
	if deleteTableResp.Code != 0 {
		log.Error("master deletetable is failed. err:[%v]", err)
		return common.INTERNAL_ERROR
	}
	return nil
}

func (s *Service) EditTable(cId int, dbName, tableName, rangeKeys string, columnJsonArray, regxsJsonArray interface{}) error {
	info, err := s.selectClusterById(cId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(info.Id, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["rangeKeys"] = rangeKeys
	reqParams["tableName"] = tableName
	var propJson = struct {
		Columns interface{} `json:"columns"`
		Regxs   interface{} `json:"regxs"`
	}{}
	propJson.Columns = columnJsonArray
	propJson.Regxs = regxsJsonArray
	p, _ := json.Marshal(propJson)
	r, _ := json.Marshal(string(p))

	//log.Debug("string(r):%v", string(r))
	r1 := strings.TrimPrefix(string(r), "\"")
	//log.Debug("trim prefix string(r):%v", r1)
	r2 := strings.TrimSuffix(r1, "\"")
	//log.Debug("trim suffix string(r):%v", r2)
	r3 := strings.Replace(r2, "\\", "", -1)
	reqParams["properties"] = r3

	var editTableResp = struct {
		Code int         `json:"code"`
		Msg  string      `json:"message"`
		Data interface{} `json:"data"`
	}{}
	if err := sendPostReqStrBody(info.MasterUrl, "/manage/table/edit", reqParams, &editTableResp); err != nil {
		return err
	}
	if editTableResp.Code != 0 {
		log.Error("master editTable is failed. err:[%v]", editTableResp)
		return common.INTERNAL_ERROR
	}

	return nil
}

func (s *Service) EditTablePolicy(cId int, dbName, tableName string, readPolicy int32) error {
	info, err := s.selectClusterById(cId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(info.Id, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["tableName"] = tableName
	reqParams["readPolicy"] = readPolicy

	var editTablePolicyResp = struct {
		Code int         `json:"code"`
		Msg  string      `json:"message"`
		Data interface{} `json:"data"`
	}{}
	if err := sendPostReqStrBody(info.MasterUrl, "/manage/table/editPolicy", reqParams, &editTablePolicyResp); err != nil {
		return err
	}
	if editTablePolicyResp.Code != 0 {
		log.Error("master editTablePolicy is failed. err:[%v]", editTablePolicyResp)
		return common.INTERNAL_ERROR
	}

	return nil
}

func (s *Service) getTableRanges(dbName, tName string, clusterId uint64) ([]*models.Route, error) {
	log.Debug("//getTableRanges")
	info, err := s.selectClusterById(int(clusterId))
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}
	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(info.Id, info.ClusterToken, ts)
	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["tableName"] = tName

	var resp = struct {
		Code int             `json:"code"`
		Msg  string          `json:"message"`
		Data []*models.Route `json:"data"`
	}{}
	log.Debug("getTableRanges sendGetReq /manage/table/route/get")
	if err := sendGetReq(info.MasterUrl, "/manage/table/route/get", reqParams, &resp); err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		log.Error("master getalltable is failed. err:[%v]", resp)
		return nil, common.INTERNAL_ERROR
	}

	log.Debug("getTableRanges//")
	return resp.Data, nil
}

func (s *Service) GetRangeViewInfo(dbName, tName string, clusterId uint64) ([]*models.Route, error) {
	return s.getTableRanges(dbName, tName, clusterId)
}

func (s *Service) GetTableColumns(cId int, tableName, dbName string) (*models.TableInfo, error) {
	allTables, err := s.GetAllTables(cId, "", dbName)
	if err != nil {
		return nil, err
	}
	var table *models.TableInfo = nil
	for _, t := range *allTables {
		if tableName == t.TableName {
			table = &t
			break
		}
	}
	if table == nil {
		return nil, common.TABLE_NOT_EXISTS
	}

	return table, nil
}

func (s *Service) GetMasterAll(cId int, token string) (*models.Member, error) {
	info, err := s.selectClusterById(cId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}
	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(cId, token, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign

	var masterNodesResp = struct {
		Code int            `json:"code"`
		Msg  string         `json:"message"`
		Data *models.Member `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "manage/master/getall", reqParams, &masterNodesResp); err != nil {
		return nil, err
	}
	if masterNodesResp.Code != 0 {
		log.Error("get cluster all node is failed. err:[%v]", masterNodesResp)
		return nil, fmt.Errorf(masterNodesResp.Msg)
	}
	return masterNodesResp.Data, nil
}

func (s *Service) GetMasterLeader(cId int, token string) (interface{}, error) {
	info, err := s.selectClusterById(cId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}
	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(cId, token, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign

	var masterLeaderResp = struct {
		Code int         `json:"code"`
		Msg  string      `json:"message"`
		Data interface{} `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "manage/master/getleader", reqParams, &masterLeaderResp); err != nil {
		return nil, err
	}
	if masterLeaderResp.Code != 0 {
		log.Error("get cluster leader is failed. err:[%v]", masterLeaderResp)
		return nil, fmt.Errorf(masterLeaderResp.Msg)
	}
	return masterLeaderResp.Data, nil
}

func (s *Service) InitCluster(cId int, masterUrl string, isolationLabel string, token string) error {
	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(cId, token, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["isolationLabel"] = isolationLabel

	var initClusterResp = struct {
		Code int         `json:"code"`
		Msg  string      `json:"message"`
		Data interface{} `json:"data"`
	}{}
	if err := sendGetReq(masterUrl, "/manage/cluster/init", reqParams, &initClusterResp); err != nil {
		return err
	}
	if initClusterResp.Code != 0 {
		log.Error("init cluster is failed. err:[%v]", initClusterResp)
		return fmt.Errorf(initClusterResp.Msg)
	}
	return nil
}

func (s *Service) GetNodeViewInfo(cId int) ([]*models.DsNode, error) {
	info, err := s.selectClusterById(cId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(cId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign

	var nodeInfoResp = struct {
		Code int              `json:"code"`
		Msg  string           `json:"message"`
		Data []*models.DsNode `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/node/getall", reqParams, &nodeInfoResp); err != nil {
		return nil, err
	}
	if nodeInfoResp.Code != 0 {
		log.Error("get node info failed. err:[%v]", nodeInfoResp)
		return nil, fmt.Errorf(nodeInfoResp.Msg)
	}
	return nodeInfoResp.Data, nil
}

func (s *Service) SetNodeLogOut(clusterId, nodeId int) error {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["nodeId"] = nodeId

	var nodeLogoutResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/node/logout", reqParams, &nodeLogoutResp); err != nil {
		return err
	}
	if nodeLogoutResp.Code != 0 {
		log.Error("set node logout failed. err:[%v]", nodeLogoutResp)
		return fmt.Errorf(nodeLogoutResp.Msg)
	}
	return nil
}

func (s *Service) SetNodeUpgrade(clusterId, nodeId int) error {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["nodeId"] = nodeId

	var nodeUpgradeResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/node/upgrade", reqParams, &nodeUpgradeResp); err != nil {
		return err
	}
	if nodeUpgradeResp.Code != 0 {
		log.Error("node upgrade failed. err:[%v]", nodeUpgradeResp)
		return fmt.Errorf(nodeUpgradeResp.Msg)
	}
	return nil
}

func (s *Service) SetNodeLogIn(clusterId, nodeId int) (error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["nodeId"] = nodeId
	reqParams["force"] = 1

	var nodeLoginResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/node/login", reqParams, &nodeLoginResp); err != nil {
		return err
	}
	if nodeLoginResp.Code != 0 {
		log.Error("set node login failed. err:[%v]", nodeLoginResp)
		return fmt.Errorf(nodeLoginResp.Msg)
	}
	return nil
}

func (s *Service) SetNodeLogLevel(clusterId, nodeId int, logLevel string) (error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["nodeId"] = nodeId
	reqParams["logLevel"] = logLevel

	var nodeLogLevelResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/node/setLogLevel", reqParams, &nodeLogLevelResp); err != nil {
		return err
	}
	if nodeLogLevelResp.Code != 0 {
		log.Error("set node log level failed. err:[%v]", nodeLogLevelResp)
		return fmt.Errorf(nodeLogLevelResp.Msg)
	}
	return nil
}

func (s *Service) TaskOperate(clusterId int, operate string, taskIds string) (interface{}, error) {
	if s == nil {
		return nil, errors.New("service is nil")
	}
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["taskIds"] = taskIds // json []

	var resp = struct {
		Code int         `json:"code"`
		Msg  string      `json:"message"`
		Data interface{} `json:"data"`
	}{}
	// only delete operate
	if err := sendGetReq(info.MasterUrl, "/manage/task/delete", reqParams, &resp); err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		log.Error("task get all failed. err:[%v]", resp)
		return nil, fmt.Errorf(resp.Msg)
	}
	return resp.Data, nil
}

func (s *Service) GetPresentTask(clusterId int) (interface{}, error) {
	if s == nil {
		return nil, errors.New("service is nil")
	}
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign

	var resp = struct {
		Code int         `json:"code"`
		Msg  string      `json:"message"`
		Data interface{} `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/task/getall", reqParams, &resp); err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		log.Error("task get all failed. err:[%v]", resp)
		return nil, fmt.Errorf(resp.Msg)
	}
	return resp.Data, nil
}

func (s *Service) DeletePeer(clusterId int, rangeId, peerId string) (interface{}, error) {
	if s == nil {
		return nil, errors.New("service is nil")
	}
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["rangeId"] = rangeId
	reqParams["peerId"] = peerId

	var peerDeleteResp = struct {
		Code int         `json:"code"`
		Msg  string      `json:"message"`
		Data interface{} `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/range/del/peer", reqParams, &peerDeleteResp); err != nil {
		return nil, err
	}
	if peerDeleteResp.Code != 0 {
		log.Error("delete node failed. err:[%v]", peerDeleteResp)
		return nil, fmt.Errorf(peerDeleteResp.Msg)
	}
	return peerDeleteResp.Data, nil
}

func (s *Service) AddPeer(clusterId int, rangeId string) error {
	if s == nil {
		return errors.New("service is nil")
	}
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["rangeId"] = rangeId

	var peerAddResp = struct {
		Code int         `json:"code"`
		Msg  string      `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/range/add/peer", reqParams, &peerAddResp); err != nil {
		return err
	}
	if peerAddResp.Code != 0 {
		log.Error("add peer failed. err:[%v]", peerAddResp)
		return fmt.Errorf(peerAddResp.Msg)
	}
	return nil
}

func (s *Service) DeleteNodes(clusterId int, nodeIds string) error {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["nodeIds"] = nodeIds

	var nodeDeleteResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/node/delete", reqParams, &nodeDeleteResp); err != nil {
		return err
	}
	if nodeDeleteResp.Code != 0 {
		log.Error("delete node failed. err:[%v]", nodeDeleteResp)
		return fmt.Errorf(nodeDeleteResp.Msg)
	}
	return nil
}

func (s *Service) GetRangeTopoByNodeId(clusterId, nodeId int) (interface{}, error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["nodeId"] = nodeId

	var getRangeTopoOfNodeResp = struct {
		Code int             `json:"code"`
		Msg  string          `json:"message"`
		Data []*models.Route `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/node/getRangeTopo", reqParams, &getRangeTopoOfNodeResp); err != nil {
		return nil, err
	}
	if getRangeTopoOfNodeResp.Code != 0 {
		log.Error("getting range topology of node[nodeId=%d] failed. err:[%v]", nodeId, getRangeTopoOfNodeResp)
		return nil, fmt.Errorf(getRangeTopoOfNodeResp.Msg)
	}
	return getRangeTopoOfNodeResp.Data, nil
}

func (s *Service) UpdateNodeIsolationLabel(clusterId, nodeId int, isolationLabel string) error {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["nodeId"] = nodeId
	reqParams["isolationLabel"] = isolationLabel

	var updateNodeIsolationLabelResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/node/updateIsolationLabel", reqParams, &updateNodeIsolationLabelResp); err != nil {
		return err
	}
	if updateNodeIsolationLabelResp.Code != 0 {
		log.Error("update isolation label of node failed. err:[%v]", updateNodeIsolationLabelResp)
		return fmt.Errorf(updateNodeIsolationLabelResp.Msg)
	}

	return nil
}

func (s *Service) SetClusterToggle(clusterId int, autoTransfer, autoFailover, autoSplit string) error {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	log.Debug("clusterId:%v, token:%v, ts:%v", clusterId, info.ClusterToken, ts)
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["autoTransferUnable"] = autoTransfer
	reqParams["autoFailoverUnable"] = autoFailover
	reqParams["autoSplitUnable"] = autoSplit

	var clusterToggleSetResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/setAutoScheduleInfo", reqParams, &clusterToggleSetResp); err != nil {
		return err
	}
	if clusterToggleSetResp.Code != 0 {
		log.Error("delete node failed. err:[%v]", clusterToggleSetResp)
		return fmt.Errorf(clusterToggleSetResp.Msg)
	} else {
		//改库
		info.AutoFailoverUnable, _ = strconv.ParseBool(autoFailover)
		info.AutoTransferUnable, _ = strconv.ParseBool(autoTransfer)
		info.AutoSplitUnable, _ =  strconv.ParseBool(autoSplit)
		log.Debug("start to update database, %v", info)
		if err := s.insertClusterById(info); err != nil {
			return fmt.Errorf("更新集群开关失败")
		}
	}
	return nil
}

func (s *Service) GetSchedulerAll(clusterId int) (map[string]bool, error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	log.Debug("get cluster scheduler, clusterId:%v", clusterId)
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign

	var getScheduleResp = struct {
		Code int             `json:"code"`
		Msg  string          `json:"message"`
		Data map[string]bool `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/scheduler/getall", reqParams, &getScheduleResp); err != nil {
		return nil, err
	}
	if getScheduleResp.Code != 0 {
		log.Error("get cluster[%d] scheduler failed. err:[%v]", clusterId, getScheduleResp)
		return nil, fmt.Errorf(getScheduleResp.Msg)
	}
	return getScheduleResp.Data, nil
}

func (s *Service) GetSchedulerDetail(clusterId int, name string) (interface{}, error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	log.Debug("get cluster scheduler detail, clusterId:%d, scheduler name:%s", clusterId, name)
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["name"] = name

	var scheduleDetailResp = struct {
		Code int             `json:"code"`
		Msg  string          `json:"message"`
		Data interface{} `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/scheduler/detail", reqParams, &scheduleDetailResp); err != nil {
		return nil, err
	}
	if scheduleDetailResp.Code != 0 {
		log.Error("get cluster[%d] scheduler %s detail failed. err:[%v]", clusterId, name, scheduleDetailResp)
		return nil, fmt.Errorf(scheduleDetailResp.Msg)
	}
	return scheduleDetailResp.Data, nil
}

func (s *Service) AdjustScheduler(clusterId, optType int, scheduler string) (error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	log.Debug("get cluster scheduler, clusterId:%v", clusterId)
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	//optType: 1,add; 2,remove

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["name"] = scheduler

	var url string
	switch optType {
	case 1:
		url = "/manage/scheduler/add"
	case 2:
		url = "/manage/scheduler/remove"
	}

	var adjustScheduleResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, url, reqParams, &adjustScheduleResp); err != nil {
		return err
	}
	if adjustScheduleResp.Code != 0 {
		log.Error("adjust cluster[%d] schedule, optType:[%s],scheduleName:[%s] err:[%v]", clusterId, optType, scheduler, adjustScheduleResp)
		return fmt.Errorf(adjustScheduleResp.Msg)
	}
	return nil
}

func (s *Service) CheckTopology(clusterId int, dbName, tableName string) (error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	log.Debug("check cluster[%v] dbName[%v] tableName[%v] topology", clusterId, dbName, tableName)
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["tableName"] = tableName

	var checkTopologyResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/debug/table/topology/check", reqParams, &checkTopologyResp); err != nil {
		return err
	}
	if checkTopologyResp.Code != 0 {
		log.Error("check cluster[%d] topology , dbName:[%s],tableName:[%s] err:[%v]", clusterId, dbName, tableName, checkTopologyResp)
		return fmt.Errorf(checkTopologyResp.Msg)
	}
	return nil
}

func (s *Service) GetTableTopologyMissing(clusterId int, dbName, tableName string) (interface{}, error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	log.Debug("get cluster[%v] dbName[%v] tableName[%v] topology missing list", clusterId, dbName, tableName)
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["tableName"] = tableName

	var topologyMResp = struct {
		Code int         `json:"code"`
		Msg  string      `json:"message"`
		Data interface{} `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/table/topology/missing", reqParams, &topologyMResp); err != nil {
		return nil, err
	}
	if topologyMResp.Code != 0 {
		log.Error("get cluster[%d] topology , dbName:[%s],tableName:[%s] missing list err:[%v]", clusterId, dbName, tableName, topologyMResp)
		return nil, fmt.Errorf(topologyMResp.Msg)
	}
	return topologyMResp.Data, nil
}

func (s *Service) CreateTopologyRange(clusterId int, dbName, tableName, startKey, endKey string) error {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	log.Debug("create cluster[%v] dbName[%v] tableName[%v] range scope [%s-%s]topology range", clusterId, dbName, tableName, startKey, endKey)
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["tableName"] = tableName
	reqParams["startKey"] = startKey
	reqParams["endKey"] = endKey

	var topologyCResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/table/topology/create", reqParams, &topologyCResp); err != nil {
		return err
	}
	if topologyCResp.Code != 0 {
		log.Error("create cluster[%d] topology missing range failed, param: dbName:[%s],tableName:[%s], err:[%v]", clusterId, dbName, tableName, topologyCResp)
		return fmt.Errorf(topologyCResp.Msg)
	}
	return nil
}

func (s *Service) BatchCreateTopologyRange(clusterId int, dbName, tableName string) error {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	log.Debug("batch create cluster[%v] dbName[%v] tableName[%v] range:", clusterId, dbName, tableName)
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["tableName"] = tableName

	var topologyCResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/table/topology/batchCreate", reqParams, &topologyCResp); err != nil {
		return err
	}
	if topologyCResp.Code != 0 {
		log.Error("batch create cluster[%d] topology missing range failed, param: dbName:[%s],tableName:[%s], err:[%v]", clusterId, dbName, tableName, topologyCResp)
		return fmt.Errorf(topologyCResp.Msg)
	}
	return nil
}

func (s *Service) GetRangeDuplicate(clusterId int, dbName, tableName string) (interface{}, error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	log.Debug("get cluster[%v] dbName[%v] tableName[%v] range duplicate list", clusterId, dbName, tableName)
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["tableName"] = tableName

	var getDuplicateResp = struct {
		Code int         `json:"code"`
		Msg  string      `json:"message"`
		Data interface{} `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/table/range/duplicate", reqParams, &getDuplicateResp); err != nil {
		return nil, err
	}
	if getDuplicateResp.Code != 0 {
		log.Error("get cluster[%d] topology , dbName:[%s],tableName:[%s] range duplicate list err:[%v]", clusterId, dbName, tableName, getDuplicateResp)
		return nil, fmt.Errorf(getDuplicateResp.Msg)
	}
	return getDuplicateResp.Data, nil
}

func (s *Service) GetClusterTopology(clusterId int) (interface{}, error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	log.Debug("get cluster[%v] topology list", clusterId)
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign

	var getTopologyResp = struct {
		Code int         `json:"code"`
		Msg  string      `json:"message"`
		Data interface{} `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/topology/query", reqParams, &getTopologyResp); err != nil {
		return nil, err
	}
	if getTopologyResp.Code != 0 {
		log.Error("get cluster[%d] topology list failed, err:[%v]", clusterId, getTopologyResp)
		return nil, fmt.Errorf(getTopologyResp.Msg)
	}
	return getTopologyResp.Data, nil
}

func (s *Service) GetTaskType(clusterId int) ([]string, error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	log.Debug("get cluster task type, clusterId:%v", clusterId)
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign

	var getTaskTypeResp = struct {
		Code int      `json:"code"`
		Msg  string   `json:"message"`
		Data []string `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/task/getTypeAll", reqParams, &getTaskTypeResp); err != nil {
		return nil, err
	}
	if getTaskTypeResp.Code != 0 {
		log.Error("get cluster[%d] task type failed. err:[%v]", clusterId, getTaskTypeResp)
		return nil, fmt.Errorf(getTaskTypeResp.Msg)
	}
	return getTaskTypeResp.Data, nil
}

func (s *Service) QueryDb(clusterId int, paramMap map[string]string) (interface{}, error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	rs, err := s.queryStoreDataBySql(info.GatewaySqlUrl, paramMap)
	if err != nil {
		log.Error("db exec is failed. err:[%v]", err)
		return nil, err
	}

	return rs, nil
}

func (s *Service) OperateDb(clusterId int, paramMap map[string]string) (int64, error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return 0, err
	}
	if info == nil {
		return 0, common.CLUSTER_NOTEXISTS_ERROR
	}

	rowsAffected, err := s.operateStoreDataBySql(info.GatewaySqlUrl, paramMap)
	if err != nil {
		log.Error("db exec is failed. err:[%v]", err)
		return 0, err
	}
	return rowsAffected, nil
}

func (s *Service) GetUnhealthyRanges(clusterId int, dbName, tableName string, rangeId string) (interface{}, error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["tableName"] = tableName
	reqParams["rangeId"] = rangeId

	var getAbnormalRangesResp = struct {
		Code int                  `json:"code"`
		Msg  string               `json:"message"`
		Data []*models.RangeBrief `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/range/unhealthy/query", reqParams, &getAbnormalRangesResp); err != nil {
		return nil, err
	}
	if getAbnormalRangesResp.Code != 0 {
		log.Error("get cluster[%d] db[%s] table[%s] unhealthy ranges failed. err:[%v]", clusterId, dbName, tableName, getAbnormalRangesResp)
		return nil, fmt.Errorf(getAbnormalRangesResp.Msg)
	}
	return getAbnormalRangesResp.Data, nil
}

func (s *Service) GetUnstableRanges(clusterId int, dbName, tableName string) (interface{}, error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["tableName"] = tableName

	var getUnstableRangesResp = struct {
		Code int                  `json:"code"`
		Msg  string               `json:"message"`
		Data []*models.RangeBrief `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/range/unstable/query", reqParams, &getUnstableRangesResp); err != nil {
		return nil, err
	}
	if getUnstableRangesResp.Code != 0 {
		log.Error("get cluster[%d] db[%s] table[%s] unstable ranges failed. err:[%v]", clusterId, dbName, tableName, getUnstableRangesResp)
		return nil, fmt.Errorf(getUnstableRangesResp.Msg)
	}
	return getUnstableRangesResp.Data, nil
}


func (s *Service) GetPeerInfo(clusterId int, dbName, tableName string, rangeId int) (interface{}, error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["tableName"] = tableName
	reqParams["rangeId"] = rangeId

	var getPeerInfoResp = struct {
		Code int                `json:"code"`
		Msg  string             `json:"message"`
		Data []models.PeerBrief `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/range/getPeerInfo", reqParams, &getPeerInfoResp); err != nil {
		return nil, err
	}
	if getPeerInfoResp.Code != 0 {
		log.Error("get cluster[%d] db[%s] table[%s] rangeId[%s] peer info failed. err:[%v]", clusterId, dbName, tableName, rangeId, getPeerInfoResp)
		return nil, fmt.Errorf(getPeerInfoResp.Msg)
	}
	return getPeerInfoResp.Data, nil
}

func (s *Service) UpdateRange(clusterId int, dbName, tableName string, rangeId, peerId int) error {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["tableName"] = tableName
	reqParams["rangeId"] = rangeId
	reqParams["peerId"] = peerId

	var updateRangeResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/range/updateRange", reqParams, &updateRangeResp); err != nil {
		return err
	}
	if updateRangeResp.Code != 0 {
		log.Error("update cluster[%d] db[%s] table[%s] rangeId[%s] meta info failed. err:[%v]", clusterId, dbName, tableName, rangeId, updateRangeResp)
		return fmt.Errorf(updateRangeResp.Msg)
	}
	return nil
}

func (s *Service) OfflineRange(clusterId int, dbName, tableName string, rangeId, peerId int) error {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["tableName"] = tableName
	reqParams["rangeId"] = rangeId
	reqParams["peerId"] = peerId

	var offlineRangeResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/range/offlineRange", reqParams, &offlineRangeResp); err != nil {
		return err
	}
	if offlineRangeResp.Code != 0 {
		log.Error("cluster[%d] db[%s] table[%s] rangeId[%s] offline range failed. err:[%v]", clusterId, dbName, tableName, rangeId, offlineRangeResp)
		return fmt.Errorf(offlineRangeResp.Msg)
	}
	return nil
}

func (s *Service) RebuildRange(clusterId int, dbName, tableName string, rangeId int) error {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["tableName"] = tableName
	reqParams["rangeId"] = rangeId
	reqParams["peerId"] = 0

	var rebuildRangeResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/range/rebuildRange", reqParams, &rebuildRangeResp); err != nil {
		return err
	}
	if rebuildRangeResp.Code != 0 {
		log.Error("cluster[%d] db[%s] table[%s] rangeId[%s] rebuild range failed. err:[%v]", clusterId, dbName, tableName, rangeId, rebuildRangeResp)
		return fmt.Errorf(rebuildRangeResp.Msg)
	}
	return nil
}

func (s *Service) ReplaceRange(clusterId int, dbName, tableName string, rangeId, peerId int) error {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["tableName"] = tableName
	reqParams["rangeId"] = rangeId
	reqParams["peerId"] = peerId

	var replaceRangeResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/range/replaceRange", reqParams, &replaceRangeResp); err != nil {
		return err
	}
	if replaceRangeResp.Code != 0 {
		log.Error("replace cluster[%d] db[%s] table[%s] rangeId[%s] meta info failed. err:[%v]", clusterId, dbName, tableName, rangeId, replaceRangeResp)
		return fmt.Errorf(replaceRangeResp.Msg)
	}
	return nil
}

func (s *Service) DeleteRange(clusterId int, rangeId int) error {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["rangeId"] = rangeId

	var deleteRangeResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/range/delete", reqParams, &deleteRangeResp); err != nil {
		return err
	}
	if deleteRangeResp.Code != 0 {
		log.Error("delete cluster[%d] rangeId[%s] failed. err:[%v]", clusterId, rangeId, deleteRangeResp)
		return fmt.Errorf(deleteRangeResp.Msg)
	}
	return nil
}

func (s *Service) GetRangeTopoByRangeId(clusterId int, rangeId int) (interface{}, error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["rangeId"] = rangeId

	var getRangeTopoResp = struct {
		Code int           `json:"code"`
		Msg  string        `json:"message"`
		Data *models.Route `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/range/getRangeTopo", reqParams, &getRangeTopoResp); err != nil {
		return nil, err
	}
	if getRangeTopoResp.Code != 0 {
		log.Error("get range topology of cluster[%d] rangeId[%s] failed. err:[%v]", clusterId, rangeId, getRangeTopoResp)
		return nil, fmt.Errorf(getRangeTopoResp.Msg)
	}
	return getRangeTopoResp.Data, nil
}

func (s *Service) BatchRecoverRange(clusterId int, dbName, tableName string) error {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["tableName"] = tableName

	var recoverRangeResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/range/unhealthy/recover", reqParams, &recoverRangeResp); err != nil {
		return err
	}
	if recoverRangeResp.Code != 0 {
		log.Error("batch recover  cluster[%d] ranges failed. err:[%v]", clusterId, recoverRangeResp)
		return fmt.Errorf(recoverRangeResp.Msg)
	}
	return nil
}

//迁移
func (s *Service) TransferRange(clusterId int, rangeId int, peerId int) error {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["rangeId"] = rangeId
	reqParams["peerId"] = peerId

	var transferRangeResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/range/transfer", reqParams, &transferRangeResp); err != nil {
		return err
	}
	if transferRangeResp.Code != 0 {
		log.Error("transfer range[%s] peer[%v] of cluster %v failed. err:[%v]", rangeId, peerId, clusterId, transferRangeResp)
		return fmt.Errorf(transferRangeResp.Msg)
	}
	return nil
}

//切换主
func (s *Service) ChangeRangeLeader(clusterId int, rangeId int, peerId int) error {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["rangeId"] = rangeId
	reqParams["peerId"] = peerId

	var changeLeaderResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/range/leader/change", reqParams, &changeLeaderResp); err != nil {
		return err
	}
	if changeLeaderResp.Code != 0 {
		log.Error("change range[%s] leader of cluster %v to %v failed. err:[%v]", rangeId, clusterId, peerId, changeLeaderResp)
		return fmt.Errorf(changeLeaderResp.Msg)
	}
	return nil
}

func (s *Service) GetRangeOpsTopN(clusterId int, topN int) (interface{}, error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["topN"] = topN

	var getTopNResp = struct {
		Code int         `json:"code"`
		Msg  string      `json:"message"`
		Data interface{} `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/range/getOpsTopN", reqParams, &getTopNResp); err != nil {
		return nil, err
	}
	if getTopNResp.Code != 0 {
		log.Error("get cluster %d range ops topN %v failed. err:[%v]", clusterId, topN, getTopNResp)
		return nil, fmt.Errorf(getTopNResp.Msg)
	}
	return getTopNResp.Data, nil
}

func (s *Service) GetPrivilegeInfo(offset, limit int, order string) ([]*models.UserPrivilege, error) {
	result := make([]*models.UserPrivilege, offset, limit)
	rows, err := s.db.Query(fmt.Sprintf(`SELECT * FROM %s order by user_name %s limit %d,%d  `, TABLE_NAME_PRIVILEGE, order, offset, limit))
	if err != nil {
		log.Error("db select is failed. err:[%v]", err)
		return nil, common.DB_ERROR
	}
	for rows.Next() {
		info := models.NewUserPrivilege()
		if err := rows.Scan(&(info.UserName), &(info.ClusterId), &(info.Privilege)); err != nil {
			log.Error("db scan is failed. err:[%v]", err)
			return nil, common.DB_ERROR
		}
		log.Debug("selected privilege:%v", info)
		result = append(result, info)
	}
	return result, nil
}

func (s *Service) UpdatePrivilege(userName string, clusterId, roleId int) error {
	rows, err := s.db.Exec(fmt.Sprintf(`insert into %s values("%s", %d, %d)`, TABLE_NAME_PRIVILEGE, userName, clusterId, roleId))
	if err != nil {
		log.Error("db exec is failed. err:[%v]", err)
		return common.DB_ERROR
	}
	rowsAffected, err := rows.RowsAffected()
	if err != nil {
		log.Error("db rowsaffected is failed. err:[%v]", err)
		return common.DB_ERROR
	}
	if rowsAffected != 1 {
		return common.CLUSTER_DUPCREATE_ERROR
	}
	return nil
}

func (s *Service) DelPrivilege(privileges []models.UserPrivilege) error {
	for _, p := range privileges {
		rows, err := s.db.Exec(fmt.Sprintf(`delete from %s where user_name = "%s" and cluster_id = %d`, TABLE_NAME_PRIVILEGE, p.UserName, p.ClusterId))
		if err != nil {
			log.Error("db exec is failed. err:[%v]", err)
			return common.DB_ERROR
		}
		rowsAffected, err := rows.RowsAffected()
		if err != nil {
			log.Error("db rowsaffected is failed. err:[%v]", err)
			return common.DB_ERROR
		}
		if rowsAffected > 1 {
			return common.CLUSTER_DUPCREATE_ERROR
		}
	}
	return nil
}

func (s *Service) GetRoleInfo(offset, limit int, order string) ([]*models.Role, error) {
	result := make([]*models.Role, offset, limit)
	rows, err := s.db.Query(fmt.Sprintf(`SELECT * FROM %s order by role_id %s limit %d,%d  `, TABLE_NAME_ROLE, order, offset, limit))
	if err != nil {
		log.Error("db select is failed. err:[%v]", err)
		return nil, common.DB_ERROR
	}
	for rows.Next() {
		info := models.NewRole()
		if err := rows.Scan(&(info.Id), &(info.RoleName)); err != nil {
			log.Error("db scan is failed. err:[%v]", err)
			return nil, common.DB_ERROR
		}
		log.Debug("selected role:%v", info)
		result = append(result, info)
	}
	return result, nil
}

func (s *Service) AddRole(roleId int, roleName string) error {
	rows, err := s.db.Exec(fmt.Sprintf(`insert into %s values(%d, "%s")`, TABLE_NAME_ROLE, roleId, roleName))
	if err != nil {
		log.Error("db exec is failed. err:[%v]", err)
		return common.DB_ERROR
	}
	rowsAffected, err := rows.RowsAffected()
	if err != nil {
		log.Error("db rowsaffected is failed. err:[%v]", err)
		return common.DB_ERROR
	}
	if rowsAffected != 1 {
		return common.CLUSTER_DUPCREATE_ERROR
	}
	return nil
}

func (s *Service) DelRole(roleIds []int) error {
	for _, r := range roleIds {
		rows, err := s.db.Exec(fmt.Sprintf(`delete from %s where role_id = %d`, TABLE_NAME_ROLE, r))
		if err != nil {
			log.Error("db exec is failed. err:[%v]", err)
			return common.DB_ERROR
		}
		rowsAffected, err := rows.RowsAffected()
		if err != nil {
			log.Error("db rowsaffected is failed. err:[%v]", err)
			return common.DB_ERROR
		}
		if rowsAffected > 1 {
			return common.CLUSTER_DUPCREATE_ERROR
		}
	}
	return nil
}

func (s *Service) SetMasterLogLevel(clusterId int, logLevel string) (error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["level"] = logLevel

	var mes string
	if err := sendGetSimpleReq(info.MasterUrl, "debug/log/setlevel", reqParams, mes); err != nil {
		return err
	}

	log.Debug("update master log level: {}", mes)
	return nil
}

func (s *Service) IsAdmin(userName string) (bool, error) {
	flag, find := s.adminCache.Get(userName)
	if find {
		log.Info("enter cache")
		return flag.(bool), nil
	}
	log.Info("enter db query")
	var exist bool
	var user string
	if err := s.db.QueryRow(fmt.Sprintf(`SELECT user_name  FROM %s WHERE user_name="%s" and privilege = 1`, TABLE_NAME_PRIVILEGE, userName)).
		Scan(&(user)); err != nil {
		if err != sql.ErrNoRows {
			log.Error("db queryrow is failed. err:[%v]", err)
			return false, common.DB_ERROR
		}
	}
	if len(user) > 0 {
		exist = true
	}
	s.adminCache.Put(userName, exist)
	return exist, nil
}

//=============lock start==============
func (s *Service) GetAllNamespace(userName string, isAdmin bool) ([]*models.NamespaceApply, error) {
	var sql string
	if isAdmin {
		sql = fmt.Sprintf(`select * from %s`, TABLE_NAME_LOCK_NSP)
	} else {
		sql = fmt.Sprintf(`select * from %s where applyer = "%s" `, TABLE_NAME_LOCK_NSP, userName)
	}
	log.Debug("get all apply lock namespace: %s", sql)

	rows, err := s.db.Query(sql)
	if err != nil {
		log.Error("db select is failed. err:[%v]", err)
		return nil, common.DB_ERROR
	}
	result := make([]*models.NamespaceApply, 0)
	for rows.Next() {
		info := new(models.NamespaceApply)
		if err := rows.Scan(&(info.NameSpace), &(info.ClusterId), &(info.Applyer), &(info.CreateTime)); err != nil {
			log.Error("db scan is failed. err:[%v]", err)
			return nil, common.DB_ERROR
		}
		result = append(result, info)
	}
	return result, nil

}

func (s *Service) ApplyLockNamespace(cId int, namespace, applyer string, cTime int64) error {
	info, err := s.selectClusterById(cId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}

	var columns []*models.Column
	lockColumn := &models.Column{Name: LOCK_COLUMN, DataType: 1, PrimaryKey: 1, Index: true}
	columns = append(columns, lockColumn)
	err = s.CreateTable(cId, LOCK_DBNAME, namespace, "", "", "", columns, nil)
	if err != nil {
		log.Warn("apply lock namespace %v cluster %v failed, err: %v", namespace, cId, err)
		return err
	}

	sql := fmt.Sprintf(`INSERT INTO %s (namespace, cluster_id, applyer, create_time) values ("%s", %d, "%s", %d)`,
		TABLE_NAME_LOCK_NSP, namespace, cId, applyer, cTime)
	_, err = s.execSql(sql)
	if err != nil {
		return err
	}

	log.Debug("%s apply lock namsspace %s success", applyer, namespace, applyer)
	return nil
}

func (s *Service) UpdateLockNsp(cId int, namespace, applyer string, cTime int64) error {
	sql := fmt.Sprintf(`Insert into %s (namespace, cluster_id, applyer, create_time) values ("%s", %d, "%s", %d)`, TABLE_NAME_LOCK_NSP, namespace, cId, applyer, cTime)
	rowsAffected, err := s.execSql(sql)
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return common.CLUSTER_DUPCREATE_ERROR
	}
	log.Debug("update applyer %s success of namespace %d and clusterId %d", namespace, cId, applyer)
	return nil
}

func (s *Service) GetLockCluster() (*models.ClusterInfo, error) {
	clusterId := s.config.LockClusterId
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}
	return info, nil
}
//=============lock end================


//=============metric add ===============

func (s *Service) GetAllMetricServer() ([]models.MetricServer, error) {
	rows, err := s.db.Query(fmt.Sprintf(`SELECT addr FROM %s`, TABLE_NAME_METRIC_SERVER))
	if err != nil {
		log.Error("metric server select is failed. err:[%v]", err)
		return nil, common.DB_ERROR
	}

	var result []models.MetricServer
	for rows.Next() {
		var info models.MetricServer
		if err := rows.Scan(&info.Addr); err != nil {
			log.Error("metric server scan is failed. err:[%v]", err)
			return nil, common.DB_ERROR
		}
		log.Debug("selected metric server:%v", info)
		result = append(result, info)
	}

	return result, nil
}

func (s *Service) CreateMetricServer(addr string) error {
	rows, err := s.db.Exec(fmt.Sprintf(`insert into %s values("%s")`, TABLE_NAME_METRIC_SERVER, addr))
	if err != nil {
		log.Error("db exec is failed. err:[%v]", err)
		return common.DB_ERROR
	}
	rowsAffected, err := rows.RowsAffected()
	if err != nil {
		log.Error("db rowsaffected is failed. err:[%v]", err)
		return common.DB_ERROR
	}
	if rowsAffected != 1 {
		return common.CLUSTER_DUPCREATE_ERROR
	}
	return nil
}

func (s *Service) DeleteMetricServer(addrs []string) error {
	for _, addr := range addrs {
		rows, err := s.db.Exec(fmt.Sprintf(`delete from %s where addr = "%s"`, TABLE_NAME_METRIC_SERVER, addr))
		if err != nil {
			log.Error("db exec is failed. err:[%v]", err)
			return common.DB_ERROR
		}
		rowsAffected, err := rows.RowsAffected()
		if err != nil {
			log.Error("db rowsaffected is failed. err:[%v]", err)
			return common.DB_ERROR
		}
		if rowsAffected != 1 {
			return common.CLUSTER_DUPCREATE_ERROR
		}
	}
	return nil
}


func (s *Service) GetMetricConfig(cId int) (map[string]*models.MetricConfig, error) {
	info, err := s.selectClusterById(cId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	msConfig := &models.MetricConfig{}
	gsConfig := &models.MetricConfig{}

	var waitLock sync.WaitGroup
	waitLock.Add(1)

	go func(msConfig *models.MetricConfig) {
		defer waitLock.Done()

		ts := time.Now().Unix()
		sign := common.CalcMsReqSign(info.Id, info.ClusterToken, ts)

		reqParams := make(map[string]interface{})
		reqParams["d"] = ts
		reqParams["s"] = sign

		var getConfigResp = struct {
			Code int         `json:"code"`
			Msg  string      `json:"message"`
			Data models.MetricConfig `json:"data"`
		}{}
		if err := sendGetReq(info.MasterUrl, "/metric/config/get", reqParams, &getConfigResp); err != nil {
			msConfig.Address = err.Error()
		} else {
			if getConfigResp.Code != 0 {
				msConfig.Address = getConfigResp.Msg
			} else {
				msConfig.Address = getConfigResp.Data.Address
				msConfig.Interval = getConfigResp.Data.Interval
			}
		}
	}(msConfig)

	waitLock.Add(1)
	 go func(gsConfig *models.MetricConfig) {
		 defer waitLock.Done()

		 ts := time.Now().Unix()
		 sign := common.CalcMsReqSign(info.Id, info.ClusterToken, ts)

		 reqParams := make(map[string]interface{})
		 reqParams["d"] = ts
		 reqParams["s"] = sign

		 var getConfigResp = struct {
			 Code int         `json:"code"`
			 Msg  string      `json:"message"`
			 Data models.MetricConfig `json:"data"`
		 }{}
		 if err := sendGetReq(info.GatewayHttpUrl, "/metric/config/get", reqParams, &getConfigResp); err != nil {
			 msConfig.Address = err.Error()
		 } else {
			 if getConfigResp.Code != 0 {
				 gsConfig.Address = getConfigResp.Msg
			 } else {
				 gsConfig.Address = getConfigResp.Data.Address
			 }
		 }
	}(gsConfig)
	waitLock.Wait()

	reply := make(map[string]*models.MetricConfig)
	reply["ms"] = msConfig
	reply["gs"] = gsConfig

	log.Debug("get metric config: {}", reply)
	return reply, nil
}


func (s *Service) SetMetricConfig(cId int, addr, interval string) (map[string]string, error) {
	info, err := s.selectClusterById(cId)
	if err != nil {
		return  nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	respose := make(map[string]string, 0)

	var waitLock sync.WaitGroup
	waitLock.Add(1)
	go func(response map[string]string) {
		defer waitLock.Done()
		ts := time.Now().Unix()
		sign := common.CalcMsReqSign(info.Id, info.ClusterToken, ts)
		reqParams := make(map[string]interface{})
		reqParams["d"] = ts
		reqParams["s"] = sign
		reqParams["interval"] = interval
		reqParams["address"] = addr
		var setConfigResp = struct {
			Code int         `json:"code"`
			Msg  string      `json:"message"`
		}{}
		if err := sendGetReq(info.MasterUrl, "/metric/config/set", reqParams, &setConfigResp); err != nil {
			respose["ms"] = err.Error()
		} else if setConfigResp.Code > 0{
			respose["ms"] = setConfigResp.Msg
		} else {
			respose["ms"] = "success"
		}

	}(respose)

	waitLock.Add(1)
	go func(response map[string]string) {
		defer waitLock.Done()

		ts := time.Now().Unix()
		sign := common.CalcMsReqSign(info.Id, info.ClusterToken, ts)

		reqParams := make(map[string]interface{})
		reqParams["d"] = ts
		reqParams["s"] = sign
		reqParams["address"] = addr
		var setConfigResp = struct {
			Code int         `json:"code"`
			Msg  string      `json:"message"`
		}{}
		if err := sendGetReq(info.GatewayHttpUrl, "/metric/config/set", reqParams, &setConfigResp); err != nil {
			respose["gs"] = err.Error()
		} else if setConfigResp.Code > 0{
			respose["gs"] = setConfigResp.Msg
		} else {
			respose["gs"] = "success"
		}
	}(respose)
	waitLock.Wait()

	log.Debug("set master client config: %v", respose)
	return respose, nil
}
//=============metric end ===============


// ------------http request -------------------
func sendGetSimpleReq(host, uri string, params map[string]interface{}, result string) (error) {
	var url []string

	url = append(url, host)
	if !strings.HasPrefix(uri, "/") {
		url = append(url, "/")
	}
	url = append(url, uri)

	if len(params) != 0 {
		url = append(url, "?")
		for k, v := range params {
			url = append(url, fmt.Sprintf("&%s=%v", k, v))
		}
	}
	finalUrl := strings.Join(url, "")
	log.Debug("send http get request to url:[%s]", finalUrl)

	resp, err := http.Get(finalUrl)
	if err != nil {
		log.Error("http get request failed. err:[%v]", err)
		return common.HTTP_REQUEST_ERROR
	}
	if resp.StatusCode != http.StatusOK {
		log.Error("http response status code error. code:[%v]", resp.StatusCode)
		return common.HTTP_REQUEST_ERROR
	}

	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.Error("read http response body error. err:[%v]", err)
		return common.HTTP_REQUEST_ERROR
	}
	log.Debug("http response body:[%v]", string(body))
	result = string(body)
	return nil
}

func sendGetReq(host, uri string, params map[string]interface{}, result interface{}) (error) {
	var url []string

	url = append(url, host)
	if !strings.HasPrefix(uri, "/") {
		url = append(url, "/")
	}
	url = append(url, uri)

	if len(params) != 0 {
		url = append(url, "?")
		for k, v := range params {
			url = append(url, fmt.Sprintf("&%s=%v", k, v))
		}
	}
	finalUrl := strings.Join(url, "")
	log.Debug("send http get request to url:[%s]", finalUrl)

	tGetStart := time.Now()
	resp, err := http.Get(finalUrl)
	log.Info("send get request token %v second", time.Since(tGetStart).Seconds())
	if err != nil {
		log.Error("http get request failed. err:[%v]", err)
		return common.HTTP_REQUEST_ERROR
	}
	if resp.StatusCode != http.StatusOK {
		log.Error("http response status code error. code:[%v]", resp.StatusCode)
		return common.HTTP_REQUEST_ERROR
	}

	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.Error("read http response body error. err:[%v]", err)
		return common.HTTP_REQUEST_ERROR
	}
	log.Debug("http response body:[%v]", string(body))

	if err := json.Unmarshal(body, result); err != nil {
		log.Error("Cannot parse http response in json. body:[%v]", string(body))
		return common.INTERNAL_ERROR
	}

	return nil
}

func sendPostReqStrBody(host, uri string, params map[string]interface{}, result interface{}) (error) {
	var url []string

	url = append(url, host)
	if !strings.HasPrefix(uri, "/") {
		url = append(url, "/")
	}
	url = append(url, uri)
	finalUrl := strings.Join(url, "")

	var body []string
	if len(params) != 0 {
		for k, v := range params {
			body = append(body, fmt.Sprintf("%s=%v&", k, v))
		}
	}
	finalBody := strings.Join(body, "")
	log.Debug("send http post request to url:[%s] with body:[%s]", finalUrl, finalBody)

	req, err := http.NewRequest("POST", finalUrl, bytes.NewReader([]byte(finalBody)))
	if err != nil {
		log.Error("http post request faield. err:[%v]", err)
		return common.HTTP_REQUEST_ERROR
	}
	req.Header.Set("Content-type", "application/x-www-form-urlencoded")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Error("http post request failed. err:[%v]", err)
		return common.HTTP_REQUEST_ERROR
	}
	if resp.StatusCode != http.StatusOK {
		log.Error("http response status code error. code:[%v]", resp.StatusCode)
		return common.HTTP_REQUEST_ERROR
	}

	data, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.Error("read http response body error. err:[%v]", err)
		return common.HTTP_REQUEST_ERROR
	}
	log.Debug("http response body:[%v]", string(data))

	if err := json.Unmarshal(data, result); err != nil {
		log.Error("Cannot parse http response in json. body:[%v]", string(data))
		return common.INTERNAL_ERROR
	}

	return nil
}

func sendPostReqJsonBody(host, uri string, params map[string]interface{}, result interface{}) (error) {
	var url []string

	url = append(url, host)
	if !strings.HasPrefix(uri, "/") {
		url = append(url, "/")
	}
	url = append(url, uri)
	finalUrl := strings.Join(url, "")

	body, err := json.Marshal(params)
	if err != nil {
		log.Error("Cannot transfer properties in json. err:[%v]", err)
		return common.PARSE_PARAM_ERROR
	}
	log.Debug("send http post request to url:[%s] with body:[%s]", finalUrl, string(body))

	req, err := http.NewRequest("POST", finalUrl, bytes.NewReader(body))
	if err != nil {
		log.Error("http post request faield. err:[%v]", err)
		return common.HTTP_REQUEST_ERROR
	}
	req.Header.Set("Content-type", "application/x-www-form-urlencoded")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Error("http post request failed. err:[%v]", err)
		return common.HTTP_REQUEST_ERROR
	}
	if resp.StatusCode != http.StatusOK {
		log.Error("http response status code error. code:[%v]", resp.StatusCode)
		return common.HTTP_REQUEST_ERROR
	}

	data, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.Error("read http response body error. err:[%v]", err)
		return common.HTTP_REQUEST_ERROR
	}
	log.Debug("http response body:[%v]", string(data))

	if err := json.Unmarshal(data, result); err != nil {
		log.Error("Cannot parse http response in json. body:[%v]", string(data))
		return common.INTERNAL_ERROR
	}

	return nil
}

// -------------------dao-------------
func (s *Service) insertClusterById(info *models.ClusterInfo) error {
	//stmt, err := s.db.Prepare(`INSERT INTO `+ TABLE_NAME_CLUSTER +` (id, cluster_name, cluster_url, gateway_url, cluster_sign,
	//	create_time, auto_transfer, auto_failover ) values (?, ?, ?, ?, ?, ?, ?, ?)`)
	//if err != nil {
	//	log.Error("db prepare is failed. err:[%v]", err)
	//	return common.DB_ERROR
	//}
	//res, err := stmt.Exec(TABLE_NAME_CLUSTER, info.Id, info.Name, info.MasterUrl,
	//	info.GatewayUrl, info.ClusterToken, info.CreateTime, info.AutoTransferUnable, info.AutoFailoverUnable)
	var autoTransfer, autoFailover, autoSplit int
	if info.AutoTransferUnable {
		autoTransfer = 1
	}
	if info.AutoFailoverUnable {
		autoFailover = 1
	}
	if info.AutoSplitUnable {
		autoSplit = 1
	}
	sql := fmt.Sprintf(`INSERT INTO %s (id, cluster_name, cluster_url, gateway_http, gateway_sql, cluster_sign,
		create_time, auto_transfer, auto_failover, auto_split ) values (%d, "%s", "%s", "%s", "%s", "%s", %d, %d, %d, %d)`, TABLE_NAME_CLUSTER,
		info.Id, info.Name, info.MasterUrl,
		info.GatewayHttpUrl, info.GatewaySqlUrl, info.ClusterToken, info.CreateTime, autoTransfer, autoFailover, autoSplit)
	rowsAffected, err := s.execSql(sql)
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return common.CLUSTER_DUPCREATE_ERROR
	}
	return nil
}

func (s *Service) selectClusterById(cId int) (*models.ClusterInfo, error) {
	var info *models.ClusterInfo = new(models.ClusterInfo)
	if err := s.db.QueryRow(fmt.Sprintf(`SELECT id, cluster_name, cluster_url, gateway_http, gateway_sql, cluster_sign, create_time  FROM %s WHERE id=%d`, TABLE_NAME_CLUSTER, cId)).
		Scan(&(info.Id), &(info.Name), &(info.MasterUrl), &(info.GatewayHttpUrl), &(info.GatewaySqlUrl), &(info.ClusterToken),
		&(info.CreateTime)); err != nil {
		if err == sql.ErrNoRows {
			log.Error("db row not exists. cid:[%d]", cId)
			return nil, nil
		} else {
			log.Error("db queryrow is failed. err:[%v]", err)
			return nil, common.DB_ERROR
		}
	}
	return info, nil
}

func (s *Service) execSql(sql string) (int64, error) {
	res, err := s.db.Exec(sql)
	if err != nil {
		log.Error("db exec is failed. err:[%v]", err)
		return 0, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Error("db rowsaffected is failed. err:[%v]", err)
		return 0, err
	}
	return rowsAffected, nil
}

/**
{
    "keys": [
      {
        "field": "pin"
      },
      {
        "field": "province"
      },
      {
        "field": "city"
      },
      {
        "field": "county"
      }
    ],
    "values": {
      "total": 1,
      "rows": [
        {
          "county": "50947",
          "pin": "\"liuyanhui\"",
          "province": "22",
          "city": "1930"
        }
      ]
    }
  }
 */

func (s *Service) queryStoreDataBySql(gatewaySqlUrl string, paramMap map[string]string) (interface{}, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8", paramMap["dbUserName"], paramMap["dbPassWord"], gatewaySqlUrl, paramMap["dbName"]))
	if err != nil {
		log.Error("open sql err, [%v]", err)
		return nil, err
	}
	rows, err := db.Query(paramMap["sql"])
	if err != nil {
		log.Error("query sql err, [%v]", err)
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	count := len(columns)
	var fileds []string
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	flag := 0
	for rows.Next() {
		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		entry := make(map[string]interface{})
		for i, col := range columns {
			if flag == 0 {
				fileds = append(fileds, col)
			}
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		flag++
		tableData = append(tableData, entry)
	}

	if len(tableData) == 0 {
		return nil, common.NO_DATA
	} else {
		result := make(map[string]interface{}, 0)
		result["keys"] = fileds
		filedValues := make(map[string]interface{}, 2)
		filedValues["total"] = len(tableData)
		filedValues["rows"] = tableData
		result["values"] = filedValues
		return result, nil
	}
}

/**
   DML
   {
    "keys": [
      {
        "field": "res"
      }
    ],
    "values": {
      "total": 1,
      "rows": [
        {
          "res": 1
        }
      ]
    }
  }
 */
func (s *Service) operateStoreDataBySql(gatewaySqlUrl string, paramMap map[string]string) (int64, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8", paramMap["dbUserName"], paramMap["dbPassWord"], gatewaySqlUrl, paramMap["dbName"]))
	if err != nil {
		log.Error("open sql err, [%v]", err)
		return 0, err
	}
	res, err := db.Exec(paramMap["sql"])
	if err != nil {
		log.Error("db exec is failed. err:[%v]", err)
		return 0, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Error("db rowsaffected is failed. err:[%v]", err)
		return 0, err
	}
	return rowsAffected, nil
}

func InitService(c *config.Config) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", c.MysqlUser, c.MysqlPasswd,
		c.MysqlHost, c.MysqlPort, DB_NAME))
	if err != nil {
		panic("Fail to initialize mysql")
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(0)
	serviceInstance = &Service{
		config:     c,
		db:         db,
		adminCache: ttlcache.NewTTLCache(2 * time.Minute),
	}
}
