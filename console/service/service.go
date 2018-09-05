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
	"github.com/satori/go.uuid"
)
import (
	"model/pkg/ds_admin"
	"model/pkg/kvrpcpb"
	"console/models"
	"console/common"
	"console/config"
	"console/right"
	"util/log"
	"strconv"
	"errors"
	"sync"
	"crypto/md5"
	"encoding/hex"
)

const (
	//DB_NAME = "fbase_mock_console"
	DB_NAME = "fbase"

	TABLE_NAME_USER          = "fbase_user"
	TABLE_NAME_CLUSTER       = "fbase_cluster"
	TABLE_NAME_ROLE          = "fbase_role"
	TABLE_NAME_PRIVILEGE     = "fbase_privilege"
	TABLE_NAME_SQL_APPLY     = "fbase_sql_apply"
	TABLE_NAME_LOCK_NSP      = "fbase_lock_nsp"
	TABLE_NAME_CONFIGURE_NSP = "fbase_configure_nsp"
	TABLE_NAME_METRIC_SERVER = "metric_server"

	STATUS_APPLY  = 1
	STATUS_AUDIT  = 2
	STATUS_REJECT = 3

	LOCK_CLIENT_NAMESPACE_PREFIX = ""
)

var Columns = []*models.Column{
	{Name: "k", DataType: 7, PrimaryKey: 1, Index: true},
	{Name: "version", DataType: 4, Index: true},
	{Name: "v", DataType: 7, Index: true},
	{Name: "extend", DataType: 7, Index: true},
}

var serviceInstance *Service = nil

type Service struct {
	config     *config.Config
	db         *sql.DB
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
		auto_transfer, auto_failover, auto_split, create_time FROM %s order by id`, TABLE_NAME_CLUSTER))
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

func (s *Service) CreateDb(cId int, dbName string) (*models.DbInfo, error) {
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
	reqParams["name"] = dbName

	var createDbResp = struct {
		Code int           `json:"code"`
		Msg  string        `json:"message"`
		Data models.DbInfo `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/database/create", reqParams, &createDbResp); err != nil {
		return nil, err
	}
	if createDbResp.Code != 0 {
		log.Error("master createdb is failed. err:[%v]", createDbResp)
		return nil, common.INTERNAL_ERROR
	}

	return &createDbResp.Data, nil
}

func (s *Service) DeleteDb(cId int, dbName string) error {
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

	var deleteDbResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/database/delete", reqParams, &deleteDbResp); err != nil {
		log.Error("send delete db error, %v", err)
		return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: err.Error()}
	}
	if deleteDbResp.Code != 0 {
		log.Error("master deleteDb is failed. err:[%v]", deleteDbResp)
		return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: deleteDbResp.Msg}
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

func (s *Service) CreateTable(cId int, dbName, tableName, policy, rangeKeys, isolationLabel string, columnJsonArray, regxsJsonArray interface{}) (*models.TableInfo, error) {
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
		Code int              `json:"code"`
		Msg  string           `json:"message"`
		Data models.TableInfo `json:"data"`
	}{}
	if err := sendPostReqStrBody(info.MasterUrl, "/manage/table/create", reqParams, &createTableResp); err != nil {
		return nil, err
	}
	if createTableResp.Code != 0 {
		log.Error("master createTable is failed. err:[%v]", createTableResp)
		return nil, common.INTERNAL_ERROR
	}

	return &createTableResp.Data, nil
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

func (s *Service) EditTablePolicy(cId int, dbName, tableName string, readPolicy int) error {
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
	reqParams["policy"] = readPolicy

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

func (s *Service) GetMasterLeader(cId int, token string) (*models.MsNode, error) {
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
		Code int            `json:"code"`
		Msg  string         `json:"message"`
		Data *models.MsNode `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/master/getleader", reqParams, &masterLeaderResp); err != nil {
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
		return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: initClusterResp.Msg}
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
		return nil, common.INTERNAL_ERROR
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
		return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: nodeLogoutResp.Msg}
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
		return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: nodeUpgradeResp.Msg}
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
		return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: nodeLoginResp.Msg}
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
		return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: nodeLogLevelResp.Msg}
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
		return nil, &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: resp.Msg}
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
		return nil, &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: resp.Msg}
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
		return nil, &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: peerDeleteResp.Msg}
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
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/range/add/peer", reqParams, &peerAddResp); err != nil {
		return err
	}
	if peerAddResp.Code != 0 {
		log.Error("add peer failed. err:[%v]", peerAddResp)
		return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: peerAddResp.Msg}
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
		return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: nodeDeleteResp.Msg}
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
		return nil, &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: getRangeTopoOfNodeResp.Msg}
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

func (s *Service) GetConfigOfNode(clusterId, nodeId int, configKeys string) (interface{}, error) {
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
	reqParams["getConfigKey"] = configKeys

	var getConfigOfNodeResp = struct {
		Code int                      `json:"code"`
		Msg  string                   `json:"message"`
		Data []*ds_adminpb.ConfigItem `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/node/getConfigOfNode", reqParams, &getConfigOfNodeResp); err != nil {
		return nil, err
	}
	if getConfigOfNodeResp.Code != 0 {
		log.Error("get config of node[nodeId=%d, clusterId=%d] failed. err:[%v]", nodeId, clusterId, getConfigOfNodeResp)
		return nil, &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: getConfigOfNodeResp.Msg}
	}
	return getConfigOfNodeResp.Data, nil
}

func (s *Service) SetConfigOfNode(clusterId, nodeId int, setConfig string) (interface{}, error) {
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
	reqParams["setConfig"] = setConfig

	var setConfigOfNodeResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/node/setConfigOfNode", reqParams, &setConfigOfNodeResp); err != nil {
		return nil, err
	}
	if setConfigOfNodeResp.Code != 0 {
		log.Error("set config of node[nodeId=%d, clusterId=%d] failed. err:[%v]", nodeId, clusterId, setConfigOfNodeResp)
		return nil, &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: setConfigOfNodeResp.Msg}
	}
	return nil, nil
}

func (s *Service) GetDsInfoOfNode(clusterId, nodeId int, path string) (interface{}, error) {
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
	reqParams["dsInfoPath"] = path

	var getDsInfoOfNodeResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
		Data string `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/node/getDsInfoOfNode", reqParams, &getDsInfoOfNodeResp); err != nil {
		return nil, err
	}
	if getDsInfoOfNodeResp.Code != 0 {
		log.Error("get ds_info of node[nodeId=%d, clusterId=%d] failed. err:[%v]", nodeId, clusterId, getDsInfoOfNodeResp)
		return nil, &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: getDsInfoOfNodeResp.Msg}
	}
	return getDsInfoOfNodeResp.Data, nil
}

func (s *Service) ClearQueueOfNode(clusterId, nodeId int, queueType string) (interface{}, error) {
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
	reqParams["queueType"] = queueType

	var clearQueueOfNodeResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
		Data uint64 `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/node/clearQueueOfNode", reqParams, &clearQueueOfNodeResp); err != nil {
		return nil, err
	}
	if clearQueueOfNodeResp.Code != 0 {
		log.Error("clear queue of node[nodeId=%d, clusterId=%d] failed. err:[%v]", nodeId, clusterId, clearQueueOfNodeResp)
		return nil, &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: clearQueueOfNodeResp.Msg}
	}
	return clearQueueOfNodeResp.Data, nil
}

func (s *Service) GetPendingQueuesOfNode(clusterId, nodeId int, pendingType, count string) (interface{}, error) {
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
	reqParams["pendingType"] = pendingType
	reqParams["count"] = count

	var getPendingQueuesOfNodeResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
		Data string `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/node/getPendingQueuesOfNode", reqParams, &getPendingQueuesOfNodeResp); err != nil {
		return nil, err
	}
	if getPendingQueuesOfNodeResp.Code != 0 {
		log.Error("get pending queues of node[nodeId=%d, clusterId=%d] failed. err:[%v]", nodeId, clusterId, getPendingQueuesOfNodeResp)
		return nil, &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: getPendingQueuesOfNodeResp.Msg}
	}
	return getPendingQueuesOfNodeResp.Data, nil
}

func (s *Service) FlushDBOfNode(clusterId, nodeId int, wait bool) (interface{}, error) {
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
	reqParams["wait"] = wait

	var flushDBOfNodeResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/node/flushDBOfNode", reqParams, &flushDBOfNodeResp); err != nil {
		return nil, err
	}
	if flushDBOfNodeResp.Code != 0 {
		log.Error("flush db of node[nodeId=%d, clusterId=%d, wait=%t] failed. err:[%v]", nodeId, clusterId, wait, flushDBOfNodeResp)
		return nil, &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: flushDBOfNodeResp.Msg}
	}
	return nil, nil
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
		return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: clusterToggleSetResp.Msg}
	} else {
		//改库
		info.AutoFailoverUnable, _ = strconv.ParseBool(autoFailover)
		info.AutoTransferUnable, _ = strconv.ParseBool(autoTransfer)
		info.AutoSplitUnable, _ = strconv.ParseBool(autoSplit)
		log.Debug("start to update database, %v", info)
		if err := s.insertClusterById(info); err != nil {
			return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: fmt.Sprintf("更新集群开关失败, %s", err.Error())}
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
		return nil, &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: getScheduleResp.Msg}
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
		Code int         `json:"code"`
		Msg  string      `json:"message"`
		Data interface{} `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/scheduler/detail", reqParams, &scheduleDetailResp); err != nil {
		return nil, err
	}
	if scheduleDetailResp.Code != 0 {
		log.Error("get cluster[%d] scheduler %s detail failed. err:[%v]", clusterId, name, scheduleDetailResp)
		return nil, &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: scheduleDetailResp.Msg}
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
		return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: adjustScheduleResp.Msg}
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
		return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: checkTopologyResp.Msg}
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
		return nil, &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: topologyMResp.Msg}
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
		return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: topologyCResp.Msg}
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
		return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: topologyCResp.Msg}
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
		return nil, &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: getDuplicateResp.Msg}
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
		return nil, &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: getTopologyResp.Msg}
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
		return nil, &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: getTaskTypeResp.Msg}
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

func (s *Service) ForceSplitRange(clusterId, rangeId int, dbName, tableName string) error {
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

	var forceSplitRangeResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/range/forceSplitRange", reqParams, &forceSplitRangeResp); err != nil {
		return err
	}
	if forceSplitRangeResp.Code != 0 {
		log.Error("cluster[%d] db[%s] table[%s] rangeId[%s] force split failed. err:[%v]", clusterId, dbName, tableName, rangeId, forceSplitRangeResp)
		return fmt.Errorf(forceSplitRangeResp.Msg)
	}
	return nil
}

func (s *Service) ForceCompactRange(clusterId, rangeId int, dbName, tableName string) (interface{}, error) {
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

	var forceCompactRangeResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
		//Data []byte `json:"data"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/range/forceCompactRange", reqParams, &forceCompactRangeResp); err != nil {
		return nil, err
	}
	if forceCompactRangeResp.Code != 0 {
		log.Error("cluster[%d] db[%s] table[%s] rangeId[%s] force compact failed. err:[%v]", clusterId, dbName, tableName, rangeId, forceCompactRangeResp)
		return nil, fmt.Errorf(forceCompactRangeResp.Msg)
	}
	return forceCompactRangeResp, nil
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
	userInfo, err := right.GetUserCluster(s.GetDb(), userName)
	if err != nil {
		log.Error("user %v isAdmin error, %v", userName, err)
		return false, err
	}
	if userInfo != nil {
		for _, right := range userInfo.Right {
			if right == 1 {
				return true, nil
			}
		}
	}
	return false, nil
}

//=============sql apply start==============
func (s *Service) GetAllSqlApply(userName string, isAdmin bool, pageInfo *models.PagerInfo) (int, []*models.SqlApply, error) {
	selectSql := fmt.Sprintf(`select id, db_name, table_name, status, applyer, create_time, remark from %s`, TABLE_NAME_SQL_APPLY)
	countSql := fmt.Sprintf(`select count(*) from %s`, TABLE_NAME_SQL_APPLY)
	if !isAdmin {
		selectSql = fmt.Sprintf(`%s where applyer = "%s"`, selectSql, userName)
		countSql = fmt.Sprintf(`%s where applyer = "%s"`, countSql, userName)
	}
	if pageInfo != nil {
		if pageInfo.SortName != "" && pageInfo.SortOrder != "" {
			selectSql = fmt.Sprintf(`%s order by "%s" "%s"`, selectSql, pageInfo.SortName, pageInfo.SortOrder)
		} else {
			selectSql = fmt.Sprintf(`%s order by create_time desc`, selectSql)
		}
		if pageInfo.PageIndex > 0 && pageInfo.PageSize > 0 {
			selectSql = fmt.Sprintf(`%s limit %d, %d`, selectSql, pageInfo.GetPageOffset(), pageInfo.GetPageSize())
			countSql = fmt.Sprintf(`%s limit %d, %d`, countSql, pageInfo.GetPageOffset(), pageInfo.GetPageSize())
		}
	}

	log.Debug("get all sql apply records:  %s", selectSql)
	var totalRecord int
	if err := s.db.QueryRow(countSql).
		Scan(&(totalRecord)); err != nil {
		log.Error("db select is failed. err:[%v]", err)
		return 0, nil, common.DB_ERROR
	}
	if totalRecord > 0 {
		rows, err := s.db.Query(selectSql)
		if err != nil {
			log.Error("db select is failed. err:[%v]", err)
			return 0, nil, common.DB_ERROR
		}
		result := make([]*models.SqlApply, 0)
		for rows.Next() {
			info := new(models.SqlApply)
			if err := rows.Scan(&(info.Id), &(info.DbName), &(info.TableName), &(info.Status), &(info.Applyer), &(info.CreateTime), &(info.Remark)); err != nil {
				log.Error("db scan is failed. err:[%v]", err)
				return 0, nil, common.DB_ERROR
			}
			result = append(result, info)
		}
		return totalRecord, result, nil
	} else {
		return totalRecord, nil, nil
	}
}

func (s *Service) ApplySql(dbName, tableName, sentence, applyer, remark string, cTime int64) error {
	id, err := uuid.NewV4()
	if err != nil {
		return err
	}
	idS := fmt.Sprintf("%s", id)

	sql := fmt.Sprintf(`INSERT INTO %s (id, db_name, table_name, sentence, status, applyer, auditor, create_time, remark) 
		values ("%s", "%s", "%s", "%s", %d, "%s", "%s", %d, "%s")`,
		TABLE_NAME_SQL_APPLY, idS, dbName, tableName, sentence, STATUS_APPLY, applyer, "", cTime, remark)
	_, err = s.execSql(sql)
	if err != nil {
		return err
	}

	log.Debug("%s apply sql success", applyer)
	return nil
}

func (s *Service) GetSqlApplyInfo(id string) (*models.SqlApply, error) {
	info := new(models.SqlApply)
	if err := s.db.QueryRow(fmt.Sprintf(`SELECT id, db_name, table_name, sentence, status, applyer, create_time, remark FROM %s WHERE id="%s"`, TABLE_NAME_SQL_APPLY, id)).
		Scan(&(info.Id), &(info.DbName), &(info.TableName), &(info.Sentence), &(info.Status), &(info.Applyer), &(info.CreateTime), &(info.Remark)); err != nil {
		if err == sql.ErrNoRows {
			log.Error("db row not exists. applyId:[%d]", id)
			return nil, nil
		} else {
			log.Error("db query row is failed. err:[%v]", err)
			return nil, common.DB_ERROR
		}
	}
	return info, nil
}

func (s *Service) AuditSql(ids []string, status int, auditor string) error {
	for _, id := range ids {
		info, err := s.GetSqlApplyInfo(id)
		if err != nil {
			continue
		}
		sql := fmt.Sprintf(`INSERT INTO %s (id, db_name, table_name, sentence, status, applyer, auditor, create_time, remark) 
		values ("%s", "%s", "%s", "%s", %d, "%s", "%s", %d, "%s")`,
			TABLE_NAME_SQL_APPLY, id, info.DbName, info.TableName, info.Sentence, status, info.Applyer, auditor, info.CreateTime, info.Remark)
		_, err = s.execSql(sql)
		if err != nil {
			return err
		}
	}

	log.Debug("%v audit sql success", auditor)
	return nil
}

//=============sql apply end==============

//=============lock start==============
func (s *Service) GetAllLockNsp(userName string, isAdmin bool, pageInfo *models.PagerInfo) (int, []*models.NamespaceApply, error) {
	return s.GetAllNamespace(userName, isAdmin, pageInfo, TABLE_NAME_LOCK_NSP)
}

func (s *Service) GetAllNamespace(userName string, isAdmin bool, pageInfo *models.PagerInfo, tableName string) (int, []*models.NamespaceApply, error) {
	selectSql := fmt.Sprintf(`select id, db_name, table_name, cluster_id, db_id, table_id, status, applyer, auditor, create_time from %s`, tableName)
	countSql := fmt.Sprintf(`select count(*) from %s`, tableName)

	if !isAdmin {
		selectSql = fmt.Sprintf(`%s where applyer = "%s"`, selectSql, userName)
		countSql = fmt.Sprintf(`%s where applyer = "%s"`, countSql, userName)
	}

	if pageInfo != nil {
		if pageInfo.SortName != "" && pageInfo.SortOrder != "" {
			selectSql = fmt.Sprintf(`%s order by "%s" "%s"`, selectSql, pageInfo.SortName, pageInfo.SortOrder)
		} else {
			selectSql = fmt.Sprintf(`%s order by create_time desc`, selectSql)
		}
		if pageInfo.PageIndex > 0 && pageInfo.PageSize > 0 {
			selectSql = fmt.Sprintf(`%s limit %d, %d`, selectSql, pageInfo.GetPageOffset(), pageInfo.GetPageSize())
			countSql = fmt.Sprintf(`%s limit %d, %d`, countSql, pageInfo.GetPageOffset(), pageInfo.GetPageSize())
		}
	}
	log.Debug("get all apply namespace: %s", selectSql)

	var totalRecord int
	if err := s.db.QueryRow(countSql).
		Scan(&(totalRecord)); err != nil {
		log.Error("db queryrow is failed. err:[%v]", err)
		return 0, nil, common.DB_ERROR
	}
	if totalRecord > 0 {
		rows, err := s.db.Query(selectSql)
		if err != nil {
			log.Error("db select is failed. err:[%v]", err)
			return 0, nil, common.DB_ERROR
		}
		result := make([]*models.NamespaceApply, 0)
		for rows.Next() {
			info := new(models.NamespaceApply)
			if err := rows.Scan(&(info.Id), &(info.DbName), &(info.TableName), &(info.ClusterId), &(info.DbId),
				&(info.TableId), &(info.Status), &(info.Applyer), &(info.Auditor), &(info.CreateTime)); err != nil {
				log.Error("db scan is failed. err:[%v]", err)
				return 0, nil, common.DB_ERROR
			}
			result = append(result, info)
		}
		return totalRecord, result, nil
	} else {
		return totalRecord, nil, nil
	}
}

func (s *Service) GetNamespaceById(applyId, storeTable string) (*models.NamespaceApply, error) {
	querySql := fmt.Sprintf(`select id, db_name, table_name, cluster_id, db_id, table_id, status, applyer, auditor, create_time from %s where id = "%s" `,
		storeTable, applyId)

	log.Debug("get single apply namespace info: %s", querySql)

	info := new(models.NamespaceApply)
	if err := s.db.QueryRow(querySql).
		Scan(&(info.Id), &(info.DbName), &(info.TableName), &(info.ClusterId), &(info.DbId),  &(info.TableId), &(info.Status), &(info.Applyer), &(info.Auditor), &(info.CreateTime)); err != nil {
		if err == sql.ErrNoRows {
			log.Error("db row not exists. ")
			return nil, nil
		} else {
			log.Error("db queryrow is failed. err:[%v]", err)
			return nil, common.DB_ERROR
		}
	}
	return info, nil
}

func (s *Service) existNspApply(dbName, tableName string, clusterId int, storeTable string) (bool, error) {
	querySql := fmt.Sprintf(`select count(*) from %s where db_name = "%s" and table_name = "%s" and cluster_id = %d`,
		storeTable, dbName, tableName, clusterId)
	log.Debug("check exist namespace info: %s", querySql)
	var count int
	if err := s.db.QueryRow(querySql).
		Scan(&(count)); err != nil {
		log.Error("db queryrow is failed. err:[%v]", err)
		return true, common.DB_ERROR
	}
	if count > 0 {
		return true, nil
	} else {
		return false, nil
	}
}

func (s *Service) existTable(dbName, tableName string, clusterId int) (bool, error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return true, err
	}
	if info == nil {
		return true, common.CLUSTER_NOTEXISTS_ERROR
	}

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(clusterId, info.ClusterToken, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["dbName"] = dbName
	reqParams["tableName"] = tableName

	var getTableResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	if err := sendGetReq(info.MasterUrl, "/manage/get/table", reqParams, &getTableResp); err != nil {
		return true, err
	}
	if getTableResp.Code == 0 {
		return true, nil
	}
	return false, nil
}

func (s *Service) ApplyLockNsp(cId int, dbName, tableName, applyer string, cTime int64) error {
	return s.ApplyNamespace(cId, dbName, tableName, applyer, cTime, TABLE_NAME_LOCK_NSP)
}

func (s *Service) ApplyNamespace(cId int, dbName, tableName, applyer string, cTime int64, storeTable string) error {
	info, err := s.selectClusterById(cId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}
	//唯一性检测
	existFlag, err := s.existNspApply(dbName, tableName, cId, storeTable)
	if err != nil {
		return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: err.Error()}
	}
	if existFlag {
		return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: "had exist apply record"}
	}

	if flag, _ := s.existTable(dbName, tableName, cId); flag {
		log.Warn("exist table [%d:%s:%s] or request error, please retry other namespace", cId, dbName, tableName)
		return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: fmt.Sprintf("exist table [%s:%s] or request error, please retry other namespace", dbName, tableName)}
	}

	id, err := uuid.NewV4()
	if err != nil {
		return err
	}

	nsp := fmt.Sprintf(`INSERT INTO %s (id, db_name, table_name, cluster_id, db_id, table_id, status, applyer, auditor, create_time) 
		values ("%s", "%s", "%s", %d, 0, 0, %d, "%s", "%s", %d)`,
		storeTable, fmt.Sprintf("%s", id), dbName, tableName, cId, STATUS_APPLY, applyer, "", cTime)
	_, err = s.execSql(nsp)
	if err != nil {
		return err
	}

	log.Debug("%s apply namespace [%s:%s] success", applyer, dbName, tableName)
	return nil
}

func (s *Service) AuditLockNsp(ids []string, status int, auditor string) error {
	for _, applyId := range ids {
		info, err := s.GetNamespaceById(applyId, TABLE_NAME_LOCK_NSP)
		if err != nil {
			continue
		}
		var dbId, tableId int
		if status == STATUS_AUDIT { // 审批通过
			dbInfo, err := s.CreateDb(info.ClusterId, info.DbName)
			if err != nil {
				log.Warn("create lock db %v on cluster %v failed, err: %v", info.DbName, info.ClusterId, err)
				return err
			}
			dbId = dbInfo.Id

			if flag, _ := s.existTable(info.DbName, info.TableName, info.ClusterId); flag {
				log.Warn("lock: exist db %v table %v in cluster %v, cannot audit", info.DbName, info.TableName, info.ClusterId)
				return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: fmt.Sprintf("exist table %v in cluster %v", info.TableName, info.ClusterId)}
			}

			tableInfo, err := s.CreateTable(info.ClusterId, info.DbName, info.TableName, "", "", "", Columns, nil)
			if err != nil {
				log.Warn("create lock  table %v on cluster %v failed, err: %v", info.TableName, info.ClusterId, err)
				return err
			}
			tableId = tableInfo.Id
		}
		nspSql := fmt.Sprintf(`INSERT INTO %s (id, db_name, table_name, cluster_id, db_id, table_id, status, applyer, auditor, create_time) values ("%s", "%s", "%s", %d,  %d,  %d, %d, "%s", "%s", %d )`,
			TABLE_NAME_LOCK_NSP, applyId, info.DbName, info.TableName, info.ClusterId, dbId, tableId, status, info.Applyer, auditor, info.CreateTime)
		rowsAffected, err := s.execSql(nspSql)
		if err != nil {
			return err
		}
		if rowsAffected != 1 {
			return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: "update apply status and return result error"}
		}
	}
	log.Debug("%v audit lock namespace success, status: %v", auditor, status)
	return nil
}

func (s *Service) UpdateLockNsp(applyId, applyer string) error {
	return s.UpdateNsp(applyId, applyer, TABLE_NAME_LOCK_NSP)
}

func (s *Service) UpdateNsp(applyId, applyer, storeTable string) error {
	applyInfo, err := s.GetNamespaceById(applyId, storeTable)
	if err != nil {
		return err
	}
	nspSql := fmt.Sprintf(`Insert into %s (id, db_name, table_name, cluster_id, db_id, table_id, status, applyer, auditor, create_time) values ("%s", "%s", "%s", %d, %d, %d, %d, "%s", "%s", %d)`,
		storeTable, applyId, applyInfo.DbName, applyInfo.TableName, applyInfo.ClusterId, applyInfo.DbId, applyInfo.TableId,
		applyInfo.Status, applyer, applyInfo.Auditor, applyInfo.CreateTime)
	rowsAffected, err := s.execSql(nspSql)
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return common.CLUSTER_DUPCREATE_ERROR
	}
	log.Debug("update applyer %s success of namespace [%s:%s] and clusterId %d", applyer, applyInfo.DbName, applyInfo.TableName, applyInfo.ClusterId)
	return nil
}

func (s *Service) DeleteLockNsp(ids []string) error {
	return s.DeleteNsp(ids, TABLE_NAME_LOCK_NSP)
}

func (s *Service) DeleteNsp(ids []string, storeTable string) error {
	for _, applyId := range ids {
		nspSql := fmt.Sprintf(`delete from %s where id = "%s"`,
			storeTable, applyId)
		_, err := s.execSql(nspSql)
		if err != nil {
			return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: err.Error()}
		}
	}
	log.Debug("%v delete lock namespace success")
	return nil
}

func (s *Service) GetLockClusterList() ([]*models.ClusterInfo, error) {
	clusterId := s.config.LockClusterId
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}
	var clusters []*models.ClusterInfo
	clusters = append(clusters, info)
	return clusters, nil
}

//go by http command
func (s *Service) GetAllLock(clusterId int, dbName, tableName string, pageInfo *models.PagerInfo) ([]*models.LockShow, error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	log.Debug("get all lock list under clusterId:%v dbName:%v tableName:%v", clusterId, dbName, tableName)

	filter := new(models.Filter_)
	if pageInfo != nil {
		if pageInfo.SortName != "" && pageInfo.SortOrder != "" {
			var descFlag bool
			switch pageInfo.SortOrder {
			case "asc":
				descFlag = true
			default:
				descFlag = false
			}
			order := &models.Order{By: pageInfo.SortName, Desc: descFlag}
			filter.Order = []*models.Order{order}
		} else {
			order := &models.Order{By: "create_time", Desc: true}
			filter.Order = []*models.Order{order}
		}
		if pageInfo.PageIndex > 0 && pageInfo.PageSize > 0 {
			filter.Limit = &models.Limit_{Offset: uint64(pageInfo.GetPageOffset()), RowCount: uint64(pageInfo.GetPageSize())}
		}
	}

	setQueryRep := &models.Query{
		DatabaseName: dbName,
		TableName:    tableName,
		Command: &models.Command{
			Type:   "get",
			Field:  []string{"k", "v", "version", "extend"},
			Filter: filter,
		},
	}
	var reply models.Reply
	if err := sendPostReqJsonBody(info.GatewayHttpUrl, "/kvcommand", setQueryRep, &reply); err != nil {
		return nil, err
	}
	if reply.Code == 5 {
		return nil, nil
	} else if reply.Code != 0 {
		log.Error("get cluster[%d] lock list failed. err:[%v]", clusterId, reply)
		return nil, &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: reply.Message}
	}

	log.Info("result: %v", reply)

	var lockInfos []*models.LockShow
	for _, lockInfo := range reply.Values {
		tInfo := new(models.LockShow)
		tInfo.K = fmt.Sprintf("%v", lockInfo[0])
		value := fmt.Sprintf("%s", lockInfo[1])//todo parse
		if len(value) > 0 {
			newValue := new(kvrpcpb.LockValue)
			if err = newValue.Unmarshal([]byte(value)); err != nil {
				log.Warn("unmarshal value %v error %v", value, err)
				tInfo.V = value
			} else {
				tInfo.V = string(newValue.GetValue())
				tInfo.LockId = newValue.GetId()
				tInfo.UpdTime = newValue.GetUpdateTime()
				tInfo.ExpiredTime = newValue.GetDeleteTime()
				//tInfo.Creator = newValue.get
			}
		}
		tInfo.Version,_ = strconv.ParseInt(fmt.Sprintf("%v", lockInfo[2]), 10, 46)
		tInfo.Extend = fmt.Sprintf("%v", lockInfo[3])
		lockInfos = append(lockInfos, tInfo)
	}
	return lockInfos, nil
}

func (s *Service) ForceUnLock(clusterId int, dbName, tableName, key string) error {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return err
	}
	if info == nil {
		return common.CLUSTER_NOTEXISTS_ERROR
	}
	log.Debug("force unlock key %v under clusterId:%v dbName:%v tableName:%v", key, clusterId, dbName, tableName)
	var reply models.Reply

	filed_ := &models.Field_{Column: "k", Value: key}
	var ands []*models.And
	ands = append(ands, &models.And{Field: filed_, Relate: "="})

	setQueryRep := &models.Query{
		DatabaseName: dbName,
		TableName:    tableName,
		Command: &models.Command{
			Type:   "del",
			Filter: &models.Filter_{And: ands},
		},
	}

	if err := sendPostReqJsonBody(info.GatewayHttpUrl, "/kvcommand", setQueryRep, &reply); err != nil {
		return err
	}
	if reply.Code != 0 || reply.RowsAffected != 1 {
		log.Error("force unlock cluster[%d] lock failed. err:[%v]", clusterId, reply)
		return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: reply.Message}
	}
	return nil
}

func (s *Service) ComputeClientToken(dbId, tableId int) string {
	namespace := fmt.Sprintf("%s%d-%d", LOCK_CLIENT_NAMESPACE_PREFIX, dbId, tableId)
	log.Info("compute client token %v", namespace)
	return createToken(namespace)
}

func createToken(namespace string) string {
	source := namespace
	if len(source) < 32 {
		var buf bytes.Buffer
		buf.WriteString(source)
		for i := 0; i < 32-len(source); i++ {
			buf.WriteString("0")
		}
		source = buf.String()
	}
	encryptStr := encrypt(source)
	var buf bytes.Buffer
	for i := 0; i < len(encryptStr)/4; i++ {
		buf.WriteString(string(encryptStr[i*4]))
	}
	return buf.String()
}

func encrypt(source string) string {
	if source == "" {
		return source
	}
	sources := []byte(source)
	return encrpytMd5(sources)
}

func encrpytMd5(source []byte) string {
	h := md5.New()
	h.Write(source)
	return hex.EncodeToString(h.Sum(nil))
}

func (s *Service) GetClusterInfo(clusterId int) (*models.ClusterInfo, error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}
	clusterInfo := &models.ClusterInfo{Id: info.Id, Name: info.Name}
	//var msNode *models.MsNode
	//msNode, err = s.GetMasterLeader(clusterId, info.ClusterToken)
	//if err != nil {
	//	log.Warn("get cluster rpc port error, %v", err)
	//	return clusterInfo, nil
	//}
	//if info.MasterUrl == "" {
	//	info.MasterUrl = msNode.RpcServerAddr
	//} else {
	//	var urlArray, urlArray2 []string
	//	if strings.HasPrefix(info.MasterUrl, "http://") {
	//		urlArray = strings.Split(info.MasterUrl[7:], ":")
	//	} else {
	//		urlArray = strings.Split(info.MasterUrl, ":")
	//	}
	//	urlArray2 = strings.Split(msNode.RpcServerAddr, ":")
	//	if len(urlArray) == 2 && len(urlArray2) == 2 {
	//		clusterInfo.MasterUrl = fmt.Sprintf("%s:%s", urlArray[0], urlArray2[1])
	//	}
	//}
	if info.MasterUrl != "" {
		var urlArray []string
		if strings.HasPrefix(info.MasterUrl, "http://") {
			urlArray = strings.Split(info.MasterUrl[7:], ":")
		} else {
			urlArray = strings.Split(info.MasterUrl, ":")
		}
		clusterInfo.MasterUrl = fmt.Sprintf("%s:%d", urlArray[0], s.config.DomainRpcPort)
	}
	return clusterInfo, nil
}

//=============lock end================

//=============configure center start================
func (s *Service) GetAllConfigureNsp(userName string, isAdmin bool, pageInfo *models.PagerInfo) (int, []*models.NamespaceApply, error) {
	return s.GetAllNamespace(userName, isAdmin, pageInfo, TABLE_NAME_CONFIGURE_NSP)
}

func (s *Service) ApplyConfigureNsp(cId int, dbName, tableName, applyer string, cTime int64) error {
	return s.ApplyNamespace(cId, dbName, tableName, applyer, cTime, TABLE_NAME_CONFIGURE_NSP)
}

func (s *Service) UpdateConfigureNsp(applyId, applyer string) error {
	return s.UpdateNsp(applyId, applyer, TABLE_NAME_CONFIGURE_NSP)
}

func (s *Service) DeleteConfigureNsp(ids []string) error {
	return s.DeleteNsp(ids, TABLE_NAME_CONFIGURE_NSP)
}

func (s *Service) GetConfigureClusterList() ([]*models.ClusterInfo, error) {
	clusterId := s.config.ConfigureClusterId
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}
	var clusters []*models.ClusterInfo
	clusters = append(clusters, info)
	return clusters, nil
}

//go by http command
func (s *Service) GetAllConfigure(clusterId int, dbName, tableName string, pageInfo *models.PagerInfo) ([]*models.ConfigureShow, error) {
	info, err := s.selectClusterById(clusterId)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, common.CLUSTER_NOTEXISTS_ERROR
	}

	log.Debug("get all configure list under clusterId:%v dbName:%v tableName:%v", clusterId, dbName, tableName)

	filter := new(models.Filter_)
	if pageInfo != nil {
		if pageInfo.SortName != "" && pageInfo.SortOrder != "" {
			var descFlag bool
			switch pageInfo.SortOrder {
			case "asc":
				descFlag = true
			default:
				descFlag = false
			}
			order := &models.Order{By: pageInfo.SortName, Desc: descFlag}
			filter.Order = []*models.Order{order}
		} else {
			order := &models.Order{By: "create_time", Desc: true}
			filter.Order = []*models.Order{order}
		}
		if pageInfo.PageIndex > 0 && pageInfo.PageSize > 0 {
			filter.Limit = &models.Limit_{Offset: uint64(pageInfo.GetPageOffset()), RowCount: uint64(pageInfo.GetPageSize())}
		}
	}

	setQueryRep := &models.Query{
		DatabaseName: dbName,
		TableName:    tableName,
		Command: &models.Command{
			Type:   "get",
			Field:  []string{"k", "v", "version", "extend"},
			Filter: filter,
		},
	}
	var reply models.Reply
	if err := sendPostReqJsonBody(info.GatewayHttpUrl, "/kvcommand", setQueryRep, &reply); err != nil {
		return nil, err
	}
	if reply.Code == 5 {//table no exist
		return nil, nil
	} else if reply.Code != 0 {
		log.Error("get cluster[%d] configure list failed. err:[%v]", clusterId, reply)
		return nil, &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: reply.Message}
	}

	log.Info("result: %v", reply)

	var configureInfos []*models.ConfigureShow
	for _, confInfo := range reply.Values {
		tInfo := new(models.ConfigureShow)
		tInfo.K = fmt.Sprintf("%v", confInfo[0])
		tInfo.V = fmt.Sprintf("%v", confInfo[1]) //todo parse
		tInfo.Version,_ = strconv.ParseInt(fmt.Sprintf("%v", confInfo[2]), 10, 46)
		tInfo.Extend = fmt.Sprintf("%v", confInfo[3])
		//tInfo.CreateTime, _ = strconv.ParseInt(fmt.Sprintf("%v", confInfo[4]), 10, 46)
		//tInfo.UpdTime, _ = strconv.ParseInt(fmt.Sprintf("%v", confInfo[5]), 10, 46)
		//tInfo.Creator = fmt.Sprintf("%v", confInfo[7])
		configureInfos = append(configureInfos, tInfo)
	}
	return configureInfos, nil
}

func (s *Service) AuditConfigureNsp(ids []string, status int, auditor string) error {
	for _, applyId := range ids {
		info, err := s.GetNamespaceById(applyId, TABLE_NAME_CONFIGURE_NSP)
		if err != nil {
			continue
		}
		var dbId, tableId int
		if status == STATUS_AUDIT { // 审批通过
			dbInfo, err := s.CreateDb(info.ClusterId, info.DbName)
			if err != nil {
				log.Warn("create configure db %v on cluster %v failed, err: %v", info.DbName, info.ClusterId, err)
				return err
			}
			dbId = dbInfo.Id

			if flag, _ := s.existTable(info.DbName, info.TableName, info.ClusterId); flag {
				log.Warn("configure: exist db %v table %v in cluster %v, cannot audit", info.DbName, info.TableName, info.ClusterId)
				return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: fmt.Sprintf("exist table %v in cluster %v", info.TableName, info.ClusterId)}
			}

			tableInfo, err := s.CreateTable(info.ClusterId, info.DbName, info.TableName, "", "", "", Columns, nil)
			if err != nil {
				log.Warn("create configure  table %v on cluster %v failed, err: %v", info.TableName, info.ClusterId, err)
				return err
			}
			tableId = tableInfo.Id
		}
		nspSql := fmt.Sprintf(`INSERT INTO %s (id, db_name, table_name, cluster_id, db_id, table_id, status, applyer, auditor, create_time) values ("%s", "%s", "%s", %d,  %d,  %d, %d, "%s", "%s", %d )`,
			TABLE_NAME_CONFIGURE_NSP, applyId, info.DbName, info.TableName, info.ClusterId, dbId, tableId, status, info.Applyer, auditor, info.CreateTime)
		rowsAffected, err := s.execSql(nspSql)
		if err != nil {
			return err
		}
		if rowsAffected != 1 {
			return &common.FbaseError{Code: common.INTERNAL_ERROR.Code, Msg: "update configure apply status and return result error"}
		}
	}
	log.Debug("%v audit configure namespace success, status: %v", auditor, status)
	return nil
}

//=============configure center end================

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
			Code int                 `json:"code"`
			Msg  string              `json:"message"`
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
		log.Debug("get master metric config: %v", msConfig)
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
			Code int                 `json:"code"`
			Msg  string              `json:"message"`
			Data models.MetricConfig `json:"data"`
		}{}
		if err := sendGetReq(info.GatewayHttpUrl, "/metric/config/get", reqParams, &getConfigResp); err != nil {
			gsConfig.Address = err.Error()
		} else {
			if getConfigResp.Code != 0 {
				gsConfig.Address = getConfigResp.Msg
			} else {
				gsConfig.Address = getConfigResp.Data.Address
			}
		}
		log.Debug("get gateway metric config: %v", gsConfig)
	}(gsConfig)
	waitLock.Wait()

	reply := make(map[string]*models.MetricConfig)
	reply["ms"] = msConfig
	reply["gs"] = gsConfig
	return reply, nil
}

func (s *Service) SetMetricConfig(cId int, addr, interval string) (map[string]string, error) {
	info, err := s.selectClusterById(cId)
	if err != nil {
		return nil, err
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
			Code int    `json:"code"`
			Msg  string `json:"message"`
		}{}
		if err := sendGetReq(info.MasterUrl, "/metric/config/set", reqParams, &setConfigResp); err != nil {
			respose["ms"] = err.Error()
		} else if setConfigResp.Code > 0 {
			respose["ms"] = setConfigResp.Msg
		} else {
			respose["ms"] = "success"
		}

	}(respose)

	//todo gw all addr
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
			Code int    `json:"code"`
			Msg  string `json:"message"`
		}{}
		if err := sendGetReq(info.GatewayHttpUrl, "/metric/config/set", reqParams, &setConfigResp); err != nil {
			respose["gs"] = err.Error()
		} else if setConfigResp.Code > 0 {
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

func sendPostReqJsonBody(host, uri string, params interface{}, result interface{}) (error) {
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
	//解决反序列化时，float64超过一定长度默认科学计数法表示
	d := json.NewDecoder(strings.NewReader(string(data)))
	d.UseNumber()
	if err := d.Decode(&result); err != nil {
		log.Error("Cannot parse http response in json. body:[%v]", string(data))
		return common.INTERNAL_ERROR
	}

	//if err := json.Unmarshal(data, result); err != nil {
	//	log.Error("Cannot parse http response in json. body:[%v]", string(data))
	//	return common.INTERNAL_ERROR
	//}

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
	}
}
