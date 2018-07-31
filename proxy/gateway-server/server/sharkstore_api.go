package server

import (
	"util/log"
	"encoding/json"
	"fmt"
	"time"
	"util"
	"proxy/store/dskv"
	"strings"
	"net/http"
	"console/common"
	"io/ioutil"
	"context"
	"model/pkg/kvrpcpb"
	"model/pkg/timestamp"
	"model/pkg/metapb"
	"net"
	"pkg-go/ds_client"
)

type SharkStoreApi struct {
}

func (api *SharkStoreApi) Insert(s *Server, dbName string, tableName string, fields []string, values [][]interface{}) *Reply {

	cmd := &Command{
		Type:   "set",
		Field:  fields,
		Values: values,
	}
	query := &Query{
		Command: cmd,
	}

	return api.execute(s, dbName, tableName, query)

}

func (api *SharkStoreApi) Select(s *Server, dbName string, tableName string, fields []string, pks map[string]interface{}, limit_ *Limit_) *Reply {
	ands := make([]*And, 0)
	for k, v := range pks {
		and := &And{
			Field:  &Field_{Column: k, Value: v},
			Relate: "=",
		}
		ands = append(ands, and)

	}
	cmd := &Command{
		Type:  "get",
		Field: fields,
		Filter: &Filter_{
			And: ands,
		},
	}
	if limit_ != nil {
		cmd.Filter.Limit = limit_
	}
	query := &Query{
		Command: cmd,
	}

	return api.execute(s, dbName, tableName, query)
}

func (api *SharkStoreApi) Delete(s *Server, dbName string, tableName string, fields []string, pks map[string]interface{}) *Reply {
	ands := make([]*And, 0)
	for k, v := range pks {
		and := &And{
			Field:  &Field_{Column: k, Value: v},
			Relate: "=",
		}
		ands = append(ands, and)

	}
	cmd := &Command{
		Type:  "del",
		Field: fields,
		Filter: &Filter_{
			And: ands,
		},
	}
	query := &Query{
		Command: cmd,
	}

	return api.execute(s, dbName, tableName, query)
}

func (api *SharkStoreApi) execute(s *Server, dbName string, tableName string, query *Query) (reply *Reply) {
	var err error
	if len(dbName) == 0 {
		log.Error("args[dbName] wrong")
		reply = &Reply{Code: errCommandNoDb, Message: fmt.Errorf("dbName %v", ErrHttpCmdEmpty).Error()}
		return reply
	}
	if len(tableName) == 0 {
		log.Error("args[tableName] wrong")
		reply = &Reply{Code: errCommandNoTable, Message: fmt.Errorf("tablename %v", ErrHttpCmdEmpty).Error()}
		return reply
	}
	if query.Command == nil {
		log.Error("args[Command] wrong")
		reply = &Reply{Code: errCommandEmpty, Message: ErrHttpCmdEmpty.Error()}
		return reply
	}

	t := s.proxy.router.FindTable(dbName, tableName)
	if t == nil {
		log.Error("table %s.%s doesn.t exist", dbName, tableName)
		reply = &Reply{Code: errCommandNoTable, Message: ErrNotExistTable.Error()}
		return reply
	}

	start := time.Now()
	var slowLogThreshold util.Duration
	query.commandFieldNameToLower()
	switch query.Command.Type {
	case "get":
		slowLogThreshold = s.proxy.config.Performance.SelectSlowLog
		reply, err = query.getCommand(s.proxy, t)
		if err != nil {
			log.Error("getcommand error: %v", err)
			reply = &Reply{Code: errCommandRun, Message: fmt.Errorf("%v: %v", ErrHttpCmdRun, err).Error()}
		}
	case "set":
		slowLogThreshold = s.proxy.config.Performance.InsertSlowLog
		reply, err = query.setCommand(s.proxy, t)
		if err != nil {
			log.Error("setcommand error: %v", err)
			reply = &Reply{Code: errCommandRun, Message: fmt.Errorf("%v: %v", ErrHttpCmdRun, err).Error()}
		}
	case "del":
		slowLogThreshold = s.proxy.config.Performance.SelectSlowLog
		reply, err = query.delCommand(s.proxy, t)
		if err != nil {
			log.Error("delcommand error: %v", err)
			reply = &Reply{Code: errCommandRun, Message: fmt.Errorf("%v: %v", ErrHttpCmdRun, err).Error()}
		}
	default:
		log.Error("unknown command")
		reply = &Reply{Code: errCommandUnknown, Message: ErrHttpCmdUnknown.Error()}
	}

	delay := time.Since(start)
	if delay > slowLogThreshold.Duration {
		cmd, _ := json.Marshal(query)

		log.Warn("[kvcommand slow log %v %v ", delay.String(), string(cmd))
	}

	return reply
}


func (api *SharkStoreApi) DoDsSelect(s *Server, dbName string, tableName string, fields []string, pks map[string]interface{}) {
	if len(dbName) == 0 {
		log.Error("args[dbName] wrong")
		return
	}
	if len(tableName) == 0 {
		log.Error("args[tableName] wrong")
		return
	}

	ands := make([]*And, 0)
	for k, v := range pks {
		and := &And{
			Field:  &Field_{Column: k, Value: v},
			Relate: "=",
		}
		ands = append(ands, and)

	}
	cmd := &Command{
		Type:  "get",
		Field: fields,
		Filter: &Filter_{
			And: ands,
		},
	}
	query := &Query{
		Command: cmd,
	}
	if query.Command == nil {
		log.Error("args[Command] wrong")
		return
	}

	t := s.proxy.router.FindTable(dbName, tableName)
	if t == nil {
		log.Error("table %s.%s doesn't exist", dbName, tableName)
		return
	}

	query.commandFieldNameToLower()
	if query.Command.Type != "get" {
		log.Error("args[Command] type must be 'get'")
		return
	}

	matches, err := query.parseMatchs(ands)
	if err != nil {
		log.Error("parse matches error: %v", err)
		return
	}
	pbMatches, err := makePBMatches(t, matches)
	if err != nil {
		log.Error("make pb matches error: %v", err)
		return
	}
	key, _, err := findPKScope(t, pbMatches)
	if err != nil {
		log.Error("find pk scope error: %v", err)
		return
	}

	log.Debug("start getting route of key: %q", key)
	route, err := getRouteOfKey(s.proxy, t, key)
	if err != nil {
		log.Error("cannot get route of key: %q, err: %v", key, err)
		return
	}
	rangeOfKey := route.Range
	log.Debug("got the range[rangeId=%d] of pks: %v, key: %q", rangeOfKey.Id, pks, key)

	leaderPeer := route.Leader
	leaderNode, err := s.proxy.router.cli.GetNode(leaderPeer.NodeId)
	if err != nil {
		log.Error("get node from MS of key: %q, err: %v", key, err)
		return
	}
	leaderNodeId := leaderPeer.NodeId
	leaderNodeAddr := leaderNode.GetServerAddr()

	log.Debug("rangeId[%d], the leader [nodeId=%d, nodeAddr=%s, peerId=%d]", rangeOfKey.Id, leaderNodeId, leaderNodeAddr, leaderPeer.Id)

	dsResponse, err := readFromNodeDirectly(s.proxy, query, t, rangeOfKey, leaderNodeAddr)
	if err != nil {
		log.Error("read from ds node[%s] err: %v", leaderNodeAddr, err)
		return
	}
	if dsResponse.GetHeader().GetError() != nil {
		log.Error("read from ds node[%s], header err: %v", leaderNodeAddr, dsResponse.GetHeader().GetError())
		return
	}

	if dsResponse.GetResp().GetCode() != 0 {
		log.Error("read from ds node[%s], code[%d] not 0.", leaderNodeAddr, dsResponse.GetResp().GetCode())
		return
	}

	log.Info("select from ds: [OK]. range info: [rangeId=%d, leaderNodeId=%d, leaderNodeAddr=%s]", rangeOfKey.Id, leaderNodeId, leaderNodeAddr)
}


func (api *SharkStoreApi) DoMigration(s *Server, dbName string, tableName string, clusterCf ClusterConfig, fields []string, pks map[string]interface{}) {
	if len(dbName) == 0 {
		log.Error("args[dbName] wrong")
		return
	}
	if len(tableName) == 0 {
		log.Error("args[tableName] wrong")
		return
	}

	ands := make([]*And, 0)
	for k, v := range pks {
		and := &And{
			Field:  &Field_{Column: k, Value: v},
			Relate: "=",
		}
		ands = append(ands, and)

	}
	cmd := &Command{
		Type:  "get",
		Field: fields,
		Filter: &Filter_{
			And: ands,
		},
	}
	query := &Query{
		Command: cmd,
	}
	if query.Command == nil {
		log.Error("args[Command] wrong")
		return
	}

	t := s.proxy.router.FindTable(dbName, tableName)
	if t == nil {
		log.Error("table %s.%s doesn't exist", dbName, tableName)
		return
	}

	query.commandFieldNameToLower()
	if query.Command.Type != "get" {
		log.Error("args[Command] type must be 'get'")
		return
	}

	matches, err := query.parseMatchs(ands)
	if err != nil {
		log.Error("parse matches error: %v", err)
		return
	}
	pbMatches, err := makePBMatches(t, matches)
	if err != nil {
		log.Error("make pb matches error: %v", err)
		return
	}
	key, _, err := findPKScope(t, pbMatches)
	if err != nil {
		log.Error("find pk scope error: %v", err)
		return
	}

	log.Info("start getting route of key: %q", key)
	route, err := getRouteOfKey(s.proxy, t, key)
	if err != nil {
		log.Error("cannot get route of key: %q, err: %v", key, err)
		return
	}
	rangeOfKey := route.Range
	log.Info("got the range[rangeId=%d] of key: %q", rangeOfKey.Id, key)

	preLeaderPeer := route.Leader
	preNode, err := s.proxy.router.cli.GetNode(preLeaderPeer.NodeId)
	if err != nil {
		log.Error("get node from MS of key: %q, err: %v", key, err)
		return
	}
	preNodeId := preLeaderPeer.NodeId
	preNodeAddr := preNode.GetServerAddr()

	log.Info("ready to migrate, the leader [nodeId=%d, nodeAddr=%s, peerId=%d]", preNodeId, preNodeAddr, preLeaderPeer.Id)

	// migrate
	err = callHttpMigration(clusterCf, rangeOfKey.Id, preLeaderPeer.Id)
	if err != nil {
		log.Error("migration err: %v", err)
		return
	}

	// check migration
	currNodeAddr := checkLeaderChanged(s.proxy, t, key, preNodeId)
	if currNodeAddr != "" {
		log.Info("migration completed. leader has changed from [%s] to [%s]", preNodeAddr, currNodeAddr)
	}

	// check previous leader
	prevDSResponse, err := readFromNodeDirectly(s.proxy, query, t, rangeOfKey, preNodeAddr)
	//log.Info("prevDSResponse: %v", prevDSResponse)
	if err != nil {
		log.Error("read from previous node[%s] err: %q", preNodeAddr, err)
		return
	}
	if prevDSResponse.GetHeader().GetError() != nil {
		if prevDSResponse.GetHeader().GetError().GetRangeNotFound() != nil {
			log.Info("check passed, previous node[%s] has no key[%q] anymore, rangeId=%d", preNodeAddr, key, rangeOfKey.Id)
		}
	} else {
		if prevDSResponse.GetResp().GetCode() != 0 {
			log.Info("check passed, previous node[%s] has no key[%q] anymore, rangeId=%d", preNodeAddr, key, rangeOfKey.Id)
		} else {
			log.Warn("check maybe failed, previous node[%s] still has key[%q], rangeId=%d", preNodeAddr, key, rangeOfKey.Id)
		}
	}

	// check curr leader
	currDSResponse, err := readFromNodeDirectly(s.proxy, query, t, rangeOfKey, currNodeAddr)
	//log.Info("currDSResponse: %v", currDSResponse)
	if err != nil {
		log.Error("read from current node[%s] err: %q", currNodeAddr, err)
		return
	}
	if currDSResponse.GetHeader().GetError() != nil {
		if currDSResponse.GetHeader().GetError().GetRangeNotFound() != nil {
			log.Info("check maybe failed, current node[%s] doesn't have key[%q], rangeId=%d", currNodeAddr, key, rangeOfKey.Id)
		}
	} else {
		if currDSResponse.GetResp().GetCode() == 0 {
			log.Info("check passed, current node[%s] has the key[%q], rangeId=%d", currNodeAddr, key, rangeOfKey.Id)
		} else {
			log.Warn("check maybe failed, current node[%s] doesn't have key[%q], rangeId=%d", currNodeAddr, key, rangeOfKey.Id)
		}
	}

}


func (api *SharkStoreApi) DoMigrationAfterUpdating(s *Server, dbName string, tableName string, clusterCf ClusterConfig, fields []string, pks map[string]interface{}, values [][]interface{}) {
	if len(dbName) == 0 {
		log.Error("args[dbName] wrong")
		return
	}
	if len(tableName) == 0 {
		log.Error("args[tableName] wrong")
		return
	}

	ands := make([]*And, 0)
	for k, v := range pks {
		and := &And{
			Field:  &Field_{Column: k, Value: v},
			Relate: "=",
		}
		ands = append(ands, and)

	}
	cmd := &Command{
		Type:  "get",
		Field: fields,
		Filter: &Filter_{
			And: ands,
		},
	}
	query := &Query{
		Command: cmd,
	}
	if query.Command == nil {
		log.Error("args[Command] wrong")
		return
	}

	t := s.proxy.router.FindTable(dbName, tableName)
	if t == nil {
		log.Error("table %s.%s doesn't exist", dbName, tableName)
		return
	}

	query.commandFieldNameToLower()
	if query.Command.Type != "get" {
		log.Error("args[Command] type must be 'get'")
		return
	}

	matches, err := query.parseMatchs(ands)
	if err != nil {
		log.Error("parse matches error: %v", err)
		return
	}
	pbMatches, err := makePBMatches(t, matches)
	if err != nil {
		log.Error("make pb matches error: %v", err)
		return
	}
	key, _, err := findPKScope(t, pbMatches)
	if err != nil {
		log.Error("find pk scope error: %v", err)
		return
	}

	log.Info(" \n ----- start getting route of key: %q", key)
	route, err := getRouteOfKey(s.proxy, t, key)
	if err != nil {
		log.Error("cannot get route of key: %q, err: %v", key, err)
		return
	}
	rangeOfKey := route.Range
	log.Info("got the range[rangeId=%d] of key: %q", rangeOfKey.Id, key)

	preLeaderPeer := route.Leader
	preNode, err := s.proxy.router.cli.GetNode(preLeaderPeer.NodeId)
	if err != nil {
		log.Error("get node from MS of key: %q, err: %v", key, err)
		return
	}
	preNodeId := preLeaderPeer.NodeId
	preNodeAddr := preNode.GetServerAddr()

	log.Info("ready to update data before migration. values: %v", values)
	// update
	insertReply := api.Insert(s, dbName, tableName, fields, values)
	if insertReply == nil {
		log.Error("update value err, reply is nil.")
		return
	}

	if insertReply.Code != 0 {
		log.Error("update value err, reply code is not 0.")
		return
	}

	// select to make sure it's changed before migration
	selectReply := api.Select(s, dbName, tableName, fields, pks, nil)
	if selectReply == nil {
		log.Error("select error after updating, reply is nil.")
		return
	}
	if selectReply.Code != 0 {
		log.Error("select error after updating, reply code is not 0.")
		return
	}
	//for i, v := range selectReply.Values {
	//	for j := range v {
	//		if values[i][j] != selectReply.Values[i][j] {
	//			log.Error("the selected value not equals the inserted one.")
	//			return
	//		}
	//	}
	//}
	log.Info("the value has been updated successfully. ")

	log.Info("ready to migrate, the leader [nodeId=%d, nodeAddr=%s, peerId=%d]", preNodeId, preNodeAddr, preLeaderPeer.Id)

	// migrate
	err = callHttpMigration(clusterCf, rangeOfKey.Id, preLeaderPeer.Id)
	if err != nil {
		log.Error("migration err: %v", err)
		return
	}

	// check migration
	currNodeAddr := checkLeaderChanged(s.proxy, t, key, preNodeId)
	if currNodeAddr != "" {
		log.Info("migration completed. leader has changed from [%s] to [%s]", preNodeAddr, currNodeAddr)
	}

	// check previous leader
	prevDSResponse, err := readFromNodeDirectly(s.proxy, query, t, rangeOfKey, preNodeAddr)
	//log.Info("prevDSResponse: %v", prevDSResponse)
	if err != nil {
		log.Error("read from previous node[%s] err: %q", preNodeAddr, err)
		return
	}
	if prevDSResponse.GetHeader().GetError() != nil {
		if prevDSResponse.GetHeader().GetError().GetRangeNotFound() != nil {
			log.Info("check passed, previous node[%s] has no key[%q] anymore, rangeId=%d", preNodeAddr, key, rangeOfKey.Id)
		}
	} else {
		if prevDSResponse.GetResp().GetCode() != 0 {
			log.Info("check passed, previous node[%s] has no key[%q] anymore, rangeId=%d", preNodeAddr, key, rangeOfKey.Id)
		} else {
			log.Warn("check maybe failed, previous node[%s] still has key[%q], rangeId=%d", preNodeAddr, key, rangeOfKey.Id)
		}
	}

	// check curr leader
	currDSResponse, err := readFromNodeDirectly(s.proxy, query, t, rangeOfKey, currNodeAddr)
	//log.Info("currDSResponse: %v", currDSResponse)
	if err != nil {
		log.Error("read from current node[%s] err: %q", currNodeAddr, err)
		return
	}
	if currDSResponse.GetHeader().GetError() != nil {
		if currDSResponse.GetHeader().GetError().GetRangeNotFound() != nil {
			log.Info("check maybe failed, current node[%s] doesn't have key[%q], rangeId=%d", currNodeAddr, key, rangeOfKey.Id)
		}
	} else {
		if currDSResponse.GetResp().GetCode() == 0 {
			log.Info("check passed, current node[%s] has the key[%q], rangeId=%d", currNodeAddr, key, rangeOfKey.Id)
		} else {
			log.Warn("check maybe failed, current node[%s] doesn't have key[%q], rangeId=%d", currNodeAddr, key, rangeOfKey.Id)
		}
	}

}

func getRouteOfKey(proxy *Proxy, t *Table, key []byte) (*metapb.Route, error) {
	routes, err := proxy.router.cli.GetRoute(t.Table.DbId, t.Table.Id, key)
	if err != nil {
		err = fmt.Errorf("get routes from MS failed, key: %q, err: %v", key, err)
		return nil, err
	}
	if len(routes) == 0 {
		err = fmt.Errorf("routes len=0 for key %q", key)
		return nil, err
	}
	for _, r := range routes {
		if len(r.GetRange().GetPeers()) == 0 {
			err = fmt.Errorf("receive Range with no peer")
			return nil, err
		}
	}
	return routes[0], nil
}

func readFromNodeDirectly(proxy *Proxy, query *Query, t *Table, rng *metapb.Range, nodeAddr string) (*kvrpcpb.DsSelectResponse, error) {
	kvProxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(kvProxy)
	kvProxy.Init(proxy.dsCli, proxy.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)

	ctx, _ := context.WithTimeout(context.Background(), 20*time.Second)
	now := proxy.clock.Now()
	pbLimit, err := makePBLimit(proxy, query.parseLimit())
	columns := query.parseColumnNames()
	fieldList := make([]*kvrpcpb.SelectField, 0, len(columns))
	for _, c := range columns {
		col := t.FindColumn(c)
		if col != nil {
			fieldList = append(fieldList, &kvrpcpb.SelectField{
				Typ:    kvrpcpb.SelectField_Column,
				Column: col,
			})
		}
	}

	matches, err := query.parseMatchs(query.Command.Filter.And)
	if err != nil {
		return nil, err
	}
	pbMatches, err := makePBMatches(t, matches)
	if err != nil {
		return nil, err
	}
	key, scope, err := findPKScope(t, pbMatches)
	if err != nil {
		return nil, err
	}

	sreq := &kvrpcpb.SelectRequest{
		Key:          key,
		Scope:        scope,
		FieldList:    fieldList,
		WhereFilters: pbMatches,
		Limit:        pbLimit,
		Timestamp:    &timestamp.Timestamp{WallTime: now.WallTime, Logical: now.Logical},
	}
	dsSelectRequest := &kvrpcpb.DsSelectRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    sreq,
	}
	dsSelectRequest.Header.RangeId = rng.Id
	dsSelectRequest.Header.RangeEpoch = rng.GetRangeEpoch()
	dsSelectRequest.Header.Timestamp = &timestamp.Timestamp{WallTime: now.WallTime, Logical: now.Logical}

	log.Debug("ready to select from ds[%s], request[%v]", nodeAddr, dsSelectRequest)
	dSResponse, err := kvProxy.Cli.Select(ctx, nodeAddr, dsSelectRequest)
	if err != nil {
		return nil, err
	}

	return dSResponse, nil
}

func callHttpMigration(clusterCf ClusterConfig, rangeId uint64, peerId uint64) error {
	// migrate

	ts := time.Now().Unix()
	sign := common.CalcMsReqSign(int(clusterCf.ID), clusterCf.Token, ts)

	reqParams := make(map[string]interface{})
	reqParams["d"] = ts
	reqParams["s"] = sign
	reqParams["rangeId"] = rangeId
	reqParams["peerId"] = peerId

	var transferRangeResp = struct {
		Code int    `json:"code"`
		Msg  string `json:"message"`
	}{}
	ip, _, err  := net.SplitHostPort(clusterCf.ServerAddr[0])
	if err != nil {
		log.Error("parse ip/port err: ", err)
		return err
	}
	if err := sendGetReq("http://" + ip + ":8887", "/manage/range/transfer", reqParams, &transferRangeResp); err != nil {
		return err
	}
	if transferRangeResp.Code != 0 {
		err := fmt.Errorf("transfer range[%d] peer[%v] of cluster %d failed. err:[%v], errCode not 0", rangeId, peerId, clusterCf.ID, transferRangeResp)
		return err
	}

	return nil
}

func checkLeaderChanged(proxy *Proxy, table *Table, key []byte, preNodeId uint64) string {
	for {
		routes, err := proxy.router.cli.GetRoute(table.DbId, table.Id, key)
		if err != nil {
			err = fmt.Errorf("get routes from MS failed, key: %q, err: %v", key, err)
			continue
		}
		if len(routes) == 0 {
			err = fmt.Errorf("routes len=0 for key %q", key)
			continue
		}
		for _, r := range routes {
			if len(r.GetRange().GetPeers()) == 0 {
				err = fmt.Errorf("receive Range with no peer")
				continue
			}
		}
		leaderNodeId := routes[0].Leader.NodeId
		if leaderNodeId != preNodeId {
			currNode, err := proxy.router.cli.GetNode(routes[0].Leader.NodeId)
			if err != nil {
				continue
			}
			return currNode.GetServerAddr()
		}

		time.Sleep(time.Duration(200) * time.Millisecond)
	}
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