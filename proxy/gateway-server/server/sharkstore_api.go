package server

import (
	"util/log"
	"encoding/json"
	"fmt"
	"time"
	"util"
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

func (api *SharkStoreApi) Select(s *Server, dbName string, tableName string, fields []string, pks map[string]interface{}) *Reply {
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

	return api.execute(s, dbName, tableName, query)
}

func (api *SharkStoreApi) Delete() *Reply {
	return nil
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
