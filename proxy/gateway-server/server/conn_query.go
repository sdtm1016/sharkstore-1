// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package server

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"time"

	"proxy/gateway-server/errors"
	"proxy/gateway-server/mysql"
	"proxy/gateway-server/sqlparser"
	"util/hack"
	golog "util/log"
	"proxy/metric"
	"util"
)

/*处理query语句*/
func (c *ClientConn) handleQuery(sql string) (err error) {
	var slowLogThreshold util.Duration
	var method string = "other"
	start := time.Now()
	defer func() {
		if e := recover(); e != nil {
			//golog.OutputSql("Error", "err:%v,sql:%s", e, sql)
			golog.Info("err:%v,sql:%s", e, sql)

			if err, ok := e.(error); ok {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]

				golog.Error("ClientConn", "handleQuery",
					err.Error(), 0,
					"stack", string(buf), "sql", sql)
			}

			if err == nil {
				err = fmt.Errorf("server run panic(%v)", e)
			}
		}
		delay := time.Now().Sub(start)
		if err != nil {
			metric.GsMetric.ProxyApiMetric(method, false, delay)
			metric.GsMetric.ErrorLogMetric(sql)
			golog.Warn("run sql [%v] error log: [%v]", sql, err)
		} else {
			metric.GsMetric.ProxyApiMetric(method, true, delay)
		}
		if delay > slowLogThreshold.Duration {
			metric.GsMetric.SlowLogMetric(sql, delay)
			golog.Warn("run sql [%v] slow log", sql)
		}
	}()

	sql = strings.TrimRight(sql, ";") //删除sql语句最后的分号
	//	hasHandled, err := c.preHandleShard(sql)
	//	if err != nil {
	//		golog.Error("server", "preHandleShard", err.Error(), 0,
	//			"sql", sql,
	//			"hasHandled", hasHandled,
	//		)
	//		return err
	//	}
	//	if hasHandled {
	//		return nil
	//	}

	var stmt sqlparser.Statement
	stmt, err = sqlparser.Parse(sql) //解析sql语句,得到的stmt是一个interface
	if err != nil {
		golog.Error("server parse sql:%s,err:%s", sql, err.Error())
		return err
	}
	if golog.GetFileLogger().IsEnableDebug() {
		golog.Debug("type:%s,sql:%s", reflect.TypeOf(stmt), sql)
	}

	switch v := stmt.(type) {
	case *sqlparser.Select:
		method = "select"
		slowLogThreshold = c.server.cfg.Performance.SelectSlowLog
		err = c.handleSelect(v, nil)
	case *sqlparser.Insert:
		method = "insert"
		slowLogThreshold = c.server.cfg.Performance.InsertSlowLog
		err = c.handleInsert(v, nil)
	case *sqlparser.Update:
		err = c.handleExec(stmt, nil, "Update")
	case *sqlparser.Delete:
		method = "delete"
		slowLogThreshold = c.server.cfg.Performance.DeleteSlowLog
		err = c.handleDelete(v, nil)
	case *sqlparser.Replace:
		err = c.handleExec(stmt, nil, "Replace")
	case *sqlparser.Set:
		err = c.handleSet(v, sql)
	case *sqlparser.Begin:
		err = c.handleBegin()
	case *sqlparser.Commit:
		err = c.handleCommit()
	case *sqlparser.Rollback:
		err = c.handleRollback()
	case *sqlparser.Admin:
		err = c.handleAdmin(v)
	case *sqlparser.UseDB:
		err = c.handleUseDB(v.DB)
	case *sqlparser.SimpleSelect:
		err = c.handleSimpleSelect(v)
	case *sqlparser.Truncate:
		err = c.handleExec(stmt, nil,"Truncate")
	case *sqlparser.Describe:
		err = c.handleDescribe(v)
	default:
		err = fmt.Errorf("statement %T not support now", v)
	}
	return err
}

func (c *ClientConn) newEmptyResultset(stmt *sqlparser.Select) *mysql.Resultset {
	r := new(mysql.Resultset)
	r.Fields = make([]*mysql.Field, len(stmt.SelectExprs))

	for i, expr := range stmt.SelectExprs {
		r.Fields[i] = &mysql.Field{}
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			r.Fields[i].Name = []byte("*")
		case *sqlparser.NonStarExpr:
			if e.As != nil {
				r.Fields[i].Name = e.As
				r.Fields[i].OrgName = hack.Slice(nstring(e.Expr))
			} else {
				r.Fields[i].Name = hack.Slice(nstring(e.Expr))
			}
		default:
			r.Fields[i].Name = hack.Slice(nstring(e))
		}
	}

	r.Values = make([][]interface{}, 0)
	r.RowDatas = make([]mysql.RowData, 0)

	return r
}

func (c *ClientConn) handleInsert(stmt *sqlparser.Insert, args []interface{}) error {
	if len(c.db) == 0 {
		return errors.ErrNoDatabase
	}
	if golog.GetFileLogger().IsEnableDebug() {
		golog.Debug("table:%v,cols:%v,rows:%v, args:%v", stmt.Table, stmt.Columns, stmt.Rows, args)
	}
	ret, err := c.server.proxy.HandleInsert(c.db, stmt, args)
	if err != nil {
		golog.Error("insert failed, err[%v]", err)
		return c.writeError(err)
	}
	golog.Debug("insert success")
	return c.writeOK(ret)
}

func (c *ClientConn) handleDelete(stmt *sqlparser.Delete, args []interface{}) error {
	if len(c.db) == 0 {
		return errors.ErrNoDatabase
	}
	if golog.GetFileLogger().IsEnableDebug() {
		golog.Debug("table:%v,where:%v, args:%v", stmt.Table, stmt.Where, args)
	}
	ret, err := c.server.proxy.HandleDelete(c.db, stmt, args)
	if err != nil {
		return err
	}
	//TODO:return execut nums
	return c.writeOK(ret)
}

func (c *ClientConn) handleExec(stmt sqlparser.Statement, args []interface{}, statement string) error {
	return  fmt.Errorf("statement %s not support now", statement)
}

func (c *ClientConn) handleDescribe(stmt *sqlparser.Describe) error {
	if len(c.db) == 0 {
		return errors.ErrNoDatabase
	}

	res, err := c.server.proxy.HandleDescribe(c.db, stmt)
	if err != nil {
		golog.Error("handle describe failed(%v), table: %s", err, string(stmt.TableName))
		return c.writeError(err)
	}

	return c.writeResultset(res.Status, res.Resultset)
}

func (c *ClientConn) handleTruncate(stmt *sqlparser.Truncate) error {
	if len(c.db) == 0 {
		return errors.ErrNoDatabase
	}

	res, err := c.server.proxy.HandleTruncate(c.db, stmt)
	if err != nil {
		if stmt != nil && stmt.Table != nil {
			golog.Error("handle truncate failed(%v), table: %s", err, hack.String(stmt.Table.Name))
		} else {
			golog.Error("handle truncate failed(%v), table[empty]", err)
		}
		return c.writeError(err)
	}

	return c.writeResultset(res.Status, res.Resultset)
}
