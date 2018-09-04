package server

import (
	"bytes"
	"errors"
	"fmt"
	"util"

	"encoding/base64"

	"proxy/gateway-server/mysql"
	"proxy/gateway-server/sqlparser"
	"proxy/store/dskv"
	"util/hack"
	"util/log"

	"golang.org/x/net/context"
)

// HandleTruncate truncate table
func (p *Proxy) HandleTruncate(db string, stmt *sqlparser.Truncate) (*mysql.Result, error) {
	var tableName string
	if stmt != nil && stmt.Table != nil {
		tableName = string(stmt.Table.Name)
	}

	t := p.router.FindTable(db, tableName)
	if t == nil {
		log.Error("[truncate] table %s.%s doesn.t exist", db, tableName)
		return nil, fmt.Errorf("Table '%s.%s' doesn't exist", db, tableName)
	}

	// TODO:
	return nil, errors.New("not implement")
}

// HandleDescribe decribe table
func (p *Proxy) HandleDescribe(db string, stmt *sqlparser.Describe) (*mysql.Result, error) {
	tableName := string(stmt.TableName)

	t := p.router.FindTable(db, tableName)
	if t == nil {
		log.Error("[describe] table %s.%s doesn.t exist", db, tableName)
		return nil, fmt.Errorf("Table '%s.%s' doesn't exist", db, tableName)
	}

	cols := t.GetAllColumns()
	if len(cols) == 0 {
		log.Error("[describe] table %s.%s invalid columns(null)", db, tableName)
		return nil, fmt.Errorf("table %s.%s invalid columns", db, tableName)
	}

	fieldNames := []string{"Field", "Type", "Null", "Key", "Default", "Extra"}
	values := make([][]interface{}, len(cols))
	for i, col := range cols {
		nullable := "NO"
		if col.Nullable {
			nullable = "YES"
		}
		key := ""
		if col.PrimaryKey == 1 {
			key = "PRI"
		}
		values[i] = []interface{}{col.Name, col.DataType.String(), nullable, key, col.DefaultValue, col.Id}
	}

	r, err := buildResultset(nil, fieldNames, values)
	if err != nil {
		log.Error("build describe result set failed(%v), columns: %v, values: %v", err, fieldNames, values)
		return nil, err
	}

	result := &mysql.Result{
		Status:       0,
		AffectedRows: 0,
		Resultset:    r,
	}
	return result, nil
}

func (p *Proxy) HandleAdmin(db string, cmd string, args []string) (*mysql.Result, error) {
	if log.GetFileLogger().IsEnableDebug() {
		log.Debug("handle admin. db=%s, args=%s", db, args)
	}
	if len(args) == 0 || args[0] == "help" {
		return p.handleAdminHelp()
	}

	switch cmd {
	case "route":
		return p.handleAdminRoute(db, args)
	}

	return nil, fmt.Errorf("not implement")
}

func (p *Proxy) handleAdminHelp() (*mysql.Result, error) {
	return nil, fmt.Errorf("not implement")
}

// Usage
// 列举某个表的路由信息:
// 	   admin route('show', 'mytable')
// 查询某个表某个主键的路由信息:
// 	  admin route('show', 'mytable', 'pk1 value', 'pk2value', ... )
func (p *Proxy) handleAdminRoute(db string, args []string) (*mysql.Result, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("admin route: subcommand is required")
	}
	subCmd := args[0]

	if len(args) < 2 {
		return nil, fmt.Errorf("admin route: table name is required")
	}
	tableName := args[1]
	t := p.router.FindTable(db, tableName)
	if t == nil {
		log.Error("[admin] table %s.%s doesn.t exist", db, tableName)
		return nil, fmt.Errorf("Table '%s.%s' doesn't exist", db, tableName)
	}

	switch subCmd {
	case "show":
		return p.handleAdminRouteShow(t, args[2:])
	default:
		return nil, fmt.Errorf("admin rounte: unknown subcommand(%v)", subCmd)
	}
}

func (p *Proxy) handleAdminRouteShow(t *Table, keys []string) (*mysql.Result, error) {
	pks := t.PKS()
	if len(keys) > len(pks) {
		return nil, fmt.Errorf("too many primary key's values(%d > %d)", len(keys), len(pks))
	}

	buf := util.EncodeStorePrefix(util.Store_Prefix_KV, t.GetId())
	var err error
	for i, key := range keys {
		col := t.FindColumn(pks[i])
		if col == nil {
			return nil, fmt.Errorf("could not find column(%v) in Table %s.%s", pks[i], t.DbName(), t.Name())
		}
		if buf, err = util.EncodePrimaryKey(buf, col, hack.Slice(key)); err != nil {
			return nil, err
		}
	}

	var routes []*dskv.KeyLocation
	searchKey := buf
	maxSearch := 20
	for {
		bo := dskv.NewBackoffer(dskv.MsMaxBackoff, context.Background())
		route, err := t.ranges.LocateKey(bo, searchKey)
		if err != nil {
			return nil, fmt.Errorf("locate route failed: %v", err)
		}
		if route == nil {
			break
		}

		routes = append(routes, route)
		if bytes.Compare(route.EndKey, buf) > 0 || len(routes) >= maxSearch {
			break
		}
		searchKey = append([]byte(nil), route.EndKey...)
	}

	fieldNames := []string{"ID", "StartKey", "EndKey", "LeaderID", "LeaderAddr", "Version"}
	if len(routes) == 0 {
		return &mysql.Result{
			Status:       0,
			AffectedRows: 0,
			Resultset:    newEmptyResultSet(fieldNames),
		}, nil
	}

	values := make([][]interface{}, len(routes))
	bo := dskv.NewBackoffer(dskv.MsMaxBackoff, context.Background())
	for i, r := range routes {
		leaderAddr, _ := t.ranges.GetNodeAddr(bo, r.NodeId)
		values[i] = []interface{}{r.Region.Id, formatRouteKey(r.StartKey), formatRouteKey(r.EndKey),
			r.NodeId, leaderAddr, fmt.Sprintf("%d:%d", r.Region.Cer, r.Region.ConfVer)}
	}
	rs, err := buildResultset(nil, fieldNames, values)
	if err != nil {
		log.Error("build admin route show result failed(%v), columns: %v, values: %v", err, fieldNames, values)
		return nil, err
	}
	result := &mysql.Result{
		Status:       0,
		AffectedRows: 0,
		Resultset:    rs,
	}
	return result, nil
}

func formatRouteKey(key []byte) []byte {
	dst := make([]byte, base64.StdEncoding.EncodedLen(len(key)))
	base64.StdEncoding.Encode(dst, key)
	return dst
}
