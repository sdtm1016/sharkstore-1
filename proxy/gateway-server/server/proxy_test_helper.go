package server

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	dsClient "pkg-go/ds_client"
	msClient "pkg-go/ms_client"
	"proxy/gateway-server/mysql"
	"proxy/gateway-server/sqlparser"
	"model/pkg/metapb"
	"util/assert"
	"util/hlc"
	"proxy/store/dskv/mock_ms"
	"proxy/store/dskv/mock_ds"
	"proxy/store/dskv"

	"golang.org/x/net/context"
	"util/deepcopy"
	"os"
	"util/log"
)

const testDBName = "testdb"
const testTableName = "testTable"
const dsPath = "/tmp/sharkstore/data"
const dsPath1 = "/tmp/sharkstore/data1"
const logPath = "/tmp/sharkstore/logs"

var MockDs *mock_ds.DsRpcServer
var MockDs1 *mock_ds.DsRpcServer
var MockMs *mock_ms.Cluster


type columnInfo struct {
	name       string
	typ        metapb.DataType
	isPK       bool
	isUnsigned bool
	autoInc	   bool
}

func bytesPrefix(prefix []byte) ([]byte, []byte) {
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return prefix, limit
}

// 创建一个只处理一个表的Proxy
func newTestProxy(db *metapb.DataBase, table *metapb.Table, rng_ *metapb.Range) *Proxy {
	rng :=deepcopy.Iface(rng_).(*metapb.Range)
	node := &metapb.Node{Id: 1, ServerAddr: "127.0.0.1:6060"}
	node1 := &metapb.Node{Id: 2, ServerAddr: "127.0.0.1:6061"}
	ms := mock_ms.NewCluster("127.0.0.1:8887", "127.0.0.1:18887")
	ms.SetDb(db)
	ms.SetTable(table)
	ms.SetNode(node)
	ms.SetNode(node1)
	ms.SetRange(rng)
	go ms.Start()
	MockMs = ms
	time.Sleep(time.Second)
	ds := mock_ds.NewDsRpcServer("127.0.0.1:6060", dsPath)
	ds.SetRange(rng)
	go ds.Start()
	MockDs = ds
	time.Sleep(time.Second)
	ds1 := mock_ds.NewDsRpcServer("127.0.0.1:6061", dsPath1)
	go ds1.Start()
	MockDs1 = ds1
	time.Sleep(time.Second)
	ctx, cancel := context.WithCancel(context.Background())

	var taskQueues []chan Task
	for i := 0; i < int(1); i++ {
		queue := make(chan Task, 1)
		taskQueues = append(taskQueues, queue)
	}
	cli, err := msClient.NewClient([]string{"127.0.0.1:8887"})
	if err != nil {
		return nil
	}
	p := &Proxy{
		msCli:   cli,
		dsCli:   dsClient.NewRPCClient(),
		router:  NewRouter(cli),
		config: &Config{MaxLimit: DefaultMaxRawCount,
						Performance: PerformConfig{
							GrpcInitWinSize: 1024 * 1024 * 10,
							GrpcPoolSize:    1,
						},
		},
		clock:      hlc.NewClock(hlc.UnixNano, 0),
		ctx:        ctx,
		cancel:     cancel,

		maxWorkNum: 1,
		taskQueues: taskQueues, // XXX otherwise insert submit devide 0 panic
		workRecover: make(chan int, 1), // insert task wait need this
	}

	// task wait need workMonitor
	for i, queue := range taskQueues {
		p.wg.Add(1)
		go p.work(i, queue)
	}
	p.wg.Add(1)
	go p.workMonitor()
	return p
}

func newTestProxy2(db *metapb.DataBase, table *metapb.Table, rngs... *metapb.Range) *Proxy {
	log.Info("create Test Proxy2")
	node := &metapb.Node{Id: 1, ServerAddr: "127.0.0.1:6060"}
	ms := mock_ms.NewCluster("127.0.0.1:8887", "127.0.0.1:18887")
	ms.SetDb(db)
	ms.SetTable(table)
	ms.SetNode(node)
	for _,rng := range rngs {
		rng :=deepcopy.Iface(rng).(*metapb.Range)
		ms.SetRange(rng)
	}

	go ms.Start()
	MockMs = ms
	time.Sleep(time.Second)
	destoryDir(dsPath)
	ds := mock_ds.NewDsRpcServer("127.0.0.1:6060", dsPath)
	for _,rng := range rngs {
		rng :=deepcopy.Iface(rng).(*metapb.Range)
		ds.SetRange(rng)
	}
	go ds.Start()
	MockDs = ds
	time.Sleep(time.Second)
	ctx, cancel := context.WithCancel(context.Background())

	var taskQueues []chan Task
	for i := 0; i < int(1); i++ {
		queue := make(chan Task, 1)
		taskQueues = append(taskQueues, queue)
	}
	cli, err := msClient.NewClient([]string{"127.0.0.1:8887"})
	if err != nil {
		return nil
	}
	p := &Proxy{
		msCli:   cli,
		dsCli:   dsClient.NewRPCClient(),
		router:  NewRouter(cli),
		config: &Config{MaxLimit: DefaultMaxRawCount,
			Performance: PerformConfig{
				GrpcInitWinSize: 1024 * 1024 * 10,
				GrpcPoolSize:    1,
			},
		},
		clock:      hlc.NewClock(hlc.UnixNano, 0),
		ctx:        ctx,
		cancel:     cancel,

		maxWorkNum: 1,
		taskQueues: taskQueues, // XXX otherwise insert submit devide 0 panic
		workRecover: make(chan int, 1), // insert task wait need this
	}

	// task wait need workMonitor
	for i, queue := range taskQueues {
		p.wg.Add(1)
		go p.work(i, queue)
	}
	p.wg.Add(1)
	go p.workMonitor()
	return p
}
func destoryDir(path string) {
	os.RemoveAll(path)
}

func CloseMock(p *Proxy){
	//p.msCli.Close()
	//p.dsCli.Close()
	//time.Sleep(time.Second*30)
	//MockDs.Stop()
	//MockMs.Stop()

}

func makeTestTable(colInfos []*columnInfo) *metapb.Table {
	columns := make([]*metapb.Column, 0, len(colInfos))
	var pks []string
	var colID uint64 = 1
	for _, info := range colInfos {
		c := &metapb.Column{
			Name:     info.name,
			Id:       colID,
			DataType: info.typ,
			Unsigned: info.isUnsigned,
		}
		if info.isPK {
			pks = append(pks, info.name)
			c.PrimaryKey = 1
		}
		if info.autoInc {
			c.AutoIncrement = true
		}
		columns = append(columns, c)
		colID++
	}
	if len(pks) == 0 {
		panic("require primary key column")
	}
	table := &metapb.Table{
		Name:    testTableName,
		DbName:  testDBName,
		DbId:    1,
		Id:      1,
		Columns: columns,
		Epoch:   &metapb.TableEpoch{ConfVer: 1, Version: 1},
	}
	return table
}

func testProxyInsert(t *testing.T, p *Proxy, expectedAffected uint64, sql string) {
	t.Logf("sql> %s ", sql)

	sqlstmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	stmt, ok := sqlstmt.(*sqlparser.Insert)
	if !ok {
		t.Fatalf("not insert stamentent: %s", sql)
	}
	res, err := p.HandleInsert(testDBName, stmt, nil)
	if err != nil {
		if  err == dskv.ErrRouteChange{
			t.Logf("insert failed, %v, sqlL%v", err, sql)
			return
		} else {
			t.Fatalf("insert failed: %v, sql: %v", err, sql)
		}
	}
	if res.Status != 0 {
		t.Fatalf("insert failed. status not ok(%d)", res.Status)
	}
	if res.AffectedRows != expectedAffected {
		time.Sleep(time.Second)
		t.Fatalf("insert failed. unexpectecd affected rows: %v, expected: %v", res.AffectedRows, expectedAffected)
	}
}

func testProxyDelete(t *testing.T, p *Proxy, expectAffected uint64, sql string) {
	t.Logf("sql> %s ", sql)

	sqlstmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	stmt, ok := sqlstmt.(*sqlparser.Delete)
	if !ok {
		t.Fatalf("not delete stamentent: %s", sql)
	}
	res, err := p.HandleDelete(testDBName, stmt, nil)
	if err != nil {
		t.Fatalf("delete faile: %v, sql: %s", err, sql)
	}
	if res.Status != 0 {
		t.Fatalf("delete failed. status not ok(%d)", res.Status)
	}
	if res.AffectedRows != expectAffected {
		t.Fatalf("delete failed. unexpected affected rows: %v, expected: %v", res.AffectedRows, expectAffected)
	}
}

func testProxySelect(t *testing.T, p *Proxy, expectResult [][]string, sql string) {
	t.Logf("sql> %s ", sql)

	sqlstmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	stmt, ok := sqlstmt.(*sqlparser.Select)
	if !ok {
		t.Fatalf("not select stamentent: %s", sql)
	}
	r, err := p.HandleSelect(testDBName, stmt, nil)
	if err != nil {
		t.Fatalf("select failed: %v, sql: %v", err, sql)
	}
	if r.Status != 0 {
		t.Fatalf("select failed: status not ok(%v), sql: %v", r.Status, sql)
	}
	result := formatSelectResult(r)
	t.Logf("select result: %v", result)
	if len(result) == 0 && len(expectResult) == 0 {
		return
	}
	if !reflect.DeepEqual(result, expectResult) {
		t.Errorf("incorrect select result. expected: %v, actual: %v", expectResult, result)
	}
}

func testProxyDescribe(t *testing.T, p *Proxy, expectResult [][]string, sql string) {
	t.Logf("sql> %s ", sql)

	sqlstmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	stmt, ok := sqlstmt.(*sqlparser.Describe)
	if !ok {
		t.Fatalf("not describe stamentent: %s", sql)
	}
	r, err := p.HandleDescribe(testDBName, stmt)
	if err != nil {
		t.Fatalf("descirbe failed: %v, sql: %v", err, sql)
	}
	if r.Status != 0 {
		t.Fatalf("describe failed: status not ok(%v), sql: %v", r.Status, sql)
	}
	result := formatSelectResult(r)
	t.Logf("describe result: %v", result)
	if len(result) == 0 && len(expectResult) == 0 {
		return
	}
	if !reflect.DeepEqual(result, expectResult) {
		t.Errorf("incorrect decribe result. expected: %v, actual: %v", expectResult, result)
	}
}

func testProxyAdmin(t *testing.T, p *Proxy, expectResult [][]string, sql string) {
	t.Logf("sql> %s ", sql)

	sqlstmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	stmt, ok := sqlstmt.(*sqlparser.Admin)
	if !ok {
		t.Fatalf("not describe stamentent: %s", sql)
	}
	cmd, args := parseAdminArgs(stmt)
	r, err := p.HandleAdmin(testDBName, cmd, args)
	if err != nil {
		t.Fatalf("admin failed: %v, sql: %v", err, sql)
	}
	if r == nil {
		t.Fatal("handleadmin reply: r is nil: ", sql)
	}
	if r.Status != 0 {
		t.Fatalf("admin failed: status not ok(%v), sql: %v", r.Status, sql)
	}
	result := formatSelectResult(r)
	t.Logf("admin result: %v", result)
	if len(result) == 0 && len(expectResult) == 0 {
		return
	}
	if !reflect.DeepEqual(result, expectResult) {
		t.Errorf("incorrect admin result. expected: %v, actual: %v", expectResult, result)
	}
}

func toSelectStmt(sql string) (*sqlparser.Select, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	selStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("not select statement: %s", sql)
	}
	return selStmt, nil
}

func formatSelectResult(r *mysql.Result) [][]string {
	rows := make([][]string, 0, len(r.Values))
	for _, rv := range r.Values {
		row := make([]string, 0, len(rv))
		for _, v := range rv {
			b, err := formatValue(v)
			if err != nil {
				panic(fmt.Errorf("formatValue failed: %v", err))
			}
			row = append(row, string(b))
		}
		rows = append(rows, row)
	}
	return rows
}

type Filter struct {
	columns []string
	matchs  []Match
}

// for rest
func testGetFilter(t *testing.T, query *Query, table *Table, p *Proxy, stmt *sqlparser.Select) *Filter {

	columns := query.parseColumnNames()
	var matchs []Match = nil
	var err error
	if query.Command.Filter != nil {
		matchs, err = query.parseMatchs(query.Command.Filter.And)
		if err != nil {
			t.Fatal("filter: parse matchs error: ", err)
		}
	}
	getFilter := &Filter{columns: columns, matchs: matchs}
	selectFilter := getSelectFilter(t, p, testDBName, stmt)
	t.Log("get filter: ", getFilter)
	t.Log("select filter: ", selectFilter)
	if reflect.DeepEqual(getFilter, selectFilter) == false {
		t.Fatal("diff getfilter selectfileter")
	}
	return getFilter
}

//the func no support aggre function
func testGetCommand(t *testing.T, table *Table, p *Proxy, filter *Filter, stmt *sqlparser.Select, expected [][]interface{}, limit_ *Limit_, order []*Order, columns []string) {
	var limit *Limit
	if limit_ != nil {
		limit = &Limit{
			offset:   limit_.Offset,
			rowCount: limit_.RowCount,
		}
	}
	var cols []*SelColumn
	for _, c := range columns {
		cols = append(cols, &SelColumn{col: c})
	}

	fieldList, err := makeFieldList(table, cols)
	if err != nil {
		t.Fatal("get command, find field list error: " , err)
	}
	rowss, err := p.doSelect(table, fieldList, filter.matchs, limit, nil)
	if err != nil {
		t.Fatal("get command run error: ", err)
	}
	//res, err := buildSelectResult(stmt, rowss, filter.columns)
	//if err != nil {
	//	t.Fatal("build select result error: ", err)
	//}
	//selectResult := formatSelectResult(res)
	reply := formatReply(table.columns, rowss, order, cols)

	assert.Equal(t, reply.Code, 0, fmt.Sprintf("reply.code %v", reply.Code))
	assert.Equal(t, len(reply.Values), len(expected), fmt.Sprintf("reply value %v, except %v", reply.Values, expected))

	t.Logf("getResult: %v", reply)
}

func testSetCommand(t *testing.T, query *Query, table *Table, p *Proxy, expected *Reply) {
	reply, err := query.setCommand(p, table)
	if err != nil {
		t.Fatal("set command error: ", err)
	}
	assert.DeepEqual(t, reply, expected)
}

func getSelectFilter(tt *testing.T, p *Proxy, db string, stmt *sqlparser.Select) *Filter {
	parser := &StmtParser{}

	// 解析表名
	tableName := parser.parseTable(stmt)
	t := p.router.FindTable(db, tableName)
	if t == nil {
		tt.Fatal("[select] table %s.%s doesn.t exist", db, tableName)
	}

	// 解析选择列
	cols, err := parser.parseSelectCols(stmt)
	if err != nil {
		tt.Fatal("[select]parse colum error: ", err)
	}
	columns := make([]string, 0, len(cols))
	for _, c := range cols {
		if c.col == "" {
			for _, mc := range t.Columns {
				columns = append(columns, mc.Name)
			}
		} else {
			columns = append(columns, c.col)
		}
	}

	// 解析where条件
	var matchs []Match
	if stmt.Where != nil {
		// TODO: 支持OR表达式
		matchs, err = parser.parseWhere(stmt.Where)
		if err != nil {
			tt.Fatal("handle select parse where error(%v)", err.Error())
		}
	}

	return &Filter{columns: columns, matchs: matchs}
}
