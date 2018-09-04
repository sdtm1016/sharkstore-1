package server

import (
	"testing"
	"model/pkg/metapb"
	"util"
	"proxy/gateway-server/sqlparser"
	"net/http"
	"bytes"
	"util/assert"
	"encoding/json"
	"fmt"
)

func TestRestKVHttp(t *testing.T) {
	s, err := mockGwServer()
	if err != nil {
		t.Fatalf("init server failed, err[%v]", err)
	}
	go s.Run()

	columnNames := []string{"id", "name", "balance"}

	setQueryRep := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
			Values: [][]interface{}{
				[]interface{}{1, "myname1", 0.1},
				[]interface{}{2, "myname2", 0.2},
				[]interface{}{3, "myname3", 0.2},
				[]interface{}{3, "myname3", 0.3},
			},
		},
	}

	dataRep, err := json.Marshal(setQueryRep)
	if err != nil {
		t.Fatal("set query marshal error: ", err)
	}
	//fmt.Println(string(dataRep))
	req, _ := http.NewRequest("POST", "http://127.0.0.1:8080/kvcommand", bytes.NewReader(dataRep))
	req.Header.Set("Content-type", "application/json")

	query, err := httpReadQuery(req)
	if err != nil {
		t.Fatal("http read query error: ", err)
	}
	assert.DeepEqual(t, query.DatabaseName, setQueryRep.DatabaseName)
	assert.DeepEqual(t, query.TableName, setQueryRep.TableName)
	t.Log("query command 1: ", query.Command)
	t.Log("query command 2: ", setQueryRep.Command)
	//w := httptest.NewRecorder()
	//s.handleKVCommand(w, req)
	//t.Logf("%v", w)
	table := s.proxy.router.FindTable(testDBName, testTableName)
	if table == nil {
		t.Fatalf("table %s.%s doesn't exist ", testDBName, testTableName)
	}
	testSetCommand(t, query, table, s.proxy, &Reply{
		Code:         0,
		RowsAffected: 4,
	})

	setQueryNoPkRep := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: []string{"name", "balance"},
			Values: [][]interface{}{
				[]interface{}{"myname1", 0.1},
				[]interface{}{"myname2", 0.2},
				[]interface{}{"myname3", 0.3},
				[]interface{}{"myname3", 0.4},
			},
		},
	}

	testSetCommand(t, setQueryNoPkRep, table, s.proxy, &Reply{
		Code:         0,
		RowsAffected: 4,
	})

	getAggreQueryRep := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: []string{},
			AggreFunc: []*AggreFunc{
				&AggreFunc{Function: "count", Field: "*"},
				&AggreFunc{Function: "max", Field: "id"},
				&AggreFunc{Function: "min", Field: "id"},
				&AggreFunc{Function: "sum", Field: "balance"},
			},
		},
	}

	expected := [][]interface{}{
		[]interface{}{4, 4, 1, 1.0},
	}
	assertGetCommand(t, getAggreQueryRep, s.proxy, expected, table )

	getQueryRep := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
		},
	}
	expected = [][]interface{}{
		[]interface{}{1, "myname1", 0.1},
		[]interface{}{2, "myname2", 0.2},
		[]interface{}{3, "myname3", 0.3},
		[]interface{}{4, "myname3", 0.4},
	}
	assertGetCommand(t, getQueryRep, s.proxy, expected, table)

	t.Logf("start to select and filter")
	// test where
	getWhereQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
			Filter: &Filter_{
				And: []*And{
					&And{
						Field:  &Field_{Column: "id", Value: uint64(1)},
						Relate: ">",
					},
				},
			},
		},
	}
	expected = [][]interface{}{
		[]interface{}{2, "myname2", 0.2},
		[]interface{}{3, "myname3", 0.3},
		[]interface{}{4, "myname3", 0.4},
	}
	assertGetCommand(t, getWhereQuery, s.proxy, expected, table)

	t.Logf("start to select and pks")
	// test pks
	getPksQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
			PKs: [][]*And{
				[]*And{
					&And{
						Field:  &Field_{Column: "id", Value: uint64(2)},
						Relate: ">",
					},
				},
				[]*And{
					&And{
						Field:  &Field_{Column: "name", Value: "myname1"},
						Relate: "=",
					},
				},
			},
		},
	}
	expected = [][]interface{}{
		[]interface{}{3, "myname3", 0.3},
		[]interface{}{1, "myname1", 0.1},
	}
	assertGetCommand(t, getPksQuery, s.proxy, expected, table)
}

func assertGetCommand(t *testing.T, query *Query, proxy *Proxy, except [][]interface{}, table *Table) {
	reply, err := query.getCommand(proxy, table)
	if err != nil {
		t.Fatalf("query %v error %v", query, err)
	}
	assert.Equal(t, err, nil, fmt.Sprintf("get command err %v", err))
	assert.Equal(t, reply.Code, 0, fmt.Sprintf("get command code %v", reply.Code))
	assert.Equal(t, len(reply.Values), len(except), fmt.Sprintf("get command value %v, except: %v", reply.Values, except))
	t.Logf("get command value %v, except: %v", reply.Values, except)
}

func mockGwServer() (*Server, error) {
	// load config file
	conf := &Config{
		Log: LogConfig{
			Dir:    logPath,
			Module: "gateway",
			Level:  "debug",
		},
		Cluster: ClusterConfig{
			ID:         1,
			ServerAddr: []string{"192.168.150.122:18887"},
		},
		Performance: PerformConfig{
			GrpcInitWinSize: 1024 * 1024 * 10,
			GrpcPoolSize:    1,
			MaxWorkNum:      10,
		},
		MaxClients: 10000,
		MaxLimit:   DefaultMaxRawCount,
		User:       "test",
		Password:   "123456",
		Charset:    "utf8",
		HttpPort:   8080,
		SqlPort:    3360,
	}
	return NewServer(conf)
}

func TestRestMset(t *testing.T) {
	//log.InitFileLog(logPath, "proxy", "debug")
	columns := []*columnInfo{
		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true, autoInc: true},
		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
		&columnInfo{name: "balance", typ: metapb.DataType_Double},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	tt := makeTestTable(columns)
	start := util.EncodeStorePrefix(util.Store_Prefix_KV, tt.GetId())
	var pks []*metapb.Column
	for _, col := range tt.Columns {
		if col.Name == "id" {
			pks = append(pks, col)
			break
		}
	}
	r := util.BytesPrefix(start)
	rng := &metapb.Range{
		Id:         1,
		TableId:    1,
		StartKey:   r.Start,
		EndKey:     r.Limit,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	rng.PrimaryKeys = pks
	p := newTestProxy(db, tt, rng)
	defer CloseMock(p)
	defer p.Close()

	table := p.router.FindTable(testDBName, testTableName)
	columnNames := []string{"id", "name", "balance"}

	// test set
	setQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
			Values: [][]interface{}{
				[]interface{}{3, "myname1", 0.003},
				[]interface{}{2, "myname10", 0.002},
				[]interface{}{1, "myname100", 0.001},
			},
		},
	}

	testSetCommand(t, setQuery, table, p, &Reply{
		Code:         0,
		RowsAffected: 3,
	})

	setNoPkQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: []string{"name", "balance"},
			Values: [][]interface{}{
				[]interface{}{"myname1", 0.003},
				[]interface{}{"myname10", 0.002},
				[]interface{}{"myname100", 0.001},
			},
		},
	}

	testSetCommand(t, setNoPkQuery, table, p, &Reply{
		Code:         0,
		RowsAffected: 3,
	})


	// test getll
	sql := "select id, name, balance from " + testTableName
	sqlstmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	stmt, ok := sqlstmt.(*sqlparser.Select)
	if !ok {
		t.Fatalf("not select stamentent: %s", sql)
	}

	getQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
		},
	}
	filter_ := testGetFilter(t, getQuery, table, p, stmt)

	expected := [][]interface{}{
		[]interface{}{1, "myname1", 0.003},
		[]interface{}{2, "myname10", 0.002},
		[]interface{}{3, "myname100", 0.001},
	}
	testGetCommand(t, table, p, filter_, stmt, expected, nil, nil, columnNames)
	//testProxySelect(t, p, expected, sql)

	// test where
	sql = "select id, name, balance from " + testTableName + " where id>1"
	sqlstmt, err = sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	stmt, ok = sqlstmt.(*sqlparser.Select)
	if !ok {
		t.Fatalf("not select stamentent: %s", sql)
	}

	//order := []*Order{
	//	&Order{
	//		By: "name",
	//	},
	//}
	////var order []*Order
	//limit_ := &Limit_{
	//	Offset:   0,
	//	RowCount: 2,
	//}
	//getQuery = &Query{
	//	DatabaseName: testDBName,
	//	TableName:    testTableName,
	//	Command: &Command{
	//		Field: columnNames,
	//		Filter: &Filter_{
	//			And: []*And{
	//				&And{
	//					Field:  &Field_{Column: "id", Value: uint64(1)},
	//					Relate: ">",
	//				},
	//			},
	//			Order: order,
	//			Limit: limit_,
	//		},
	//	},
	//}
	////data, _ := json.Marshal(getQuery)
	////fmt.Println("~~~~~~~~~~~~~ %v", string(data))
	//filter_ = testGetFilter(t, getQuery, table, p, stmt)
	//expected = [][]interface{}{
	//	//[]string{"1", "myname", "0.001"},
	//	[]interface{}{2, "myname2", 0.002},
	//	[]interface{}{3, "myname3", 0.003},
	//}
	//testGetCommand(t, table, p, filter_, stmt, expected, limit_, order, columnNames)
}

func TestRestKVCommand(t *testing.T) {
	columns := []*columnInfo{
		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
		&columnInfo{name: "balance", typ: metapb.DataType_Double},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	table := makeTestTable(columns)
	var pks []*metapb.Column
	for _, col := range table.Columns {
		if col.Name == "id" {
			pks = append(pks, col)
			break
		}
	}
	start := util.EncodeStorePrefix(util.Store_Prefix_KV, table.GetId())
	r := util.BytesPrefix(start)
	rng := &metapb.Range{
		Id:         1,
		TableId:    1,
		StartKey:   r.Start,
		EndKey:     r.Limit,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	rng.PrimaryKeys = pks
	p := newTestProxy(db, table, rng)
	defer CloseMock(p)
	defer p.Close()

	// this is sql insert
	//testProxyInsert(t, p, 1, "insert into " + testTableName + "(id,name,balance) values(1,'myname',0.0075)")

	table_ := p.router.FindTable(testDBName, testTableName)
	columnNames := []string{"id", "name", "balance"}

	// test set
	setQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
			Values: [][]interface{}{
				[]interface{}{1, "myname1", 0.001},
			},
		},
	}
	testSetCommand(t, setQuery, table_, p, &Reply{
		Code:         0,
		RowsAffected: 1,
	})

	setQuery = &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
			Values: [][]interface{}{
				[]interface{}{2, "myname2", 0.002},
			},
		},
	}
	testSetCommand(t, setQuery, table_, p, &Reply{
		Code:         0,
		RowsAffected: 1,
	})

	setQuery = &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
			Values: [][]interface{}{
				[]interface{}{3, "myname3", 0.003},
			},
		},
	}
	testSetCommand(t, setQuery, table_, p, &Reply{
		Code:         0,
		RowsAffected: 1,
	})

	t.Logf("start to select all")
	// test getll
	sql := "select id, name, balance from " + testTableName
	sqlstmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	stmt, ok := sqlstmt.(*sqlparser.Select)
	if !ok {
		t.Fatalf("not select stamentent: %s", sql)
	}

	getQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
		},
	}
	filter_ := testGetFilter(t, getQuery, table_, p, stmt)

	expected := [][]interface{}{
		[]interface{}{1, "myname", 0.001},
		[]interface{}{2, "myname2", 0.002},
		[]interface{}{3, "myname3", 0.003},
	}
	testGetCommand(t, table_, p, filter_, stmt, expected, nil, nil, columnNames)

	//todo mock ds for where 、limt and so on
	//t.Logf("start to select and filter")
	//// test where
	//sql = "select id, name, balance from " + testTableName + " where id>1"
	//sqlstmt, err = sqlparser.Parse(sql)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//stmt, ok = sqlstmt.(*sqlparser.Select)
	//if !ok {
	//	t.Fatalf("not select stamentent: %s", sql)
	//}
	//
	//getQuery = &Query{
	//	DatabaseName: testDBName,
	//	TableName:    testTableName,
	//	Command: &Command{
	//		Field: []string{"id", "name", "balance"},
	//		Filter: &Filter_{
	//			And: []*And{
	//				&And{
	//					Field:  &Field_{Column: "id", Value: uint64(1)},
	//					Relate: ">",
	//				},
	//			},
	//		},
	//	},
	//}
	//filter_ = testGetFilter(t, getQuery, table_, p, stmt)
	//
	//expected = [][]interface{}{
	//	//[]string{"1", "myname", "0.001"},
	//	[]interface{}{2, "myname2", 0.002},
	//	[]interface{}{3, "myname3", 0.003},
	//}
	//testGetCommand(t, table_, p, filter_, stmt, expected, nil, nil, nil)
	//

	//// test pks
	//t.Logf("start to select and pks")
	//getQuery = &Query{
	//	DatabaseName: testDBName,
	//	TableName:    testTableName,
	//	Command: &Command{
	//		Field: columnNames,
	//		PKs: [][]*And{
	//			[]*And{
	//				&And{
	//					Field:  &Field_{Column: "id", Value: uint64(2)},
	//					Relate: ">",
	//				},
	//			},
	//			[]*And{
	//				&And{
	//					Field:  &Field_{Column: "name", Value: "myname1"},
	//					Relate: "=",
	//				},
	//			},
	//		},
	//	},
	//}
	//
	//reply, err := getQuery.getCommand(p, table_)
	//if err != nil {
	//	t.Fatalf("error %v", err)
	//}
	//t.Logf("getResult====%v", reply)
	//assert.Equal(t, reply.Code, 0, "code")
	//assert.Equal(t, len(reply.Values), 1, "value")
}

//support
func TestRestInsert(t *testing.T) {
	//log.InitFileLog(logPath, "proxy", "debug")
	columns := []*columnInfo{
		&columnInfo{name: "k", typ: metapb.DataType_Varchar, isPK: true},
		&columnInfo{name: "v", typ: metapb.DataType_Varchar},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	tt := makeTestTable(columns)
	start := util.EncodeStorePrefix(util.Store_Prefix_KV, tt.GetId())
	var pks []*metapb.Column
	for _, col := range tt.Columns {
		if col.Name == "k" {
			pks = append(pks, col)
			break
		}
	}
	r := util.BytesPrefix(start)
	rng := &metapb.Range{
		Id:         1,
		TableId:    1,
		StartKey:   r.Start,
		EndKey:     r.Limit,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	rng.PrimaryKeys = pks
	p := newTestProxy(db, tt, rng)
	defer CloseMock(p)
	defer p.Close()

	table := p.router.FindTable(testDBName, testTableName)
	columnNames := []string{"k", "v"}

	content, err := ZipFile("../conf/config.zip")
	if err != nil {
		t.Fatalf("read zip file err %v", err)
	}

	// test set
	setQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
			Values: [][]interface{}{
				[]interface{}{"key1", content},
			},
		},
	}

	testSetCommand(t, setQuery, table, p, &Reply{
		Code:         0,
		RowsAffected: 1,
	})


	// test getll
	sql := "select k,v from " + testTableName
	sqlstmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	stmt, ok := sqlstmt.(*sqlparser.Select)
	if !ok {
		t.Fatalf("not select stamentent: %s", sql)
	}

	getQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
		},
	}
	filter_ := testGetFilter(t, getQuery, table, p, stmt)

	expected := [][]interface{}{
		[]interface{}{"key1", content},
	}
	testGetCommand(t, table, p, filter_, stmt, expected, nil, nil, columnNames)

}