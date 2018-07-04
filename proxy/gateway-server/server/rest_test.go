package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"proxy/gateway-server/sqlparser"
	"model/pkg/kvrpcpb"
	"model/pkg/metapb"
	"util/assert"
	"util"
)

func TestRestKVHttp(t *testing.T) {
	// load config file
	conf := &Config{
		//Addr:              ":33600",
		Log: LogConfig{
			Dir:            "./log",
			Module:         "gateway",
			Level:          "debug",
		},
		Cluster: ClusterConfig{
			ID: 1,
			ServerAddr: []string{"192.168.211.149:8887"},
		},
		Performance: PerformConfig{
			GrpcInitWinSize:   1024 * 1024 * 10,
			GrpcPoolSize:      1,
		},
		MaxClients:        10000,
		User:              "fbase",
		Password:          "123123",
		Charset:           "utf8",
		HttpPort:          3360,
	}
	s, err := NewServer(conf)
	if err != nil {
		t.Fatalf("init server failed, err[%v]", err)
	}
	go s.Run()

	setQueryRep := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: []string{"id", "name", "balance"},
			Values: [][]interface{}{
				[]interface{}{1, "myname", 0.007},
			},
		},
	}
	dataRep, err := json.Marshal(setQueryRep)
	if err != nil {
		t.Fatal("set query marshal error: ", err)
	}
	fmt.Println(string(dataRep))
	req, _ := http.NewRequest("POST", "http://example.com/kvcommand", bytes.NewReader(dataRep))
	req.Header.Set("Content-type", "application/json")
	//w := httptest.NewRecorder()
	query, err := httpReadQuery(req)
	if err != nil {
		t.Fatal("http read query error: ", err)
	}
	//if reflect.DeepEqual(query, setQueryRep) == false {
	//	t.Fatal("diff setquery from httpread")
	//}
	assert.DeepEqual(t, query.DatabaseName, setQueryRep.DatabaseName)
	assert.DeepEqual(t, query.TableName, setQueryRep.TableName)
	t.Log("query command 1: ", query.Command)
	t.Log("query command 2: ", setQueryRep.Command)
}

func TestRestMset(t *testing.T) {
	columns := []*columnInfo{
		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
		&columnInfo{name: "balance", typ: metapb.DataType_Double},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	tt := makeTestTable(columns)
	start := util.EncodeStorePrefix(util.Store_Prefix_KV, tt.GetId())
	r := util.BytesPrefix(start)
	rng := &metapb.Range{
		Id:  1,
		TableId: 1,
		StartKey: r.Start,
		EndKey: r.Limit,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers: []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	p := newTestProxy(db, tt, rng)

	table := p.router.FindTable(testDBName, testTableName)

	// test set
	setQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: []string{"id", "name", "balance"},
			Values: [][]interface{}{
				[]interface{}{3, "myname1", 0.003},
				[]interface{}{2, "myname10", 0.002},
				[]interface{}{1, "myname100", 0.001},
			},
		},
	}
	set, _ := json.Marshal(setQuery)
	fmt.Println("@@@", string(set))
	testSetCommand(t, setQuery, table, p, &Reply{
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
			Field: []string{"id", "name", "balance"},
		},
	}
	filter_ := testGetFilter(t, getQuery, table, p, stmt)

	expected := [][]string{
		[]string{"3", "myname3", "0.003"},
		[]string{"1", "myname1", "0.001"},
		[]string{"2", "myname2", "0.002"},
	}
	testGetCommand(t, table, p, filter_, stmt, expected, nil, nil, nil)
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
	columnNames := []string{"id", "name", "balance"}
	order := []*Order{
		&Order{
			By: "name",
		},
	}
	//var order []*Order
	limit_ := &Limit_{
		Offset:   0,
		RowCount: 2,
	}
	getQuery = &Query{
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
				Order: order,
				Limit: limit_,
			},
		},
	}
	//data, _ := json.Marshal(getQuery)
	//fmt.Println("~~~~~~~~~~~~~ %v", string(data))
	filter_ = testGetFilter(t, getQuery, table, p, stmt)
	expected = [][]string{
		//[]string{"1", "myname", "0.001"},
		[]string{"2", "myname2", "0.002"},
		[]string{"3", "myname3", "0.003"},
	}
	testGetCommand(t, table, p, filter_, stmt, expected, limit_, order, columnNames)
}

func TestRestKVCommand(t *testing.T) {
	columns := []*columnInfo{
		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
		&columnInfo{name: "balance", typ: metapb.DataType_Double},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	table := makeTestTable(columns)
	start := util.EncodeStorePrefix(util.Store_Prefix_KV, table.GetId())
	r := util.BytesPrefix(start)
	rng := &metapb.Range{
		Id:  1,
		TableId: 1,
		StartKey: r.Start,
		EndKey: r.Limit,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers: []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	p := newTestProxy(db, table, rng)

	// this is sql insert
	//testProxyInsert(t, p, 1, "insert into " + testTableName + "(id,name,balance) values(1,'myname',0.0075)")

	table_ := p.router.FindTable(testDBName, testTableName)

	// test set
	setQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: []string{"id", "name", "balance"},
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
			Field: []string{"id", "name", "balance"},
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
			Field: []string{"id", "name", "balance"},
			Values: [][]interface{}{
				[]interface{}{3, "myname3", 0.003},
			},
		},
	}
	testSetCommand(t, setQuery, table_, p, &Reply{
		Code:         0,
		RowsAffected: 1,
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
			Field: []string{"id", "name", "balance"},
		},
	}
	filter_ := testGetFilter(t, getQuery, table_, p, stmt)

	expected := [][]string{
		[]string{"1", "myname", "0.001"},
		[]string{"2", "myname2", "0.002"},
		[]string{"3", "myname3", "0.003"},
	}
	testGetCommand(t, table_, p, filter_, stmt, expected, nil, nil, nil)

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

	getQuery = &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: []string{"id", "name", "balance"},
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
	filter_ = testGetFilter(t, getQuery, table_, p, stmt)

	expected = [][]string{
		//[]string{"1", "myname", "0.001"},
		[]string{"2", "myname2", "0.002"},
		[]string{"3", "myname3", "0.003"},
	}
	testGetCommand(t, table_, p, filter_, stmt, expected, nil, nil, nil)

	// test pks
	getQuery = &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: []string{"id", "name", "balance"},
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

	columnstr := getQuery.parseColumnNames()
	fieldList := make([]*kvrpcpb.SelectField, 0, len(columnstr))
	for _, c := range columnstr {
		col := table_.FindColumn(c)
		if col == nil {
			t.Errorf("invalid column(%s)", c)
			return
		}
		fieldList = append(fieldList, &kvrpcpb.SelectField{
			Typ:    kvrpcpb.SelectField_Column,
			Column: col,
		})
	}

	var allRows [][]*Row
	var tasks []*SelectTask
	for _, pk := range getQuery.Command.PKs {
		//matchs, err := getQuery.parseMatchs(pk)
		////filter := &Filter{columns: columns, matchs: matchs}
		//if err != nil {
		//	t.Errorf("pks: get command parse matchs error: %v", err)
		//	return
		//}
		////rowss, err := proxy.doRangeSelect(t, filter, nil, nil)
		//rowss, err := p.doSelect(table_, fieldList, matchs, nil, nil)
		//if err != nil {
		//	t.Errorf("pks: getcommand doselect error: %v", err)
		//	return
		//}
		//reply, _ := json.Marshal(formatReply(table_.columns, rowss, nil, columnstr))
		//t.Log("getResult---: ", string(reply))
		//=============

		matchs, err := getQuery.parseMatchs(pk)
		//filter := &Filter{columns: columns, matchs: matchs}
		if err != nil {
			t.Errorf("[get] handle parse where error: %v", err)
			return
		}
		//rowss, err := proxy.doRangeSelect(t, filter, nil, nil)
		//rowss, err := proxy.doSelect(t, fieldList, matchs, nil, nil)
		//if err != nil {
		//	log.Error("getcommand doselect error: %v", err)
		//	return nil, err
		//}
		//allRows = append(allRows,rowss...)
		task := &SelectTask{
			p:         p,
			table:     table_,
			fieldList: fieldList,
			matches:   matchs,
			done:      make(chan error, 1),
		}
		err = p.Submit(task)
		if err != nil {
			t.Errorf("submit insert task failed, err[%v]", err)
			return
		}
		tasks = append(tasks, task)
	}
	for _, task := range tasks {
		err := task.Wait()
		if err != nil {
			t.Errorf("select task do failed, err[%v]", err)
			return
		}
		rowss := task.rest.rows
		if rowss != nil {
			allRows = append(allRows, rowss...)
		}
	}
	reply, _ := json.Marshal(formatReply(table_.columns, allRows, nil, columnstr))
	t.Log("getResult====", string(reply))
}
