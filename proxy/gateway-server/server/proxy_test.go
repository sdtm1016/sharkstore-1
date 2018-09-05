package server

import (
	"testing"
	"fmt"
	"strconv"
	"os"
	"bufio"
	"strings"
	"io"
	"util"
	"util/log"
	"util/deepcopy"
	"model/pkg/metapb"
	"database/sql"
	"util/assert"
)

//
//import (
//	"bytes"
//	"fmt"
//	"math"
//	"reflect"
//	"testing"
//	"time"
//
//	"model/pkg/kvrpcpb"
//	"model/pkg/metapb"
//	"util"
//	"util/encoding"
//	"util/log"
//	"util/config"
//)
//
//func TestProxyBasic(t *testing.T) {
//	columns := []*columnInfo{
//		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
//		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
//		&columnInfo{name: "balance", typ: metapb.DataType_Double},
//	}
//	var start, end []byte
//	start = util.EncodeStorePrefix(util.Store_Prefix_KV, 1)
//	_, end = bytesPrefix(start)
//	ranges := []*util.Range{
//		&util.Range{Start: start, Limit: end},
//	}
//	p := newTestProxy(columns, ranges)
//	defer p.Close()
//
//	//testProxySelect(t, p, [][]string{}, "select * from "+testTableName)
//
//	// insert
//	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(1, 'myname', 0.0075)")
//	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(2, 'myname2', 1)")
//	//testProxyInsert(t, p, 1, "insert into "+testTableName+"(iD,nAMe,bAlaNce) values(3, 'myname3', 3.1)")
//	//testProxyInsert(t, p, 1, "insert into "+testTableName+"(iD,nAMe,bAlaNce) values(4, 'myname4', NULL)")
//	//
//	//// select and check
//	expected := [][]string{
//		[]string{"1", "myname", "0.0075"},
//		[]string{"2", "myname2", "1"},
//	//	[]string{"3", "myname3", "3.1"},
//	//	[]string{"4", "myname4", "NULL"},
//	}
//	testProxySelect(t, p, expected, "select * from "+testTableName)
//	//
//	//// delete one
//	//testProxyDelete(t, p, 1, "delete from "+testTableName+" where id = 1")
//	//expected = [][]string{
//	//	[]string{"2", "myname2", "1"},
//	//	[]string{"3", "myname3", "3.1"},
//	//	[]string{"4", "myname4", "NULL"},
//	//}
//	//testProxySelect(t, p, expected, "select * from "+testTableName)
//	//
//	//// delete all
//	//testProxyDelete(t, p, 3, "delete from "+testTableName)
//	//expected = [][]string{}
//	//testProxySelect(t, p, expected, "select * from "+testTableName)
//}
//
//func TestProxyBigUInt(t *testing.T) {
//	columns := []*columnInfo{
//		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
//		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
//		&columnInfo{name: "balance", typ: metapb.DataType_Double},
//	}
//	p := newTestProxy(columns, nil)
//	defer p.Close()
//
//	// insert
//	var biggest uint64 = math.MaxUint64
//	testProxyInsert(t, p, 1, fmt.Sprintf("insert into %s values (%d, 'name', 1)", testTableName, biggest))
//
//	// select and check
//	expected := [][]string{
//		[]string{fmt.Sprintf("%d", biggest), "name", "1"},
//	}
//	testProxySelect(t, p, expected, "select * from "+testTableName)
//}
//
//func TestProxySelectWhere(t *testing.T) {
//	columns := []*columnInfo{
//		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
//		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
//		&columnInfo{name: "balance", typ: metapb.DataType_Double},
//	}
//	p := newTestProxy(columns, nil)
//	var rows [][]string
//	for i := 0; i < 10; i++ {
//		id := fmt.Sprintf("%d", i)
//		name := fmt.Sprintf("name-%02d", i)
//		balance := fmt.Sprintf("%d.05", i)
//		sql := fmt.Sprintf("insert into %s values(%s, '%s', %s)", testTableName, id, name, balance)
//		testProxyInsert(t, p, 1, sql)
//		rows = append(rows, []string{id, name, balance})
//	}
//
//	selectSQL := "select * from " + testTableName + " "
//	testProxySelect(t, p, rows, selectSQL)
//
//	// 测试等于
//	testProxySelect(t, p, rows[1:2], selectSQL+"where id = 1")
//	testProxySelect(t, p, rows[1:2], selectSQL+"where name = 'name-01'")
//	testProxySelect(t, p, rows[1:2], selectSQL+"where balance = 1.05")
//
//	// 测试不等于
//	testProxySelect(t, p, rows[1:], selectSQL+"where id != 0")
//	testProxySelect(t, p, rows[1:], selectSQL+"where name != 'name-00'")
//	testProxySelect(t, p, rows[1:], selectSQL+"where balance != 0.05")
//
//	// 测试大于
//	testProxySelect(t, p, rows[2:], selectSQL+"where id > 1")
//	testProxySelect(t, p, rows[2:], selectSQL+"where name > 'name-01'")
//	testProxySelect(t, p, rows[2:], selectSQL+"where balance > 1.05")
//
//	// 测试大于等于
//	testProxySelect(t, p, rows[2:], selectSQL+"where id >= 2")
//	testProxySelect(t, p, rows[2:], selectSQL+"where name >= 'name-02'")
//	testProxySelect(t, p, rows[2:], selectSQL+"where balance >= 2.05")
//
//	// 测试小于
//	testProxySelect(t, p, rows[:5], selectSQL+"where id < 5")
//	testProxySelect(t, p, rows[:5], selectSQL+"where name < 'name-05'")
//	testProxySelect(t, p, rows[:5], selectSQL+"where balance < 5.05")
//
//	// 测试小于等于
//	testProxySelect(t, p, rows[:6], selectSQL+"where id <= 5")
//	testProxySelect(t, p, rows[:6], selectSQL+"where name <= 'name-05'")
//	testProxySelect(t, p, rows[:6], selectSQL+"where balance <= 5.05")
//
//	// 测试AND
//	testProxySelect(t, p, rows[1:2], selectSQL+"where id = 1 and name = 'name-01'")
//	testProxySelect(t, p, rows[1:2], selectSQL+"where id = 1 and name = 'name-01' and balance=1.05")
//	testProxySelect(t, p, [][]string{}, selectSQL+"where id = 1 and name = 'name-02'")
//	testProxySelect(t, p, [][]string{}, selectSQL+"where id = 1 and name = 'name-01' and balance='2.05'")
//}
//
//func TestProxyDeleteWhere(t *testing.T) {
//	columns := []*columnInfo{
//		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isPK: true},
//		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
//		&columnInfo{name: "balance", typ: metapb.DataType_Double},
//	}
//
//	selectSQL := "select * from " + testTableName + " "
//	deleteSQL := "delete from " + testTableName + " "
//
//	p := newTestProxy(columns, nil)
//	var rows [][]string
//	for i := 0; i < 10; i++ {
//		id := fmt.Sprintf("%d", i)
//		name := fmt.Sprintf("name-%02d", i)
//		balance := fmt.Sprintf("%d.05", i)
//		sql := fmt.Sprintf("insert into %s values(%s, '%s', %s)", testTableName, id, name, balance)
//		testProxyInsert(t, p, 1, sql)
//		rows = append(rows, []string{id, name, balance})
//	}
//
//	// 清空，重新插入
//	reset := func(alreadyDeleted uint64) {
//		testProxyDelete(t, p, 10-alreadyDeleted, deleteSQL)
//		for i := 0; i < 10; i++ {
//			id := fmt.Sprintf("%d", i)
//			name := fmt.Sprintf("name-%02d", i)
//			balance := fmt.Sprintf("%d.05", i)
//			sql := fmt.Sprintf("insert into %s values(%s, '%s', %s)", testTableName, id, name, balance)
//			testProxyInsert(t, p, 1, sql)
//		}
//	}
//
//	// 测试等于
//	testProxyDelete(t, p, 1, deleteSQL+"where id = 9")
//	testProxySelect(t, p, rows[:9], selectSQL)
//	reset(1)
//
//	// 测试不等于
//	testProxyDelete(t, p, 9, deleteSQL+"where id != 0")
//	testProxySelect(t, p, rows[:1], selectSQL)
//	reset(9)
//
//	// 测试大于
//	testProxyDelete(t, p, 7, deleteSQL+"where id > 2")
//	testProxySelect(t, p, rows[:3], selectSQL)
//	reset(7)
//
//	testProxyDelete(t, p, 10, deleteSQL+"where id > -1")
//	testProxySelect(t, p, [][]string{}, selectSQL)
//	reset(10)
//
//	// 测试大于等于
//	testProxyDelete(t, p, 7, deleteSQL+"where id >= 3")
//	testProxySelect(t, p, rows[:3], selectSQL)
//	reset(7)
//
//	// 测试小于
//	testProxyDelete(t, p, 4, deleteSQL+"where id < 4")
//	testProxySelect(t, p, rows[4:], selectSQL)
//	reset(4)
//
//	// 测试小于等于
//	testProxyDelete(t, p, 4, deleteSQL+"where id <= 3")
//	testProxySelect(t, p, rows[4:], selectSQL)
//	reset(4)
//
//	// 测试AND
//	testProxyDelete(t, p, 1, deleteSQL+"where id = 9 AND name='name-09'")
//	testProxySelect(t, p, rows[:9], selectSQL)
//	reset(1)
//
//	testProxyDelete(t, p, 0, deleteSQL+"where id = 9 AND name='name-02'")
//	testProxySelect(t, p, rows, selectSQL)
//	testProxyDelete(t, p, 0, deleteSQL+"where balance = 9 AND name='name-02'")
//	testProxySelect(t, p, rows, selectSQL)
//	reset(0)
//}
//
//func TestProxyMultiPK(t *testing.T) {
//	columns := []*columnInfo{
//		&columnInfo{name: "company_id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
//		&columnInfo{name: "department", typ: metapb.DataType_Varchar, isPK: true},
//		&columnInfo{name: "staff_id", typ: metapb.DataType_BigInt, isPK: true},
//	}
//	p := newTestProxy(columns, nil)
//
//	testProxyInsert(t, p, 1, "insert into "+testTableName+" values(1, 'a', 1)")
//	testProxyInsert(t, p, 1, "insert into "+testTableName+" values(1, 'a', 2)")
//	testProxyInsert(t, p, 1, "insert into "+testTableName+" values(1, 'a', 3)")
//	testProxyInsert(t, p, 1, "insert into "+testTableName+" values(1, 'b', 1)")
//	testProxyInsert(t, p, 1, "insert into "+testTableName+" values(1, 'b', 2)")
//	testProxyInsert(t, p, 1, "insert into "+testTableName+" values(1, 'b', 3)")
//	testProxyInsert(t, p, 1, "insert into "+testTableName+" values(2, 'a', 1)")
//	testProxyInsert(t, p, 1, "insert into "+testTableName+" values(2, 'a', 2)")
//	testProxyInsert(t, p, 1, "insert into "+testTableName+" values(2, 'a', 3)")
//	testProxyInsert(t, p, 1, "insert into "+testTableName+" values(2, 'b', 1)")
//	testProxyInsert(t, p, 1, "insert into "+testTableName+" values(2, 'b', 2)")
//	testProxyInsert(t, p, 1, "insert into "+testTableName+" values(2, 'b', 3)")
//
//	var expected [][]string
//
//	// test select all
//	expected = [][]string{
//		[]string{"1", "a", "1"},
//		[]string{"1", "a", "2"},
//		[]string{"1", "a", "3"},
//		[]string{"1", "b", "1"},
//		[]string{"1", "b", "2"},
//		[]string{"1", "b", "3"},
//		[]string{"2", "a", "1"},
//		[]string{"2", "a", "2"},
//		[]string{"2", "a", "3"},
//		[]string{"2", "b", "1"},
//		[]string{"2", "b", "2"},
//		[]string{"2", "b", "3"},
//	}
//	testProxySelect(t, p, expected, "select * from "+testTableName)
//
//	expected = [][]string{
//		[]string{"2", "b", "3"},
//	}
//	testProxySelect(t, p, expected, "select * from "+testTableName+" where company_id=2 and department = 'b' and staff_id=3")
//
//	expected = [][]string{
//		[]string{"1", "a", "1"},
//		[]string{"1", "a", "2"},
//		[]string{"1", "a", "3"},
//		[]string{"1", "b", "1"},
//		[]string{"1", "b", "2"},
//		[]string{"1", "b", "3"},
//	}
//	testProxySelect(t, p, expected, "select * from "+testTableName+" where company_id = 1")
//
//	expected = [][]string{
//		[]string{"1", "a", "1"},
//		[]string{"1", "a", "2"},
//		[]string{"1", "a", "3"},
//	}
//	testProxySelect(t, p, expected, "select * from "+testTableName+" where company_id=1 and department = 'a'")
//
//	expected = [][]string{
//		[]string{"2", "b", "1"},
//		[]string{"2", "b", "2"},
//		[]string{"2", "b", "3"},
//	}
//	testProxySelect(t, p, expected, "select * from "+testTableName+" where company_id=2 and department = 'b'")
//
//	expected = [][]string{
//		[]string{"2", "b", "3"},
//	}
//	testProxySelect(t, p, expected, "select * from "+testTableName+" where company_id=2 and department = 'b' and staff_id=3")
//
//	expected = [][]string{
//		[]string{"2", "a", "2"},
//		[]string{"2", "b", "2"},
//	}
//	testProxySelect(t, p, expected, "select * from "+testTableName+" where company_id=2 and staff_id=2")
//
//	expected = [][]string{
//		[]string{"1", "a", "2"},
//		[]string{"1", "b", "2"},
//		[]string{"2", "a", "2"},
//		[]string{"2", "b", "2"},
//	}
//	testProxySelect(t, p, expected, "select * from "+testTableName+" where staff_id=2")
//
//	expected = [][]string{
//		[]string{"2", "b", "1"},
//		[]string{"2", "b", "2"},
//		[]string{"2", "b", "3"},
//	}
//	testProxySelect(t, p, expected, "select * from "+testTableName+" where company_id > 1 and department = 'b'")
//}
//
//func TestProxyLimit(t *testing.T) {
//	columns := []*columnInfo{
//		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
//		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
//		&columnInfo{name: "balance", typ: metapb.DataType_Double},
//	}
//	p := newTestProxy(columns, nil)
//
//	var rows [][]string
//	for i := 1; i < 10; i++ {
//		id := fmt.Sprintf("%d", i)
//		name := fmt.Sprintf("name-%02d", i)
//		balance := fmt.Sprintf("%d.05", i)
//		sql := fmt.Sprintf("insert into %s values(%s, '%s', %s)", testTableName, id, name, balance)
//		testProxyInsert(t, p, 1, sql)
//		rows = append(rows, []string{id, name, balance})
//	}
//	testProxySelect(t, p, rows[:5], "select * from "+testTableName+" limit 5")
//	testProxySelect(t, p, rows[2:5], "select * from "+testTableName+" limit 2,3")
//	testProxySelect(t, p, rows[2:], "select * from "+testTableName+" limit 2,100")
//	testProxySelect(t, p, [][]string{}, "select * from "+testTableName+" limit 100,2")
//}
//
//func TestProxyLimit1(t *testing.T) {
//	columns := []*columnInfo{
//		&columnInfo{name: "id", typ: metapb.DataType_Varchar, isUnsigned: true, isPK: true},
//		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
//		&columnInfo{name: "balance", typ: metapb.DataType_Double},
//	}
//	ranges := []*util.Range{
//		&util.Range{nil, []byte("5")},
//		&util.Range{[]byte("5"), nil},
//	}
//	//ranges := []*util.Range {
//	//	&util.Range{[]byte("a"), []byte("b")},
//	//}
//	col := &metapb.Column{
//		Name:       "id",
//		Id:         1,
//		DataType:   metapb.DataType_Varchar,
//		PrimaryKey: 1,
//	}
//	for _, r := range ranges {
//		var start, limit []byte
//		var err error
//		if r.Start != nil {
//			start, err = util.EncodePrimaryKey(start, col, r.Start)
//			if err != nil {
//				t.Fatal("encode PK failed")
//				return
//			}
//		}
//
//		if r.Limit != nil {
//			limit, err = util.EncodePrimaryKey(limit, col, r.Limit)
//			if err != nil {
//				t.Fatal("encode PK failed")
//				return
//			}
//		}
//
//		r.Start = start
//		r.Limit = limit
//	}
//	p := newTestProxy(columns, ranges)
//	defer p.Close()
//
//	var rows [][]string
//	for i := 1; i < 10; i++ {
//		id := fmt.Sprintf("%d", i)
//		name := fmt.Sprintf("name-%02d", i)
//		balance := fmt.Sprintf("%d.05", i)
//		sql := fmt.Sprintf("insert into %s values('%s', '%s', %s)", testTableName, id, name, balance)
//		testProxyInsert(t, p, 1, sql)
//		rows = append(rows, []string{id, name, balance})
//	}
//	testProxySelect(t, p, rows[:5], "select * from "+testTableName+" limit 5")
//	testProxySelect(t, p, rows[5:8], "select * from "+testTableName+" limit 5,3")
//	time.Sleep(time.Second)
//	testProxySelect(t, p, rows[2:6], "select * from "+testTableName+" limit 2,4")
//	testProxySelect(t, p, rows[2:], "select * from "+testTableName+" limit 2,100")
//	testProxySelect(t, p, [][]string{}, "select * from "+testTableName+" limit 100,2")
//}
//
//func TestProxyOrderBy(t *testing.T) {
//	columns := []*columnInfo{
//		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
//		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
//		&columnInfo{name: "balance", typ: metapb.DataType_Double},
//	}
//	p := newTestProxy(columns, nil)
//
//	rows := [][]string{
//		[]string{"1", "b", "2.2"},
//		[]string{"2", "c", "1.1"},
//		[]string{"3", "a", "3.3"},
//	}
//	for _, r := range rows {
//		sql := fmt.Sprintf("insert into "+testTableName+" values(%s, '%s', %s)", r[0], r[1], r[2])
//		testProxyInsert(t, p, 1, sql)
//	}
//
//	selectSQL := "select * from " + testTableName + " "
//	testProxySelect(t, p, rows, selectSQL+"order by id")
//	testProxySelect(t, p, [][]string{rows[2], rows[0], rows[1]}, selectSQL+"order by name")
//	testProxySelect(t, p, [][]string{rows[1], rows[0], rows[2]}, selectSQL+"order by balance")
//	// desc
//	testProxySelect(t, p, [][]string{rows[2], rows[1], rows[0]}, selectSQL+"order by id desc")
//	testProxySelect(t, p, [][]string{rows[1], rows[0], rows[2]}, selectSQL+"order by name desc")
//	testProxySelect(t, p, [][]string{rows[2], rows[0], rows[1]}, selectSQL+"order by balance desc")
//}
//
//func TestProxyDescribe(t *testing.T) {
//	columns := []*columnInfo{
//		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
//		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
//		&columnInfo{name: "balance", typ: metapb.DataType_Double},
//	}
//	p := newTestProxy(columns, nil)
//
//	expected := [][]string{
//		[]string{"id", "BigInt", "NO", "PRI", "", "1"},
//		[]string{"name", "Varchar", "NO", "", "", "2"},
//		[]string{"balance", "Double", "NO", "", "", "3"},
//	}
//	testProxyDescribe(t, p, expected, "describe "+testTableName)
//}
//
//func TestProxyMergeResult(t *testing.T) {
//	var rows []*Row
//	var stringRows [][]string
//	for i := 0; i < 10; i++ {
//		id := fmt.Sprintf("%d", i)
//		name := fmt.Sprintf("name-%02d", i)
//		balance := fmt.Sprintf("%d.05", i)
//		stringRows = append(stringRows, []string{id, name, balance})
//
//		f := []Field{
//			Field{col: "id", value: id},
//			Field{col: "name", value: name},
//			Field{col: "balance", value: balance},
//		}
//		rows = append(rows, &Row{fields: f})
//	}
//
//	fieldList := []*kvrpcpb.SelectField{
//		&kvrpcpb.SelectField{
//			Typ:    kvrpcpb.SelectField_Column,
//			Column: &metapb.Column{Name: "id"},
//		},
//		&kvrpcpb.SelectField{
//			Typ:    kvrpcpb.SelectField_Column,
//			Column: &metapb.Column{Name: "name"},
//		},
//		&kvrpcpb.SelectField{
//			Typ:    kvrpcpb.SelectField_Column,
//			Column: &metapb.Column{Name: "balance"},
//		},
//	}
//
//	sql := "select * from test"
//	stmt, err := toSelectStmt(sql)
//	if err != nil {
//		t.Fatal(err)
//	}
//	columns, _ := fieldList2ColNames(fieldList)
//	r, err := buildSelectResult(stmt, [][]*Row{rows[0:3], rows[3:6], rows[6:]}, columns)
//	if err != nil {
//		t.Fatalf("build select result failed: %v", err)
//	}
//	results := formatSelectResult(r)
//	if !reflect.DeepEqual(results, stringRows) {
//		t.Errorf("incorrect result. expected: %v, actual: %v", stringRows, results)
//	}
//
//	sql = "select * from test order by balance"
//	stmt, err = toSelectStmt(sql)
//	if err != nil {
//		t.Fatal(err)
//	}
//	r, err = buildSelectResult(stmt, [][]*Row{rows[3:6], rows[0:3], rows[6:]}, columns)
//	if err != nil {
//		t.Fatalf("build select result failed: %v", err)
//	}
//	results = formatSelectResult(r)
//	if !reflect.DeepEqual(results, stringRows) {
//		t.Errorf("incorrect result. expected: %v, actual: %v", stringRows, results)
//	}
//}
//
//func TestProxyFindPKScope(t *testing.T) {
//	columnInfos := []*columnInfo{
//		&columnInfo{name: "company_id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
//		&columnInfo{name: "department", typ: metapb.DataType_Varchar, isPK: true},
//		&columnInfo{name: "staff_id", typ: metapb.DataType_BigInt, isPK: true},
//	}
//	p := newTestProxy(columnInfos, nil)
//	table := p.router.FindTable(testDBName, testTableName)
//	if table == nil {
//		t.Fatal("could not find table")
//	}
//	cols := table.GetAllColumns()
//	if len(cols) != 3 {
//		t.Fatal("expected three columns")
//	}
//
//	var matches []*kvrpcpb.Match
//	var scope *kvrpcpb.Scope
//	var key []byte
//	var err error
//	matches = []*kvrpcpb.Match{
//		&kvrpcpb.Match{
//			Column:    cols[0],
//			Threshold: []byte("123"),
//			MatchType: kvrpcpb.MatchType_Equal,
//		},
//		&kvrpcpb.Match{
//			Column:    cols[1],
//			Threshold: []byte("abc"),
//			MatchType: kvrpcpb.MatchType_Equal,
//		},
//		&kvrpcpb.Match{
//			Column:    cols[2],
//			Threshold: []byte("456"),
//			MatchType: kvrpcpb.MatchType_Equal,
//		},
//	}
//	key, scope, err = findPKScope(table, matches)
//	if err != nil {
//		t.Fatal(err)
//	}
//	if len(key) == 0 {
//		t.Fatal("expect single get")
//	}
//	buf := encoding.EncodeUvarintAscending(nil, 123)
//	buf = encoding.EncodeBytesAscending(buf, []byte("abc"))
//	buf = encoding.EncodeUvarintAscending(buf, 456)
//	if !bytes.Equal(buf, key) {
//		t.Fatalf("wrong key. expected: %v, actual: %v", buf, key)
//	}
//
//	matches = []*kvrpcpb.Match{
//		&kvrpcpb.Match{
//			Column:    cols[0],
//			Threshold: []byte("123"),
//			MatchType: kvrpcpb.MatchType_Equal,
//		},
//		&kvrpcpb.Match{
//			Column:    cols[1],
//			Threshold: []byte("abc"),
//			MatchType: kvrpcpb.MatchType_LargerOrEqual,
//		},
//		&kvrpcpb.Match{
//			Column:    cols[2],
//			Threshold: []byte("456"),
//			MatchType: kvrpcpb.MatchType_Equal,
//		},
//	}
//	key, scope, err = findPKScope(table, matches)
//	if err != nil {
//		t.Fatal(err)
//	}
//	if len(key) != 0 {
//		t.Fatal("expect not single get")
//	}
//	start := encoding.EncodeUvarintAscending(nil, 123)
//	start = encoding.EncodeBytesAscending(start, []byte("abc"))
//	end := encoding.EncodeUvarintAscending(nil, 123)
//	end = nextComparableBytes(end)
//	if !bytes.Equal(start, scope.Start) {
//		t.Fatalf("wrong scope start. expected: %v, actual: %v", start, scope.Start)
//	}
//	if !bytes.Equal(end, scope.Limit) {
//		t.Fatalf("wrong scope limit. expected: %v, actual: %v", end, scope.Limit)
//	}
//
//	matches = []*kvrpcpb.Match{
//		&kvrpcpb.Match{
//			Column:    cols[0],
//			Threshold: []byte("123"),
//			MatchType: kvrpcpb.MatchType_Equal,
//		},
//		&kvrpcpb.Match{
//			Column:    cols[1],
//			Threshold: []byte("abc"),
//			MatchType: kvrpcpb.MatchType_Equal,
//		},
//		&kvrpcpb.Match{
//			Column:    cols[2],
//			Threshold: []byte("456"),
//			MatchType: kvrpcpb.MatchType_LargerOrEqual,
//		},
//		&kvrpcpb.Match{
//			Column:    cols[2],
//			Threshold: []byte("789"),
//			MatchType: kvrpcpb.MatchType_Less,
//		},
//	}
//	key, scope, err = findPKScope(table, matches)
//	if err != nil {
//		t.Fatal(err)
//	}
//	if len(key) != 0 {
//		t.Fatal("expect not single get")
//	}
//	buf = encoding.EncodeUvarintAscending(nil, 123)
//	buf = encoding.EncodeBytesAscending(buf, []byte("abc"))
//	start = encoding.EncodeUvarintAscending(buf, 456)
//	end = encoding.EncodeUvarintAscending(buf, 789)
//	if !bytes.Equal(start, scope.Start) {
//		t.Fatalf("wrong scope start. expected: %v, actual: %v", start, scope.Start)
//	}
//	if !bytes.Equal(end, scope.Limit) {
//		t.Fatalf("wrong scope limit. expected: %v, actual: %v", end, scope.Limit)
//	}
//
//	matches = []*kvrpcpb.Match{
//		&kvrpcpb.Match{
//			Column:    cols[0],
//			Threshold: []byte("123"),
//			MatchType: kvrpcpb.MatchType_Equal,
//		},
//		&kvrpcpb.Match{
//			Column:    cols[1],
//			Threshold: []byte("abc"),
//			MatchType: kvrpcpb.MatchType_Equal,
//		},
//		&kvrpcpb.Match{
//			Column:    cols[2],
//			Threshold: []byte("123"),
//			MatchType: kvrpcpb.MatchType_LargerOrEqual,
//		},
//		&kvrpcpb.Match{
//			Column:    cols[2],
//			Threshold: []byte("456"),
//			MatchType: kvrpcpb.MatchType_Larger,
//		},
//		&kvrpcpb.Match{
//			Column:    cols[2],
//			Threshold: []byte("789"),
//			MatchType: kvrpcpb.MatchType_LessOrEqual,
//		},
//		&kvrpcpb.Match{
//			Column:    cols[2],
//			Threshold: []byte("999"),
//			MatchType: kvrpcpb.MatchType_Less,
//		},
//	}
//	key, scope, err = findPKScope(table, matches)
//	if err != nil {
//		t.Fatal(err)
//	}
//	if len(key) != 0 {
//		t.Fatal("expect not single get")
//	}
//	buf = encoding.EncodeUvarintAscending(nil, 123)
//	buf = encoding.EncodeBytesAscending(buf, []byte("abc"))
//	start = encoding.EncodeUvarintAscending(buf, 456)
//	start = nextComparableBytes(start)
//	end = encoding.EncodeUvarintAscending(buf, 789)
//	end = nextComparableBytes(end)
//	if !bytes.Equal(start, scope.Start) {
//		t.Fatalf("wrong scope start. expected: %v, actual: %v", start, scope.Start)
//	}
//	if !bytes.Equal(end, scope.Limit) {
//		t.Fatalf("wrong scope limit. expected: %v, actual: %v", end, scope.Limit)
//	}
//}
//
//func TestProxyTTL(t *testing.T) {
//	columns := []*columnInfo{
//		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
//		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
//		&columnInfo{name: "ttl", typ: metapb.DataType_BigInt},
//	}
//	p := newTestProxy(columns, nil)
//
//	// insert
//	rows := [][]string{
//		[]string{"1", "myname", fmt.Sprintf("%d", time.Now().Add(time.Millisecond*10).UnixNano()/1000000)},
//		[]string{"2", "myname", fmt.Sprintf("%d", time.Now().Add(time.Millisecond*20).UnixNano()/1000000)},
//		[]string{"3", "myname2", fmt.Sprintf("%d", time.Now().Add(time.Second).UnixNano()/1000000)},
//		[]string{"4", "myname3", "0"},
//	}
//
//	for _, r := range rows {
//		testProxyInsert(t, p, 1, fmt.Sprintf("insert into %s (id,name,ttl) values(%s, '%s', %s)", testTableName, r[0], r[1], r[2]))
//	}
//
//	// select and check
//	testProxySelect(t, p, rows, "select * from "+testTableName)
//
//	time.Sleep(time.Millisecond * 10)
//	testProxySelect(t, p, rows[1:], "select * from "+testTableName)
//	time.Sleep(time.Millisecond * 10)
//	testProxySelect(t, p, rows[2:], "select * from "+testTableName)
//}
//
//func TestProxyAggregate(t *testing.T) {
//	columns := []*columnInfo{
//		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
//		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
//		&columnInfo{name: "balance", typ: metapb.DataType_Double},
//	}
//	p := newTestProxy(columns, nil)
//
//	rows := [][]string{
//		[]string{"1", "b", "2.2"},
//		[]string{"2", "c", "1.1"},
//		[]string{"3", "a", "3.3"},
//	}
//	for _, r := range rows {
//		sql := fmt.Sprintf("insert into "+testTableName+" values(%s, '%s', %s)", r[0], r[1], r[2])
//		testProxyInsert(t, p, 1, sql)
//	}
//	selectSQL := "select count(*) from " + testTableName + " "
//	testProxySelect(t, p, [][]string{[]string{"3"}}, selectSQL)
//
//	sql := fmt.Sprintf("insert into "+testTableName+" (id,name) values(%d, '%s')", 4, "d")
//	testProxyInsert(t, p, 1, sql)
//
//	selectSQL = "select count(id) from " + testTableName + " "
//	testProxySelect(t, p, [][]string{[]string{"4"}}, selectSQL)
//
//	selectSQL = "select count(balance) from " + testTableName + " "
//	testProxySelect(t, p, [][]string{[]string{"3"}}, selectSQL)
//
//	selectSQL = "select sum(id) from " + testTableName + " "
//	testProxySelect(t, p, [][]string{[]string{"10"}}, selectSQL)
//
//	selectSQL = "select sum(balance) from " + testTableName + " "
//	testProxySelect(t, p, [][]string{[]string{"6.6"}}, selectSQL)
//}
//
//func TestProxyInsertRows1(t *testing.T) {
//	columns := []*columnInfo{
//		&columnInfo{name: "id", typ: metapb.DataType_Varchar, isPK: true},
//		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
//	}
//	ranges := []*util.Range{
//		&util.Range{nil, []byte("a")},
//		&util.Range{[]byte("a"), []byte("b")},
//		&util.Range{[]byte("b"), []byte("c")},
//		&util.Range{[]byte("c"), []byte("d")},
//		&util.Range{[]byte("d"), []byte("e")},
//		&util.Range{[]byte("e"), []byte("f")},
//		&util.Range{[]byte("f"), []byte("g")},
//		&util.Range{[]byte("g"), []byte("h")},
//		&util.Range{[]byte("h"), []byte("i")},
//		&util.Range{[]byte("i"), []byte("j")},
//		&util.Range{[]byte("j"), []byte("k")},
//		&util.Range{[]byte("k"), []byte("l")},
//		&util.Range{[]byte("l"), []byte("m")},
//		&util.Range{[]byte("m"), []byte("n")},
//		&util.Range{[]byte("n"), []byte("o")},
//		&util.Range{[]byte("o"), []byte("p")},
//		&util.Range{[]byte("p"), []byte("q")},
//		&util.Range{[]byte("q"), []byte("r")},
//		&util.Range{[]byte("r"), []byte("s")},
//		&util.Range{[]byte("s"), []byte("t")},
//		&util.Range{[]byte("t"), []byte("u")},
//		&util.Range{[]byte("u"), []byte("v")},
//		&util.Range{[]byte("v"), []byte("w")},
//		&util.Range{[]byte("w"), []byte("x")},
//		&util.Range{[]byte("x"), nil},
//	}
//	//ranges := []*util.Range {
//	//	&util.Range{[]byte("a"), []byte("b")},
//	//}
//	col := &metapb.Column{
//		Name:       "id",
//		Id:         1,
//		DataType:   metapb.DataType_Varchar,
//		PrimaryKey: 1,
//	}
//	for _, r := range ranges {
//		var start, limit []byte
//		var err error
//		if r.Start != nil {
//			start, err = util.EncodePrimaryKey(start, col, r.Start)
//			if err != nil {
//				t.Fatal("encode PK failed")
//				return
//			}
//		}
//
//		if r.Limit != nil {
//			limit, err = util.EncodePrimaryKey(limit, col, r.Limit)
//			if err != nil {
//				t.Fatal("encode PK failed")
//				return
//			}
//		}
//
//		r.Start = start
//		r.Limit = limit
//	}
//	p := newTestProxy(columns, ranges)
//	defer p.Close()
//	// insert
//	//sql := "insert into "+testTableName+` (id,name) values('a', 'amyname')`
//	sql := "insert into " + testTableName + ` (id,name) values
//	('a', 'amyname'),('b', 'bmyname'),('c', 'cmyname'),('d', 'dmyname'),
//	('e', 'emyname'),('f', 'fmyname'),('g', 'gmyname'),('h', 'hmyname'),
//	('i', 'imyname'),('j', 'jmyname'),('k', 'kmyname'),('l', 'lmyname'),
//	('m', 'mmyname'),('n', 'nmyname'),('o', 'omyname'),('p', 'pmyname'),
//	('q', 'qmyname'),('r', 'rmyname'),('s', 'smyname'),('t', 'tmyname'),
//	('u', 'umyname'),('v', 'vmyname')`
//	testProxyInsert(t, p, 22, sql)
//	// select and check
//	expected := [][]string{
//		[]string{"a", "amyname"}, []string{"b", "bmyname"}, []string{"c", "cmyname"}, []string{"d", "dmyname"},
//		[]string{"e", "emyname"}, []string{"f", "fmyname"}, []string{"g", "gmyname"}, []string{"h", "hmyname"},
//		[]string{"i", "imyname"}, []string{"j", "jmyname"}, []string{"k", "kmyname"}, []string{"l", "lmyname"},
//		[]string{"m", "mmyname"}, []string{"n", "nmyname"}, []string{"o", "omyname"}, []string{"p", "pmyname"},
//		[]string{"q", "qmyname"}, []string{"r", "rmyname"}, []string{"s", "smyname"}, []string{"t", "tmyname"},
//		[]string{"u", "umyname"}, []string{"v", "vmyname"},
//	}
//	testProxySelect(t, p, expected, "select * from "+testTableName)
//	testProxySelect(t, p, [][]string{[]string{"22"}}, "select count(*) from "+testTableName)
//	testProxySelect(t, p, [][]string{[]string{"2", "2", "2"}}, "select count(*),count(id),count(name) from "+testTableName+" where id > 't'")
//	testProxySelect(t, p, [][]string{[]string{"22"}}, "select count(id) from "+testTableName)
//	testProxySelect(t, p, [][]string{[]string{"v"}}, "select max(id) from "+testTableName)
//	testProxySelect(t, p, [][]string{[]string{"a"}}, "select min(id) from "+testTableName)
//}
//
//func TestProxyInsertRows2(t *testing.T) {
//	columns := []*columnInfo{
//		&columnInfo{name: "id", typ: metapb.DataType_Varchar, isPK: true},
//		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
//	}
//	ranges := []*util.Range{
//		&util.Range{nil, []byte("a")},
//		&util.Range{[]byte("a"), []byte("b")},
//		&util.Range{[]byte("b"), []byte("c")},
//		&util.Range{[]byte("c"), []byte("d")},
//		&util.Range{[]byte("d"), []byte("e")},
//		&util.Range{[]byte("e"), []byte("f")},
//		&util.Range{[]byte("f"), []byte("g")},
//		&util.Range{[]byte("g"), []byte("h")},
//		&util.Range{[]byte("h"), []byte("i")},
//		&util.Range{[]byte("i"), []byte("j")},
//		&util.Range{[]byte("j"), []byte("k")},
//		&util.Range{[]byte("k"), []byte("l")},
//		&util.Range{[]byte("l"), []byte("m")},
//		&util.Range{[]byte("m"), []byte("n")},
//		&util.Range{[]byte("n"), []byte("o")},
//		&util.Range{[]byte("o"), []byte("p")},
//		&util.Range{[]byte("p"), []byte("q")},
//		&util.Range{[]byte("q"), []byte("r")},
//		&util.Range{[]byte("r"), []byte("s")},
//		&util.Range{[]byte("s"), []byte("t")},
//		&util.Range{[]byte("t"), []byte("u")},
//		&util.Range{[]byte("u"), []byte("v")},
//		&util.Range{[]byte("v"), []byte("w")},
//		&util.Range{[]byte("w"), []byte("x")},
//		&util.Range{[]byte("x"), nil},
//	}
//	//ranges := []*util.Range {
//	//	&util.Range{[]byte("a"), []byte("b")},
//	//}
//	col := &metapb.Column{
//		Name:       "id",
//		Id:         1,
//		DataType:   metapb.DataType_Varchar,
//		PrimaryKey: 1,
//	}
//	for _, r := range ranges {
//		var start, limit []byte
//		var err error
//		if r.Start != nil {
//			start, err = util.EncodePrimaryKey(start, col, r.Start)
//			if err != nil {
//				t.Fatal("encode PK failed")
//				return
//			}
//		}
//
//		if r.Limit != nil {
//			limit, err = util.EncodePrimaryKey(limit, col, r.Limit)
//			if err != nil {
//				t.Fatal("encode PK failed")
//				return
//			}
//		}
//
//		r.Start = start
//		r.Limit = limit
//	}
//	p := newTestProxy(columns, ranges)
//	defer p.Close()
//	// insert
//	//sql := "insert into "+testTableName+` (id,name) values('a', 'amyname')`
//	sql := "insert into " + testTableName + ` (id,name) values
//	('a', 'amyname'),('b', 'bmyname'),('c', 'cmyname'),('d', 'dmyname'),
//	('e', 'emyname'),('f', 'fmyname'),('g', 'gmyname'),('h', 'hmyname'),
//	('i', 'imyname'),('j', 'jmyname'),('k', 'kmyname'),('l', 'lmyname'),
//	('m', 'mmyname'),('n', 'nmyname'),('o', 'omyname'),('p', 'pmyname'),
//	('q', 'qmyname'),('r', 'rmyname'),('s', 'smyname'),('t', 'tmyname')`
//	testProxyInsert(t, p, 20, sql)
//
//	// select and check
//	expected := [][]string{
//		[]string{"a", "amyname"}, []string{"b", "bmyname"}, []string{"c", "cmyname"}, []string{"d", "dmyname"},
//		[]string{"e", "emyname"}, []string{"f", "fmyname"}, []string{"g", "gmyname"}, []string{"h", "hmyname"},
//		[]string{"i", "imyname"}, []string{"j", "jmyname"}, []string{"k", "kmyname"}, []string{"l", "lmyname"},
//		[]string{"m", "mmyname"}, []string{"n", "nmyname"}, []string{"o", "omyname"}, []string{"p", "pmyname"},
//		[]string{"q", "qmyname"}, []string{"r", "rmyname"}, []string{"s", "smyname"}, []string{"t", "tmyname"},
//	}
//	testProxySelect(t, p, expected, "select * from "+testTableName)
//}
//
//func TestProxyInsertRows3(t *testing.T) {
//	columns := []*columnInfo{
//		&columnInfo{name: "id", typ: metapb.DataType_Varchar, isPK: true},
//		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
//	}
//	ranges := []*util.Range{
//		&util.Range{nil, []byte("a")},
//		&util.Range{[]byte("a"), []byte("b")},
//		&util.Range{[]byte("b"), []byte("c")},
//		&util.Range{[]byte("c"), []byte("d")},
//		&util.Range{[]byte("d"), []byte("e")},
//		&util.Range{[]byte("e"), []byte("f")},
//		&util.Range{[]byte("f"), []byte("g")},
//		&util.Range{[]byte("g"), []byte("h")},
//		&util.Range{[]byte("h"), []byte("i")},
//		&util.Range{[]byte("i"), []byte("j")},
//		&util.Range{[]byte("j"), []byte("k")},
//		&util.Range{[]byte("k"), []byte("l")},
//		&util.Range{[]byte("l"), []byte("m")},
//		&util.Range{[]byte("m"), []byte("n")},
//		&util.Range{[]byte("n"), []byte("o")},
//		&util.Range{[]byte("o"), []byte("p")},
//		&util.Range{[]byte("p"), []byte("q")},
//		&util.Range{[]byte("q"), []byte("r")},
//		&util.Range{[]byte("r"), []byte("s")},
//		&util.Range{[]byte("s"), []byte("t")},
//		&util.Range{[]byte("t"), []byte("u")},
//		&util.Range{[]byte("u"), []byte("v")},
//		&util.Range{[]byte("v"), []byte("w")},
//		&util.Range{[]byte("w"), []byte("x")},
//		&util.Range{[]byte("x"), nil},
//	}
//	//ranges := []*util.Range {
//	//	&util.Range{[]byte("a"), []byte("b")},
//	//}
//	col := &metapb.Column{
//		Name:       "id",
//		Id:         1,
//		DataType:   metapb.DataType_Varchar,
//		PrimaryKey: 1,
//	}
//	for _, r := range ranges {
//		var start, limit []byte
//		var err error
//		if r.Start != nil {
//			start, err = util.EncodePrimaryKey(start, col, r.Start)
//			if err != nil {
//				t.Fatal("encode PK failed")
//				return
//			}
//		}
//
//		if r.Limit != nil {
//			limit, err = util.EncodePrimaryKey(limit, col, r.Limit)
//			if err != nil {
//				t.Fatal("encode PK failed")
//				return
//			}
//		}
//
//		r.Start = start
//		r.Limit = limit
//	}
//	p := newTestProxy(columns, ranges)
//	defer p.Close()
//
//	// insert
//	//sql := "insert into "+testTableName+` (id,name) values('a', 'amyname')`
//	sql := "insert into " + testTableName + ` (id,name) values
//	('a', 'amyname'),('b', 'bmyname'),('c', 'cmyname'),('d', 'dmyname'),
//	('e', 'emyname'),('f', 'fmyname'),('g', 'gmyname'),('h', 'hmyname'),
//	('i', 'imyname')`
//	testProxyInsert(t, p, 9, sql)
//
//	// select and check
//	expected := [][]string{
//		[]string{"a", "amyname"}, []string{"b", "bmyname"}, []string{"c", "cmyname"}, []string{"d", "dmyname"},
//		[]string{"e", "emyname"}, []string{"f", "fmyname"}, []string{"g", "gmyname"}, []string{"h", "hmyname"},
//		[]string{"i", "imyname"},
//	}
//	testProxySelect(t, p, expected, "select * from "+testTableName)
//}
//
//func TestProxyInsertRows4(t *testing.T) {
//	columns := []*columnInfo{
//		&columnInfo{name: "id", typ: metapb.DataType_Varchar, isPK: true},
//		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
//	}
//	ranges := []*util.Range{
//		&util.Range{nil, []byte("a")},
//		&util.Range{[]byte("a"), []byte("b")},
//		&util.Range{[]byte("b"), []byte("c")},
//		&util.Range{[]byte("c"), []byte("d")},
//		&util.Range{[]byte("d"), []byte("e")},
//		&util.Range{[]byte("e"), []byte("f")},
//		&util.Range{[]byte("f"), []byte("g")},
//		&util.Range{[]byte("g"), []byte("h")},
//		&util.Range{[]byte("h"), []byte("i")},
//		&util.Range{[]byte("i"), []byte("j")},
//		&util.Range{[]byte("j"), []byte("k")},
//		&util.Range{[]byte("k"), []byte("l")},
//		&util.Range{[]byte("l"), []byte("m")},
//		&util.Range{[]byte("m"), []byte("n")},
//		&util.Range{[]byte("n"), []byte("o")},
//		&util.Range{[]byte("o"), []byte("p")},
//		&util.Range{[]byte("p"), []byte("q")},
//		&util.Range{[]byte("q"), []byte("r")},
//		&util.Range{[]byte("r"), []byte("s")},
//		&util.Range{[]byte("s"), []byte("t")},
//		&util.Range{[]byte("t"), []byte("u")},
//		&util.Range{[]byte("u"), []byte("v")},
//		&util.Range{[]byte("v"), []byte("w")},
//		&util.Range{[]byte("w"), []byte("x")},
//		&util.Range{[]byte("x"), nil},
//	}
//	//ranges := []*util.Range {
//	//	&util.Range{[]byte("a"), []byte("b")},
//	//}
//	col := &metapb.Column{
//		Name:       "id",
//		Id:         1,
//		DataType:   metapb.DataType_Varchar,
//		PrimaryKey: 1,
//	}
//	for _, r := range ranges {
//		var start, limit []byte
//		var err error
//		if r.Start != nil {
//			start, err = util.EncodePrimaryKey(start, col, r.Start)
//			if err != nil {
//				t.Fatal("encode PK failed")
//				return
//			}
//		}
//
//		if r.Limit != nil {
//			limit, err = util.EncodePrimaryKey(limit, col, r.Limit)
//			if err != nil {
//				t.Fatal("encode PK failed")
//				return
//			}
//		}
//
//		r.Start = start
//		r.Limit = limit
//	}
//	p := newTestProxy(columns, ranges)
//	defer p.Close()
//
//	// insert
//	//sql := "insert into "+testTableName+` (id,name) values('a', 'amyname')`
//	sql := "insert into " + testTableName + ` (id,name) values
//	('a', 'amyname')`
//	testProxyInsert(t, p, 1, sql)
//
//	// select and check
//	expected := [][]string{
//		[]string{"a", "amyname"},
//	}
//	testProxySelect(t, p, expected, "select * from "+testTableName)
//}
//

func TestProxyAdminRoute(t *testing.T) {
	columns := []*columnInfo{
		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
		&columnInfo{name: "balance", typ: metapb.DataType_Double},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	table := makeTestTable(columns)
	var start, end []byte
	start = util.EncodeStorePrefix(util.Store_Prefix_KV, table.GetId())
	_, end = bytesPrefix(start)
	rng := &metapb.Range{
		Id:         1,
		TableId:    1,
		StartKey:   start,
		EndKey:     end,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	p := newTestProxy(db, table, rng)
	defer p.Close()

	skey := formatRouteKey(start)
	ekey := formatRouteKey(end)

	testProxyAdmin(t, p, [][]string{
		[]string{fmt.Sprintf("%d", rng.GetId()), string(skey), string(ekey), "1", "127.0.0.1:6060", fmt.Sprintf("%d:%d", rng.RangeEpoch.ConfVer, rng.RangeEpoch.Version)},
	}, fmt.Sprintf("admin route('show', '%s')", testTableName))
}

//
//func TestProxyRoute(t *testing.T){
//	conf := new(Config)
//	*config.ConfigFileName="/home/dingjun/workspace/fbase/src/gateway-server/conf/app.conf"
//	conf.LoadConfig()
//	log.InitFileLog(conf.LogDir,conf.LogModule,conf.LogLevel)
//	p := NewProxy(conf.MasterServerAddrs, conf);
//	db := p.router.FindDB("db1")
//	fmt.Println(db.String())
//
//	table := p.router.FindTable("db1","table1")
//
//	fmt.Println(table.String())
//}

func TestProxyInsert(t *testing.T) {
	log.InitFileLog(logPath, "proxy", "debug")
	columns := []*columnInfo{
		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
		&columnInfo{name: "balance", typ: metapb.DataType_Double},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	table := makeTestTable(columns)
	start := util.EncodeStorePrefix(util.Store_Prefix_KV, table.GetId())
	r := util.BytesPrefix(start)
	var pks []*metapb.Column
	for _, col := range table.Columns {
		if col.Name == "id" {
			pks = append(pks, col)
			break
		}
	}
	rng := &metapb.Range{
		Id:          1,
		TableId:     1,
		StartKey:    r.Start,
		EndKey:      r.Limit,
		RangeEpoch:  &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:       []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
		PrimaryKeys: pks,
	}
	p := newTestProxy(db, table, rng)

	defer CloseMock(p)
	defer p.Close()
	// testProxySelect(t, p, [][]string{}, "select * from "+testTableName+" where id > 1 and id < 100")

	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(1, 'myname', 0.0075)")
	testProxyInsert(t, p, 2, "insert into "+testTableName+"(id,name,balance) values(2, 'myname', 0.0075),(3, 'myname', 0.0075)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(4, 'myname', 0.0075)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(5, 'myname2', 1)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(iD,nAMe,bAlaNce) values(6, 'myname3', 3.1)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(iD,nAMe,bAlaNce) values(7, 'myname4', 2)")

	expected := [][]string{
		[]string{"1", "myname", "0.0075"},
		[]string{"2", "myname", "0.0075"},
		[]string{"3", "myname", "0.0075"},
		[]string{"4", "myname", "0.0075"},
		[]string{"5", "myname2", "1"},
		[]string{"6", "myname3", "3.1"},
		[]string{"7", "myname4", "2"},
	}
	testProxySelect(t, p, expected, "select * from "+testTableName)
}

func TestProxyInsert_AutoIncrement(t *testing.T) {
	//log.InitFileLog(logPath, "proxy", "debug")
	//autoIncrement only is tinyint,
	columns := []*columnInfo{
		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true, autoInc: true},
		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
		&columnInfo{name: "balance", typ: metapb.DataType_Double},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	table := makeTestTable(columns)
	start := util.EncodeStorePrefix(util.Store_Prefix_KV, table.GetId())
	r := util.BytesPrefix(start)
	var pks []*metapb.Column
	for _, col := range table.Columns {
		if col.Name == "id" {
			pks = append(pks, col)
			break
		}
	}
	rng := &metapb.Range{
		Id:          1,
		TableId:     1,
		StartKey:    r.Start,
		EndKey:      r.Limit,
		RangeEpoch:  &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:       []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
		PrimaryKeys: pks,
	}
	p := newTestProxy(db, table, rng)

	defer CloseMock(p)
	defer p.Close()
	testProxySelect(t, p, [][]string{}, "select * from "+testTableName+" where id > 1 and id < 100")

	testProxyInsert(t, p, 1, "insert into "+testTableName+"(name,balance) values('myname', 0.0075)")
	testProxyInsert(t, p, 3, "insert into "+testTableName+"(name,balance) values('myname', 0.0075),('myname', 0.0075), ('myname', 0.0075)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(name,balance) values('myname', 0.0075)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(name,balance) values('myname2', 1)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(nAMe,bAlaNce) values('myname3', 3.1)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(nAMe,bAlaNce) values('myname4', 2)")
	//
	// select and check
	expected := [][]string{
		[]string{"1", "myname", "0.0075"},
		[]string{"2", "myname", "0.0075"},
		[]string{"3", "myname", "0.0075"},
		[]string{"4", "myname", "0.0075"},
		[]string{"5", "myname", "0.0075"},
		[]string{"6", "myname2", "1"},
		[]string{"7", "myname3", "3.1"},
		[]string{"8", "myname4", "2"},
	}
	testProxySelect(t, p, expected, "select * from "+testTableName)
}

//no support
func TestProxyInsert_Binary(t *testing.T) {
	columns := []*columnInfo{
		&columnInfo{name: "k", typ: metapb.DataType_Varchar, isPK: true},
		&columnInfo{name: "v", typ: metapb.DataType_Binary},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	table := makeTestTable(columns)
	start := util.EncodeStorePrefix(util.Store_Prefix_KV, table.GetId())
	r := util.BytesPrefix(start)
	var pks []*metapb.Column
	for _, col := range table.Columns {
		if col.Name == "k" {
			pks = append(pks, col)
			break
		}
	}
	rng := &metapb.Range{
		Id:          1,
		TableId:     1,
		StartKey:    r.Start,
		EndKey:      r.Limit,
		RangeEpoch:  &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:       []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
		PrimaryKeys: pks,
	}
	p := newTestProxy(db, table, rng)

	defer CloseMock(p)
	defer p.Close()

	content, err := ZipFile("../conf/config.zip")
	if err != nil {
		t.Fatalf("read zip file err %v", err)
	}
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(k,v) values('key1', " + content+ ")")

	//
	// select and check
	expected := [][]string{
		[]string{"key1", content},
	}
	testProxySelect(t, p, expected, "select * from "+testTableName)

}

func ZipFile(zipPath string) (string, error) {
	f, err := os.Open(zipPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	rd := bufio.NewReader(f)

	var lines []string
	for {
		line, err := rd.ReadString('\n')
		if err != nil && io.EOF == err {
			break
		}
		lines = append(lines, line)
	}
	content := strings.Join(lines, "")
	return content, nil
}

func TestProxyInsertOneRowWithRangeChange(t *testing.T) {
	log.InitFileLog(logPath, "proxy", "debug")
	columns := []*columnInfo{
		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
		&columnInfo{name: "balance", typ: metapb.DataType_Double},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	table := makeTestTable(columns)
	start := util.EncodeStorePrefix(util.Store_Prefix_KV, table.GetId())

	var pkCol *metapb.Column
	for _, col := range table.Columns {
		if col.Name == "id" {
			pkCol = col
			break
		}
	}
	middle, _ := util.EncodePrimaryKey(start, pkCol, []byte(strconv.Itoa(5)));
	r := util.BytesPrefix(start)
	rng := &metapb.Range{
		Id:         1,
		TableId:    1,
		StartKey:   r.Start,
		EndKey:     r.Limit,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	p := newTestProxy(db, table, rng)
	defer CloseMock(p)
	defer p.Close()

	oldRng := deepcopy.Iface(rng).(*metapb.Range)
	newRng := deepcopy.Iface(rng).(*metapb.Range)

	newRng.Id++
	newRng.StartKey = middle
	oldRng.EndKey = middle
	oldRng.RangeEpoch.Version++

	MockDs.RangeSplit(oldRng, newRng)

	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(2, 'myname2', 1)")
	testProxyInsert(t, p, 2, "insert into "+testTableName+"(id,name,balance) values(1, 'myname', 0.0075),(2, 'myname', 0.0075)")

	//time.Sleep(time.Second*5)
}

func TestProxyInsertOneRowWithErrorNode(t *testing.T) {
	log.InitFileLog(logPath, "proxy", "debug")
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
		Id:         1,
		TableId:    1,
		StartKey:   r.Start,
		EndKey:     r.Limit,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	p := newTestProxy(db, table, rng)
	defer CloseMock(p)
	defer p.Close()

	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(2, 'myname2', 1)")
	testProxyInsert(t, p, 2, "insert into "+testTableName+"(id,name,balance) values(1, 'myname', 0.0075),(2, 'myname', 0.0075)")

	oldRng := deepcopy.Iface(rng).(*metapb.Range)
	newRng := deepcopy.Iface(rng).(*metapb.Range)
	var peers []*metapb.Peer
	peers = append(peers, &metapb.Peer{Id: 3, NodeId: 2})
	newRng.Peers = peers
	MockDs.DelRange(oldRng.GetId())
	MockDs1.SetRange(newRng)
	MockMs.SetRange(newRng)

	testProxyInsert(t, p, 0, "insert into "+testTableName+"(id,name,balance) values(2, 'myname2', 1)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(2, 'myname2', 1)")
	testProxyInsert(t, p, 2, "insert into "+testTableName+"(id,name,balance) values(1, 'myname', 0.0075),(2, 'myname', 0.0075)")
}

func TestProxyInsertWithRangeChange(t *testing.T) {
	log.InitFileLog(logPath, "proxy", "debug")
	columns := []*columnInfo{
		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
		&columnInfo{name: "balance", typ: metapb.DataType_Double},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	table := makeTestTable(columns)
	start := util.EncodeStorePrefix(util.Store_Prefix_KV, table.GetId())

	var pkCol *metapb.Column
	for _, col := range table.Columns {
		if col.Name == "id" {
			pkCol = col
			break
		}
	}

	r := util.BytesPrefix(start)
	middle, _ := util.EncodePrimaryKey(start, pkCol, []byte(strconv.Itoa(8)));
	rng := &metapb.Range{
		Id:         1,
		TableId:    1,
		StartKey:   r.Start,
		EndKey:     middle,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	rng1 := &metapb.Range{
		Id:         2,
		TableId:    1,
		StartKey:   middle,
		EndKey:     r.Limit,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	p := newTestProxy2(db, table, rng, rng1)
	defer CloseMock(p)
	defer p.Close()

	testProxyInsert(t, p, 4, "insert into "+testTableName+"(id,name,balance) values(2, 'myname2', 1),(6, 'myname6', 1),(7, 'myname7', 1),(9, 'myname9', 1)")

	oldRng := deepcopy.Iface(rng).(*metapb.Range)

	middle2, _ := util.EncodePrimaryKey(start, pkCol, []byte(strconv.Itoa(5)));
	oldRng.EndKey = middle2
	oldRng.RangeEpoch.Version++

	newRng := deepcopy.Iface(rng).(*metapb.Range)
	newRng.Id++
	newRng.StartKey = middle2

	MockDs.RangeSplit(oldRng, newRng)
	MockMs.SetRange(oldRng)
	MockMs.SetRange(newRng)
	log.Info("**********change range route***********8")

	testProxyInsert(t, p, 4, "insert into "+testTableName+"(id,name,balance) values(2, 'myname2', 1),(6, 'myname6', 1),(7, 'myname7', 1),(9, 'myname9', 1)")

	//time.Sleep(time.Second*5)
}

func TestSimpleSelect(t *testing.T) {
	log.InitFileLog(logPath, "proxy", "debug")
	columns := []*columnInfo{
		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isPK: true},
		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
		&columnInfo{name: "balance", typ: metapb.DataType_Double},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	table := makeTestTable(columns)
	start := util.EncodeStorePrefix(util.Store_Prefix_KV, table.GetId())

	var pkCol *metapb.Column
	var pks []*metapb.Column
	for _, col := range table.Columns {
		if col.Name == "id" {
			pkCol = col
			pks = append(pks, col)
			break
		}
	}

	r := util.BytesPrefix(start)
	middle, _ := util.EncodePrimaryKey(start, pkCol, []byte(strconv.Itoa(8)));
	rng := &metapb.Range{
		Id:         1,
		TableId:    1,
		StartKey:   r.Start,
		EndKey:     middle,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	rng.PrimaryKeys = pks
	rng1 := &metapb.Range{
		Id:         2,
		TableId:    1,
		StartKey:   middle,
		EndKey:     r.Limit,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	rng1.PrimaryKeys = pks
	p := newTestProxy2(db, table, rng, rng1)
	defer CloseMock(p)
	defer p.Close()

	testProxySelect(t, p, [][]string{}, "select * from "+testTableName)

	//insert
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(1, 'myname', 0.0075)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(2, 'myname2', 1)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(3, 'myname3', 3.1)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(4, 'myname4', 10.1)")

	// select and check
	expected := [][]string{
		[]string{"1", "myname", "0.0075"},
		[]string{"2", "myname2", "1"},
		[]string{"3", "myname3", "3.1"},
		[]string{"4", "myname4", "10.1"},
	}
	testProxySelect(t, p, expected, "select * from "+testTableName)

	log.Info("*****=======prepare delete======*****")
	//delete one
	testProxyDelete(t, p, 1, "delete from "+testTableName+" where id = 1")
	expected = [][]string{
		[]string{"2", "myname2", "1"},
		[]string{"3", "myname3", "3.1"},
		[]string{"4", "myname4", "10.1"},
	}
	testProxySelect(t, p, expected, "select * from "+testTableName)

}

func TestSelectRoutChange(t *testing.T) {
	log.InitFileLog(logPath, "proxy", "debug")
	columns := []*columnInfo{
		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isPK: true},
		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
		&columnInfo{name: "balance", typ: metapb.DataType_Double},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	table := makeTestTable(columns)
	start := util.EncodeStorePrefix(util.Store_Prefix_KV, table.GetId())

	var pkCol *metapb.Column
	var pks []*metapb.Column
	for _, col := range table.Columns {
		if col.Name == "id" {
			pkCol = col
			pks = append(pks, col)
			break
		}
	}

	r := util.BytesPrefix(start)
	middle, _ := util.EncodePrimaryKey(start, pkCol, []byte(strconv.Itoa(8)));
	lastRangId := uint64(1)

	rng := &metapb.Range{
		Id:         lastRangId,
		TableId:    1,
		StartKey:   r.Start,
		EndKey:     middle,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	rng.PrimaryKeys = pks

	lastRangId += 1
	rng1 := &metapb.Range{
		Id:         lastRangId,
		TableId:    1,
		StartKey:   middle,
		EndKey:     r.Limit,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	rng1.PrimaryKeys = pks
	p := newTestProxy2(db, table, rng, rng1)

	defer CloseMock(p)
	defer p.Close()

	testProxySelect(t, p, [][]string{}, "select * from "+testTableName)

	//insert
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(1, 'myname', 0.0075)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(2, 'myname2', 1)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(6, 'myname3', 3.1)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(9, 'myname4', 10.1)")

	// select and check
	expected := [][]string{
		[]string{"1", "myname", "0.0075"},
		[]string{"2", "myname2", "1"},
		[]string{"6", "myname3", "3.1"},
		[]string{"9", "myname4", "10.1"},
	}

	testProxySelect(t, p, expected, "select id,name,balance from "+testTableName)

	oldRng := deepcopy.Iface(rng).(*metapb.Range)

	middle2, _ := util.EncodePrimaryKey(start, pkCol, []byte(strconv.Itoa(5)));
	oldRng.EndKey = middle2
	oldRng.RangeEpoch.Version++

	newRng := deepcopy.Iface(rng).(*metapb.Range)
	lastRangId += 1
	newRng.Id = lastRangId
	newRng.StartKey = middle2

	MockDs.RangeSplit(oldRng, newRng)
	MockMs.SetRange(oldRng)
	MockMs.SetRange(newRng)
	log.Info("**********change range route***********8")

	testProxySelect(t, p, expected, "select * from "+testTableName)

}

func TestRowDeleteRoutChange(t *testing.T) {
	log.InitFileLog(logPath, "proxy", "debug")
	columns := []*columnInfo{
		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isPK: true},
		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
		&columnInfo{name: "balance", typ: metapb.DataType_Double},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	table := makeTestTable(columns)
	start := util.EncodeStorePrefix(util.Store_Prefix_KV, table.GetId())

	var pkCol *metapb.Column
	var pks []*metapb.Column
	for _, col := range table.Columns {
		if col.Name == "id" {
			pkCol = col
			pks = append(pks, col)
			break
		}
	}

	r := util.BytesPrefix(start)
	middle, _ := util.EncodePrimaryKey(start, pkCol, []byte(strconv.Itoa(8)));
	lastRangId := uint64(1)

	rng := &metapb.Range{
		Id:         lastRangId,
		TableId:    1,
		StartKey:   r.Start,
		EndKey:     middle,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	rng.PrimaryKeys = pks

	lastRangId += 1
	rng1 := &metapb.Range{
		Id:         lastRangId,
		TableId:    1,
		StartKey:   middle,
		EndKey:     r.Limit,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	rng1.PrimaryKeys = pks
	p := newTestProxy2(db, table, rng, rng1)

	defer CloseMock(p)
	defer p.Close()

	testProxySelect(t, p, [][]string{}, "select * from "+testTableName)

	//insert
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(1, 'myname', 0.0075)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(2, 'myname2', 1)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(6, 'myname3', 3.1)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(9, 'myname4', 10.1)")

	// select and check
	expected := [][]string{
		[]string{"1", "myname", "0.0075"},
		[]string{"2", "myname2", "1"},
		[]string{"6", "myname3", "3.1"},
		[]string{"9", "myname4", "10.1"},
	}

	testProxySelect(t, p, expected, "select id,name,balance from "+testTableName)

	oldRng := deepcopy.Iface(rng).(*metapb.Range)

	middle2, _ := util.EncodePrimaryKey(start, pkCol, []byte(strconv.Itoa(5)));
	oldRng.EndKey = middle2
	oldRng.RangeEpoch.Version++

	newRng := deepcopy.Iface(rng).(*metapb.Range)
	lastRangId += 1
	newRng.Id = lastRangId
	newRng.StartKey = middle2

	MockDs.RangeSplit(oldRng, newRng)
	MockMs.SetRange(oldRng)
	MockMs.SetRange(newRng)
	log.Info("**********change range route***********8")
	log.Info("*****=======prepare delete======*****")
	//delete one
	testProxyDelete(t, p, 1, "delete from "+testTableName+" where id = 1")
	expected = [][]string{
		[]string{"2", "myname2", "1"},
		[]string{"6", "myname3", "3.1"},
		[]string{"9", "myname4", "10.1"},
	}
	testProxySelect(t, p, expected, "select * from "+testTableName)

}

func TestAllDeleteRoutChange(t *testing.T) {
	log.InitFileLog(logPath, "proxy", "debug")
	columns := []*columnInfo{
		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isPK: true},
		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
		&columnInfo{name: "balance", typ: metapb.DataType_Double},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	table := makeTestTable(columns)
	start := util.EncodeStorePrefix(util.Store_Prefix_KV, table.GetId())

	var pkCol *metapb.Column
	var pks []*metapb.Column
	for _, col := range table.Columns {
		if col.Name == "id" {
			pkCol = col
			pks = append(pks, col)
			break
		}
	}

	r := util.BytesPrefix(start)
	middle, _ := util.EncodePrimaryKey(start, pkCol, []byte(strconv.Itoa(8)));
	lastRangId := uint64(1)

	rng := &metapb.Range{
		Id:         lastRangId,
		TableId:    1,
		StartKey:   r.Start,
		EndKey:     middle,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	rng.PrimaryKeys = pks

	lastRangId += 1
	rng1 := &metapb.Range{
		Id:         lastRangId,
		TableId:    1,
		StartKey:   middle,
		EndKey:     r.Limit,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	rng1.PrimaryKeys = pks
	p := newTestProxy2(db, table, rng, rng1)

	defer CloseMock(p)
	defer p.Close()

	testProxySelect(t, p, [][]string{}, "select * from "+testTableName)

	//insert
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(1, 'myname', 0.0075)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(2, 'myname2', 1)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(6, 'myname3', 3.1)")
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(9, 'myname4', 10.1)")

	// select and check
	expected := [][]string{
		[]string{"1", "myname", "0.0075"},
		[]string{"2", "myname2", "1"},
		[]string{"6", "myname3", "3.1"},
		[]string{"9", "myname4", "10.1"},
	}

	testProxySelect(t, p, expected, "select id,name,balance from "+testTableName)

	oldRng := deepcopy.Iface(rng).(*metapb.Range)

	middle2, _ := util.EncodePrimaryKey(start, pkCol, []byte(strconv.Itoa(5)));
	oldRng.EndKey = middle2
	oldRng.RangeEpoch.Version++

	newRng := deepcopy.Iface(rng).(*metapb.Range)
	lastRangId += 1
	newRng.Id = lastRangId
	newRng.StartKey = middle2

	MockDs.RangeSplit(oldRng, newRng)
	MockMs.SetRange(oldRng)
	MockMs.SetRange(newRng)
	log.Info("**********change range route***********8")
	log.Info("*****=======prepare delete======*****")
	//delete one
	testProxyDelete(t, p, 4, "delete from "+testTableName+" ")
	expected = [][]string{
	}
	testProxySelect(t, p, expected, "select * from "+testTableName)
}

func TestSqlDriver_Insert(t *testing.T) {
	s, err := mockGwServer()
	if err != nil {
		t.Fatalf("init server failed, err[%v]", err)
	}
	go s.Run()

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", s.cfg.User, s.cfg.Password,
		"192.168.108.111", s.cfg.SqlPort, "test"))
	if err != nil {
		t.Fatalf("Fail to initialize mysql")
	}

	res, err := db.Exec("insert into test(id, name, balance) values (1, 'maggie', 10.1)")
	if err != nil {
		t.Fatalf("db exec is failed. err:[%v]", err)
	}
	lastId, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("insert error: %v", err)
	}
	assert.Equal(t, lastId, int64(0), "no auto_increment")

	res, err = db.Exec("insert into test2(name, balance) values ('maggie', 10.33)")
	if err != nil {
		t.Fatalf("db exec is failed. err:[%v]", err)
	}
	lastId, err = res.LastInsertId()
	if err != nil {
		t.Fatalf("insert error: %v", err)
	}
	assert.NotEqual(t, lastId, int64(0), "auto_increment")
	t.Logf("%v", lastId)
}