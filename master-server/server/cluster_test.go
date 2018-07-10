package server

import (
	"testing"
	"model/pkg/metapb"
	"encoding/json"
	"util/deepcopy"
	"time"
	"fmt"
)

var (
	TABLE_NAME   = "t0"
	DB_NAME      = "d0"
	TABLE_PK_INT = []*metapb.Column{
		&metapb.Column{Name: "id", DataType: metapb.DataType_BigInt, PrimaryKey: 1,},
	}
	TABLE_PK_VARCHAR = []*metapb.Column{
		&metapb.Column{Name: "id", DataType: metapb.DataType_Varchar, PrimaryKey: 1,},
	}
)

func TestCreateTable(t *testing.T) {
	cluster := newLocalCluster(newMockIDAllocator())
	defer closeLocalCluster(cluster)

	if _, err := cluster.CreateDatabase(DB_NAME, ""); err != nil {
		t.Fatalf("create db error: %v", err)
	}

	table, err := cluster.CreateTable(DB_NAME, TABLE_NAME, "", 0, TABLE_PK_INT, nil, false, nil)
	if err != nil {
		t.Fatalf("create table error: %v", err)
	}
	t.Logf("table: %v", *table)

	if table.Status != metapb.TableStatus_TableInit {
		t.Fatal("table status is not TABLE_CREATING")
	}
	if len(cluster.GetTableAllRanges(table.GetId())) != 0 {
		t.Fatal("table ranges number != 0")
	}
	tt, err := cluster.loadTable(table.GetId())
	if tt != nil {
		t.Error("test failed")
		return
	}
}

func TestDeleteTableFast(t *testing.T) {
	cluster := newLocalCluster(newMockIDAllocator())
	defer closeLocalCluster(cluster)

	if _, err := cluster.CreateDatabase(DB_NAME, ""); err != nil {
		t.Fatalf("create db error: %v", err)
	}

	table, err := cluster.CreateTable(DB_NAME, TABLE_NAME, "", 0, TABLE_PK_INT, nil, false, nil)
	if err != nil {
		t.Fatalf("create table error: %v", err)
	}
	t.Logf("table: %v", *table)
	// 模拟table创建成功
	table.Status = metapb.TableStatus_TableRunning
	cluster.storeTable(table.Table)
	cluster.creatingTables.Delete(table.GetId())
	tt, err := cluster.DeleteTable(DB_NAME, TABLE_NAME, true)
	if err != nil || tt == nil {
		t.Error("test failed")
		return
	}
	if tt.Status != metapb.TableStatus_TableDeleting {
		t.Error("test failed")
		return
	}
	if _, find := cluster.FindTableById(table.GetId()); find {
		t.Error("test failed")
		return
	}
}

func TestDeleteTableSlow(t *testing.T) {
	cluster := newLocalCluster(newMockIDAllocator())
	defer closeLocalCluster(cluster)

	if _, err := cluster.CreateDatabase(DB_NAME, ""); err != nil {
		t.Fatalf("create db error: %v", err)
	}

	table, err := cluster.CreateTable(DB_NAME, TABLE_NAME, "", 0, TABLE_PK_INT, nil, false, nil)
	if err != nil {
		t.Fatalf("create table error: %v", err)
	}
	t.Logf("table: %v", *table)
	// 模拟table创建成功
	table.Status = metapb.TableStatus_TableRunning
	cluster.storeTable(table.Table)
	cluster.creatingTables.Delete(table.GetId())
	tt, err := cluster.DeleteTable(DB_NAME, TABLE_NAME, false)
	if err != nil || tt == nil {
		t.Error("test failed")
		return
	}
	if tt.Status != metapb.TableStatus_TableDelete {
		t.Error("test failed")
		return
	}
	if len(tt.Expand) != 8 {
		t.Error("test failed")
		return
	}
	if _, find := cluster.FindTableById(table.GetId()); find {
		t.Error("test failed")
		return
	}
}

func TestCreateTableSqlParse(t *testing.T) {
	sql := `CREATE TABLE ` + "fbase_user_bean_BEHAVIOR" + ` (
	` + "userPin" + ` varchar(50) NOT NULL COMMENT '用户pin',
	` + "periods" + ` varchar(12) NOT NULL COMMENT '周期',
	` + "isPlus" + ` int(11) DEFAULT NULL COMMENT '是否plus',
	` + "test" + ` int(123123123123)                  primary     key,
	PRIMARY KEY (` + "userPin" + `,` + "periods" + `),
	)`
	table := parseCreateTableSql(sql)
	if t == nil {
		t.Fatal("create table sql parse failed")
	}
	t.Log("create table with sql: ", *table)
	if table.GetName() != "fbase_user_bean_behavior" {
		t.Fatal("table name parse error")
	}
	cs := table.GetColumns()
	c := cs[0]
	if c.GetId() != 1 || c.GetName() != "userpin" || c.GetDataType() != metapb.DataType_Varchar || c.GetPrimaryKey() != 1 {
		t.Fatal("column 1 parse error")
	}
	c = cs[1]
	if c.GetId() != 2 || c.GetName() != "periods" || c.GetDataType() != metapb.DataType_Varchar || c.GetPrimaryKey() != 1 {
		t.Fatal("column 2 parse error")
	}
	c = cs[2]
	if c.GetId() != 3 || c.GetName() != "isplus" || c.GetDataType() != metapb.DataType_Int {
		t.Fatal("column 3 parse error")
	}
	c = cs[3]
	if c.GetId() != 4 || c.GetName() != "test" || c.GetDataType() != metapb.DataType_Int || c.GetPrimaryKey() != 1 {
		t.Fatal("column 4 parse error")
	}
}

func TestCreateTableWithLetterRangeKeys(t *testing.T) {
	cluster := newLocalCluster(newMockIDAllocator())
	defer closeLocalCluster(cluster)
	if _, err := cluster.CreateDatabase(DB_NAME, ""); err != nil {
		t.Fatalf("create db error: %v", err)
	}

	var err error
	var sliceKeys [][]byte
	rangeKeys := `a,b,c`
	if sliceKeys, err = rangeKeysSplit(rangeKeys, ","); err != nil {
		t.Fatalf("create table error: %v", err)
	}

	table, err := cluster.CreateTable(DB_NAME, TABLE_NAME, "", 0, TABLE_PK_VARCHAR, nil, false, sliceKeys)
	if err != nil {
		t.Fatalf("create table error: %v", err)
	}
	t.Log("table: ", *table)

	if table.Status != metapb.TableStatus_TableInit {
		t.Fatal("table status is not TABLE_CREATING")
	}
	if len(cluster.GetTableAllRanges(table.GetId())) != 0 {
		t.Fatal("table ranges number != 0")
	}
	tt, err := cluster.loadTable(table.GetId())
	if tt != nil {
		t.Error("test failed")
		return
	}
}

func TestCreateTableWithNumericRangeKeys(t *testing.T) {
	cluster := newLocalCluster(newMockIDAllocator())
	defer closeLocalCluster(cluster)
	if _, err := cluster.CreateDatabase(DB_NAME, ""); err != nil {
		t.Fatalf("create db error: %v", err)
	}

	var err error
	var sliceKeys [][]byte
	rangeKeys := `1,2,3`
	if sliceKeys, err = rangeKeysSplit(rangeKeys, ","); err != nil {
		t.Fatalf("create table error: %v", err)
	}

	table, err := cluster.CreateTable(DB_NAME, TABLE_NAME, "", 0, TABLE_PK_VARCHAR, nil, false, sliceKeys)
	if err != nil {
		t.Fatalf("create table error: %v", err)
	}
	t.Log("table: ", *table)

	if table.Status != metapb.TableStatus_TableInit {
		t.Fatal("table status is not TABLE_CREATING")
	}
	if len(cluster.GetTableAllRanges(table.GetId())) != 0 {
		t.Fatal("table ranges number != 0")
	}
	tt, err := cluster.loadTable(table.GetId())
	if tt != nil {
		t.Error("test failed")
		return
	}
}

func TestCreateTableWithRangeNumber1(t *testing.T) {
	cluster := newLocalCluster(newMockIDAllocator())
	defer closeLocalCluster(cluster)
	if _, err := cluster.CreateDatabase(DB_NAME, ""); err != nil {
		t.Fatalf("create db error: %v", err)
	}

	var err error
	var sliceKeys [][]byte
	var rangeKeysStart = "a"
	var rangeKeysEnd = "b"
	var rangeKeysNum uint64 = 1
	sliceKeys, err = ScopeSplit([]byte(rangeKeysStart), []byte(rangeKeysEnd), rangeKeysNum, nil)
	if err != nil {
		t.Fatalf("create table error: %v", err)
	}

	table, err := cluster.CreateTable(DB_NAME, TABLE_NAME, "", 0, TABLE_PK_VARCHAR, nil, false, sliceKeys)
	if err != nil {
		t.Fatalf("create table error: %v", err)
	}
	t.Log("table: ", *table)

	if table.Status != metapb.TableStatus_TableInit {
		t.Fatal("table status is not TABLE_CREATING")
	}
	if len(cluster.GetTableAllRanges(table.GetId())) != 0 {
		t.Fatal("table ranges number != 0")
	}
	tt, err := cluster.loadTable(table.GetId())
	if tt != nil {
		t.Error("test failed")
		return
	}
}

func TestCreateTableWithRangeNumber2(t *testing.T) {
	cluster := newLocalCluster(newMockIDAllocator())
	defer closeLocalCluster(cluster)
	if _, err := cluster.CreateDatabase(DB_NAME, ""); err != nil {
		t.Fatalf("create db error: %v", err)
	}

	var err error
	var sliceKeys [][]byte
	var rangeKeysStart = "a"
	var rangeKeysEnd = "b"
	var rangeKeysNum uint64 = 2
	sliceKeys, err = ScopeSplit([]byte(rangeKeysStart), []byte(rangeKeysEnd), rangeKeysNum, nil)
	if err != nil {
		t.Fatalf("create table error: %v", err)
	}

	table, err := cluster.CreateTable(DB_NAME, TABLE_NAME, "", 0, TABLE_PK_VARCHAR, nil, false, sliceKeys)
	if err != nil {
		t.Fatalf("create table error: %v", err)
	}
	t.Log("table: ", *table)

	if table.Status != metapb.TableStatus_TableInit {
		t.Fatal("table status is not TABLE_CREATING")
	}
	if len(cluster.GetTableAllRanges(table.GetId())) != 0 {
		t.Fatal("table ranges number != 0")
	}
	tt, err := cluster.loadTable(table.GetId())
	if tt != nil {
		t.Error("test failed")
		return
	}
}

func TestCreateTableWithRangeNumber3(t *testing.T) {
	cluster := newLocalCluster(newMockIDAllocator())
	defer closeLocalCluster(cluster)
	if _, err := cluster.CreateDatabase(DB_NAME, ""); err != nil {
		t.Fatalf("create db error: %v", err)
	}

	var err error
	var sliceKeys [][]byte
	var rangeKeysStart = "a"
	var rangeKeysEnd = "b"
	var rangeKeysNum uint64 = 3
	sliceKeys, err = ScopeSplit([]byte(rangeKeysStart), []byte(rangeKeysEnd), rangeKeysNum, nil)
	if err != nil {
		t.Fatalf("create table error: %v", err)
	}

	table, err := cluster.CreateTable(DB_NAME, TABLE_NAME, "", 0, TABLE_PK_VARCHAR, nil, false, sliceKeys)
	if err != nil {
		t.Fatalf("create table error: %v", err)
	}
	t.Log("table: ", *table)

	if table.Status != metapb.TableStatus_TableInit {
		t.Fatal("table status is not TABLE_CREATING")
	}
	if len(cluster.GetTableAllRanges(table.GetId())) != 0 {
		t.Fatal("table ranges number != 0")
	}
	tt, err := cluster.loadTable(table.GetId())
	if tt != nil {
		t.Error("test failed")
		return
	}
}

func TestTableColumnEdit(t *testing.T) {
	cluster := newLocalCluster(newMockIDAllocator())
	defer closeLocalCluster(cluster)
	if _, err := cluster.CreateDatabase(DB_NAME, ""); err != nil {
		t.Fatalf("create db error: %v", err)
	}
	table0 := []*metapb.Column{
		&metapb.Column{Name: "RANGEID", DataType: metapb.DataType_BigInt, PrimaryKey: 1,},
		&metapb.Column{Name: "size1", DataType: metapb.DataType_BigInt,},
	}
	property, err := json.Marshal(TableProperty{Columns: table0})
	if err != nil {
		t.Fatalf("marshal old property error: %v", err)
	}
	columns, _, err := ParseProperties(string(property))
	if err != nil {
		t.Fatal("parse properties error: ", err)
	}
	table, err := cluster.CreateTable(DB_NAME, TABLE_NAME, "", 0, columns, nil, false, nil)
	if err != nil {
		t.Fatalf("create table failed, err[%v]", err)
	}
	table1 := deepcopy.Iface(table.GetColumns()).([]*metapb.Column)
	for _, col := range table1 {
		switch col.Name {
		case "rangeid":
			col.Name = "ID"
		case "size1":
			col.Name = "size2"
		}
	}
	table1 = append(table1, &metapb.Column{
		Name:     "addCol",
		DataType: metapb.DataType_BigInt,
	})

	t.Logf("new table scheme: %v", table1)
	property, err = json.Marshal(TableProperty{Columns: table1})
	if err != nil {
		t.Fatalf("marshal new property error: %s\n", err)
	}
	err = cluster.EditTable(table, string(property))
	if err != nil {
		t.Fatalf("test failed, err[%v]", err)
	}
	t.Logf("new edit table scheme: %v", table.GetColumns())
	_, find := table.GetColumnByName("size2")
	if !find {
		t.Fatal("test failed")
	}
	_, find = table.GetColumnByName("id")
	if !find {
		t.Fatal("test failed")
	}
	if col, find := table.GetColumnByName("addcol"); !find {
		t.Fatal("test failed")
	} else {
		t.Log("new column: ", *col)
		if col.GetDataType() != metapb.DataType_BigInt || col.GetId() == 0 {
			t.Fatal("test failed")
		}
	}

	t.Log("test success!!! ", table.GetColumns())
}

func TestAllocPeerAndSelectNode(t *testing.T) {
	cluster := MockCluster(t)
	defer closeLocalCluster(cluster)
	if _, err := cluster.CreateDatabase(DB_NAME, ""); err != nil {
		t.Fatalf("create db error: %v", err)
	}

	cluster.workerManger = NewWorkerManager(cluster, nil)
	cluster.AddCreateTableWorker()

	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var err error
			var sliceKeys [][]byte
			rangeKeys := `1,2,3,4,5,6,7,8,9,10,11,12,13,15,18,20,24,28,32,38`
			if sliceKeys, err = rangeKeysSplit(rangeKeys, ","); err != nil {
				t.Fatalf("create table error: %v", err)
			}

			table, err := cluster.CreateTable(DB_NAME, fmt.Sprintf("%s%d", TABLE_NAME, i), "", 0, TABLE_PK_VARCHAR, nil, false, sliceKeys)
			if err != nil {
				t.Fatalf("create table error: %v", err)
			}
			t.Log("table: ", *table)

			time.Sleep(20 * time.Second)

			rngStat := cluster.GetNodeRangeStatByTable(table.GetId())
			t.Logf("init table %v: distribution: %v", table.GetName(), rngStat)

			ranges := cluster.GetTableAllRanges(table.GetId())
			t.Logf("table %v total range size : %v", table.GetName(), len(ranges))

			for _, rng := range ranges {
				go func(rng *Range) {
					newPeer, err := cluster.allocPeerAndSelectNode(rng, false)
					if err != nil {
						t.Errorf("alloc rangeId:%d, %s", rng.GetId(), err.Error())
						return
					}
					peers := rng.GetPeers()
					peers = append(peers, newPeer)
					rng.Peers = peers
					cluster.AddRange(rng)
				}(rng)
			}
			time.Sleep(30 * time.Second)

			rngStat = cluster.GetNodeRangeStatByTable(table.GetId())
			t.Logf("worker table %v: distribution: %v", table.GetName(), rngStat)

			for _, rng := range ranges {
				go func(rng *Range) {
					newPeer, err := cluster.allocPeerAndSelectNode(rng, false)
					if err != nil {
						t.Errorf("alloc rangeId:%d, %s", rng.GetId(), err.Error())
						return
					}
					peers := rng.GetPeers()
					peers = append(peers, newPeer)
					rng.Peers = peers
					cluster.AddRange(rng)
				}(rng)
			}

			time.Sleep(30 * time.Second)
			rngStat = cluster.GetNodeRangeStatByTable(table.GetId())
			t.Logf("final worker table %v: distribution: %v", table.GetName(), rngStat)
		}(i)
	}

	wg.Wait()
}

func TestConfig_LoadFromFile(t *testing.T) {
	cluster := MockCluster(t)
	defer closeLocalCluster(cluster)

	get(cluster, t)
	cluster.UpdateAutoScheduleInfo(false, true, false)
	get(cluster, t)
}

func get(cluster *Cluster, t *testing.T)  {

	if err := cluster.loadAutoSplit(); err != nil {
		t.Errorf("error1 %v", err)
	}

	t.Logf("split %v", cluster.autoSplitUnable)


	if err := cluster.loadAutoFailover(); err != nil {
		t.Errorf("error2 %v", err)
	}

	t.Logf("failover %v", cluster.autoFailoverUnable)

	if err := cluster.loadAutoTransfer(); err != nil {
		t.Errorf("error3 %v", err)
	}

	t.Logf("transfer %v", cluster.autoTransferUnable)
}