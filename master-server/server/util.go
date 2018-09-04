package server

import (
	"strings"
	"fmt"
	"time"
	"runtime"

	"util/log"
	"model/pkg/metapb"
	"encoding/binary"
	"regexp"
	"bytes"
	"math"
	"errors"
)

const (
	KB   uint64 = 1024
	MB          = 1024 * KB
	GB          = 1024 * MB
	PB          = 1024 * GB
)

/*
  USE 数据库名 CREATE TABLE 表名 (列名 类型(大小) DEFAULT'默认值' CONSTRAINT 约束名 约束定义,
                                列名 类型(大小) DEFAULT'默认值' CONSTRAINT 约束名 约束定义,
                                列名 类型(大小) DEFAULT'默认值' CONSTRAINT 约束名 约束定义,
                                ... ...);
    注：(1) 绿色部份是可以省略的。
       (2) 一个列是可以有多个约束的。
    约束定义：
    （1）NULL | NOT NULL  用于定义列的空值约束。(定义列)  (下面的蓝色部份是单选其中之一)
            语法：CONSTRAINT 约束名 NULL | NOT NULL
            注意：
                a. NOT NULL 约束强制列不接受 NULL 值。
                b. NOT NULL 约束强制字段始终包含值。这意味着，如果不向字段添加值，就无法插入新纪录或者更新记录。

    （2）UNIQUE  约束唯一标识数据库表中的每条记录。(即可以定义列也可能定义表)
            语法：CONSTRAINT 约束名 UNIQUE (列名, 列名, ... ...);
            说明：用于指定基本表在某一个列或多个列的组合上取值必须唯一。定义了UNIQUE约束的那些列称为唯一键。如果为基本表的革一列或多个列的组合指定了UNIQUE约束，
                 则系统将为这些列建立唯一索引，从而保证在表中的任意两行记录在指定的列或列组合上不能取同样的值。
            注意：
                    a. UNIQUE 约束唯一标识数据库表中的每条记录。
                    b. UNIQUE 和 PRIMARY KEY 约束均为列或列集合提供了唯一性的保证。
                    c. PRIMARY KEY 拥有自动定义的 UNIQUE 约束。
                    d.请注意，每个表可以有多个 UNIQUE 约束，但是每个表只能有一个 PRIMARY KEY 约束。

    （3）PRIMARY KEY 约束唯一标识数据库表中的每条记录。(即可以定义列也可能定义表)
            语法：CONSTRAINT 约束名 PRIMARY KEY (列名, 列名, ... ...);
            说明：用于定义基本表的主键。与UNIQUE约束类似，PRIMARY KEY 约束也是通过建立唯一索引来保证基本表在主键列（某一个列或多个列的组合）上取值的唯一性。
                 然而它们之间也存在着很大差别：在一个基本表中只能定义一个 PRIMARY KEY 约束，却能定义多个UNIQUE约束。
                 如果为基本表的某一个列或多个列的组合指定了 PRIMARY KEY 约束，
                 那么其中在任何一个列都不能出现空值；而 UNIQUE 约束允许出现空值。
            注意：
                    a. 主键必须包含唯一的值。
                    b. 主键列不能包含 NULL 值。
                    c. 每个表应该都一个主键，并且每个表只能有一个主键。

     （4）FOREIGN KEY 外键 (即可以定义列也可能定义表)
            语法：CONSTRAINT 约束名 FOREIGN KEY (列名, 列名, ... ...) REFERENCES (列名, 列名, ... ...) ;
            说明：指定某一个列或多个列的组合作为外部键，并在外部键和它所引用的主键或唯一键之间建立联系。在这种联系中，包含外部键的基本表称为从表，包含外部键引用的主键或唯一键的表称为主表。
                 一旦为一列或列的组合定义了 FOREIGN KEY 约束，系统将保证从表在外部键上的取值要么是主表中某一个主键值或唯一键值，要么取空值。
            注意：
                    a.在REFERENCES 中引用的列必须和 FOREIGN KEY 的外部键列一一对应，即列数目相等并且相应列的数据类型相同。

     （5）CHECK 约束用于限制列中的值的范围。 (即可以定义列也可能定义表)
            语法：CONSTRAINT 约束名 CHECK (约束条件);
            说明：用于指定基本表中的每一条记录必须满足的条件，可以对基本表在各个列上的值做进一步的约束，如成绩列的取值既不能大于100，
                 也不能小于0。
            注意：
                    a. 如果对单个列定义 CHECK 约束，那么该列只允许特定的值。
                    b. 如果对一个表定义 CHECK 约束，那么此约束会在特定的列中对值进行限制。
*/

func SqlParse(_sql string) (t *metapb.Table, err error) {
	defer func() {
		if r := recover(); r != nil {
			fn := func() string {
				n := 10000
				var trace []byte
				for i := 0; i < 5; i++ {
					trace = make([]byte, n)
					nbytes := runtime.Stack(trace, false)
					if nbytes < len(trace) {
						return string(trace[:nbytes])
					}
					n *= 2
				}
				return string(trace)
			}
			err = fmt.Errorf("panic:%v", r)
			log.Error("panic:%v", r)
			log.Error("Stack: %s", fn())
			return
		}
	}()
	sql := strings.ToLower(_sql)
	if !strings.HasPrefix(sql, "create table ") {
		log.Error("SqlParse: invalid create table")
		return nil, ErrSQLSyntaxError
	}
	// 解析出表名
	fIndex := strings.Index(sql, "(")
	if fIndex <= 0 {
		log.Warn("sql %s invalid", sql)
		return nil, ErrSQLSyntaxError
	}
	if fIndex <= 13 {
		log.Warn("sql %s invalid", sql)
		return nil, ErrSQLSyntaxError
	}
	tableName := string([]byte(sql)[13:fIndex])
	tableName = strings.Replace(tableName, " ", "", -1)
	if len(tableName) == 0 {
		log.Warn("sql %s invalid", sql)
		return nil, ErrSQLSyntaxError
	}
	log.Debug("table name %s", tableName)
	lIndex := strings.LastIndex(sql, ")")
	if lIndex <= 0 {
		log.Warn("sql %s invalid", sql)
		return nil, ErrSQLSyntaxError
	}
	if fIndex + 1 > lIndex {
		log.Warn("sql %s invalid", sql)
		return nil, ErrSQLSyntaxError
	}
	sql = string([]byte(sql)[fIndex + 1:lIndex])
	sql = strings.Replace(sql, "\t", "", -1)
	sql = strings.Replace(sql, "\n", "", -1)
	sql = strings.Replace(sql, "\n\t", "", -1)
	colStrs := strings.Split(sql, ",")
	cols, err := ColumnParse(colStrs)
	if err != nil {
		return nil, err
	}
	t = &metapb.Table{
		Name:       tableName,
		Columns:    cols,
		Epoch:      &metapb.TableEpoch{ConfVer: uint64(1), Version: uint64(1)},
		CreateTime: time.Now().Unix(),
	}
	return t, nil
}

func ColumnParse(cols []string) ([]*metapb.Column, error) {
	var pks, unique []string
	var fIndex, lIndex int
	var index uint64 = 0
	var columns []*metapb.Column
	for _, col := range cols {
		if strings.HasPrefix(col, "primary key") {
			fIndex := strings.Index(col, "(")
			if fIndex <= 0 {
				log.Warn("sql %s invalid", col)
				return nil, ErrSQLSyntaxError
			}
			lIndex := strings.LastIndex(col, ")")
			if lIndex <= 0 {
				log.Warn("sql %s invalid", col)
				return nil, ErrSQLSyntaxError
			}
			col := string([]byte(col)[fIndex + 1: lIndex])
			col = strings.Replace(col, " ", "", -1)
			pks = strings.Split(col, ",")
			log.Debug("PKS %v", pks)
		} else if strings.HasPrefix(col, "unique") {
			fIndex = strings.Index(col, "(")
			if fIndex <= 0 {
				log.Warn("sql %s invalid", col)
				return nil, ErrSQLSyntaxError
			}
			lIndex = strings.LastIndex(col, ")")
			if lIndex <= 0 {
				log.Warn("sql %s invalid", col)
				return nil, ErrSQLSyntaxError
			}
			col := string([]byte(col)[fIndex + 1: lIndex])
			col = strings.Replace(col, " ", "", -1)
			unique = strings.Split(col, ",")
			log.Debug("UNIQUE %v", unique)
		} else if strings.Contains(col, "constraint") {
			log.Warn("sql %s invalid", col)
			return nil, fmt.Errorf("statement constraint not support now")
		} else if strings.HasPrefix(col, "check") {
			log.Warn("sql %s invalid", col)
			return nil, fmt.Errorf("statement check not support now")
		} else if strings.HasPrefix(col, "foreing key") {
			log.Warn("sql %s invalid", col)
			return nil, fmt.Errorf("statement foreing key not support now")
		} else {
			log.Debug("col %s", col)
			ctc := strings.SplitN(col, " ", 3)
			if len(ctc) < 2 {
				log.Warn("sql %s invalid", col)
				return nil, ErrSQLSyntaxError
			}
			index++
			c := &metapb.Column{Name: ctc[0], Id: index}
			err := parseColType(ctc[1], c)
			if err != nil {
				return nil, err
			}
			if len(ctc) == 3 {
				if strings.HasPrefix(ctc[2], "default") {
					dd := strings.Split(ctc[2], " ")
					_default := strings.Replace(dd[1], " ", "", -1)
					_default = strings.Replace(_default, "'", "", -1)
					log.Debug("col %s default %s", ctc[0], _default)
					c.DefaultValue = []byte(_default)
				} else if strings.HasPrefix(ctc[2], "not null") {
					c.Nullable = false
				} else if strings.HasPrefix(ctc[2], "null") {
					c.Nullable = true
				} else {
					log.Warn("sql %s invalid", col)
					return nil, ErrSQLSyntaxError
				}
			}
			columns = append(columns, c)
		}
	}
	if len(pks) == 0 {
		return nil, ErrPkMustNotNull
	}
	for _, pk := range pks {
		find := false
		for _, c := range columns {
			if c.GetName() == pk {
				if len(c.DefaultValue) != 0 {
					return nil, ErrPkMustNotSetDefaultValue
				}
				c.PrimaryKey = 1
				find = true
				break
			}
		}
		if !find {
			return nil, ErrSQLSyntaxError
		}
	}
	return columns, nil
}

/*
integer(size)
int(size)
smallint(size)
tinyint(size)
仅容纳整数。在括号内规定数字的最大位数。

float
double
decimal(size,d)
numeric(size,d)
容纳带有小数的数字。
"size" 规定数字的最大位数。"d" 规定小数点右侧的最大位数。

char(size)
容纳固定长度的字符串（可容纳字母、数字以及特殊字符）。
在括号中规定字符串的长度。

varchar(size)
容纳可变长度的字符串（可容纳字母、数字以及特殊的字符）。
在括号中规定字符串的最大长度。

date(yyyymmdd)	容纳日期。
datetime
timestamp
*/
func parseColType(_type string, col *metapb.Column) error {
	if strings.HasPrefix(_type, "integer") {
		col.DataType = metapb.DataType_Int
	} else if strings.HasPrefix(_type, "int") {
		col.DataType = metapb.DataType_Int
	} else if strings.HasPrefix(_type, "smallint") {
		col.DataType = metapb.DataType_Smallint
	} else if strings.HasPrefix(_type, "tinyint") {
		col.DataType = metapb.DataType_Tinyint
	} else if strings.HasPrefix(_type, "bigint") {
		col.DataType = metapb.DataType_BigInt
	} else if strings.HasPrefix(_type, "float") {
		col.DataType = metapb.DataType_Float
	} else if strings.HasPrefix(_type, "double") {
		col.DataType = metapb.DataType_Double
	} else if strings.HasPrefix(_type, "char") {
		col.DataType = metapb.DataType_Varchar
	} else if strings.HasPrefix(_type, "varchar") {
		col.DataType = metapb.DataType_Varchar
	} else if strings.HasPrefix(_type, "date") {
		col.DataType = metapb.DataType_Date
	} else if strings.HasPrefix(_type, "timestamp") {
		col.DataType = metapb.DataType_TimeStamp
	} else {
        return fmt.Errorf("%s not support now", _type)
	}
	return nil
}


func parsePreprocessing(_sql string) (string, []string) {
	sql := strings.ToLower(_sql)
	sql = strings.TrimSpace(sql)
	var pks []string

	sql = strings.Replace(sql, "`", "", -1)
	sql = strings.Replace(sql, "\r", "", -1)
	sql = strings.Replace(sql, "\n", "", -1)
	sql = strings.Replace(sql, "\t", "", -1)

	// drop multiple space 1
	if reg, err := regexp.Compile("[ ]+"); err == nil {
		sql = reg.ReplaceAllString(sql, " ")
	}
	// drop comment
	if reg, err := regexp.Compile("comment '[^']+'"); err == nil {
		sql = reg.ReplaceAllString(sql, "")
	}
	// find pks
	if reg, err := regexp.Compile("primary key[ ]?\\(.*\\)"); err == nil {
		for _, pkeys := range reg.FindAllString(sql, -1) {
			pks_ := strings.SplitN(pkeys, "(", 2)
			if len(pks_) != 2 {
				log.Error("cannot parse prefix primay keys: no ( after 'primary key '")
				return "", nil
			}
			_pks_ := strings.SplitN(pks_[1], ")", 2)
			if len(_pks_) != 2 {
				log.Error("cannot parse prefix primay keys: no ) at end of primary key '")
				return "", nil
			}

			_pks := strings.Split(_pks_[0], ",")
			if len(_pks) == 0 {
				log.Error("not found primary key")
				return "", nil
			}
			for _, pk := range _pks {
				for _, p := range pks {
					if p == strings.TrimSpace(pk) {
						log.Error("primary key [%v] is repeated", strings.TrimSpace(pk))
						return "", nil
					}
				}
				pks = append(pks, strings.TrimSpace(pk))
			}
		}
		sql = reg.ReplaceAllString(sql, "")
	}
	// drop multiple space 2
	if reg, err := regexp.Compile("[ ]+"); err == nil {
		sql = reg.ReplaceAllString(sql, " ")
	}

	return sql, pks
}

//todo opt
func parseCreateTableSql(sql string) *metapb.Table {
	var primaryKeys []string
	sql, primaryKeys = parsePreprocessing(sql)

	str := strings.SplitN(sql, "(", 2)
	if len(str) != 2 {
		log.Error("cannot parse this sql: no (")
		return nil
	}
	str0 := strings.Split(str[0], " ")
	if len(str0) < 3 {
		log.Error("cannot parse tablename: len(create table <t>) < 3")
		return nil
	}

	var tableName string
	if str0[0] == "create" && str0[1] == "table" {
		tableName = strings.TrimSpace(str0[2])
	} else {
		log.Error("tablename syntax error: create table mismatch")
		return nil
	}

	var columns []*metapb.Column
	str1 := strings.Split(str[1], ",")
	for _, col_ := range str1 {
		col := strings.TrimSpace(col_)
		if len(col) == 0 {
			continue
		}
		if strings.HasSuffix(col, " primary key") {
			c := strings.Split(col, " ")
			if len(c) < 4 {
				// name type primary key
				log.Error("cannot parse primay key [%v]", c[0])
				return nil
			}
			cName := strings.TrimSpace(c[0])
			for _, p := range primaryKeys {
				if p == cName {
					log.Error("primary key [%v] is repeated", cName)
					return nil
				}
			}
			cType_ := strings.TrimSpace(c[1])
			cType := strings.Split(cType_, "(")
			cTy := dataType(cType[0])
			if cTy == metapb.DataType_Invalid {
				log.Error("invalid datetype: %v", cType_)
				return nil
			}

			columns = append(columns, &metapb.Column{
				Name: cName,
				DataType: cTy,
				PrimaryKey: 1,
			})
		} else {
			c := strings.Split(col, " ")
			cType_ := strings.TrimSpace(c[1])
			cType := strings.Split(cType_, "(")
			cTy := dataType(cType[0])
			if cTy == metapb.DataType_Invalid {
				log.Error("unsupport syntax: %v", c)
				return nil
			} else {
				cName := strings.TrimSpace(c[0])
				columns = append(columns, &metapb.Column{
					Name: cName,
					DataType: cTy,
				})
			}
		}
	}
	var id uint64
	var findPks bool
	for _, c := range columns {
		id++
		c.Id = id
		for _, p := range primaryKeys {
			findPks = true
			if p == c.GetName() {
				c.PrimaryKey = 1
			}
		}
	}
	if !findPks {
		log.Error("create table sql has no primary key definition: %v", sql)
		return nil
	}

	return &metapb.Table{
		Name:       tableName,
		Columns:    columns,
		Epoch:      &metapb.TableEpoch{ConfVer: uint64(1), Version: uint64(1)},
		CreateTime: time.Now().Unix(),
	}
}

func dataType(_type string) metapb.DataType {
	if strings.HasPrefix(_type, "integer") {
		return metapb.DataType_Int
	} else if strings.HasPrefix(_type, "int") {
		return metapb.DataType_Int
	} else if strings.HasPrefix(_type, "smallint") {
		return metapb.DataType_Smallint
	} else if strings.HasPrefix(_type, "tinyint") {
		return metapb.DataType_Tinyint
	} else if strings.HasPrefix(_type, "bigint") {
		return metapb.DataType_BigInt
	} else if strings.HasPrefix(_type, "float") {
		return metapb.DataType_Float
	} else if strings.HasPrefix(_type, "double") {
		return metapb.DataType_Double
	} else if strings.HasPrefix(_type, "char") {
		return metapb.DataType_Varchar
	} else if strings.HasPrefix(_type, "varchar") {
		return metapb.DataType_Varchar
	} else if strings.HasPrefix(_type, "date") {
		return metapb.DataType_Date
	} else if strings.HasPrefix(_type, "timestamp") {
		return metapb.DataType_TimeStamp
	} else {
		return metapb.DataType_Invalid
	}
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

var SQLReservedWord = []string{
	"abs", "absolute", "action", "add", "all", "allocate", "alter", "analyse", "analyze", "and", "any", "are", "array",
	"array_agg", "array_max_cardinality", "as", "asc", "asensitive", "assertion", "asymmetric", "at", "atomic", "attributes",
	"authorization", "avg", "begin", "begin_frame", "begin_partition", "between", "bigint", "binary", "bit", "bit_length",
	"blob", "boolean", "both", "by", "call", "called", "cardinality", "cascade", "cascaded", "case", "cast", "catalog", "ceil",
	"ceiling", "char", "character", "character_length", "char_length", "check", "clob", "close", "coalesce", "collate",
	"collation", "collect", "column", "commit", "condition", "connect", "connection", "constraint", "constraints", "contains",
	"continue", "convert", "corr", "corresponding", "count", "covar_pop", "covar_samp", "create", "cross", "cube", "cume_dist",
	"current", "current_catalog", "current_date", "current_default_transform_group", "current_path", "current_role",
	"current_row", "current_schema", "current_time", "current_timestamp", "current_transform_group_for_type",
	"current_user", "cursor", "cycle", "datalink", "date", "day", "deallocate", "dec", "decimal", "declare", "default",
	"deferrable", "deferred", "delete", "dense_rank", "deref", "desc", "describe", "descriptor", "deterministic",
	"diagnostics", "disconnect", "distinct", "dlnewcopy", "dlpreviouscopy", "dlurlcomplete", "dlurlcompleteonly",
	"dlurlcompletewrite", "dlurlpath", "dlurlpathonly", "dlurlpathwrite", "dlurlscheme", "dlurlserver", "dlvalue", "do",
	"domain", "double", "drop", "dynamic", "each", "element", "else", "end", "end-exec", "end_frame", "end_partition", "equals",
	"escape", "every", "except", "exception", "exec", "execute", "exists", "external", "extract", "false", "fetch", "filter",
	"first", "first_value", "float", "floor", "for", "foreign", "found", "frame_row", "free", "from", "full", "function",
	"fusion", "get", "global", "go", "goto", "grant", "group", "grouping", "groups", "having", "hold", "hour", "identity",
	"immediate", "import", "in", "indicator", "initially", "inner", "inout", "input", "insensitive", "insert", "int", "integer",
	"intersect", "intersection", "interval", "into", "is", "isolation", "join", "key", "lag", "language", "large", "last",
	"last_value", "lateral", "lead", "leading", "left", "level", "like", "like_regex", "limit", "ln", "local", "localtime",
	"localtimestamp", "lower", "match", "max", "max_cardinality", "member", "merge", "method", "min", "minute", "mod",
	"modifies", "module", "month", "multiset", "names", "national", "natural", "nchar", "nclob", "new", "next", "no", "none",
	"normalize", "not", "nth_value", "ntile", "null", "nullif", "numeric", "occurrences_regex", "octet_length", "of", "offset",
	"old", "on", "only", "open", "option", "or", "order", "out", "outer", "output", "over", "overlaps", "overlay", "pad",
	"parameter", "partial", "partition", "percent", "percentile_cont", "percentile_disc", "percent_rank", "period", "placing",
	"portion", "position", "position_regex", "power", "precedes", "precision", "prepare", "preserve", "primary", "prior",
	"privileges", "procedure", "public", "range", "rank", "read", "reads", "real", "recursive", "ref", "references",
	"referencing", "regr_avgx", "regr_avgy", "regr_count", "regr_intercept", "regr_r2", "regr_slope", "regr_sxx", "regr_sxy",
	"regr_syy", "relative", "release", "restrict", "result", "return", "returned_cardinality", "returning", "returns",
	"revoke", "right", "rollback", "rollup", "row", "rows", "row_number", "savepoint", "schema", "scope", "scroll", "search",
	"second", "section", "select", "sensitive", "session", "session_user", "set", "similar", "size", "smallint", "some", "space",
	"specific", "specifictype", "sql", "sqlcode", "sqlerror", "sqlexception", "sqlstate", "sqlwarning", "sqrt", "start",
	"static", "stddev_pop", "stddev_samp", "submultiset", "substring", "substring_regex", "succeeds", "sum", "symmetric",
	"system", "system_time", "system_user", "table", "tablesample", "temporary", "then", "time", "timestamp", "timezone_hour",
	"timezone_minute", "to", "trailing", "transaction", "translate", "translate_regex", "translation", "treat", "trigger",
	"trim", "trim_array", "true", "truncate", "uescape", "union", "unique", "unknown", "unnest", "update", "upper", "usage",
	"user", "using", "value", "values", "value_of", "varbinary", "varchar", "variadic", "varying", "var_pop", "var_samp",
	"versioning", "view", "when", "whenever", "where", "width_bucket", "window", "with", "within", "without", "work", "write",
	"xml", "xmlagg", "xmlattributes", "xmlbinary", "xmlcast", "xmlcomment", "xmlconcat", "xmldocument", "xmlelement",
	"xmlexists", "xmlforest", "xmliterate", "xmlnamespaces", "xmlparse", "xmlpi", "xmlquery", "xmlserialize", "xmltable",
	"xmltext", "xmlvalidate", "year", "zone",
}

func isSqlReservedWord(col string) bool {
	for _, w := range SQLReservedWord {
		if col == w {
			return true
		}
	}
	return false
}

func bytesToUint64(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("invalid data, must 8 bytes, but %d", len(b))
	}

	return binary.BigEndian.Uint64(b), nil
}

func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func middleByte(min, max byte) []byte {
	var minInt uint8
	var maxInt uint8
	var midInt uint8
	var ret []byte

	minInt = uint8(min)
	maxInt = uint8(max)
	midInt = minInt+(maxInt-minInt)/2
	//fmt.Printf("min: %v, max: %v,  mid: %v\n", minInt, maxInt, midInt)

	if midInt == minInt {
		ret = append(ret, byte(midInt))
		ret = append(ret, middleByte(0, math.MaxUint8)...)
		return ret
	}
	ret = append(ret, byte(midInt))
	return ret
}

func middleSlice(min, max []byte) ([]byte, error) {
	var ret []byte
	minLen := len(min)
	maxLen := len(max)

	var loopLen int
	switch {
	case minLen > maxLen:
		loopLen = maxLen
	case minLen < maxLen:
		loopLen = minLen
	default:
		loopLen = minLen
		if bytes.Compare(min, max) == 0 {
			return nil, fmt.Errorf("middleSlice error: min %v == max %v", min, max)
		}
	}

	var i int
	for i = 0; i < loopLen; i++{
		if min[i] != max[i] {
			ret = append(ret, middleByte(min[i], max[i])...)
			for {
				if bytes.Compare(min, ret) >= 0 {
					ret = append(ret, middleByte(0, math.MaxUint8)...)
				} else {
					break
				}
			}
			return ret, nil
		} else {
			ret = append(ret, min[i])
		}
	}

	ok := false
	for ; i < maxLen; i++ {
		switch max[i] {
		case 0:
			ret = append(ret, max[i])
		case 1:
			ret = append(ret, 0)
			ok = true
			break
		default:
			ret = append(ret, max[i]/2)
			ok = true
			break
		}
	}
	if !ok {
		return nil, fmt.Errorf("middleSlice error: no one slice between min %v and max %v", min, max)
	}
	return ret, nil
}

// define char set 1. letter 2. ascii
func scopeFix(slice [][]byte, minIdx, maxIdx uint64) error {
	midIdx := (minIdx+maxIdx)/2
	if midIdx == minIdx {
		return nil
	}
	//fmt.Printf("minidx: %v, min: %v, maxidx: %v, max: %v\n", minIdx, slice[minIdx], maxIdx, slice[maxIdx])
	var err error
	slice[midIdx], err = middleSlice(slice[minIdx], slice[maxIdx])
	//fmt.Printf("mididx: %v, mid: %v\n", midIdx, slice[midIdx])
	if slice[midIdx] == nil {
		//fmt.Printf("!!!!!! minIdx: %v, maxIdx: %v, min: %v, max: %v\n", minIdx, maxIdx, slice[minIdx], slice[maxIdx])
		return err
	}
	if err := scopeFix(slice, minIdx, midIdx); err != nil {
		return err
	}
	if err := scopeFix(slice, midIdx, maxIdx); err != nil {
		return err
	}
	return nil
}

func ScopeSplit(a, b []byte, n uint64, charSet []byte) ([][]byte, error) {
	defer func() {
		if x := recover(); x != nil {
			b := make([]byte, 1024)
			n := runtime.Stack(b, false)
			log.Error(string(b[:n]))
		}
	}()
	var min []byte
	var max []byte
	if a == nil || b == nil {
		return nil, errors.New("ScopeSplit input slice both nil")
	}
	switch bytes.Compare(a, b) {
	case 1:
		min = b
		max = a
	case -1:
		min = a
		max = b
	default:
		min = a
		max = b
	}

	switch n {
	case 0:
		return nil, errors.New("ScopeSplit input number is 0")
	case 1:
		ret := make([][]byte, 0)
		ret = append(ret, min)
		return ret, nil
	case 2:
		ret := make([][]byte, 0)
		ret = append(ret, min, max)
		return ret, nil
	default:
		ret := make([][]byte, n)
		ret[0] = min
		ret[n-1] = max

		if err := scopeFix(ret, 0, n-1); err != nil {
			return nil, err
		}
		return ret, nil
	}
}


func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func minFloat64(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func maxFloat64(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}