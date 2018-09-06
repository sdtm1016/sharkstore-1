package server

import (
	"errors"
	"fmt"

	"util/bufalloc"
	"util/log"
	"model/pkg/metapb"
)

type Field_ struct {
	Column string      `json:"column"`
	Value  interface{} `json:"value"`
}

type And struct {
	Field  *Field_ `json:"field"`
	Relate string  `json:"relate"`
	Or     []*Or   `json:"or"`
}

type Or struct {
	Field  *Field_   `json:"field"`
	Relate MatchType `json:"relate"`
	And    []*And    `json:"and"`
}

type Limit_ struct {
	Offset   uint64 `json:"offset"`
	RowCount uint64 `json:"rowcount"`
}

type Order struct {
	By   string `json:"by"`
	Desc bool   `json:"desc"`
}

type Scope struct {
	Start []byte `json:"start"`
	End   []byte `json:"end"`
}

type Filter_ struct {
	And   []*And   `json:"and"`
	Scope *Scope   `json:"scope"`
	Limit *Limit_  `json:"limit"`
	Order []*Order `json:"order"`
}

type AggreFunc struct {
	Function string `json:"func"`
	Field    string `json:"field"`
}

type Command struct {
	Version   string          `json:"version"`
	Type      string          `json:"type"`
	Field     []string        `json:"field"`
	Values    [][]interface{} `json:"values"`
	Filter    *Filter_        `json:"filter"`
	PKs       [][]*And        `json:"pks"`
	AggreFunc []*AggreFunc    `json:"aggrefunc"`
}

type Query struct {
	Sign         string   `json:"sign"`
	DatabaseName string   `json:"databasename"`
	TableName    string   `json:"tablename"`
	Command      *Command `json:"command"`
}

type CreateDatabase struct {
	Sign         string `json:"sign"`
	DatabaseName string `json:"databasename"`
}

type Column struct {
	Name       string `json:"name"`
	DataType   string `json:"datatype"`
	PrimaryKey bool   `json:"primarykey"`
	Unsigned   bool   `json:"unsigned"`
}

type CreateTable struct {
	Sign         string    `json:"sign"`
	DatabaseName string    `json:"databasename"`
	TableName    string    `json:"tablename"`
	Columns      []*Column `json:"columns"`
}

type Reply struct {
	Code         int             `json:"code"`
	RowsAffected uint64          `json:"rowsaffected"`
	Values       [][]interface{} `json:"values"`
	Message      string          `json:"message"`
}

type TableProperty struct {
	Columns []*metapb.Column `json:"columns"`
	Regxs   []*metapb.Column `json:"regxs"`
}

func (q *Query) parseColumnNames() []string {
	return q.Command.Field
}

func (q *Query) parseAggreFuncs() []*AggreFunc {
	return q.Command.AggreFunc
}

func (q *Query) parseRowValues(buffer bufalloc.Buffer) ([]InsertRowValue, error) {
	if len(q.Command.Field) == 0 {
		return nil, errors.New("len(command.field) == 0")
	}

	var err error
	indexes := make([]int, 0, len(q.Command.Values) * len(q.Command.Field))
	for _, vs := range q.Command.Values {
		if len(vs) != len(q.Command.Field) {
			return nil, errors.New(fmt.Sprintf("len(values) != len(field) %v,%v", vs, q.Command.Field))
		}
		for _, v := range vs {
			if v == nil {
				indexes = append(indexes, buffer.Len())
				continue
			}
			_, err = fmt.Fprintf(buffer, "%v", v)
			if err != nil {
				return nil, err
			}
			indexes = append(indexes, buffer.Len())
		}
	}

	var values []InsertRowValue
	data := buffer.Bytes()
	var idx, prevIdx, pos int
	for i := 0; i < len(q.Command.Values); i++ {
		value := make([]SQLValue, 0, len(q.Command.Values[i]))
		for j := 0; j < len(q.Command.Values[i]); j++ {
			idx = indexes[pos]
			log.Debug("prev: %v, idx: %v", prevIdx, idx)
			value = append(value, data[prevIdx:idx])
			prevIdx = idx
			pos++
		}
		values = append(values, value)
	}

	return values, nil
}

// func (q *Query) parseRowValues(t *Table) ([]InsertRowValue, error) {
// 	var values []InsertRowValue

// 	if len(q.Command.Field) == 0 {
// 		return nil, errors.New("len(command.field) == 0")
// 	}
// 	for _, vs := range q.Command.Values {
// 		if len(vs) != len(q.Command.Field) {
// 			return nil, errors.New("len(values) != len(field)")
// 		}
// 		var value []SQLValue
// 		for _, v := range vs {
// 			value = append(value, []byte(fmt.Sprintf("%v", v)))
// 		}
// 		values = append(values, value)
// 	}
// 	return values, nil
// }

func (q *Query) parseMatchs(ands []*And) ([]Match, error) {
	// TODO just AND now
	matchs := make([]Match, 0)

	var (
		column_ string
		sqlValue_  []byte
		matchType_ MatchType
	)

	/*if q.Command.Filter == nil {
		return nil, nil
	}*/

	for _, and := range ands {
		if and.Field == nil {
			continue
		}
		if and.Field.Column == "" {
			continue
		}
		column_ = and.Field.Column
		//col := t.FindColumn(column_)
		switch and.Relate {
		case "=":
			matchType_ = Equal
		case "!=":
			matchType_ = NotEqual
		case "<":
			matchType_ = Less
		case "<=":
			matchType_ = LessOrEqual
		case ">":
			matchType_ = Larger
		case ">=":
			matchType_ = LargerOrEqual
		default:
			return nil, errors.New("invalid type")
		}

		//switch col.GetDataType() {
		//case metapb.DataType_Tinyint:
		//	fallthrough
		//case metapb.DataType_Smallint:
		//	fallthrough
		//case metapb.DataType_Int:
		//	fallthrough
		//case metapb.DataType_BigInt:
		//	//if col.GetUnsigned() {
		//	//	// uint64
		//	//} else {
		//	//	// int64
		//	//}
		//	if _, ok := and.Field.Value.(uint64); !ok {
		//		return nil, fmt.Errorf("value of type tinyint|smallint|int|bigint is not uint64: %v is %T", and.Field.Column, and.Field.Value)
		//	}
		//	sqlValue_ = Uint64ToByte(and.Field.Value.(uint64))
		//case metapb.DataType_Float:
		//	fallthrough
		//case metapb.DataType_Double:
		//	if _, ok := and.Field.Value.(float64); !ok {
		//		return nil, fmt.Errorf("value of type float|double is not float64: %v is %T", and.Field.Column, and.Field.Value)
		//	}
		//	sqlValue_ = Float64ToByte(and.Field.Value.(float64))
		//case metapb.DataType_Varchar:
		//	fallthrough
		//case metapb.DataType_Date:
		//	fallthrough
		//case metapb.DataType_TimeStamp:
		//	if _, ok := and.Field.Value.(string); !ok {
		//		return nil, fmt.Errorf("value of type varchar|date|timestamp is not string: %v is %T", and.Field.Column, and.Field.Value)
		//	}
		//	sqlValue_ = []byte(and.Field.Value.(string))
		//case metapb.DataType_Binary:
		//	sqlValue_ = []byte(and.Field.Value.([]byte)) // TODO
		//default:
		//	return nil, errors.New("invalid type")
		//}

		sqlValue_ = []byte(fmt.Sprintf("%v", and.Field.Value))
		matchs = append(matchs, Match{
			column:    column_,
			sqlValue:  sqlValue_,
			matchType: matchType_,
		})
	}

	return matchs, nil
}

func (q *Query) parseLimit() *Limit {
	if q.Command.Filter == nil || q.Command.Filter.Limit == nil {
		return nil
	}
	l := q.Command.Filter.Limit
	if l.Offset != 0 || l.RowCount != 0 {
		return &Limit{
			offset:   l.Offset,
			rowCount: l.RowCount,
		}
	} else {
		return nil
	}
}

func (q *Query) parseOrder() []*Order {
	if q.Command.Filter == nil || len(q.Command.Filter.Order) == 0 {
		return nil
	}
	o := q.Command.Filter.Order
	if len(o) != 0 {
		return o
	} else {
		return nil
	}

}

func (q *Query) parseScope() *Scope {
	if q.Command.Filter == nil {
		return nil
	}
	return q.Command.Filter.Scope
}

func (q *Query) parseSelectCols(t *Table) []*SelColumn {
	var columns []*SelColumn
	for _, c := range q.parseColumnNames() {
		columns = append(columns, &SelColumn{col: c})
	}
	for _, aggre := range q.parseAggreFuncs() {
		if aggre.Function == "count" && aggre.Field == "*" {
			columns = append(columns, &SelColumn{aggreFunc: aggre.Function})
		} else {
			columns = append(columns, &SelColumn{aggreFunc: aggre.Function, col:aggre.Field})
		}
	}
	return columns
}

//func Float64ToByte(float float64) []byte {
//	bits := math.Float64bits(float)
//	bytes := make([]byte, 8)
//	binary.LittleEndian.PutUint64(bytes, bits)
//	return bytes
//}
//
//func Uint64ToByte(int uint64) []byte {
//	bytes := make([]byte, 8)
//	binary.BigEndian.PutUint64(bytes, int)
//	return bytes
//}
