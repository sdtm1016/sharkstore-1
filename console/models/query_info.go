package models

type Field_ struct {
	Column string      `json:"column"`
	Value  interface{} `json:"value"`
}

type And struct {
	Field  *Field_ `json:"field"`
	Relate string  `json:"relate"`
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

type Command struct {
	Type   string   `json:"type"`
	Field  []string `json:"field"`
	Filter *Filter_ `json:"filter"`
}

type Query struct {
	Sign         string   `json:"sign"`
	DatabaseName string   `json:"databasename"`
	TableName    string   `json:"tablename"`
	Command      *Command `json:"command"`
}

type Reply struct {
	Code         int             `json:"code"`
	RowsAffected uint64          `json:"rowsaffected"`
	Values       [][]interface{} `json:"values"`
	Message      string          `json:"message"`
}
