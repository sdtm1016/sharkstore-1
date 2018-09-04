package models

import "model/pkg/metapb"

type ColumnInfo struct {
	Id            int    `json:"id"`
	ColumnName    string `json:"name"`
	DataType      int    `json:"data_type"`
	Unsigned      bool   `json:"unsigned"`
	PrimaryKey    int    `json:"primary_key"`
	Index         bool   `json:"index"`
	AutoIncrement bool   `json:"auto_increment"`
}

type Epoch struct {
	ConfVer int `json:"conf_ver"`
	Ver     int `json:"version"`
}
type TableInfo struct {
	Id         int          `json:"id"`
	TableName  string       `json:"name"`
	DbId       int          `json:"db_id"`
	DbName     string       `json:"db_name"`
	Columns    []ColumnInfo `json:"columns"`
	Epoch      Epoch        `json:"epoch"`
	CreateTime int64        `json:"create_time"`
	Status     int          `json:"status"`
	IsolationLabel string 	`json:"isolation_label"`
	ReadFromNode metapb.ReadFromNode `json:"read_from_node"`
}

type Peer struct {
	Id   uint64  `json:"id,omitempty"`
	Node *DsNode `json:"node,omitempty"`
}
type Range struct {
	Id uint64 `json:"id,omitempty"`
	// Range key range [start_key, end_key).
	StartKey   []byte             `json:"start_key,omitempty"`
	EndKey     []byte             `json:"end_key,omitempty"`
	RangeEpoch *metapb.RangeEpoch `json:"range_epoch,omitempty"`
	Peers      []*Peer            `json:"peers,omitempty"`
	// Range state
	State      int32  `json:"state,omitempty"`
	DbName     string `json:"db_name,omitempty"`
	TableName  string `json:"table_name,omitempty"`
	TableId    uint64 `json:"table_id,omitempty"`
	CreateTime int64  `json:"create_time,omitempty"`
	LastHbTime string `json:"last_hb_time,omitempty"`
}
type Route struct {
	Range  *Range `json:"range,omitempty"`
	Leader *Peer  `json:"leader,omitempty"`
}

type Column struct {
	// max size 128 bytes
	Name string `json:"name,omitempty"`
	// 列名映射的ID
	Id       uint64 `json:"id,omitempty"`
	DataType int    `json:"data_type,omitempty"`
	// 针对int类型,是否是无符号类型
	Unsigned bool `son:"unsigned,omitempty"`
	// 针对float和varchar类型
	Scale int32 `json:"scale,omitempty"`
	// 针对float类型
	Precision int32 `json:"precision,omitempty"`
	// 是否可以为空
	Nullable bool `json:"nullable,omitempty"`
	// 是否主键
	PrimaryKey uint64 `json:"primary_key,omitempty"`
	// 列的顺序
	Ordinal int32 `json:"ordinal,omitempty"`
	// 索引 Binary不支持索引，其他类型列默认均是索引列
	Index        bool   `json:"index,omitempty"`
	DefaultValue []byte `json:"default_value,omitempty"`
	Properties   string `json:"properties,omitempty"`
}
