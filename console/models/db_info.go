/**
 * 库数据结构
 */
package models

type DbInfo struct {
	Id 			int           `json:"id"`
	Name 		string        `json:"name"`
	Properties	string        `json:"properties"`
	CreateTime  int64        `json:"create_time"`
}