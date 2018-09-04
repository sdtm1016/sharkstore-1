/**
 * 集群数据结构
 */
package models

type ClusterInfo struct {
	Id             int    `json:"id"`
	Name           string `json:"name"`
	MasterUrl      string `json:"master_url"`
	GatewayHttpUrl string `json:"gateway_http"`
	GatewaySqlUrl  string `json:"gateway_sql"`
	ClusterToken   string `json:"cluster_sign"`
	CreateTime     int64  `json:"create_time"`

	//true 表示禁止自动分片
	AutoSplitUnable 	bool        `json:"auto_split"`
	//true 表示禁止自动迁移
	AutoTransferUnable 	bool        `json:"auto_transfer"`
	// true 表示禁止自动failover
	AutoFailoverUnable 	bool		`json:"auto_failover"`
}

func NewClusterInfo() *ClusterInfo {
	return &ClusterInfo{
	}
}

func BuildClusterInfo(clusterId int, clusterName string, masterUrl string, gatewayHttpUrl string,
	gatewaySqlUrl string, createSign string, createTime int64) *ClusterInfo {
	return &ClusterInfo{
		Id:             clusterId,
		Name:           clusterName,
		MasterUrl:      masterUrl,
		GatewayHttpUrl: gatewayHttpUrl,
		GatewaySqlUrl:  gatewaySqlUrl,
		CreateTime:     createTime,
	}
}

type Member struct {
	LeaderId uint64    `json:"leader_id,omitempty"`
	Node     []*MsNode `json:"node,omitempty"`
}

type MsNode struct {
	Id                uint64 `json:"id"`
	WebManageAddr     string `json:"web_addr"`
	RpcServerAddr     string `json:"rpc_addr"`
	RaftHeartbeatAddr string `json:"raft_hb_addr"`
	RaftReplicateAddr string `json:"raft_rp_addr"`
}

type DsNode struct {
	Id uint64 				`json:"id,omitempty"`
	// rpc 服务地址
	ServerAddr string 		`json:"server_addr,omitempty"`
	// raft　服务地址
	RaftAddr string 		`json:"raft_addr,omitempty"`
	// http 管理地址
	AdminAddr string 		`json:"admin_addr,omitempty"`
	State    int32 			`json:"state,omitempty"`
	Version  string     	`json:"version,omitempty"`
	IsolationLabel string 	`json:"isolation_label,omitempty"`
}