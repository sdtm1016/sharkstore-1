package alarm2

const (
	TABLENAME_APP 				= "sharkstore_app"
	TABLENAME_GLOBAL_RULE 		= "sharkstore_global_rule"
	TABLENAME_CLUSTER_RULE 		= "sharkstore_cluster_rule"
	TABLENAME_RECEIVER 			= "sharkstore_receiver"

	TABLESCHEMA_APP 				= "cluster_id, ip_addr, process_name" // all pk
	TABLESCHEMA_GLOBAL_RULE 		= "name, threshold, alarm_durable, trigger_count, alarm_interval, receiver_role, enable" // name pk
	TABLESCHEMA_CLUSTER_RULE 		= "cluster_id, rule_name, threshold, alarm_durable, trigger_count, alarm_interval, receiver_role, enable" // cluster_id, rule_name pk
	TABLESCHEMA_RECEIVER 			= "erp, cluster_id, role, mail, tel" // erp, cluster_id pk
)

const (
	APPNAME_GATEWAY 		= "gateway"
	APPNAME_MASTER 			= "master"
	APPNAME_METRIC 			= "metric"
	APPNAME_JOIN_LETTER 	= "_"
)

const (
	ALARMROLE_SYSTEM_ADMIN 		= "system_admin"
	ALARMROLE_CLUSTER_ADMIN 	= "cluster_admin"
	ALARMROLE_CLUSTER_USER 		= "cluster_user"
)

const (
	ALARMRULE_APP_NOTALIVE 		= "app_not_alive"
	ALARMRULE_GATEWAY_SLOWLOG 	= "gateway_slowlog"
	ALARMRULE_GATEWAY_ERRORLOG 	= "gateway_errorlog"

	ALARMRULE_RANGE_NO_HEARTBEAT 		= "range_no_heartbeat"
	ALARMRULE_RANGE_WRITE_BPS 			= "range_write_bps"
	ALARMRULE_RANGE_WRITE_OPS 			= "range_write_ops"
	ALARMRULE_RANGE_READ_BPS 			= "range_read_bps"
	ALARMRULE_RANGE_READ_OPS 			= "range_read_ops"

	ALARMRULE_NODE_CAPACITY_USED_RATE 	= "node_capacity_used_rate"
	ALARMRULE_NODE_WRITE_BPS 			= "node_write_bps"
	ALARMRULE_NODE_WRITE_OPS 			= "node_write_ops"
	ALARMRULE_NODE_READ_BPS 			= "node_read_bps"
	ALARMRULE_NODE_READ_OPS 			= "node_read_ops"
	RANGE_PEER_INDEX					= "range_peer_index"

)
