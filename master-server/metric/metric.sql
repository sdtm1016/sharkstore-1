CREATE DATABASE IF NOT EXISTS fbase;
CREATE DATABASE IF NOT EXISTS fbase DEFAULT CHARACTER SET = utf8 DEFAULT COLLATE = utf8_unicode_ci;

USE fbase;

CREATE TABLE IF NOT EXISTS `fbase_cluster` (
    `id` bigint(20) NOT NULL ,
    `cluster_name` varchar(64) NOT NULL,
    `cluster_url` varchar(256) NOT NULL,
    `gateway_http` varchar(256) NOT NULL COMMENT '网关httpUrl',
    `gateway_sql` varchar(256) NOT NULL COMMENT '网关sqlUrl',
    `cluster_sign` varchar(64) NOT NULL,
    `auto_transfer` tinyint NOT NULL,
    `auto_failover` tinyint NOT NULL,
    `auto_split` tinyint NOT NULL,
    `create_time` bigint(20) NOT NULL,
	PRIMARY KEY (`id`)
)

CREATE TABLE IF NOT EXISTS `fbase_role` (
    `role_id` bigint(20) NOT NULL,
    `role_name`varchar(64) NOT NULL,
	PRIMARY KEY (`role_id`)
)

CREATE TABLE IF NOT EXISTS `fbase_user` (
    `id` bigint(20) NOT NULL,
    `erp`varchar(256) NOT NULL,
    `mail`varchar(128) NOT NULL,
    `tel`varchar(64) NOT NULL,
    `user_name`varchar(256) NOT NULL,
    `real_name`varchar(256) NOT NULL,
    `superior_name`varchar(256) NOT NULL,
    `department1`varchar(256) NOT NULL,
    `department2`varchar(256) NOT NULL,
    `organization_name`varchar(256) NOT NULL,
    `create_time` timestamp NOT NULL,
    `update_time` timestamp NOT NULL,
	PRIMARY KEY (`id`)
)

CREATE TABLE IF NOT EXISTS `fbase_privilege` (
    `user_name` varchar(256) NOT NULL,
    `cluster_id` bigint(20) NOT NULL,
    `privilege` bigint(20) NOT NULL,
	PRIMARY KEY (`user_name`, `cluster_id`)
)

CREATE TABLE IF NOT EXISTS `cluster_meta` (
  `cluster_id` bigint(20) NOT NULL,
  `update_time` bigint(20) NOT NULL,
  `total_capacity` bigint(20) NOT NULL COMMENT '总容量',
  `used_capacity` bigint(20) NOT NULL COMMENT '已用容量',
  `range_count` bigint(20) NOT NULL COMMENT '分片数',
  `db_count` bigint(20) NOT NULL COMMENT '数据库个数',
  `table_count` bigint(20) NOT NULL COMMENT '表个数',
  `ds_count` bigint(20) NOT NULL COMMENT '服务节点数',
  `gs_count` bigint(20) NOT NULL COMMENT '网关节点数',
  `fault_list`varchar(2048) NOT NULL COMMENT '故障服务节点',
  PRIMARY KEY (`cluster_id`, `update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `cluster_net` (
  `cluster_id` bigint(20) NOT NULL,
  `update_time` bigint(20) NOT NULL,
  `tps` bigint(20) NOT NULL,
  `min_tp` float NOT NULL,
  `max_tp` float NOT NULL,
  `avg_tp` float NOT NULL,
  `tp50` float NOT NULL,
  `tp90` float NOT NULL,
  `tp99` float NOT NULL,
  `tp999` float NOT NULL,
  `total_number` bigint(20) NOT NULL,
  `err_number` bigint(20) NOT NULL,
  `net_in_per_sec` bigint(20) NOT NULL,
  `net_out_per_sec` bigint(20) NOT NULL,
  `clients_count` bigint(20) NOT NULL,
  `open_clients_per_sec` bigint(20) NOT NULL,
  PRIMARY KEY (`cluster_id`, `update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `cluster_slowlog` (
  `cluster_id` bigint(20) NOT NULL,
  `update_time` bigint(20) NOT NULL,
  `su` bigint(20) NOT NULL,
  `type`  varchar(32) NOT NULL,
  `addr`  varchar(32) NOT NULL,
  `lats` bigint(20) NOT NULL,
  `slowlog` varchar(2048) NOT NULL,
  PRIMARY KEY (`cluster_id`, `update_time`, `su`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `mac_meta` (
  `cluster_id` bigint(20) NOT NULL,
  `type`  varchar(32) NOT NULL,
  `ip`  varchar(32) NOT NULL,
  `update_time` bigint(20) NOT NULL,
  `cpu_rate` float NOT NULL,
  `load1` float NOT NULL,
  `load5` float NOT NULL,
  `load15` float NOT NULL,
  `process_num` bigint(20) NOT NULL,
  `thread_num` bigint(20) NOT NULL,
  `handle_num` bigint(20) NOT NULL,
  PRIMARY KEY (`cluster_id`, `type`, `ip`, `update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `mac_net` (
  `cluster_id` bigint(20) NOT NULL,
  `type`  varchar(32) NOT NULL,
  `ip`  varchar(32) NOT NULL,
  `update_time` bigint(20) NOT NULL,
  `net_io_in_byte_per_sec` bigint(20) NOT NULL,
  `net_io_out_byte_per_sec` bigint(20) NOT NULL,
  `net_io_in_package_per_sec` bigint(20) NOT NULL,
  `net_io_out_package_per_sec` bigint(20) NOT NULL,
  `net_tcp_connections` bigint(20) NOT NULL,
  `net_tcp_active_opens_per_sec` bigint(20) NOT NULL,
  `net_ip_recv_package_per_sec` bigint(20) NOT NULL,
  `net_ip_send_package_per_sec` bigint(20) NOT NULL,
  `net_ip_drop_package_per_sec` bigint(20) NOT NULL,
  `net_tcp_recv_package_per_sec` bigint(20) NOT NULL,
  `net_tcp_send_package_per_sec` bigint(20) NOT NULL,
  `net_tcp_err_package_per_sec` bigint(20) NOT NULL,
  `net_tcp_retransfer_package_per_sec` bigint(20) NOT NULL,
  PRIMARY KEY (`cluster_id`, `type`, `ip`, `update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `mac_mem` (
  `cluster_id` bigint(20) NOT NULL,
  `type`  varchar(32) NOT NULL,
  `ip`  varchar(32) NOT NULL,
  `update_time` bigint(20) NOT NULL,
  `memory_total` bigint(20) NOT NULL,
  `memory_used_rss` bigint(20) NOT NULL,
  `memory_used` bigint(20) NOT NULL,
  `memory_free` bigint(20) NOT NULL,
  `memory_used_percent` float NOT NULL,
  `swap_memory_total` bigint(20) NOT NULL,
  `swap_memory_used` bigint(20) NOT NULL,
  `swap_memory_free` bigint(20) NOT NULL,
  `swap_memory_used_percent` float NOT NULL,
  PRIMARY KEY (`cluster_id`, `type`, `ip`, `update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `mac_disk` (
  `cluster_id` bigint(20) NOT NULL,
  `type`  varchar(32) NOT NULL,
  `ip`  varchar(32) NOT NULL,
  `update_time` bigint(20) NOT NULL,
  `disk_path` varchar(128) NOT NULL,
  `disk_total` bigint(20) NOT NULL,
  `disk_used` bigint(20) NOT NULL,
  `disk_free` bigint(20) NOT NULL,
  `disk_proc_rate` float NOT NULL,
  `disk_read_byte_per_sec` bigint(20) NOT NULL,
  `disk_write_byte_per_sec` bigint(20) NOT NULL,
  `disk_read_count_per_sec` bigint(20) NOT NULL,
  `disk_write_count_per_sec` bigint(20) NOT NULL,
  PRIMARY KEY (`cluster_id`, `type`, `ip`, `disk_path`, `update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `process_meta` (
  `cluster_id` bigint(20) NOT NULL,
  `type`  varchar(32) NOT NULL,
  `addr`  varchar(32) NOT NULL,
  `update_time` bigint(20) NOT NULL,
  `cpu_rate` float NOT NULL,
  `thread_num` bigint(20) NOT NULL,
  `handle_num` bigint(20) NOT NULL,
  `memory_used` bigint(20) NOT NULL,
  `memory_total` bigint(20) NOT NULL,
  `start_time` bigint(20) NOT NULL,
  PRIMARY KEY (`cluster_id`, `type`, `addr`, `update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `process_disk` (
  `cluster_id` bigint(20) NOT NULL,
  `type`  varchar(32) NOT NULL,
  `addr`  varchar(32) NOT NULL,
  `update_time` bigint(20) NOT NULL,
  `disk_path` varchar(128) NOT NULL,
  `disk_total` bigint(20) NOT NULL,
  `disk_used` bigint(20) NOT NULL,
  `disk_free` bigint(20) NOT NULL,
  `disk_proc_rate` float NOT NULL,
  `disk_read_byte_per_sec` bigint(20) NOT NULL,
  `disk_write_byte_per_sec` bigint(20) NOT NULL,
  `disk_read_count_per_sec` bigint(20) NOT NULL,
  `disk_write_count_per_sec` bigint(20) NOT NULL,
  PRIMARY KEY (`cluster_id`, `type`, `addr`, `update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `process_net` (
  `cluster_id` bigint(20) NOT NULL,
  `type`  varchar(32) NOT NULL,
  `addr`  varchar(32) NOT NULL,
  `update_time` bigint(20) NOT NULL,
  `tps` bigint(20) NOT NULL,
  `min_tp` float NOT NULL,
  `max_tp` float NOT NULL,
  `avg_tp` float NOT NULL,
  `tp50` float NOT NULL,
  `tp90` float NOT NULL,
  `tp99` float NOT NULL,
  `tp999` float NOT NULL,
  `total_number` bigint(20) NOT NULL,
  `err_number` bigint(20) NOT NULL,
  `connect_count` bigint(20) NOT NULL,
  PRIMARY KEY (`cluster_id`, `type`, `addr`, `update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `process_ds` (
  `cluster_id` bigint(20) NOT NULL,
  `type`  varchar(32) NOT NULL,
  `addr`  varchar(32) NOT NULL,
  `update_time` bigint(20) NOT NULL,
  `range_count` bigint(20) NOT NULL,
  `range_split_count` bigint(20) NOT NULL,
  `sending_snap_count` bigint(20) NOT NULL,
  `receiving_snap_count` bigint(20) NOT NULL,
  `applying_snap_count` bigint(20) NOT NULL,
  `range_leader_count` bigint(20) NOT NULL,
  `version` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`cluster_id`, `type`, `addr`, `update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `db_meta` (
  `cluster_id` bigint(20) NOT NULL,
  `db_name`  varchar(32) NOT NULL,
  `update_time` bigint(20) NOT NULL,
  `table_num` bigint(20) NOT NULL,
  `range_size` bigint(20) NOT NULL COMMENT '库的range大小',
  PRIMARY KEY (`cluster_id`, `db_name`, `update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `table_meta` (
  `cluster_id` bigint(20) NOT NULL,
  `db_name`  varchar(32) NOT NULL,
  `table_name`  varchar(32) NOT NULL,
  `update_time` bigint(20) NOT NULL,
  `range_count` bigint(20) NOT NULL,
  `range_size` bigint(20) NOT NULL COMMENT '表的range大小',
  PRIMARY KEY (`cluster_id`, `db_name`, `table_name`, `update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `fbase_sql_apply` (
    `id`          varchar(64) NOT NULL,
    `db_name`     varchar(64) NOT NULL,
    `table_name`  varchar(64)) NOT NULL,
    `sentence`    varchar(512)  NOT NULL,
    `status`      tinyint(1)  NOT NULL COMMENT '1：待审核， 2：通过，3：驳回，',
    `applyer`     varchar(64) NOT NULL,
    `auditor`     varchar(64),
    `create_time` bigint(20) NOT NULL,
    `remark`      varchar(128),
	  PRIMARY KEY (`id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `fbase_lock_nsp` (
    `id`          varchar(64) NOT NULL,
    `db_name`     varchar(64) NOT NULL,
    `table_name`  varchar(64) NOT NULL,
    `cluster_id`  bigint(20) NOT NULL,
    `db_id`       bigint(20),
    `table_id`    bigint(20),
    `status`      tinyint(1)  Not null COMMENT '1：待审核， 2：通过，3：驳回，',
    `applyer`     varchar(64) NOT NULL,
    `auditor`     varchar(64),
    `create_time` bigint(20) NOT NULL,
	  PRIMARY KEY (`id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `fbase_configure_nsp` (
    `id`          varchar(64) NOT NULL,
    `db_name`     varchar(64) NOT NULL,
    `table_name`  varchar(64) NOT NULL,
    `cluster_id`  bigint(20) NOT NULL,
    `db_id`       bigint(20),
    `table_id`    bigint(20),
    `status`      tinyint(1)  Not null COMMENT '1：待审核， 2：通过，3：驳回，',
    `applyer`     varchar(64) NOT NULL,
    `auditor`     varchar(64),
    `create_time` bigint(20) NOT NULL,
	  PRIMARY KEY (`id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `range_stats` (
    `cluster_id`  bigint(20) NOT NULL,
    `range_id`  bigint(20) NOT NULL,
    `addr`  varchar(32) NOT NULL,
    `bytes_written`     bigint(20) NOT NULL,
    `bytes_read`     bigint(20) NOT NULL,
    `keys_written`     bigint(20) NOT NULL,
    `keys_read`     bigint(20) NOT NULL,
    `approximate_size`     bigint(20) NOT NULL,
    `update_time` bigint(20) NOT NULL,
	  PRIMARY KEY (`cluster_id`, `range_id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `metric_server` (
    `addr` varchar(32) NOT NULL,
	  PRIMARY KEY (`addr`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

#需要初始化fbase_role, fbase_privilege
insert into fbase_role values (3, "普通用户")
insert into fbase_role values (2, "集群管理员")
insert into fbase_role values (1, "系统管理员")
