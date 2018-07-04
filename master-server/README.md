1. cp conf/config.toml ../config.toml (跟cmd/start.sh中的配置对应)
2. 修改 config.toml 内容
    a. master的数据目录datapath 和 日志目录logpath、日志级别loglevel
    b. cluster.members 配置集群成员，
     [[cluster.peer]]
     id = 1
     host = "127.0.165.52"
     http-port = 8887
     rpc-port = 18887
     raft-ports = [8877,8867]
     
     [[cluster.peer]]
      id = 2
      host = "127.0.165.53"
      http-port = 8887
      rpc-port = 18887
      raft-ports = [8877,8867]
      
      [[cluster.peer]]
      id = 3
      host = "127.0.165.54"
      http-port = 8887
      rpc-port = 18887
      raft-ports = [8877,8867]

3. master的role配置
    a. master
        [metric]
        interval： master 发送监控数据到监控服务到间隔
        address：  监控服务的地址
    b. metric
        [metric.server]
         address: 配置为本监控服务负责接收处理metric的IP:端口
         store-url: 为写监控的mysql【可以是shorkstore ms-server的http端口】或者为elasticsearch
         store-type: 为sql 或者 elasticsearch, 存储在sharkstore的类型也为sql
4. run
    one. 打包
        sh build.sh
    two. 启动 [停止]
        sh start.sh [sh stop.sh]

