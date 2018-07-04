1. cp conf/config.toml cmd/gw.conf
2. 修改 gw.conf 内容
    a. gateway的日志目录：log.dir、日志级别log.level
    b. master.address 配置master集群成员列表，端口为master服务配置的http_port端口
        ["127.0.165.52:8887","127.0.165.53:8887","127.0.165.54:8887"]
    c. metric.address 为 接收metric上报的服务IP:端口
3. run
    one. 打包
        sh build.sh
    two. 启动
        sh start.sh


