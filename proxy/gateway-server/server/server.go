// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package server

import (
	"fmt"
	"net"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"proxy/gateway-server/errors"
	"proxy/gateway-server/mysql"
	"util/log"
	"util/server"
	"util"
	"proxy/metric"
	"model/pkg/lockpb"

	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc"
)

type BlacklistSqls struct {
	sqls    map[string]string
	sqlsLen int
}

const (
	Offline = iota
	Online
	Unknown
)

type Server struct {
	cfg      *Config
	addr     string
	user     string
	password string
	//db       string

	lockRpcAddr string

	statusIndex        int32
	status             [2]int32
	logSqlIndex        int32
	logSql             [2]string
	slowLogTimeIndex   int32
	slowLogTime        [2]int
	blacklistSqlsIndex int32
	blacklistSqls      [2]*BlacklistSqls
	allowipsIndex      int32
	allowips           [2][]net.IP

	proxy   *Proxy
	httpSvr *server.Server

	listener net.Listener
	running  bool
}

func (s *Server) Status() string {
	var status string
	switch s.status[s.statusIndex] {
	case Online:
		status = "online"
	case Offline:
		status = "offline"
	case Unknown:
		status = "unknown"
	default:
		status = "unknown"
	}
	return status
}

func NewServer(cfg *Config) (*Server, error) {
	s := new(Server)

	s.cfg = cfg
	//s.counter = new(Counter)
	s.addr = fmt.Sprintf(":%d", cfg.SqlPort)
	s.lockRpcAddr = fmt.Sprintf(":%d", cfg.LockRpcPort)
	s.user = cfg.User
	s.password = cfg.Password
	atomic.StoreInt32(&s.statusIndex, 0)
	s.status[s.statusIndex] = Online
	//	atomic.StoreInt32(&s.logSqlIndex, 0)
	//	s.logSql[s.logSqlIndex] = cfg.LogSql
	//	atomic.StoreInt32(&s.slowLogTimeIndex, 0)
	//	s.slowLogTime[s.slowLogTimeIndex] = cfg.SlowLogTime

	if len(cfg.Charset) == 0 {
		cfg.Charset = mysql.DEFAULT_CHARSET //utf8
	}
	cid, ok := mysql.CharsetIds[cfg.Charset]
	if !ok {
		return nil, errors.ErrInvalidCharset
	}
	//change the default charset
	mysql.DEFAULT_CHARSET = cfg.Charset
	mysql.DEFAULT_COLLATION_ID = cid
	mysql.DEFAULT_COLLATION_NAME = mysql.Collations[cid]

	initMetricSender(cfg)

	var l net.Listener
	var err error
	netProto := "tcp"
	l, err = net.Listen(netProto, s.addr)
	if err != nil {
		return nil, err
	}

	l = LimitListener(l, cfg.MaxClients)
	s.listener = l
	proxy := NewProxy(cfg.Cluster.ServerAddr, cfg)
	if proxy == nil {
		log.Fatal("proxy fault")
		return nil, nil
	}
	s.proxy = proxy
	// start http server for manage
	svr := server.NewServer()
	config := &server.ServerConfig{
		Port:      fmt.Sprintf("%d", cfg.HttpPort),
		Version:   "v1",
		ConnLimit: cfg.MaxClients,
	}
	svr.Init("gateway", config)
	svr.Handle("/debug/log/setlevel", func(w http.ResponseWriter, r *http.Request) {
		log.SetLevel(r.FormValue("level"))
		w.Write([]byte("OK"))
	})
	svr.Handle("/kvcommand", s.handleKVCommand)
	svr.Handle("/tableinfo", s.handleTableInfo)
	svr.Handle("/createdatabase", s.handleCreateDatabase)
	svr.Handle("/createtable", s.handleCreateTable)
	svr.Handle("/lock/debug", s.handleLockDebug)
	svr.Handle("/metric/config/set", s.handleMetricConfigSet)
	svr.Handle("/metric/config/get", s.handleMetricConfigGet)
	go svr.Run()
	s.httpSvr = svr

	log.Info("NewServer: Server running. netProto: %v, address: %v", netProto, s.addr)

	return s, nil
}

func initMetricSender(cfg *Config)  {
	ips := util.GetLocalIps()
	addr := fmt.Sprintf("%s:%d", ips[0], cfg.SqlPort)
	//performance monitor about mysql port [addr] transport to metric server[cfg.MetricAddr]
	metric.GsMetric= metric.NewMetric(cfg.Cluster.ID, addr, cfg.Metric.Address, cfg.Performance.SlowLogMaxLen, cfg.Alarm.Address)
}

func (s *Server) GetCfg() *Config{
	return s.cfg
}

func (s *Server) flushCounter() {
	for {
		//s.counter.FlushCounter()
		time.Sleep(1 * time.Second)
	}
}

func (s *Server) newClientConn(co net.Conn) *ClientConn {
	c := new(ClientConn)
	switch conn := co.(type) {
	case *net.TCPConn:
		conn.SetNoDelay(false)
	case *limitListenerConn:
		conn.Conn.(*net.TCPConn).SetNoDelay(false)
	}
	//tcpConn := co.(*net.TCPConn)
	//
	////SetNoDelay controls whether the operating system should delay packet transmission
	//// in hopes of sending fewer packets (Nagle's algorithm).
	//// The default is true (no delay),
	//// meaning that data is sent as soon as possible after a Write.
	////I set this option false.
	//tcpConn.SetNoDelay(false)
	//c.c = tcpConn
	//
	////c.schema = s.GetSchema()
	//
	//c.pkg = mysql.NewPacketIO(tcpConn)
	c.c = co
	c.pkg = mysql.NewPacketIO(co)
	c.server = s

	c.pkg.Sequence = 0

	c.connectionId = atomic.AddUint32(&baseConnId, 1)

	c.status = mysql.SERVER_STATUS_AUTOCOMMIT

	c.salt, _ = mysql.RandomBuf(20)

	//c.txConns = make(map[*backend.Node]*backend.BackendConn)

	c.closed = false

	c.charset = mysql.DEFAULT_CHARSET
	c.collation = mysql.DEFAULT_COLLATION_ID

	c.stmtId = 0
	c.stmts = make(map[uint32]*Stmt)

	return c
}

func (s *Server) onConn(c net.Conn) {
	//s.counter.IncrClientConns()
	conn := s.newClientConn(c) //新建一个conn

	defer func() {
		err := recover()
		if err != nil {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)] //获得当前goroutine的stacktrace
			log.Error("server onConn error remoteAddr:%s stack:%s", c.RemoteAddr().String(), string(buf),
			)
		}

		conn.Close()
		//s.counter.DecrClientConns()
	}()

	if allowConnect := conn.IsAllowConnect(); allowConnect == false {
		err := mysql.NewError(mysql.ER_ACCESS_DENIED_ERROR, "ip address access denied by kingshard.")
		conn.writeError(err)
		conn.Close()
		return
	}
	if err := conn.Handshake(); err != nil {
		log.Info("server onConn %v ",err.Error())
		conn.writeError(err)
		conn.Close()
		return
	}

	conn.Run()
}

func (s *Server) ChangeProxy(v string) error {
	var status int32
	switch v {
	case "online":
		status = Online
	case "offline":
		status = Offline
	default:
		status = Unknown
	}
	if status == Unknown {
		return errors.ErrCmdUnsupport
	}

	if s.statusIndex == 0 {
		s.status[1] = status
		atomic.StoreInt32(&s.statusIndex, 1)
	} else {
		s.status[0] = status
		atomic.StoreInt32(&s.statusIndex, 0)
	}

	return nil
}

func (s *Server) Run() error {
	s.running = true

	lis, err := net.Listen("tcp", s.lockRpcAddr)
	if err != nil {
		log.Fatal("failed to listen: %v", err)
	}

	gServer := grpc.NewServer()
	lockrpcpb.RegisterDLockServiceServer(gServer, s)
	reflection.Register(gServer)
	go func() {
		if err = gServer.Serve(lis); err != nil {
			log.Fatal("failed to lock-server: %v", err)
		}
	}()

	// flush counter
	//go s.flushCounter()

	for s.running {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Error("server", "Run", err.Error(), 0)
			continue
		}

		go s.onConn(conn)
	}

	return nil
}

func (s *Server) Close() {
	s.running = false
	if s.listener != nil {
		s.listener.Close()
	}
}

