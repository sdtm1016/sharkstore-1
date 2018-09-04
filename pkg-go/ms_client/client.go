package client

import (
	"errors"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"model/pkg/metapb"
	"model/pkg/mspb"
	"util/log"
)

const (
	RequestMSTimeout = time.Second
	ConnectMSTimeout = time.Second * 3
)

// Client is a MS (master server) client.
// It should not be used after calling Close().
type Client interface {
	// GetRoute gets a range and its leader Peer from MS by key.
	// The range may expire after split. Caller is responsible for caching and
	// taking care of range change.
	// Also it may return nil if MS finds no range for the key temporarily,
	// client should retry later.
	GetRoute(dbId, tableId uint64, key []byte) ([]*metapb.Route, error)
	GetNode(nodeId uint64) (*metapb.Node, error)
	GetDB(dbName string) (*metapb.DataBase, error)
	GetTable(dbName, tableName string) (*metapb.Table, error)
	GetTableById(dbId, tableId uint64) (*metapb.Table, error)
	GetColumns(dbId, tableId uint64) ([]*metapb.Column, error)
	GetColumnByName(dbId, tableId uint64, columnName string) (*metapb.Column, error)
	GetColumnByID(dbId, tableId uint64, columnId uint64) (*metapb.Column, error)
	// columns输入参数只需要填写name和data type即可
	// 返回master server处理后的columns列表(本次新增部分)
	AddColumns(dbId, tableId uint64, columns []*metapb.Column) ([]*metapb.Column, error)
	TruncateTable(dbId, tableId uint64) error
	CreateDatabase(dbName string) error
	CreateTable(dbName, tableName, properties string) error
	GetAutoIncId(dbId, tableId uint64, size uint32) ([]uint64, error)

	NodeHeartbeat(*mspb.NodeHeartbeatRequest) (*mspb.NodeHeartbeatResponse, error)
	RangeHeartbeat(*mspb.RangeHeartbeatRequest) (*mspb.RangeHeartbeatResponse, error)
	NodeLogin(*mspb.NodeLoginRequest) (*mspb.NodeLoginResponse, error)
	GetNodeId(*mspb.GetNodeIdRequest) (*mspb.GetNodeIdResponse, error)
	// Close closes the client.
	Close()
}

// errInvalidResponse represents response message is invalid.
var (
	errGrpcError             = errors.New("grpc error")
	errNoLeader              = errors.New("no leader")
	errInvalidRequest        = errors.New("invalid request")
	errInvalidResponse       = errors.New("invalid response")
	errInvalidResponseHeader = errors.New("response header not set")
)

type RPCClient struct {
	msAddrs []string
	lock    sync.RWMutex
	leader  string
	pool    map[string]*Conn
}

func NewClient(msAddrs []string) (Client, error) {
	pool := make(map[string]*Conn)
	for _, addr := range msAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithTimeout(ConnectMSTimeout))
		if err != nil {
			log.Panic("did not connect addr[%s], err[%v]", addr, err)
			return nil, err
		}
		cli := mspb.NewMsServerClient(conn)
		pool[addr] = &Conn{addr: addr, conn: conn, Cli: cli}
	}
	return &RPCClient{msAddrs: msAddrs, pool: pool}, nil
}

func (c *RPCClient) GetRoute(dbId, tableId uint64, key []byte) ([]*metapb.Route, error) {
	req := &mspb.GetRouteRequest{
		Header:  &mspb.RequestHeader{},
		DbId:    dbId,
		TableId: tableId,
		Key:     key,
	}
	resp, err := c.callRPC(req, RequestMSTimeout)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errInvalidResponse
	}
	if _resp, ok := resp.(*mspb.GetRouteResponse); ok {
		return _resp.GetRoutes(), nil
	}
	return nil, errInvalidResponse
}

func (c *RPCClient) GetNode(nodeId uint64) (*metapb.Node, error) {
	req := &mspb.GetNodeRequest{
		Header: &mspb.RequestHeader{},
		Id:     nodeId,
	}
	resp, err := c.callRPC(req, RequestMSTimeout)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errInvalidResponse
	}
	if _resp, ok := resp.(*mspb.GetNodeResponse); ok {
		return _resp.GetNode(), nil
	}
	return nil, errInvalidResponse
}

func (c *RPCClient) GetDB(dbName string) (*metapb.DataBase, error) {
	req := &mspb.GetDBRequest{
		Header: &mspb.RequestHeader{},
		Name:   dbName,
	}
	resp, err := c.callRPC(req, RequestMSTimeout)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errInvalidResponse
	}
	if _resp, ok := resp.(*mspb.GetDBResponse); ok {
		return _resp.GetDb(), nil
	}
	return nil, errInvalidResponse
}

func (c *RPCClient) GetTable(dbName, tableName string) (*metapb.Table, error) {
	req := &mspb.GetTableRequest{
		Header:    &mspb.RequestHeader{},
		DbName:    dbName,
		TableName: tableName,
	}
	resp, err := c.callRPC(req, RequestMSTimeout)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errInvalidResponse
	}
	if _resp, ok := resp.(*mspb.GetTableResponse); ok {
		return _resp.GetTable(), nil
	}
	return nil, errInvalidResponse
}

func (c *RPCClient) GetTableById(dbId uint64, tableId uint64) (*metapb.Table, error) {
	req := &mspb.GetTableByIdRequest{
		Header:  &mspb.RequestHeader{},
		DbId:    dbId,
		TableId: tableId,
	}
	resp, err := c.callRPC(req, RequestMSTimeout)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errInvalidResponse
	}
	if _resp, ok := resp.(*mspb.GetTableByIdResponse); ok {
		return _resp.GetTable(), nil
	}
	return nil, errInvalidResponse
}

func (c *RPCClient) GetColumns(dbId, tableId uint64) ([]*metapb.Column, error) {
	req := &mspb.GetColumnsRequest{
		Header:  &mspb.RequestHeader{},
		DbId:    dbId,
		TableId: tableId,
	}
	resp, err := c.callRPC(req, RequestMSTimeout)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errInvalidResponse
	}
	if _resp, ok := resp.(*mspb.GetColumnsResponse); ok {
		return _resp.GetColumns(), nil
	}
	return nil, errInvalidResponse
}

func (c *RPCClient) GetColumnByName(dbId, tableId uint64, columnName string) (*metapb.Column, error) {
	req := &mspb.GetColumnByNameRequest{
		Header:  &mspb.RequestHeader{},
		DbId:    dbId,
		TableId: tableId,
		ColName: columnName,
	}
	resp, err := c.callRPC(req, RequestMSTimeout)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errInvalidResponse
	}
	if _resp, ok := resp.(*mspb.GetColumnByNameResponse); ok {
		return _resp.GetColumn(), nil
	}
	return nil, errInvalidResponse
}

func (c *RPCClient) GetColumnByID(dbId, tableId uint64, columnId uint64) (*metapb.Column, error) {
	req := &mspb.GetColumnByIdRequest{
		Header:  &mspb.RequestHeader{},
		DbId:    dbId,
		TableId: tableId,
		ColId:   columnId,
	}
	resp, err := c.callRPC(req, RequestMSTimeout)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errInvalidResponse
	}
	if _resp, ok := resp.(*mspb.GetColumnByIdResponse); ok {
		return _resp.GetColumn(), nil
	}
	return nil, errInvalidResponse
}

// columns输入参数只需要填写name和data type即可
// 返回master server处理后的columns列表(本次新增部分)
func (c *RPCClient) AddColumns(dbId, tableId uint64, columns []*metapb.Column) ([]*metapb.Column, error) {
	req := &mspb.AddColumnRequest{
		Header:  &mspb.RequestHeader{},
		DbId:    dbId,
		TableId: tableId,
		Columns: columns,
	}
	resp, err := c.callRPC(req, RequestMSTimeout)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errInvalidResponse
	}
	if _resp, ok := resp.(*mspb.AddColumnResponse); ok {
		return _resp.GetColumns(), nil
	}
	return nil, errInvalidResponse
}

func (c *RPCClient) NodeHeartbeat(req *mspb.NodeHeartbeatRequest) (*mspb.NodeHeartbeatResponse, error) {
	resp, err := c.callRPC(req, RequestMSTimeout)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errInvalidResponse
	}
	if _resp, ok := resp.(*mspb.NodeHeartbeatResponse); ok {
		return _resp, nil
	}
	return nil, errInvalidResponse
}

func (c *RPCClient) RangeHeartbeat(req *mspb.RangeHeartbeatRequest) (*mspb.RangeHeartbeatResponse, error) {
	resp, err := c.callRPC(req, RequestMSTimeout)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errInvalidResponse
	}
	if _resp, ok := resp.(*mspb.RangeHeartbeatResponse); ok {
		return _resp, nil
	}
	return nil, errInvalidResponse
}

func (c *RPCClient) TruncateTable(dbId, tableId uint64) error {
	req := &mspb.TruncateTableRequest{
		Header:  &mspb.RequestHeader{},
		DbId:    dbId,
		TableId: tableId,
	}
	resp, err := c.callRPC(req, RequestMSTimeout)
	if err != nil {
		return err
	}
	if resp == nil {
		return errInvalidResponse
	}
	if _, ok := resp.(*mspb.TruncateTableResponse); ok {
		return nil
	}
	return errInvalidResponse
}

func (c *RPCClient) CreateDatabase(dbName string) error {
	req := &mspb.CreateDatabaseRequest{
		Header: &mspb.RequestHeader{},
		DbName: dbName,
	}
	resp, err := c.callRPC(req, RequestMSTimeout)
	if err != nil {
		return err
	}
	if resp == nil {
		return errInvalidResponse
	}
	if _, ok := resp.(*mspb.CreateDatabaseResponse); ok {
		return nil
	}
	return errInvalidResponse
}

func (c *RPCClient) CreateTable(dbName, tableName, properties string) error {
	req := &mspb.CreateTableRequest{
		Header:     &mspb.RequestHeader{},
		DbName:     dbName,
		TableName:  tableName,
		Properties: properties,
	}
	resp, err := c.callRPC(req, RequestMSTimeout)
	if err != nil {
		return err
	}
	if resp == nil {
		return errInvalidResponse
	}
	if _, ok := resp.(*mspb.CreateTableResponse); ok {
		return nil
	}
	return errInvalidResponse
}

//get auto_increment id
func (c *RPCClient) GetAutoIncId(dbId, tableId uint64, size uint32) ([]uint64, error) {
	req := &mspb.GetAutoIncIdRequest{
		Header:  &mspb.RequestHeader{},
		DbId:    dbId,
		TableId: tableId,
		Size_:   size}
	resp, err := c.callRPC(req, RequestMSTimeout)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errInvalidResponse
	}
	if _resp, ok := resp.(*mspb.GetAutoIncIdResponse); ok {
		return _resp.GetIds(), nil
	}
	return nil, errInvalidResponse
}
func (c *RPCClient) NodeLogin(req *mspb.NodeLoginRequest) (*mspb.NodeLoginResponse, error) {
	resp, err := c.callRPC(req, RequestMSTimeout)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errInvalidResponse
	}
	if _resp, ok := resp.(*mspb.NodeLoginResponse); ok {
		return _resp, nil
	}
	return nil, errInvalidResponse
}

func (c *RPCClient) GetNodeId(req *mspb.GetNodeIdRequest) (*mspb.GetNodeIdResponse, error) {
	resp, err := c.callRPC(req, RequestMSTimeout)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errInvalidResponse
	}
	if _resp, ok := resp.(*mspb.GetNodeIdResponse); ok {
		return _resp, nil
	}
	return nil, errInvalidResponse
}

// Close closes the client.
func (c *RPCClient) Close() {
	for _, conn := range c.pool {
		conn.Close()
	}
}

func (c *RPCClient) getLeader(conn *Conn) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), RequestMSTimeout)
	defer cancel()
	in := &mspb.GetMSLeaderRequest{
		Header: &mspb.RequestHeader{},
	}
	var leader string
	out, err := conn.Cli.GetMSLeader(ctx, in)
	if err != nil {
		return leader, errors.New(grpc.ErrorDesc(err))
	}
	leader = out.GetLeader().GetAddress()
	if len(leader) == 0 {
		return leader, errNoLeader
	}
	return leader, nil
}

func (c *RPCClient) callRPC(req interface{}, timeout time.Duration) (resp interface{}, err error) {
	var conn *Conn
	var retry int
	var header *mspb.ResponseHeader
	var pbErr *mspb.Error
	retry = 3
	for i := 0; i < retry; i++ {
		conn, err = c.getConn()
		if err != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		switch in := req.(type) {
		case *mspb.GetRouteRequest:
			out, _err := conn.Cli.GetRoute(ctx, in)
			cancel()
			if _err != nil {
				return nil, errors.New(grpc.ErrorDesc(_err))
			}
			header = out.GetHeader()
			if header == nil {
				err = errInvalidResponseHeader
				return
			}
			pbErr = header.GetError()
			if pbErr == nil {
				return out, nil
			}
		case *mspb.GetColumnByIdRequest:
			out, _err := conn.Cli.GetColumnById(ctx, in)
			cancel()
			if _err != nil {
				return nil, errors.New(grpc.ErrorDesc(_err))
			}
			header = out.GetHeader()
			if header == nil {
				err = errInvalidResponseHeader
				return
			}
			pbErr = header.GetError()
			if pbErr == nil {
				return out, nil
			}
		case *mspb.NodeHeartbeatRequest:
			out, _err := conn.Cli.NodeHeartbeat(ctx, in)
			cancel()
			if _err != nil {
				return nil, errors.New(grpc.ErrorDesc(_err))
			}
			header = out.GetHeader()
			if header == nil {
				err = errInvalidResponseHeader
				return
			}
			pbErr = header.GetError()
			if pbErr == nil {
				return out, nil
			}
		case *mspb.RangeHeartbeatRequest:
			out, _err := conn.Cli.RangeHeartbeat(ctx, in)
			cancel()
			if _err != nil {
				return nil, errors.New(grpc.ErrorDesc(_err))
			}
			header = out.GetHeader()
			if header == nil {
				err = errInvalidResponseHeader
				return
			}
			pbErr = header.GetError()
			if pbErr == nil {
				return out, nil
			}
		case *mspb.GetNodeRequest:
			out, _err := conn.Cli.GetNode(ctx, in)
			cancel()
			if _err != nil {
				return nil, errors.New(grpc.ErrorDesc(_err))
			}
			header = out.GetHeader()
			if header == nil {
				err = errInvalidResponseHeader
				return
			}
			pbErr = header.GetError()
			if pbErr == nil {
				return out, nil
			}
		case *mspb.GetNodeIdRequest:
			out, _err := conn.Cli.GetNodeId(ctx, in)
			cancel()
			if _err != nil {
				return nil, errors.New(grpc.ErrorDesc(_err))
			}
			header = out.GetHeader()
			if header == nil {
				err = errInvalidResponseHeader
				return
			}
			pbErr = header.GetError()
			if pbErr == nil {
				return out, nil
			}
		case *mspb.GetDBRequest:
			out, _err := conn.Cli.GetDB(ctx, in)
			cancel()
			if _err != nil {
				return nil, errors.New(grpc.ErrorDesc(_err))
			}
			header = out.GetHeader()
			if header == nil {
				err = errInvalidResponseHeader
				return
			}
			pbErr = header.GetError()
			if pbErr == nil {
				return out, nil
			}
		case *mspb.GetTableRequest:
			out, _err := conn.Cli.GetTable(ctx, in)
			cancel()
			if _err != nil {
				return nil, errors.New(grpc.ErrorDesc(_err))
			}
			header = out.GetHeader()
			if header == nil {
				err = errInvalidResponseHeader
				return
			}
			pbErr = header.GetError()
			if pbErr == nil {
				return out, nil
			}
		case *mspb.GetTableByIdRequest:
			out, _err := conn.Cli.GetTableById(ctx, in)
			cancel()
			if _err != nil {
				return nil, errors.New(grpc.ErrorDesc(_err))
			}
			header = out.GetHeader()
			if header == nil {
				err = errInvalidResponseHeader
				return
			}
			pbErr = header.GetError()
			if pbErr == nil {
				return out, nil
			}
		case *mspb.GetColumnsRequest:
			out, _err := conn.Cli.GetColumns(ctx, in)
			cancel()
			if _err != nil {
				return nil, errors.New(grpc.ErrorDesc(_err))
			}
			header = out.GetHeader()
			if header == nil {
				err = errInvalidResponseHeader
				return
			}
			pbErr = header.GetError()
			if pbErr == nil {
				return out, nil
			}
		case *mspb.GetColumnByNameRequest:
			out, _err := conn.Cli.GetColumnByName(ctx, in)
			cancel()
			if _err != nil {
				return nil, errors.New(grpc.ErrorDesc(_err))
			}
			header = out.GetHeader()
			if header == nil {
				err = errInvalidResponseHeader
				return
			}
			pbErr = header.GetError()
			if pbErr == nil {
				return out, nil
			}
		case *mspb.GetMSLeaderRequest:
			out, _err := conn.Cli.GetMSLeader(ctx, in)
			cancel()
			if _err != nil {
				return nil, errors.New(grpc.ErrorDesc(_err))
			}
			header = out.GetHeader()
			if header == nil {
				err = errInvalidResponseHeader
				return
			}
			pbErr = header.GetError()
			if pbErr == nil {
				return out, nil
			}
		case *mspb.TruncateTableRequest:
			out, _err := conn.Cli.TruncateTable(ctx, in)
			cancel()
			if _err != nil {
				return nil, errors.New(grpc.ErrorDesc(_err))
			}
			header = out.GetHeader()
			if header == nil {
				err = errInvalidResponseHeader
				return
			}
			pbErr = header.GetError()
			if pbErr == nil {
				return out, nil
			}
		case *mspb.AddColumnRequest:
			out, _err := conn.Cli.AddColumn(ctx, in)
			cancel()
			if _err != nil {
				return nil, errors.New(grpc.ErrorDesc(_err))
			}
			header = out.GetHeader()
			if header == nil {
				err = errInvalidResponseHeader
				return
			}
			pbErr = header.GetError()
			if pbErr == nil {
				return out, nil
			}
		case *mspb.CreateTableRequest:
			out, _err := conn.Cli.CreateTable(ctx, in)
			cancel()
			if _err != nil {
				return nil, errors.New(grpc.ErrorDesc(_err))
			}
			header = out.GetHeader()
			if header == nil {
				err = errInvalidResponseHeader
				return
			}
			pbErr = header.GetError()
			if pbErr == nil {
				return out, nil
			}
		case *mspb.CreateDatabaseRequest:
			out, _err := conn.Cli.CreateDatabase(ctx, in)
			cancel()
			if _err != nil {
				return nil, errors.New(grpc.ErrorDesc(_err))
			}
			header = out.GetHeader()
			if header == nil {
				err = errInvalidResponseHeader
				return
			}
			pbErr = header.GetError()
			if pbErr == nil {
				return out, nil
			}
		case *mspb.NodeLoginRequest:
			out, _err := conn.Cli.NodeLogin(ctx, in)
			cancel()
			if _err != nil {
				return nil, errors.New(grpc.ErrorDesc(_err))
			}
			header = out.GetHeader()
			if header == nil {
				err = errInvalidResponseHeader
				return
			}
			pbErr = header.GetError()
			if pbErr == nil {
				return out, nil
			}
		case *mspb.GetAutoIncIdRequest:
			out, _err := conn.Cli.GetAutoIncId(ctx, in)
			cancel()
			if _err != nil {
				return nil, errors.New(grpc.ErrorDesc(_err))
			}
			header = out.GetHeader()
			if header == nil {
				err = errInvalidResponseHeader
				return
			}
			pbErr = header.GetError()
			if pbErr == nil {
				return out, nil
			}
		default:
			cancel()
			return nil, errInvalidRequest
		}

		if pbErr.GetNewLeader() != nil {
			c.leader = pbErr.GetNewLeader().GetAddress()
			continue
		} else if pbErr.GetNoLeader() != nil {
			time.Sleep(time.Millisecond * 20)
			// 主动查询一次leader
			leader, _err := c.getLeader(conn)
			if err != nil {
				return nil, _err
			}
			c.leader = leader
			continue
		}
		return nil, errInvalidResponse
	}
	return nil, errGrpcError
}

func (c *RPCClient) getConn() (*Conn, error) {
	leader := c.leader
	// 第一次获取leader
	if len(leader) == 0 {
		err := func() error {
			c.lock.Lock()
			defer c.lock.Unlock()
			leader = c.leader
			// 并发控制
			if len(leader) == 0 {
				var _err error
				for _, addr := range c.msAddrs {
					leader, _err = c.getLeader(c.pool[addr])
					if _err != nil {
						log.Warn("get MS leader from[%s] failed, err[%v]", addr, _err)
						continue
					}
					// 找到MS leader
					break
				}
				if _err != nil {
					return _err
				}
				if len(leader) > 0 {
					// 新节点
					if _, ok := c.pool[leader]; !ok {
						pool := make(map[string]*Conn)
						for k, v := range c.pool {
							pool[k] = v
						}
						conn, err := grpc.Dial(leader, grpc.WithInsecure(), grpc.WithTimeout(ConnectMSTimeout))
						if err != nil {
							log.Error("did not connect addr[%s], err[%v]", leader, err)
							return err
						}
						cli := mspb.NewMsServerClient(conn)
						pool[leader] = &Conn{addr: leader, conn: conn, Cli: cli}
						c.pool = pool
					}
					c.leader = leader
				}
			}
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}
	if len(leader) > 0 {
		return c.pool[leader], nil
	}
	return nil, errors.New("no MS leader")
}
