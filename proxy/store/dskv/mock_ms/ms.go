package mock_ms

import (
	"net"
	"fmt"
	"sync"
	"util/log"
	"strings"
	"strconv"
	"net/http"
	"encoding/json"
	"sync/atomic"

	"google.golang.org/grpc"
	"model/pkg/mspb"
	"golang.org/x/net/context"
	"model/pkg/metapb"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/peer"
	"util/server"
	"pkg-go/ds_client"
)

var idBase uint64
type Cluster struct {
	id       uint64
	db       *DbCache
	tables   *TableCache
	nodes     map[uint64]*metapb.Node
	rpc      string
    host     string

	rLock       sync.RWMutex

	cli      client.SchClient
	server   *server.Server
	grpcServer *grpc.Server
}


func NewCluster(rpc, host string) *Cluster {
    return &Cluster{
	    id: 1,
	    rpc: rpc,
	    host: host,
	    db: NewDbCache(),
	    tables: NewTableCache(),
	    nodes: make(map[uint64]*metapb.Node),
    }
}

func (c *Cluster) SetDb(db *metapb.DataBase) {
	c.db.Add(NewDatabase(db))
}

func (c *Cluster) SetTable(t *metapb.Table) {
	c.rLock.Lock()
	defer c.rLock.Unlock()
	db, _ := c.db.FindDb(t.GetDbName())
	table := NewTable(t)
	db.tables.Add(table)
	c.tables.Add(table)
}

func (c *Cluster) SetNode(node *metapb.Node) {
	c.rLock.Lock()
	defer c.rLock.Unlock()
	c.nodes[node.GetId()] = node
}

func (c *Cluster) SetRange(r *metapb.Range) {
	c.rLock.Lock()
	defer c.rLock.Unlock()
	t, _ := c.tables.FindTableById(r.GetTableId())
	t.ranges.Add(NewRange(r))
}

func (c *Cluster) Start() {
	lis, err := net.Listen("tcp", c.rpc)
	if err != nil {
		log.Fatal("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	mspb.RegisterMsServerServer(s, c)
	// Register reflection service on gRPC server.
	reflection.Register(s)

	if err = s.Serve(lis); err != nil {
		log.Fatal("failed to serve: %v", err)
	}
	c.grpcServer = s

}

func (c *Cluster) Stop() {
	c.grpcServer.Stop()

}

func (c *Cluster) GetRoute(ctx context.Context, req *mspb.GetRouteRequest) (*mspb.GetRouteResponse, error) {
	t, find := c.tables.FindTableById(req.GetTableId())
	if !find {
		return nil, ErrNotExistTable
	}
	r, find := t.ranges.SearchRange(req.GetKey())
	if !find {
		return nil, ErrNotExistRange
	}
	resp := &mspb.GetRouteResponse{
		Header: &mspb.ResponseHeader{},
		Routes: []*metapb.Route{
			&metapb.Route{
				Range: r.Range,
				Leader: r.Range.GetPeers()[0],
			},
		},
	}
	return resp, nil
}

func (c *Cluster) NodeHeartbeat(ctx context.Context, req *mspb.NodeHeartbeatRequest) (*mspb.NodeHeartbeatResponse, error) {
	resp := &mspb.NodeHeartbeatResponse{Header: &mspb.ResponseHeader{}, NodeId: req.GetNodeId()}
	return resp, nil
}

func (c *Cluster) RangeHeartbeat(ctx context.Context, req *mspb.RangeHeartbeatRequest) (*mspb.RangeHeartbeatResponse, error) {
	resp := &mspb.RangeHeartbeatResponse{Header: &mspb.ResponseHeader{}, RangeId: 1}
	return resp, nil
}

func (c *Cluster) AskSplit(ctx context.Context, req *mspb.AskSplitRequest) (*mspb.AskSplitResponse, error) {
	return nil, nil
}

func (c *Cluster) ReportSplit(ctx context.Context, req *mspb.ReportSplitRequest) (*mspb.ReportSplitResponse, error) {
	return nil, nil
}

func (c *Cluster) NodeLogin(ctx context.Context, req *mspb.NodeLoginRequest) (*mspb.NodeLoginResponse, error) {
	resp := &mspb.NodeLoginResponse{Header: &mspb.ResponseHeader{}}
	return resp, nil
}

func (c *Cluster) GetNode(ctx context.Context, req *mspb.GetNodeRequest) (*mspb.GetNodeResponse, error) {
	resp := &mspb.GetNodeResponse{Header: &mspb.ResponseHeader{}, Node: c.nodes[req.GetId()]}
	return resp, nil
}

func (c *Cluster) GetNodeId(ctx context.Context, req *mspb.GetNodeIdRequest) (*mspb.GetNodeIdResponse, error) {
	if c.nodes == nil || len(c.nodes) == 0 {
		if client, ok := peer.FromContext(ctx); ok {
			ip, _, err := net.SplitHostPort(client.Addr.String())
			if err != nil {
				return nil, err
			}
			serverAddr := fmt.Sprintf("%s:%d", ip, req.GetServerPort())
			raftAddr := fmt.Sprintf("%s:%d", ip, req.GetRaftPort())
			httpAddr := fmt.Sprintf("%s:%d", ip, req.GetAdminPort())
			node := &metapb.Node{
				Id: 1,
				ServerAddr: serverAddr,
				RaftAddr: raftAddr,
				AdminAddr: httpAddr,
			}
			c.nodes[1] = node
		}
	}
	resp := &mspb.GetNodeIdResponse{Header: &mspb.ResponseHeader{}, NodeId: c.nodes[1].GetId(), Clearup: false}
	return resp, nil
}

func (c *Cluster) GetDB(ctx context.Context, req *mspb.GetDBRequest) (*mspb.GetDBResponse, error) {
	db, find := c.db.FindDb(req.GetName())
	if !find {
		return nil, ErrNotExistDatabase
	}
	resp := &mspb.GetDBResponse{Header: &mspb.ResponseHeader{}, Db: db.DataBase}
	return resp, nil
}
func (c *Cluster) GetTable(ctx context.Context, req *mspb.GetTableRequest) (*mspb.GetTableResponse, error) {
	t, find := c.tables.FindTableByName(req.GetTableName())
	if !find {
		return nil, ErrNotExistTable
	}
	resp := &mspb.GetTableResponse{Header: &mspb.ResponseHeader{}, Table: t.Table}
	return resp, nil
}

func (c *Cluster) GetTableById(ctx context.Context, req *mspb.GetTableByIdRequest) (*mspb.GetTableByIdResponse, error) {
	t, find := c.tables.FindTableById(req.GetTableId())
	if !find {
		return nil, ErrNotExistTable
	}
	resp := &mspb.GetTableByIdResponse{Header: &mspb.ResponseHeader{}, Table: t.Table}
	return resp, nil
}

func (c *Cluster) GetColumns(ctx context.Context, req *mspb.GetColumnsRequest) (*mspb.GetColumnsResponse, error) {
	t, find := c.tables.FindTableById(req.GetTableId())
	if !find {
		return nil, ErrNotExistTable
	}
	resp := &mspb.GetColumnsResponse{Header: &mspb.ResponseHeader{}, Columns: t.Table.GetColumns()}
	return resp, nil
}
func (c *Cluster) GetColumnByName(ctx context.Context, req *mspb.GetColumnByNameRequest) (*mspb.GetColumnByNameResponse, error) {
	resp := &mspb.GetColumnByNameResponse{Header: &mspb.ResponseHeader{}}
	return resp, nil
}
func (c *Cluster) GetColumnById(ctx context.Context, req *mspb.GetColumnByIdRequest) (*mspb.GetColumnByIdResponse, error) {
	resp := &mspb.GetColumnByIdResponse{Header: &mspb.ResponseHeader{}}
	return resp, nil
}
func (c *Cluster) GetMSLeader(ctx context.Context, req *mspb.GetMSLeaderRequest) (*mspb.GetMSLeaderResponse, error) {
	resp := &mspb.GetMSLeaderResponse{Header: &mspb.ResponseHeader{}, Leader: &mspb.MSLeader{Id: c.id, Address: c.rpc}}
	return resp, nil
}

func (c *Cluster) TruncateTable(context.Context, *mspb.TruncateTableRequest) (*mspb.TruncateTableResponse, error) {
	return &mspb.TruncateTableResponse{}, nil
}

func (c *Cluster) AddColumn(ctx context.Context, req *mspb.AddColumnRequest) (*mspb.AddColumnResponse, error) {
	resp := &mspb.AddColumnResponse{Header: &mspb.ResponseHeader{}}
	return resp, nil
}

func (c *Cluster) CreateDatabase(ctx context.Context, req *mspb.CreateDatabaseRequest) (*mspb.CreateDatabaseResponse, error) {
	return nil, nil
}

func (c *Cluster) CreateTable(ctx context.Context, req *mspb.CreateTableRequest) (*mspb.CreateTableResponse, error) {
	return nil, nil
}

func (c *Cluster) GetAutoIncId(ctx context.Context, req *mspb.GetAutoIncIdRequest) (*mspb.GetAutoIncIdResponse, error) {
	log.Info("ms mock: get auto_increment id")
	size := req.GetSize_()
	ids := make([]uint64, 0)
	for len(ids) < int(size) {
		ids = append(ids, atomic.AddUint64(&idBase, 1))
	}
	log.Info("auto_increment ids : %v", ids)
	resp := &mspb.GetAutoIncIdResponse{Header: &mspb.ResponseHeader{}, Ids: ids}
	return resp, nil
}

type HttpReply httpReply

type httpReply struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

func sendReply(w http.ResponseWriter, httpreply *httpReply) {
	reply, err := json.Marshal(httpreply)
	if err != nil {
		log.Error("http reply marshal error: %s", err)
		w.WriteHeader(500)
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.Error("http reply[%s] len[%d] write error: %v", string(reply), len(reply), err)
	}
}

const (
	HTTP_DB_NAME = "dbName"
	HTTP_DB_ID = "dbId"
	HTTP_TABLE_NAME = "tableName"
	HTTP_TABLE_ID = "tableId"
	HTTP_CLUSTER_ID = "clusterId"
	HTTP_RANGE_ID = "rangeId"
	HTTP_NODE_ID = "nodeId"
	HTTP_PEER_ID = "peerId"
	HTTP_NAME = "name"
	HTTP_PROPERTIES = "properties"
	HTTP_PKDUPCHECK = "pkDupCheck"
)

func parseColumn(cols []*metapb.Column) error {
	var hasPk bool
	for _, c := range cols {
		c.Name = strings.ToLower(c.Name)
		if len(c.GetName()) > 255 {
			return ErrColumnNameTooLong
		}
		if c.PrimaryKey == 1 {
			if c.Nullable {
				return ErrPkMustNotNull
			}
			if len(c.DefaultValue) > 0 {
				return ErrPkMustNotSetDefaultValue
			}
			hasPk = true
		}
		if c.DataType == metapb.DataType_Invalid {
			return ErrInvalidColumn
		}
	}
	if !hasPk {
		return ErrMissingPk
	}
	columnName := make(map[string]interface{})
	//sort.Sort(ByPrimaryKey(cols))
	var id uint64 = 1
	for _, c := range cols {

		// check column name
		if _, ok := columnName[c.Name]; ok {
			return ErrDupColumnName
		} else {
			columnName[c.Name] = nil
		}

		// set column id
		c.Id = id
		id++

	}
	return nil
}

func ParseProperties(properties string) ([]*metapb.Column, []*metapb.Column, error) {
	tp := new(TableProperty)
	if err := json.Unmarshal([]byte(properties), tp); err != nil {
		return nil, nil, err
	}
	if tp.Columns == nil {
		return nil, nil, ErrInvalidColumn
	}

	err := parseColumn(tp.Columns)
	if err != nil {
		return nil, nil, err
	}

	for _, c := range tp.Regxs {
		// TODO check regx compile if error or not, error return
		if c.DataType == metapb.DataType_Invalid {
			return nil, nil, ErrInvalidColumn
		}
	}

	return tp.Columns, tp.Regxs, nil
}

func bytesPrefix(prefix []byte) ([]byte, []byte) {
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return prefix, limit
}