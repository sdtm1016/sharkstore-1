package server

import (
	"golang.org/x/net/context"
	"model/pkg/mspb"
)

func (service *Server) checkClusterValid() *mspb.Error {
	if !service.IsLeader() {
		var err *mspb.Error
		point := service.GetLeader()
		if point == nil {
			err = &mspb.Error{
				NoLeader: &mspb.NoLeader{},
			}
		} else {
			err = &mspb.Error{
				NewLeader: &mspb.LeaderHint{
					Address: point.RpcServerAddr,
				},
			}
		}
		return err
	}
	return nil
}

func (service *Server) GetRoute(ctx context.Context, req *mspb.GetRouteRequest) (*mspb.GetRouteResponse, error) {
	if err := service.checkClusterValid(); err != nil {
		resp := &mspb.GetRouteResponse{Header: &mspb.ResponseHeader{Error: err}}
		return resp, nil
	}
	return service.handleGetRoute(ctx, req)
}

func (service *Server) NodeHeartbeat(ctx context.Context, req *mspb.NodeHeartbeatRequest) (*mspb.NodeHeartbeatResponse, error) {
	if err := service.checkClusterValid(); err != nil {
		resp := &mspb.NodeHeartbeatResponse{Header: &mspb.ResponseHeader{Error: err}}
		return resp, nil
	}

	resp := service.handleNodeHeartbeat(ctx, req)
	return resp, nil
}

func (service *Server) RangeHeartbeat(ctx context.Context, req *mspb.RangeHeartbeatRequest) (*mspb.RangeHeartbeatResponse, error) {
	if err := service.checkClusterValid(); err != nil {
		resp := &mspb.RangeHeartbeatResponse{Header: &mspb.ResponseHeader{Error: err}}
		return resp, nil
	}
	resp := service.handleRangeHeartbeat(ctx, req)
	return resp, nil
}

func (service *Server) AskSplit(ctx context.Context, req *mspb.AskSplitRequest) (*mspb.AskSplitResponse, error) {
	if err := service.checkClusterValid(); err != nil {
		resp := &mspb.AskSplitResponse{Header: &mspb.ResponseHeader{Error: err}}
		return resp, nil
	}
	return service.handleAskSplit(ctx, req)
}

func (service *Server) ReportSplit(ctx context.Context, req *mspb.ReportSplitRequest) (*mspb.ReportSplitResponse, error) {
	if err := service.checkClusterValid(); err != nil {
		resp := &mspb.ReportSplitResponse{Header: &mspb.ResponseHeader{Error: err}}
		return resp, nil
	}
	return service.handleReportSplit(ctx, req)
}

func (service *Server) NodeLogin(ctx context.Context, req *mspb.NodeLoginRequest) (*mspb.NodeLoginResponse, error) {
	if err := service.checkClusterValid(); err != nil {
		resp := &mspb.NodeLoginResponse{Header: &mspb.ResponseHeader{Error: err}}
		return resp, nil
	}
	return service.handleNodeLogin(ctx, req)
}

func (service *Server) GetNode(ctx context.Context, req *mspb.GetNodeRequest) (*mspb.GetNodeResponse, error) {
	if err := service.checkClusterValid(); err != nil {
		resp := &mspb.GetNodeResponse{Header: &mspb.ResponseHeader{Error: err}}
		return resp, nil
	}
	return service.handleGetNode(ctx, req)
}

func (service *Server) GetNodeId(ctx context.Context, req *mspb.GetNodeIdRequest) (*mspb.GetNodeIdResponse, error) {
	if err := service.checkClusterValid(); err != nil {
		resp := &mspb.GetNodeIdResponse{Header: &mspb.ResponseHeader{Error: err}}
		return resp, nil
	}
	return service.handleGetNodeId(ctx, req)
}

func (service *Server) GetDB(ctx context.Context, req *mspb.GetDBRequest) (*mspb.GetDBResponse, error) {
	if err := service.checkClusterValid(); err != nil {
		resp := &mspb.GetDBResponse{Header: &mspb.ResponseHeader{Error: err}}
		return resp, nil
	}
	return service.handleGetDb(ctx, req)
}
func (service *Server) GetTable(ctx context.Context, req *mspb.GetTableRequest) (*mspb.GetTableResponse, error) {
	if err := service.checkClusterValid(); err != nil {
		resp := &mspb.GetTableResponse{Header: &mspb.ResponseHeader{Error: err}}
		return resp, nil
	}
	return service.handleGetTable(ctx, req)
}

func (service *Server) GetTableById(ctx context.Context, req *mspb.GetTableByIdRequest) (*mspb.GetTableByIdResponse, error) {
	if err := service.checkClusterValid(); err != nil {
		resp := &mspb.GetTableByIdResponse{Header: &mspb.ResponseHeader{Error: err}}
		return resp, nil
	}
	return service.handleGetTableById(ctx, req)
}

func (service *Server) GetColumns(ctx context.Context, req *mspb.GetColumnsRequest) (*mspb.GetColumnsResponse, error) {
	if err := service.checkClusterValid(); err != nil {
		resp := &mspb.GetColumnsResponse{Header: &mspb.ResponseHeader{Error: err}}
		return resp, nil
	}
	return service.handleGetColumns(ctx, req)
}
func (service *Server) GetColumnByName(ctx context.Context, req *mspb.GetColumnByNameRequest) (*mspb.GetColumnByNameResponse, error) {
	if err := service.checkClusterValid(); err != nil {
		resp := &mspb.GetColumnByNameResponse{Header: &mspb.ResponseHeader{Error: err}}
		return resp, nil
	}
	return service.handleGetColumnByName(ctx, req)
}
func (service *Server) GetColumnById(ctx context.Context, req *mspb.GetColumnByIdRequest) (*mspb.GetColumnByIdResponse, error) {
	if err := service.checkClusterValid(); err != nil {
		resp := &mspb.GetColumnByIdResponse{Header: &mspb.ResponseHeader{Error: err}}
		return resp, nil
	}
	return service.handleGetColumnById(ctx, req)
}
func (service *Server) GetMSLeader(ctx context.Context, req *mspb.GetMSLeaderRequest) (*mspb.GetMSLeaderResponse, error) {
	return service.handleGetMsLeader(ctx, req)
}

func (service *Server) TruncateTable(context.Context, *mspb.TruncateTableRequest) (*mspb.TruncateTableResponse, error) {
	return &mspb.TruncateTableResponse{}, nil
}

func (service *Server) AddColumn(ctx context.Context, req *mspb.AddColumnRequest) (*mspb.AddColumnResponse, error) {
	if err := service.checkClusterValid(); err != nil {
		resp := &mspb.AddColumnResponse{Header: &mspb.ResponseHeader{Error: err}}
		return resp, nil
	}
	return service.handleAddColumns(ctx, req)
}

func (service *Server) CreateDatabase(ctx context.Context, req *mspb.CreateDatabaseRequest) (*mspb.CreateDatabaseResponse, error) {
	if err := service.checkClusterValid(); err != nil {
		resp := &mspb.CreateDatabaseResponse{Header: &mspb.ResponseHeader{Error: err}}
		return resp, nil
	}
	return service.handleCreateDatabase(ctx, req)
}

func (service *Server) CreateTable(ctx context.Context, req *mspb.CreateTableRequest) (*mspb.CreateTableResponse, error) {
	if err := service.checkClusterValid(); err != nil {
		resp := &mspb.CreateTableResponse{Header: &mspb.ResponseHeader{Error: err}}
		return resp, nil
	}
	return service.handleCreateTable(ctx, req)
}

func (service *Server) GetAutoIncId(ctx context.Context, req *mspb.GetAutoIncIdRequest) (*mspb.GetAutoIncIdResponse, error) {
	if err := service.checkClusterValid(); err != nil {
		resp := &mspb.GetAutoIncIdResponse{Header: &mspb.ResponseHeader{Error: err}}
		return resp, nil
	}
	return service.handleAutoIncId(ctx, req)
}