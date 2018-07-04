package alarm

import (
	"net"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"model/pkg/alarmpb"
	"fmt"
	"errors"
	"util/log"
	"time"
	"golang.org/x/net/context"
)

type AlarmServer interface {
	TaskAlarm(context.Context, *alarmpb.TaskAlarmRequest) (*alarmpb.TaskAlarmResponse, error)
	NodeRangeAlarm(context.Context, *alarmpb.NodeRangeAlarmRequest) (*alarmpb.NodeRangeAlarmResponse, error)
	AliveAlarm(context.Context, *alarmpb.AliveRequest) (*alarmpb.AliveResponse, error)
	SimpleAlarm(ctx context.Context, req *alarmpb.SimpleRequest) (*alarmpb.SimpleResponse, error)
}

type Server struct {
	gateway *MessageGateway
	filters []alarmFilter

}

func newServer(ctx context.Context, gatewayAddr, alarmServerAddr string, receiver MessageReceiver) *Server {
	gateway := NewMessageGateway(ctx, gatewayAddr, alarmServerAddr, receiver)
	if gateway == nil {
		return nil
	}

	s := new(Server)
	s.gateway = gateway
	s.filters = append(s.filters, new(clusterIdFilter))
	return s
}


func NewAlarmServer(ctx context.Context, port int, gatewayAddress, remoteAlarmServerAddress string, receiver MessageReceiver) (*Server, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		return nil, err
	}
	s := grpc.NewServer()
	server := newServer(ctx, gatewayAddress, remoteAlarmServerAddress, receiver)
	if server == nil {
		return nil, errors.New("server is nil")
	}
	// register alarm server
	alarmpb.RegisterAlarmServer(s, server)
	reflection.Register(s)
	go s.Serve(lis)

	return server, nil
}

func (s *Server) TaskAlarm(ctx context.Context, req *alarmpb.TaskAlarmRequest) (*alarmpb.TaskAlarmResponse, error) {
	log.Info("receive task alarm message: %v", req.String())
	var err error
	resp := new(alarmpb.TaskAlarmResponse)
	clusterId := req.GetHead().GetClusterId()
	// todo
	switch req.GetType() {
	case alarmpb.TaskAlarmType_TIMEOUT:
	case alarmpb.TaskAlarmType_LONG_TIME_RUNNING:
	}

	for _, f := range s.filters {
		if ok := f.FilteredByInt(req.GetHead().GetClusterId()); ok {
			return resp, nil
		}
	}

	if err := s.gateway.notify(Message{
		ClusterId: clusterId,
		Title: "task timeout",
		Content: req.GetDescribe(),
		samples: req.SampleJson,
	}, time.Second); err != nil {
		log.Error("task alarm notify timeout")
	}

	return resp, err
}

func (s *Server) NodeRangeAlarm(ctx context.Context, req *alarmpb.NodeRangeAlarmRequest) (*alarmpb.NodeRangeAlarmResponse, error) {
	log.Info("receive node/range alarm message: %v", req.String())
	var err error
	resp := new(alarmpb.NodeRangeAlarmResponse)
	clusterId := req.GetHead().GetClusterId()
	// todo
	switch req.GetType() {
	case alarmpb.NodeRangeAlarmType_RANGE_NO_HEARTBEAT:
	case alarmpb.NodeRangeAlarmType_NODE_NO_HEARTBEAT:
	case alarmpb.NodeRangeAlarmType_NODE_DISK_SIZE:
	case alarmpb.NodeRangeAlarmType_NODE_LEADER_COUNT:
	}

	for _, f := range s.filters {
		if ok := f.FilteredByInt(req.GetHead().GetClusterId()); ok {
			return resp, nil
		}
	}
	if err := s.gateway.notify(Message{
		ClusterId: clusterId,
		Title: "node/range alarm",
		Content: req.GetDescribe(),
		samples: req.SampleJson,
	}, time.Second); err != nil {
		log.Error("node/range alarm notify timeout")
	}

	return resp, err
}

func (s *Server) AliveAlarm(ctx context.Context, req *alarmpb.AliveRequest) (*alarmpb.AliveResponse, error) {
	log.Info("receive alive alarm message: %v", req.String())
	var err error
	resp := new(alarmpb.AliveResponse)
	// todo
	for _, f := range s.filters {
		if ok := f.FilteredByInt(req.GetHead().GetClusterId()); ok {
			return resp, nil
		}
	}
	return resp, err
}

func (s *Server) SimpleAlarm(ctx context.Context, req *alarmpb.SimpleRequest) (*alarmpb.SimpleResponse, error) {
	log.Info("receive simple alarm message: %v", req.String())
	var err error
	resp := new(alarmpb.SimpleResponse)

	clusterId := req.GetHead().GetClusterId()

	for _, f := range s.filters {
		if ok := f.FilteredByInt(req.GetHead().GetClusterId()); ok {
			return resp, nil
		}
	}
	if err := s.gateway.notify(Message{
		ClusterId: clusterId,
		Title: req.GetTitle(),
		Content: req.GetContent(),
		samples: req.SampleJson,
	}, time.Second); err != nil {
		log.Error("simple alarm notify timeout")
	}

	return resp, err
}
