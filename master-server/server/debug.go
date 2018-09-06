package server

import (
	"net/http"
	"strconv"
	"time"

	"model/pkg/metapb"
	"model/pkg/taskpb"
	"util/deepcopy"
	"util/log"
)

type NodeDebug struct {
	*metapb.Node
	Ranges      []*Range     `json:"ranges"`
	LastHbTime  time.Time    `json:"last_hb_time"`
	LastSchTime time.Time    `json:"last_sch_time"`
	LastOpt     *taskpb.Task `json:"last_opt"`
}

type RangeDebug struct {
	*metapb.Range
	Leader      *metapb.Peer         `json:"leader,omitempty"`
	PeersStatus []*metapb.PeerStatus `json:"peers_status,omitempty"`
	LastHbTime  time.Time            `json:"last_hb_time,omitempty"`
	Task        *taskpb.Task         `json:"task,omitempty"`
}

func (service *Server) handleDebugNodeInfo(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	nodeid, err := strconv.ParseUint(r.FormValue("nodeId"), 10, 64)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = "wrong nodeid"
		return
	}
	node := service.cluster.FindNodeById(nodeid)
	if node == nil {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistNode.Error()
		return
	}
	reply.Data = node.Node
}

func (service *Server) handleDebugRangeInfo(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	cluster := service.cluster
	rangeid, err := strconv.ParseUint(r.FormValue("rangeId"), 10, 64)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = "wrong rangeid"
		return
	}
	rng := cluster.FindRange(rangeid)
	if rng == nil {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistRange.Error()
		return
	}
	reply.Data = &RangeDebug{
		Range:       deepcopy.Iface(rng.Range).(*metapb.Range),
		Leader:      deepcopy.Iface(rng.Leader).(*metapb.Peer),
		PeersStatus: rng.PeersStatus,
		LastHbTime:  rng.LastHbTimeTS,
	}
}

// 把miss的分片归到正式路由中
func (service *Server) handleDebugRangeRaftStatus(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	_, err := strconv.ParseUint(r.FormValue("rangeid"), 10, 64)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
}

func (service *Server) handleDebugLogSetLevel(w http.ResponseWriter, r *http.Request) {
	//reply := &httpReply{}
	//defer sendReply(w, reply)
	level := r.FormValue("level")
	log.Info("set master log level:{}", level)
	log.SetLevel(level)
	w.Write([]byte("OK"))
}

func (service *Server) handleDebugRangeTrace(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	//rangeid, err := strconv.ParseUint(r.FormValue("rangeid"), 10, 64)
	//if err != nil {
	//	reply.Code = HTTP_ERROR
	//	reply.Message = "wrong rangeid"
	//	return
	//}
	//trace := r.FormValue("trace")
	//cluster := service.cluster
	//if r, find := cluster.FindRange(rangeid); find {
	//	if trace == "true" {
	//		r.Trace = true
	//	} else {
	//		r.Trace = false
	//	}
	//	return
	//}
	//
	//log.Info("range[%d] trace success!!", rangeid)
}

func (service *Server) handleDebugNodeTrace(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	//nodeid, err := strconv.ParseUint(r.FormValue("nodeid"), 10, 64)
	//if err != nil {
	//	reply.Code = HTTP_ERROR
	//	reply.Message = "wrong rangeid"
	//	return
	//}
	//trace := r.FormValue("trace")
	//cluster := service.cluster
	//if node, find := cluster.FindNode(nodeid); find {
	//	if trace == "true" {
	//		node.Trace = true
	//	} else {
	//		node.Trace = false
	//	}
	//}
	//log.Info("node[%d] trace success!!", nodeid)
}
