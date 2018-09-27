package server

import (
	"bytes"
	"errors"
	"fmt"
	"master-server/alarm2"
	"model/pkg/alarmpb2"
	"net"
	"time"

	"model/pkg/metapb"
	"model/pkg/mspb"
	"util"
	"util/deepcopy"
	"util/log"

	"golang.org/x/net/context"
	"google.golang.org/grpc/peer"
)

func (service *Server) handleGetRoute(ctx context.Context, req *mspb.GetRouteRequest) (resp *mspb.GetRouteResponse, err error) {
	cluster := service.cluster
	var routes []*metapb.Route
	var key []byte = req.GetKey()
	var max int = 10

	if table, find := cluster.FindTableById(req.GetTableId()); find {
		if table.Status != metapb.TableStatus_TableRunning {
			log.Warn("table[%d] not ready for work", req.GetTableId())
			err = ErrNotExistTable
			return
		}
		ranges := cluster.MultipleSearchRanges(key, max)
		if ranges == nil {
			log.Warn("table[%d] not found range for key[%v]", req.GetTableId(), key)
			err = ErrNotExistRange
			return
		}
		routes = make([]*metapb.Route, 0, max)
		for ind, rng := range ranges {
			if rng.GetTableId() != req.GetTableId() {
				log.Warn("table id is not equal %d , %d", rng.GetTableId(), req.GetTableId())
				continue
			}
			leader := rng.GetLeader()
			// 暂时没有leader,默认peer[0]
			if leader == nil {
				peers := rng.GetPeers()
				if len(peers) == 0 {
					log.Warn("invalid range %v", rng)
					err = ErrNotExistRange
					return
				}
				leader = peers[0]
			}
			routes = append(routes, &metapb.Route{
				Range:  deepcopy.Iface(rng.Range).(*metapb.Range),
				Leader: leader,
			})

			if ind == 0 {
				//just trace,
				log.Info("get route table %d ,range %d", rng.GetTableId(), rng.GetId())
			}
		}
	} else {
		log.Warn("get route: table %d not found", req.GetTableId())
		err = ErrNotExistTable
		return
	}
	resp = new(mspb.GetRouteResponse)
	resp.Header = &mspb.ResponseHeader{}
	resp.Routes = routes
	return
}

func (service *Server) handleNodeHeartbeat(ctx context.Context, req *mspb.NodeHeartbeatRequest) (resp *mspb.NodeHeartbeatResponse) {
	cluster := service.cluster

	nodeId := req.GetNodeId()
	resp = new(mspb.NodeHeartbeatResponse)
	resp.Header = &mspb.ResponseHeader{}
	resp.NodeId = nodeId
	node := cluster.FindNodeById(nodeId)
	if node == nil {
		log.Error("Received heartbeat from invalid node[%d], node not in resources pool", nodeId)
		return
	}
	if log.IsEnableInfo() {
		log.Info("Node:[%d] report heartbeat. state:[%s]", nodeId, node.State.String())
	}

	switch node.State {
	case metapb.NodeState_N_Initial, metapb.NodeState_N_Logout:
		log.Error("Received heartbeat from unactived node:[%d]. Reject!", nodeId)
		return
	case metapb.NodeState_N_Offline:
		if err := cluster.UpdateNodeState(node, metapb.NodeState_N_Login); err != nil {
			return
		}
	case metapb.NodeState_N_Tombstone:
		// 暂时先回到offline状态,如果还能收到心跳,那么就会正式成为login
		cluster.UpdateNodeState(node, metapb.NodeState_N_Offline)
		return
	}

	// set node stats score 状态正确才可以信任上报数据
	node.stats = req.GetStats()
	node.opsStat.Hit(node.stats.GetBytesWritten())
	node.LastHeartbeatTS = time.Now()
	var delete_ranges []uint64
	for _, rangeId := range req.GetIsolatedReplicas() {
		r := cluster.FindRange(rangeId)
		if r == nil {
			delete_ranges = append(delete_ranges, rangeId)
			continue
		}
		if peer := r.GetNodePeer(nodeId); peer == nil {
			delete_ranges = append(delete_ranges, rangeId)
		}
	}
	resp.DeleteReplicas = delete_ranges
	if delete_ranges != nil {
		log.Warn("node[%s] need delete replicas[%v]", node.GetServerAddr(), node.stats, delete_ranges)
	}
	log.Debug("node[%s] heartbeat report data: stats:[%v], delete replicas[%v]", node.GetServerAddr(), node.stats, delete_ranges)

	return
}

func checkQuorumDown(req *mspb.RangeHeartbeatRequest) bool {
	totalVoters := 0
	downVoters := 0
	for _, peer := range req.GetRange().GetPeers() {
		if peer.GetType() != metapb.PeerType_PeerType_Learner {
			totalVoters++
		}
	}
	for _, status := range req.GetPeersStatus() {
		if status.GetDownSeconds() > 0 && status.GetPeer().GetType() != metapb.PeerType_PeerType_Learner {
			downVoters++
		}
	}
	quorum := totalVoters/2 + 1
	return totalVoters-downVoters < quorum
}

func (service *Server) handleRangeHeartbeat(ctx context.Context, req *mspb.RangeHeartbeatRequest) (resp *mspb.RangeHeartbeatResponse) {
	r := req.GetRange()
	log.Debug("[HB] range[%d:%d] heartbeat, from ip[%s], peers: %v, status: %v",
		r.GetTableId(), r.GetId(), util.GetIpFromContext(ctx), req.GetRange().GetPeers(), req.GetPeersStatus())

	cluster := service.cluster
	resp = new(mspb.RangeHeartbeatResponse)
	resp.Header = &mspb.ResponseHeader{}
	resp.RangeId = r.GetId()
	delFlag := false
	table, find := cluster.FindTableById(r.GetTableId())
	if !find {
		table, find = cluster.FindDeleteTableById(r.GetTableId()) //删除表已经到期被删除
		if !find {
			delFlag = true
		}
	}
	if _, found := cluster.deletedRanges.FindRange(r.GetId()); found {
		log.Error("range[%v] had been deleted, but still exist heartbeat from nodeId[%d]. Please check ds log for more detail!", r.GetId(), req.GetLeader().GetNodeId())
		return
	}

	go service.rangePeerIndexAlarm(req)

	var saveStore, saveCache bool
	rng := cluster.FindRange(r.GetId())
	if rng == nil {
		log.Info("range[%d:%d] not found", r.GetTableId(), r.GetId())
		rng = NewRange(r, req.GetLeader())
		if !delFlag {
			saveCache = true
			saveStore = true
		}
	}
	// 临时标记需要删除，并不持久化
	if delFlag {
		rng.State = metapb.RangeState_R_Remove
	}

	if rng.State == metapb.RangeState_R_Init {
		rng.State = metapb.RangeState_R_Normal
	}

	//if rng.Trace || log.IsEnableInfo() {
	//	log.Info("[HB] range[%s] heartbeat, from ip[%s]", rng.SString(), util.GetIpFromContext(ctx))
	//}

	//range心跳恢复
	if rng.State == metapb.RangeState_R_Abnormal && !checkQuorumDown(req) {
		rng.State = metapb.RangeState_R_Normal
		//执行store GC .
		oldRng, found := cluster.FindPreGCRangeById(rng.GetId())
		if found {
			for _, peer := range oldRng.GetPeers() {
				peerGC(cluster, oldRng, peer)
			}
			cluster.preGCRanges.Delete(rng.GetId())
			if err := cluster.deleteRangeGC(rng.GetId()); err != nil {
				log.Warn("delete range gc remark from store failed. err: [%v]", err)
			}
		}
	}

	// Range meta is stale, return.
	if r.GetRangeEpoch().GetVersion() < rng.GetRangeEpoch().GetVersion() ||
		r.GetRangeEpoch().GetConfVer() < rng.GetRangeEpoch().GetConfVer() {
		log.Warn("range[%v] stale %v", rng.Range, r)
		return
	}

	// Stale term
	// req.GetTerm() != 0: for backward compatible
	// TODO: remove req.GetTerm() != 0 in the future
	if req.GetTerm() != 0 {
		if req.GetTerm() < rng.Term {
			log.Warn("range[%v] stale term(%d < %d), stale leader: [%v], current leader: [%v]",
				rng.GetId(), req.GetTerm(), rng.Term, req.GetLeader(), rng.GetLeader())
			return
		} else if req.GetTerm() > rng.Term {
			saveCache = true
			log.Info("range[%d] term change from %d to %d, new leader: [%v], prev leader: [%v]",
				rng.GetId(), rng.Term, req.GetTerm(), req.GetLeader(), rng.GetLeader())
		}
	}

	if r.GetRangeEpoch().GetVersion() > rng.GetRangeEpoch().GetVersion() {
		saveCache = true
		saveStore = true
		log.Info("range[%d] version change from %d to %d",
			rng.GetId(), rng.GetRangeEpoch().GetVersion(), r.GetRangeEpoch().GetVersion())
	}
	if r.GetRangeEpoch().GetConfVer() > rng.GetRangeEpoch().GetConfVer() {
		saveCache = true
		saveStore = true
		log.Info("range %d conf version change from %d to %d",
			rng.GetId(), rng.GetRangeEpoch().GetConfVer(), r.GetRangeEpoch().GetConfVer())
	}
	if req.GetLeader().GetNodeId() != rng.GetLeader().GetNodeId() {
		log.Info("range[%d] leader change from Node: %d to Node: %d", rng.GetId(),
			rng.GetLeader().GetNodeId(), req.GetLeader().GetNodeId())
		saveCache = true
	}
	// 有down peer 或者 pending peer
	for _, status := range req.GetPeersStatus() {
		if status.GetSnapshotting() || status.GetDownSeconds() > 0 {
			saveCache = true
		}
	}
	// 之前有down peer 或者 pending peer
	if len(rng.GetDownPeers()) > 0 || len(rng.GetPendingPeers()) > 0 {
		saveCache = true
	}
	// 检查有learner提升
	for _, peer := range r.GetPeers() {
		oldPeer := rng.GetPeer(peer.GetId())
		if oldPeer == nil || oldPeer.Type != peer.Type {
			saveCache = true
			saveStore = true
		}
	}

	if saveStore {
		err := cluster.storeRange(r)
		if err != nil {
			log.Error("store range[%s] failed, err[%v]", rng.SString(), err)
			return
		}
		saveCache = true
	}
	if saveCache {
		// 更新node的分片副本信息
		for _, p := range rng.GetPeers() {
			find := false
			for _, peer := range r.GetPeers() {
				if p.GetId() == peer.GetId() {
					find = true
					break
				}
			}
			if !find {
				cluster.FindNodeById(p.GetNodeId()).DeleteRange(rng.GetId())
			}
		}

		rng.Range = r
		rng.Term = req.GetTerm()
		rng.PeersStatus = req.GetPeersStatus()
		rng.Leader = req.GetLeader()

		cluster.AddRange(rng)
	}
	rng.LastHbTimeTS = time.Now()
	cluster.updateStatus(rng, req.GetStats())
	// 暂时不能参与任务调度
	if table != nil && table.Status == metapb.TableStatus_TableInit {
		return
	}
	task := cluster.Dispatch(rng)
	if task != nil {
		if rng.Trace || log.IsEnableInfo() {
			log.Info("[HB] range[%s] dispatch task[%v]", rng.SString(), task)
		}
		resp.Task = task
	}
	return
}

func (service *Server) rangePeerIndexAlarm(req *mspb.RangeHeartbeatRequest) {
	clusterId := service.cluster.clusterId
	peerStatuses := req.GetPeersStatus()
	if peerStatuses == nil {
		log.Warn("peer_status of range[%d] is nil.", req.GetRange().GetId())
		return
	}
	var leaderIndex uint64
	for _, peerStatus := range peerStatuses {
		if peerStatus.GetPeer().GetId() == req.GetLeader().GetId() {
			leaderIndex = peerStatus.GetIndex()
			break
		}
	}

	leaderAddr := service.cluster.FindNodeById(req.GetLeader().GetNodeId()).GetServerAddr()
	ruleName := alarm2.RANGE_PEER_INDEX
	compareType := alarmpb2.AlarmValueCompareType_NOT_LESS_THAN

	for _, peerStatus := range peerStatuses {
		if peerStatus.GetPeer().GetId() != req.GetLeader().GetId() {
			peerAddr := service.cluster.FindNodeById(peerStatus.GetPeer().GetNodeId()).GetServerAddr()
			remark := []string{fmt.Sprintf("[clusterId=%d, rangeId=%d], leader[nodeAddr=%s, index=%d], peer[peerAddr=%s, index=%d]",
				clusterId, req.GetRange().GetId(), leaderAddr, leaderIndex, peerAddr, peerStatus.GetIndex())}

			diff := float64(leaderIndex - peerStatus.GetIndex())
			err := service.alarmClient.RuleAlarm(int64(clusterId), leaderAddr, "master-server", ruleName, diff, compareType, remark)
			if err != nil {
				log.Warn("run rule alarm error in range peer index check, err:%v", err)
			}
		}
	}

}

func (service *Server) handleAskSplit(ctx context.Context, req *mspb.AskSplitRequest) (resp *mspb.AskSplitResponse, err error) {
	cluster := service.cluster
	//集群不允许分裂
	if !req.GetForce() && cluster.autoSplitUnable {
		log.Debug("cluster is not allowed split")
		err = ErrNotAllowSplit
		return
	}
	// 标记删除的table拒绝分片分裂
	table, find := cluster.FindTableById(req.GetRange().GetTableId())
	if !find {
		log.Warn("table[%d] not found", req.GetRange().GetTableId())
		err = ErrNotExistTable
		return
	}
	// 不支持分裂
	if table.Status != metapb.TableStatus_TableRunning {
		log.Warn("table[%d] not allow split", req.GetRange().GetTableId())
		err = ErrNotAllowSplit
		return
	}
	rng := cluster.FindRange(req.GetRange().GetId())
	if rng == nil {
		log.Warn("range[%d] not found", req.GetRange().GetId())
		err = ErrNotExistRange
		return
	}
	// 检查元数据
	rm := rng.Range
	rd := req.GetRange()
	if rm.GetRangeEpoch().GetConfVer() == rd.GetRangeEpoch().GetConfVer() &&
		rm.GetRangeEpoch().GetVersion() == rd.GetRangeEpoch().GetVersion() {
		if bytes.Compare(rm.GetStartKey(), rd.GetStartKey()) != 0 || bytes.Compare(rm.GetEndKey(), rd.GetEndKey()) != 0 {
			log.Error("range[%v] meta abnormal %v", rm, rd)
			err = ErrRangeMetaConflict
			return
		}
	} else if rm.GetRangeEpoch().GetVersion() != rd.GetRangeEpoch().GetVersion() {
		if bytes.Compare(rm.GetEndKey(), rd.GetEndKey()) == 0 {
			log.Error("range[%v] meta abnormal %v", rm, rd)
			err = ErrRangeMetaConflict
			return
		}
	}
	newRangeId, _err := cluster.GenId()
	if _err != nil {
		err = _err
		return
	}
	var peerIds []uint64
	for range req.GetRange().GetPeers() {
		id, _err := cluster.GenId()
		if _err != nil {
			err = _err
			return
		}
		peerIds = append(peerIds, id)
	}
	resp = &mspb.AskSplitResponse{
		Header:     &mspb.ResponseHeader{},
		NewRangeId: newRangeId,
		Range:      req.GetRange(),
		NewPeerIds: peerIds,
		SplitKey:   req.GetSplitKey()}
	log.Info("range %d ask split[%v] success, new range[%d]",
		req.GetRange().GetId(), req.GetSplitKey(), newRangeId)
	return
}

func (service *Server) handleReportSplit(ctx context.Context, req *mspb.ReportSplitRequest) (resp *mspb.ReportSplitResponse, err error) {
	left := req.GetLeft()
	right := req.GetRight()
	if !bytes.Equal(left.GetEndKey(), right.GetStartKey()) {
		err = errors.New("invalid split range")
		log.Error("invalid split range %v %v", left, right)
		return
	}

	if len(right.GetEndKey()) == 0 || bytes.Compare(left.GetStartKey(), right.GetEndKey()) < 0 {
		// TODO metrics
	} else {
		err = errors.New("invalid split range")
		log.Error("invalid split range %v %v", left, right)
		return
	}
	resp = &mspb.ReportSplitResponse{Header: &mspb.ResponseHeader{}}
	return
}

func (service *Server) handleNodeLogin(ctx context.Context, req *mspb.NodeLoginRequest) (*mspb.NodeLoginResponse, error) {
	err := service.cluster.NodeLogin(req.GetNodeId())
	if err != nil {
		log.Error("node %d login err %v", req.GetNodeId(), err)
		return nil, err
	}
	resp := &mspb.NodeLoginResponse{
		Header: &mspb.ResponseHeader{},
	}
	return resp, nil
}

func (service *Server) handleGetMsLeader(ctx context.Context, req *mspb.GetMSLeaderRequest) (resp *mspb.GetMSLeaderResponse, err error) {
	resp = &mspb.GetMSLeaderResponse{Header: &mspb.ResponseHeader{}}
	point := service.GetLeader()
	if point == nil {
		err = errors.New("no leader at this time")
		log.Error(err.Error())
		return nil, err
	}
	resp.Leader = &mspb.MSLeader{
		Id:      point.ID,
		Address: point.RpcServerAddr,
	}

	return
}

func (service *Server) handleGetDb(ctx context.Context, req *mspb.GetDBRequest) (resp *mspb.GetDBResponse, err error) {
	resp = new(mspb.GetDBResponse)
	resp.Header = &mspb.ResponseHeader{}
	dbname := req.GetName()
	if dbname == "" {
		return nil, ErrInvalidParam
	}
	db, ok := service.cluster.FindDatabase(dbname)
	if !ok {
		log.Error("invalid database[%s]", dbname)
		return nil, ErrNotExistDatabase
	}
	resp.Db = db.DataBase

	return
}

func (service *Server) handleGetTable(ctx context.Context, req *mspb.GetTableRequest) (resp *mspb.GetTableResponse, err error) {
	resp = new(mspb.GetTableResponse)
	resp.Header = &mspb.ResponseHeader{}
	dbName := req.GetDbName()
	tname := req.GetTableName()

	if dbName == "" || tname == "" {
		return nil, ErrInvalidParam
	}
	d, ok := service.cluster.FindDatabase(dbName)
	if !ok {
		log.Error("invalid database[%s]", dbName)
		return
	}
	t, ok := d.FindTable(tname)
	if !ok {
		log.Error("invalid table[%s:%s]", d.GetName(), tname)
		return
	}
	if t.Status != metapb.TableStatus_TableRunning {
		log.Error("table[%s:%s] do not works", d.GetName(), tname)
		return
	}
	table := deepcopy.Iface(t.Table).(*metapb.Table)
	resp.Table = table

	return
}

func (service *Server) handleGetTableById(ctx context.Context, req *mspb.GetTableByIdRequest) (resp *mspb.GetTableByIdResponse, err error) {
	resp = new(mspb.GetTableByIdResponse)
	resp.Header = &mspb.ResponseHeader{}
	dbId := req.GetDbId()
	tId := req.GetTableId()

	if dbId == 0 || tId == 0 {
		return nil, ErrInvalidParam
	}
	d, ok := service.cluster.FindDatabaseById(dbId)
	if !ok {
		log.Error("invalid database[%d]", dbId)
		return nil, ErrNotExistDatabase
	}
	t, ok := d.FindTableById(tId)
	if !ok {
		log.Error("invalid table[%s:%d]", d.GetName(), tId)
		return nil, ErrNotExistTable
	}
	if t.Status != metapb.TableStatus_TableRunning {
		log.Error("table[%s:%s] do not works", d.GetName(), t.GetName())
		return
	}
	table := deepcopy.Iface(t.Table).(*metapb.Table)
	resp.Table = table

	return
}

func (service *Server) handleGetColumns(ctx context.Context, req *mspb.GetColumnsRequest) (resp *mspb.GetColumnsResponse, err error) {
	resp = new(mspb.GetColumnsResponse)
	resp.Header = &mspb.ResponseHeader{}
	dbId := req.GetDbId()
	tId := req.GetTableId()

	if dbId == 0 || tId == 0 {
		return nil, errors.New("parameter is nil")
	}
	t, ok := service.cluster.FindTableById(tId)
	if !ok {
		return nil, errors.New("table is not existed")
	}
	resp.Columns = t.GetColumns()

	return
}

func (service *Server) handleAddColumns(ctx context.Context, req *mspb.AddColumnRequest) (resp *mspb.AddColumnResponse, err error) {
	resp = new(mspb.AddColumnResponse)
	resp.Header = &mspb.ResponseHeader{}
	dbId := req.GetDbId()
	tId := req.GetTableId()
	columns := req.GetColumns()

	if dbId == 0 || tId == 0 || columns == nil || len(columns) == 0 {
		return nil, errors.New("parameter is nil")
	}
	t, ok := service.cluster.FindTableById(tId)
	if !ok {
		return nil, errors.New("table is not existed")
	}

	cols, err := t.UpdateSchema(columns, service.cluster)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("column add err %s", err.Error()))
	}
	resp.Columns = cols

	return
}

func (service *Server) handleGetColumnByName(ctx context.Context, req *mspb.GetColumnByNameRequest) (resp *mspb.GetColumnByNameResponse, err error) {
	resp = new(mspb.GetColumnByNameResponse)
	resp.Header = &mspb.ResponseHeader{}
	dbId := req.GetDbId()
	tId := req.GetTableId()
	name := req.GetColName()

	if dbId == 0 || tId == 0 || name == "" {
		return nil, errors.New("parameter is nil")
	}
	t, ok := service.cluster.FindTableById(tId)
	if !ok {
		return nil, ErrNotExistTable
	}
	c, ok := t.GetColumnByName(name)
	if !ok {
		return nil, errors.New("column is not existed")
	}
	resp.Column = c
	return
}

func (service *Server) handleGetColumnById(ctx context.Context, req *mspb.GetColumnByIdRequest) (resp *mspb.GetColumnByIdResponse, err error) {
	resp = new(mspb.GetColumnByIdResponse)
	resp.Header = &mspb.ResponseHeader{}
	dbId := req.GetDbId()
	tId := req.GetTableId()
	id := req.GetColId()

	if dbId == 0 || tId == 0 {
		return nil, errors.New("parameter is nil")
	}
	t, ok := service.cluster.FindTableById(tId)
	if !ok {
		return nil, errors.New("table is not existed")
	}
	c, ok := t.GetColumnById(id)
	if !ok {
		return nil, errors.New("column is not existed")
	}
	resp.Column = c
	return
}

func (service *Server) handleGetNode(ctx context.Context, req *mspb.GetNodeRequest) (resp *mspb.GetNodeResponse, err error) {
	resp = new(mspb.GetNodeResponse)
	resp.Header = &mspb.ResponseHeader{}
	id := req.GetId()

	n := service.cluster.FindNodeById(id)
	if n == nil {
		return nil, ErrNotExistNode
	}
	node := deepcopy.Iface(n.Node).(*metapb.Node)
	resp.Node = node
	return
}

func (service *Server) handleGetNodeId(ctx context.Context, req *mspb.GetNodeIdRequest) (resp *mspb.GetNodeIdResponse, err error) {
	if client, ok := peer.FromContext(ctx); ok {
		ip, _, err := net.SplitHostPort(client.Addr.String())
		if err != nil {
			return nil, err
		}
		serverAddr := fmt.Sprintf("%s:%d", ip, req.GetServerPort())
		raftAddr := fmt.Sprintf("%s:%d", ip, req.GetRaftPort())
		adminAddr := fmt.Sprintf("%s:%d", ip, req.GetAdminPort())
		node, cleanUp, err := service.cluster.GetNodeId(serverAddr, raftAddr, adminAddr, req.GetVersion())
		if err != nil {
			return nil, err
		}

		if cleanUp {
			log.Warn("node[%d] login out, we need clearup residual data firstly", node.GetId())
		}

		resp := &mspb.GetNodeIdResponse{
			Header:  &mspb.ResponseHeader{},
			NodeId:  node.GetId(),
			Clearup: cleanUp,
		}
		return resp, nil
	}
	return nil, errors.New("invalid grpc!!!!")
}

func (service *Server) handleCreateDatabase(ctx context.Context, req *mspb.CreateDatabaseRequest) (resp *mspb.CreateDatabaseResponse, err error) {
	_, err = service.cluster.CreateDatabase(req.GetDbName(), "")
	if err != nil {
		return
	}
	resp = new(mspb.CreateDatabaseResponse)
	resp.Header = &mspb.ResponseHeader{}
	return
}

func (service *Server) handleCreateTable(ctx context.Context, req *mspb.CreateTableRequest) (resp *mspb.CreateTableResponse, err error) {
	resp = new(mspb.CreateTableResponse)
	resp.Header = &mspb.ResponseHeader{}

	columns, regxs, err := ParseProperties(req.GetProperties())
	if err != nil {
		log.Error("parse cols[%s] failed, err[%v]", req.GetProperties(), err)
		err = errors.New("invalid properties")
		return
	}
	if _, err = service.cluster.CreateTable(req.GetDbName(), req.GetTableName(), columns, regxs, false, nil); err != nil {
		log.Error("http sql table create : %v", err)
		return
	}
	log.Info("create table[%s:%s] success", req.GetDbName(), req.GetTableName())
	return
}

func (service *Server) handleAutoIncId(ctx context.Context, req *mspb.GetAutoIncIdRequest) (resp *mspb.GetAutoIncIdResponse, err error) {
	cluster := service.cluster
	ids := make([]uint64, 0)
	if table, find := cluster.FindTableById(req.GetTableId()); find {
		if table.Status != metapb.TableStatus_TableRunning {
			log.Warn("table[%d] not ready for work", req.GetTableId())
			err = ErrNotExistTable
			return
		}
		size := req.GetSize_()
		if size == 0 {
			size = 1
		}
		ids, err = table.GetAutoIncId(service.store, size)
		if err != nil {
			return nil, err
		}
	} else {
		log.Warn("get auto_increment id: table %d not found", req.GetTableId())
		err = ErrNotExistTable
		return
	}
	resp = new(mspb.GetAutoIncIdResponse)
	resp.Header = &mspb.ResponseHeader{}
	resp.Ids = ids
	return
}
