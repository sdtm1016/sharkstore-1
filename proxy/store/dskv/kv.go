package dskv

import (
	"errors"
	"fmt"
	"time"

	"model/pkg/errorpb"
	"model/pkg/kvrpcpb"
	"model/pkg/metapb"
	"model/pkg/timestamp"
	"pkg-go/ds_client"
	"util/hlc"
	"util/log"

	"golang.org/x/net/context"
)

type KvProxy struct {
	Cli          client.KvClient
	Clock        *hlc.Clock
	RangeCache   *RangeCache
	WriteTimeout time.Duration
	ReadTimeout  time.Duration
}

func (p *KvProxy) Init(cli client.KvClient, clock *hlc.Clock, cache *RangeCache, wTimeout, rTimeout time.Duration) {
	p.Cli = cli
	p.Clock = clock
	p.RangeCache = cache
	p.WriteTimeout = wTimeout
	p.ReadTimeout = rTimeout
}

func (p *KvProxy) Reset() {
	*p = KvProxy{}
}

func (p *KvProxy) send(bo *Backoffer, _ctx *Context, req *Request) (resp *Response, retry bool, err error) {
	resp = &Response{Type: req.GetType()}
	ctx, cancel := context.WithTimeout(bo.ctx, _ctx.Timeout)
	defer cancel()
	addr := _ctx.NodeAddr
	switch req.GetType() {
	case Type_Lock:
		_resp, _err := p.Cli.Lock(ctx, addr, req.GetLockReq())
		if _err != nil {
			err = _err
			goto Err
		}
		resp.LockResp = _resp
	case Type_LockUpdate:
		_resp, _err := p.Cli.LockUpdate(ctx, addr, req.GetLockUpdateReq())
		if _err != nil {
			err = _err
			goto Err
		}
		resp.LockUpdateResp = _resp
	case Type_Unlock:
		_resp, _err := p.Cli.Unlock(ctx, addr, req.GetUnlockReq())
		if _err != nil {
			err = _err
			goto Err
		}
		resp.UnlockResp = _resp
	case Type_UnlockForce:
		_resp, _err := p.Cli.UnlockForce(ctx, addr, req.GetUnlockForceReq())
		if _err != nil {
			err = _err
			goto Err
		}
		resp.UnlockForceResp = _resp
	case Type_RawPut:
		_resp, _err := p.Cli.RawPut(ctx, addr, req.GetRawPutReq())
		if _err != nil {
			err = _err
			goto Err
		}
		resp.RawPutResp = _resp
	case Type_RawGet:
		_resp, _err := p.Cli.RawGet(ctx, addr, req.GetRawGetReq())
		if _err != nil {
			err = _err
			goto Err
		}
		resp.RawGetResp = _resp
	case Type_RawDelete:
		_resp, _err := p.Cli.RawDelete(ctx, addr, req.GetRawDeleteReq())
		if _err != nil {
			err = _err
			goto Err
		}
		resp.RawDeleteResp = _resp
	case Type_Insert:
		_resp, _err := p.Cli.Insert(ctx, addr, req.GetInsertReq())
		if _err != nil {
			err = _err
			goto Err
		}
		resp.InsertResp = _resp
	case Type_Select:
		_resp, _err := p.Cli.Select(ctx, addr, req.GetSelectReq())
		if _err != nil {
			err = _err
			goto Err
		}
		resp.SelectResp = _resp
	case Type_Delete:
		_resp, _err := p.Cli.Delete(ctx, addr, req.GetDeleteReq())
		if _err != nil {
			err = _err
			goto Err
		}
		resp.DeleteResp = _resp
	case Type_KvSet:
		_resp, _err := p.Cli.KvSet(ctx, addr, req.GetKvSetReq())
		if _err != nil {
			err = _err
			goto Err
		}
		resp.KvSetResp = _resp
	case Type_KvBatchSet:
		_resp, _err := p.Cli.KvBatchSet(ctx, addr, req.GetKvBatchSetReq())
		if _err != nil {
			err = _err
			goto Err
		}
		resp.KvBatchSetResp = _resp
	case Type_KvGet:
		_resp, _err := p.Cli.KvGet(ctx, addr, req.GetKvGetReq())
		if _err != nil {
			err = _err
			goto Err
		}
		resp.KvGetResp = _resp
	case Type_KvBatchGet:
		_resp, _err := p.Cli.KvBatchGet(ctx, addr, req.GetKvBatchGetReq())
		if _err != nil {
			err = _err
			goto Err
		}
		resp.KvBatchGetResp = _resp
	case Type_KvScan:
		_resp, _err := p.Cli.KvScan(ctx, addr, req.GetKvScanReq())
		if _err != nil {
			err = _err
			goto Err
		}
		resp.KvScanResp = _resp
	case Type_KvDelete:
		_resp, _err := p.Cli.KvDelete(ctx, addr, req.GetKvDeleteReq())
		if _err != nil {
			err = _err
			goto Err
		}
		resp.KvDeleteResp = _resp
	case Type_KvBatchDel:
		_resp, _err := p.Cli.KvBatchDelete(ctx, addr, req.GetKvBatchDelReq())
		if _err != nil {
			err = _err
			goto Err
		}
		resp.KvBatchDelResp = _resp
	case Type_KvRangeDel:
		_resp, _err := p.Cli.KvRangeDelete(ctx, addr, req.GetKvRangeDelReq())
		if _err != nil {
			err = _err
			goto Err
		}
		resp.KvRangeDelResp = _resp
	default:
		return nil, false, ErrInternalError
	}
	return resp, false, nil

Err:
	if err != nil {
		log.Error("send %s to %s error:%v, range=%v", req.Type.String(), addr, err, _ctx.RequestHeader.GetRangeId())
		// 链路有问题,有可能是节点故障了，需要重新拉去
		if e := p.doSendFail(bo, _ctx, err); e != nil {
			return nil, false, e
		}
		return nil, true, nil
	}
	return
}

func (p *KvProxy) prepare(location *KeyLocation, req *Request) (time.Duration, *kvrpcpb.RequestHeader, error) {
	var header *kvrpcpb.RequestHeader
	var timeout time.Duration
	now := p.Clock.Now()
	switch req.Type {
	case Type_RawPut:
		header = req.RawPutReq.GetHeader()
		timeout = client.ReadTimeoutShort
	case Type_RawGet:
		header = req.RawGetReq.GetHeader()
		timeout = client.ReadTimeoutShort
	case Type_RawDelete:
		header = req.RawDeleteReq.GetHeader()
		timeout = client.ReadTimeoutShort

	case Type_Insert:
		header = req.InsertReq.GetHeader()
		timeout = client.ReadTimeoutShort
	case Type_Select:
		header = req.SelectReq.GetHeader()
		timeout = client.ReadTimeoutMedium
	case Type_Delete:
		header = req.DeleteReq.GetHeader()
		timeout = client.ReadTimeoutMedium

	case Type_Lock:
		header = req.LockReq.GetHeader()
		timeout = client.ReadTimeoutShort
	case Type_LockUpdate:
		header = req.LockUpdateReq.GetHeader()
		timeout = client.ReadTimeoutShort
	case Type_Unlock:
		header = req.UnlockReq.GetHeader()
		timeout = client.ReadTimeoutShort
	case Type_UnlockForce:
		header = req.UnlockForceReq.GetHeader()
		timeout = client.ReadTimeoutShort
	case Type_LockScan:
		header = req.LockScanReq.GetHeader()
		timeout = client.ReadTimeoutShort

	case Type_KvSet:
		header = req.KvSetReq.GetHeader()
		timeout = client.ReadTimeoutShort
	case Type_KvBatchSet:
		header = req.KvBatchSetReq.GetHeader()
		timeout = client.ReadTimeoutShort
	case Type_KvGet:
		header = req.KvGetReq.GetHeader()
		timeout = client.ReadTimeoutShort
	case Type_KvBatchGet:
		header = req.KvBatchGetReq.GetHeader()
		timeout = client.ReadTimeoutMedium
	case Type_KvScan:
		header = req.KvScanReq.GetHeader()
		timeout = client.ReadTimeoutMedium
	case Type_KvDelete:
		header = req.KvDeleteReq.GetHeader()
		timeout = client.ReadTimeoutShort
	case Type_KvBatchDel:
		header = req.KvBatchDelReq.GetHeader()
		timeout = client.ReadTimeoutShort
	case Type_KvRangeDel:
		header = req.KvRangeDelReq.GetHeader()
		timeout = client.ReadTimeoutShort
	default:
		return timeout, header, fmt.Errorf("invalid request type %s", req.Type.String())
	}
	header.RangeId = location.Region.Id
	header.RangeEpoch = &metapb.RangeEpoch{ConfVer: location.Region.ConfVer, Version: location.Region.Cer}
	header.Timestamp = &timestamp.Timestamp{WallTime: now.WallTime, Logical: now.Logical}
	return timeout, header, nil
}

func (p *KvProxy) doSendFail(bo *Backoffer, ctx *Context, err error) error {
	if err == client.ErrNetworkIO {
		select {
		case <-bo.ctx.Done():
			return err
		default:
			// If we don't cancel, but the error code is Canceled, it must be from grpc remote.
			// This may happen when ds is killed and exiting.
			// Backoff and retry in this case.
			log.Warn("receive a rpc cancel signal from remote, err=%v", err)
		}
	}
	p.RangeCache.OnRequestFail(ctx.VID, ctx.NodeId, err)

	// Retry on request failure when it's not canceled.
	// When a ds is not available, the leader of related region should be elected quickly.
	// TODO: the number of retry time should be limited: since region may be unavailable
	// when some unrecoverable disaster happened.
	err = bo.Backoff(BoKVRPC, fmt.Errorf("send ds request error: %v, ctx: %s, try next peer later", err, ctx.RequestHeader))
	return err
}

func (p *KvProxy) do(bo *Backoffer, req *Request, key []byte) (resp *Response, l *KeyLocation, err error) {
	var addr string
	var timeout time.Duration
	var reqHeader *kvrpcpb.RequestHeader
	//var pErr *errorpb.Error

	l, err = p.RangeCache.LocateKey(bo, key)
	if err != nil {
		log.Error("locate key=%v failed, err=%v", key, err)
		return
	}

	readNodeId := l.NodeId
	if req.Type == Type_Select {
		//metapbTable, _ := p.RangeCache.msClient.GetTableById(p.RangeCache.dbId, p.RangeCache.tableId)
		if req.SelectReq != nil && req.SelectReq.Header != nil && req.SelectReq.Header.ReadIndex > 0 {
			foundRange := p.RangeCache.getRegionByIDFromCache(l.Region.Id)
			if foundRange != nil {
				if foundRange.reader != nil && foundRange.reader.NodeId > 0 {
					readNodeId = foundRange.reader.NodeId
				} else {
					for _, _p := range foundRange.meta.Peers {
						if _p.NodeId != l.NodeId {
							foundRange.reader = _p
							readNodeId = _p.NodeId
							break
						}
					}
				}

				for _, peer_status := range foundRange.peerStatus {
					if foundRange.reader.Id == peer_status.Peer.Id {
						// invalid follower read_index, switch back to leader
						readIndex := peer_status.Applied
						if readIndex <= 0 {
							readNodeId = l.NodeId
							p.RangeCache.DropRegion(foundRange.VerID())
						}
						// todo the period of updating peer index
						req.SelectReq.Header.ReadIndex = readIndex
						break
					}
				}
			}
		}
	}

	addr, err = p.RangeCache.GetNodeAddr(bo, readNodeId)
	if err != nil {
		log.Error("locate node=%d failed, err=%v", readNodeId, err)
		return
	}
	log.Debug("send request key: %v, addr: %v", key, addr)
	timeout, reqHeader, err = p.prepare(l, req)
	if err != nil {
		log.Error("prepare request[%v] failed, err=%v", req, err)
		return
	}
	metricSend := time.Now().UnixNano()
	ctx := &Context{VID: l.Region, NodeId: readNodeId, NodeAddr: addr, RequestHeader: reqHeader, Timeout: timeout}
	resp, err = p.sendReq(bo, ctx, req)
	sendDelay := (time.Now().UnixNano() - metricSend) / int64(time.Millisecond)
	if sendDelay <= 50 {
		// do nothing
	} else if sendDelay <= 200 {
		log.Info("request to %s type:%s execute time %d ms, msg:%d rangeId:%d", addr, req.GetType().String(), sendDelay, reqHeader.TraceId, l.Region.Id)
	} else if sendDelay <= 500 {
		log.Warn("request to %s type:%s execute time %d ms, msg:%d rangeId:%d", addr, req.GetType().String(), sendDelay,  reqHeader.TraceId, l.Region.Id)
	} else {
		log.Error("request to %s type:%s execute time %d ms, msg:%d rangeId:%d", addr, req.GetType().String(), sendDelay, reqHeader.TraceId, l.Region.Id)
	}

	if err != nil {
		log.Error("send failed, ctx %v, err %v", ctx, err)
		return
	}

	return
}

func (p *KvProxy) sendReq(bo *Backoffer, ctx *Context, req *Request) (resp *Response, err error) {
	var retry bool
	var pErr *errorpb.Error
	var staleIndexErrTimes = 0
	for {
		resp, retry, err = p.send(bo, ctx, req)
		if err != nil {
			log.Error("send failed, ctx %v, err %v", ctx, err)
			return
		}
		if retry {
			log.Warn("will retry %s %s", ctx.NodeAddr, req.Type.String())
			continue
		}

		pErr, err = resp.GetErr()
		if err != nil {
			return
		}
		if pErr != nil {
			retry, err = p.doRangeError(bo, pErr, ctx, &staleIndexErrTimes)
			if err != nil {
				return
			}
			if retry {
				log.Warn("will retry %s %s %s", ctx.NodeAddr, req.Type.String(), pErr.GetMessage())
				continue
			}
		}

		return
	}
}

func (p *KvProxy) doRangeError(bo *Backoffer, rangeErr *errorpb.Error, ctx *Context, times *int) (retry bool, err error) {
	if rangeErr.GetNotLeader() != nil {
		notLeader := rangeErr.GetNotLeader()
		log.Warn("range leader changed, ctx: %s, old leader[%s], new leader %v", ctx.RequestHeader.String(), ctx.NodeAddr, notLeader.GetLeader().GetNodeId())
		// no leader
		if notLeader.GetLeader() == nil {
			err = bo.Backoff(BoRangeMiss, fmt.Errorf("no leader: %v, range: %d", notLeader, ctx.VID.Id))
			if err != nil {
				//可能gateway没有拿到最新的拓扑
				p.RangeCache.DropRegion(ctx.VID)
				return false, ErrRouteChange
			} else {
				return true, nil
			}
		} else {
			// update leader
			p.RangeCache.UpdateLeader(ctx.VID, notLeader.GetLeader().GetNodeId())
			var addr string
			addr, err = p.RangeCache.GetNodeAddr(bo, notLeader.GetLeader().GetNodeId())
			if err != nil {
				log.Error("locate node=%d failed, err=%v", notLeader.GetLeader().GetNodeId(), err)
				return
			}
			ctx.NodeAddr = addr
			ctx.NodeId = notLeader.GetLeader().GetNodeId()
		}

		//retry = true
		return false, ErrRouteChange
	}

	if staleEpoch := rangeErr.GetStaleEpoch(); staleEpoch != nil {
		log.Warn("ds reports `StaleEpoch`, ctx: %s, [%d %d], retry later %s",
			ctx.RequestHeader.String(), staleEpoch.GetOldRange().GetId(), staleEpoch.GetNewRange().GetId(), ctx.NodeAddr)
		var ranges []*metapb.Range
		if staleEpoch.GetOldRange() != nil {
			ranges = append(ranges, staleEpoch.GetOldRange())
		}
		if staleEpoch.GetNewRange() != nil {
			ranges = append(ranges, staleEpoch.GetNewRange())
		}
		if len(ranges) != 2 {
			log.Error("DS bug for stale epoch, ctx: %s, %s", ctx.RequestHeader.String(), ctx.NodeAddr)
		}
		err = p.RangeCache.OnRegionStale(ctx, ranges)
		if err != nil {
			return false, err
		}
		return false, ErrRouteChange
	}
	if rangeErr.GetServerIsBusy() != nil {
		log.Warn("ds reports `ServerIsBusy`, reason: %s, ctx: %s, retry later %s",
			rangeErr.GetServerIsBusy().GetReason(), ctx.RequestHeader.String(), ctx.NodeAddr)
		err = bo.Backoff(BoServerBusy, ErrServerBusy)
		if err != nil {
			return false, err
		}
		return true, nil
	}
	if rangeErr.GetStaleCommand() != nil {
		log.Warn("ds reports `StaleCommand`, ctx: %s %s", ctx.RequestHeader.String(), ctx.NodeAddr)
		return true, nil
	}
	if rangeErr.GetEntryTooLarge() != nil {
		log.Warn("ds reports `RaftEntryTooLarge`, ctx: %s %s", ctx.RequestHeader.String(), ctx.NodeAddr)
		return false, errors.New(rangeErr.String())
	}
	if rangeErr.GetStaleReadIndex() != nil {
		readIndex := rangeErr.GetStaleReadIndex().GetReadIndex()
		replicaIndex := rangeErr.StaleReadIndex.GetReplicaIndex()
		log.Warn("ds reports peer_read_index are stale[read_index=%d, replica_index=%d], ctx: %s %s",
			readIndex, replicaIndex, ctx.RequestHeader.String(), ctx.NodeAddr)

		if *times > 0 {
			p.RangeCache.DropRegion(ctx.VID)
			return false, errors.New(fmt.Sprintf("stale_read_index, read_index: %d replica_index: %d", readIndex, replicaIndex))
		}

		rng := p.RangeCache.getRegionByIDFromCache(ctx.RequestHeader.RangeId)
		for _, peer := range rng.meta.Peers {
			if peer.NodeId != rng.Leader().NodeId && peer.NodeId != rng.reader.NodeId {
				rng.reader = peer
				for _, peer_status := range rng.peerStatus {
					if peer.Id == peer_status.Peer.Id {
						ctx.RequestHeader.ReadIndex = peer_status.Applied
						break
					}
				}
				break
			}
		}
		newNodeId:= rng.reader.NodeId
		newAddr, err := p.RangeCache.GetNodeAddr(bo, newNodeId)
		if err != nil {
			log.Warn("get node[id=%d] addr err in rangeErr.GetStaleReadIndex", newNodeId)
			return false, err
		}
		ctx.NodeAddr = newAddr
		*times++
		return true, nil
	}
	// For other errors, we only drop cache here.
	// Because caller may need to re-split the request.
	log.Warn("ds reports range error: %s, ctx: %s %s", rangeErr, ctx.RequestHeader.String(), ctx.NodeAddr)
	p.RangeCache.DropRegion(ctx.VID)
	return false, nil
}
