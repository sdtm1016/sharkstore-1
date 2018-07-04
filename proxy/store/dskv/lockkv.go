package dskv

import (
	"model/pkg/kvrpcpb"
	"context"
)

func (p *KvProxy) Lock(req *kvrpcpb.LockRequest) (*kvrpcpb.LockResponse, error) {
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_Lock
	in.LockReq = &kvrpcpb.DsLockRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}

	bo := NewBackoffer(RawkvMaxBackoff, context.Background())
	resp, _, err := p.do(bo, in, req.GetKey())
	if err != nil {
		return nil, err
	}
	return resp.GetLockResp().GetResp(), nil
}
func (p *KvProxy) LockUpdate(req *kvrpcpb.LockUpdateRequest) (*kvrpcpb.LockResponse, error) {
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_LockUpdate
	in.LockUpdateReq = &kvrpcpb.DsLockUpdateRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}

	bo := NewBackoffer(RawkvMaxBackoff, context.Background())
	resp, _, err := p.do(bo, in, req.GetKey())
	if err != nil {
		return nil, err
	}
	return resp.GetLockUpdateResp().GetResp(), nil
}
func (p *KvProxy) Unlock(req *kvrpcpb.UnlockRequest) (*kvrpcpb.LockResponse, error) {
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_Unlock
	in.UnlockReq = &kvrpcpb.DsUnlockRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}

	bo := NewBackoffer(RawkvMaxBackoff, context.Background())
	resp, _, err := p.do(bo, in, req.GetKey())
	if err != nil {
		return nil, err
	}
	return resp.GetUnlockResp().GetResp(), nil
}
func (p *KvProxy) UnlockForce(req *kvrpcpb.UnlockForceRequest) (*kvrpcpb.LockResponse, error) {
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_UnlockForce
	in.UnlockForceReq = &kvrpcpb.DsUnlockForceRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}

	bo := NewBackoffer(RawkvMaxBackoff, context.Background())
	resp, _, err := p.do(bo, in, req.GetKey())
	if err != nil {
		return nil, err
	}
	return resp.GetUnlockForceResp().GetResp(), nil
}

func (p *KvProxy) LockScan(req *kvrpcpb.LockScanRequest) (*kvrpcpb.LockScanResponse, error) {
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_LockScan
	in.LockScanReq = &kvrpcpb.DsLockScanRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}

	bo := NewBackoffer(RawkvMaxBackoff, context.Background())
	resp, _, err := p.do(bo, in, req.GetStart())
	if err != nil {
		return nil, err
	}
	return resp.GetLockScanResp().GetResp(), nil
}

func (p *KvProxy) ConditionUpdate(req *kvrpcpb.LockUpdateRequest) (*kvrpcpb.LockResponse, error) {
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_LockUpdate
	in.LockUpdateReq = &kvrpcpb.DsLockUpdateRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}

	bo := NewBackoffer(RawkvMaxBackoff, context.Background())
	resp, _, err := p.do(bo, in, req.GetKey())
	if err != nil {
		return nil, err
	}
	return resp.GetLockUpdateResp().GetResp(), nil
}