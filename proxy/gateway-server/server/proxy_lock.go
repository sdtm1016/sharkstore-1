package server

import (
	"proxy/store/dskv"
	"model/pkg/kvrpcpb"
	"pkg-go/ds_client"
	"util"
	"util/encoding"
	"util/log"
)

func encodeLockName(prefix uint64, key string) []byte {
	ret := util.EncodeStorePrefix(util.Store_Prefix_KV, prefix)
	ret = encoding.EncodeBytesAscending(ret, []byte(key))
	return ret
}

func decodeLockName(key []byte) []byte {
	if len(key) < 10 {
		log.Error("scan key[%v] len < 10", string(key))
		return nil
	}
	_, k, err := encoding.DecodeBytesAscending(key[9:], nil)
	if err != nil {
		log.Error("decode scan key error: %v", err)
		return nil
	}
	return k
}

func (p *Proxy) Lock(dbName, tableName string, lockName string, userCondition []byte, uuid string, deleteTime int64, userName string) (*kvrpcpb.LockResponse, error) {
	t := p.router.FindTable(dbName, tableName)
	if t == nil {
		return nil, ErrNotExistTable
	}
	req := &kvrpcpb.LockRequest{
		Key:   encodeLockName(t.GetId(), lockName),
		Value: &kvrpcpb.LockValue{
			Value: userCondition,
			Id: uuid,
			DeleteTime: deleteTime,
		},
		By: userName,
	}
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
	resp, err := proxy.Lock(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *Proxy) LockUpdate(dbName, tableName string, lockName string, uuid string, condition []byte) (*kvrpcpb.LockResponse, error) {
	t := p.router.FindTable(dbName, tableName)
	if t == nil {
		return nil, ErrNotExistTable
	}
	req := &kvrpcpb.LockUpdateRequest{
		Key:   encodeLockName(t.GetId(), lockName),
		Id: uuid,
		UpdateValue: condition,
	}
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
	resp, err := proxy.LockUpdate(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *Proxy) Unlock(dbName, tableName string, lockName, uuid, userName string) (*kvrpcpb.LockResponse, error) {
	t := p.router.FindTable(dbName, tableName)
	if t == nil {
		return nil, ErrNotExistTable
	}
	req := &kvrpcpb.UnlockRequest{
		Key:   encodeLockName(t.GetId(), lockName),
		Id: uuid,
		By: userName,
	}
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
	resp, err := proxy.Unlock(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *Proxy) UnlockForce(dbName, tableName string, lockName, userName string) (*kvrpcpb.LockResponse, error) {
	t := p.router.FindTable(dbName, tableName)
	if t == nil {
		return nil, ErrNotExistTable
	}
	req := &kvrpcpb.UnlockForceRequest{
		Key:   encodeLockName(t.GetId(), lockName),
		By: userName,
	}
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
	resp, err := proxy.UnlockForce(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *Proxy) LockScan(dbName, tableName string, startKey, endKey string, number uint32) (*kvrpcpb.LockScanResponse, error) {
	t := p.router.FindTable(dbName, tableName)
	if t == nil {
		return nil, ErrNotExistTable
	}
	req := &kvrpcpb.LockScanRequest{
		Start: encodeLockName(t.GetId(), startKey),
		Limit: encodeLockName(t.GetId(), endKey),
		Count: number,
	}
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutLong)
	resp, err := proxy.LockScan(req)
	if err != nil {
		return nil, err
	}
	// decode key
	for _, k := range resp.GetInfo() {
		k.Key = decodeLockName(k.GetKey())
	}
	resp.LastKey = decodeLockName(resp.GetLastKey())
	return resp, nil
}

func (p *Proxy) UpdateCondition(dbName, tableName string, lockName string, userCondition []byte) (*kvrpcpb.LockResponse, error) {
	t := p.router.FindTable(dbName, tableName)
	if t == nil {
		return nil, ErrNotExistTable
	}
	req := &kvrpcpb.LockUpdateRequest{
		Key:   encodeLockName(t.GetId(), lockName),
		UpdateValue: userCondition,
	}
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
	resp, err := proxy.ConditionUpdate(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}