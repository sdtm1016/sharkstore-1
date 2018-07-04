package client

import (
	"errors"

	"model/pkg/kvrpcpb"
	"util/log"
	"golang.org/x/net/context"
)

// Client is a client that sends RPC.
// It should not be used after calling Close().
type KvClient interface {
	// Close should release all data.
	Close() error
	// SendKVReq sends kv request.
	RawPut(ctx context.Context, addr string, req *kvrpcpb.DsKvRawPutRequest) (*kvrpcpb.DsKvRawPutResponse, error)
	RawGet(ctx context.Context, addr string, req *kvrpcpb.DsKvRawGetRequest) (*kvrpcpb.DsKvRawGetResponse, error)
	RawDelete(ctx context.Context, addr string, req *kvrpcpb.DsKvRawDeleteRequest) (*kvrpcpb.DsKvRawDeleteResponse, error)
	Insert(ctx context.Context, addr string, req *kvrpcpb.DsInsertRequest) (*kvrpcpb.DsInsertResponse, error)
	Select(ctx context.Context, addr string, req *kvrpcpb.DsSelectRequest) (*kvrpcpb.DsSelectResponse, error)
	Delete(ctx context.Context, addr string, req *kvrpcpb.DsDeleteRequest) (*kvrpcpb.DsDeleteResponse, error)

	Lock(ctx context.Context, addr string, req *kvrpcpb.DsLockRequest) (*kvrpcpb.DsLockResponse, error)
	LockUpdate(ctx context.Context, addr string, req *kvrpcpb.DsLockUpdateRequest) (*kvrpcpb.DsLockUpdateResponse, error)
	Unlock(ctx context.Context, addr string, req *kvrpcpb.DsUnlockRequest) (*kvrpcpb.DsUnlockResponse, error)
	UnlockForce(ctx context.Context, addr string, req *kvrpcpb.DsUnlockForceRequest) (*kvrpcpb.DsUnlockForceResponse, error)
	LockScan(ctx context.Context, addr string, req *kvrpcpb.DsLockScanRequest) (*kvrpcpb.DsLockScanResponse, error)

	KvSet(ctx context.Context, addr string, req *kvrpcpb.DsKvSetRequest) (*kvrpcpb.DsKvSetResponse, error)
	KvGet(ctx context.Context, addr string, req *kvrpcpb.DsKvGetRequest) (*kvrpcpb.DsKvGetResponse, error)
	KvBatchSet(ctx context.Context, addr string, req *kvrpcpb.DsKvBatchSetRequest) (*kvrpcpb.DsKvBatchSetResponse, error)
	KvBatchGet(ctx context.Context, addr string, req *kvrpcpb.DsKvBatchGetRequest) (*kvrpcpb.DsKvBatchGetResponse, error)
	KvScan(ctx context.Context, addr string, req *kvrpcpb.DsKvScanRequest) (*kvrpcpb.DsKvScanResponse, error)
	KvDelete(ctx context.Context, addr string, req *kvrpcpb.DsKvDeleteRequest) (*kvrpcpb.DsKvDeleteResponse, error)
	KvBatchDelete(ctx context.Context, addr string, req *kvrpcpb.DsKvBatchDeleteRequest) (*kvrpcpb.DsKvBatchDeleteResponse, error)
	KvRangeDelete(ctx context.Context, addr string, req *kvrpcpb.DsKvRangeDeleteRequest) (*kvrpcpb.DsKvRangeDeleteResponse, error)
}

type KvRpcClient struct {
	pool *ResourcePool
}

func NewRPCClient(opts ...int) KvClient {
	var size int
	if len(opts) == 0 {
		size = DefaultPoolSize
	} else if len(opts) > 1 {
		log.Panic("invalid client param!!!")
		return nil
	} else {
		size = opts[0]
		if size == 0 {
			log.Panic("invalid client param!!!")
			return nil
		}
	}
	return &KvRpcClient{pool: NewResourcePool(size)}
}

func (c *KvRpcClient) Close() error {
	c.pool.Close()
	return nil
}

func (c *KvRpcClient) RawPut(ctx context.Context, addr string, req *kvrpcpb.DsKvRawPutRequest) (*kvrpcpb.DsKvRawPutResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := conn.KvRawPut(ctx, req)
	return resp, err
}

func (c *KvRpcClient) RawGet(ctx context.Context, addr string, req *kvrpcpb.DsKvRawGetRequest) (*kvrpcpb.DsKvRawGetResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := conn.KvRawGet(ctx, req)
	return resp, err
}

func (c *KvRpcClient) RawDelete(ctx context.Context, addr string, req *kvrpcpb.DsKvRawDeleteRequest) (*kvrpcpb.DsKvRawDeleteResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := conn.KvRawDelete(ctx, req)
	return resp, err
}

func (c *KvRpcClient) Insert(ctx context.Context, addr string, req *kvrpcpb.DsInsertRequest) (*kvrpcpb.DsInsertResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := conn.Insert(ctx, req)
	return resp, err
}

func (c *KvRpcClient) Select(ctx context.Context, addr string, req *kvrpcpb.DsSelectRequest) (*kvrpcpb.DsSelectResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := conn.Select(ctx, req)
	return resp, err
}

func (c *KvRpcClient) Delete(ctx context.Context, addr string, req *kvrpcpb.DsDeleteRequest) (*kvrpcpb.DsDeleteResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := conn.Delete(ctx, req)
	return resp, err
}

func (c *KvRpcClient) Lock(ctx context.Context, addr string, req *kvrpcpb.DsLockRequest) (*kvrpcpb.DsLockResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := conn.Lock(ctx, req)
	return resp, err
}
func (c *KvRpcClient) LockUpdate(ctx context.Context, addr string, req *kvrpcpb.DsLockUpdateRequest) (*kvrpcpb.DsLockUpdateResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := conn.LockUpdate(ctx, req)
	return resp, err
}
func (c *KvRpcClient) Unlock(ctx context.Context, addr string, req *kvrpcpb.DsUnlockRequest) (*kvrpcpb.DsUnlockResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := conn.Unlock(ctx, req)
	return resp, err
}
func (c *KvRpcClient) UnlockForce(ctx context.Context, addr string, req *kvrpcpb.DsUnlockForceRequest) (*kvrpcpb.DsUnlockForceResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := conn.UnlockForce(ctx, req)
	return resp, err
}
func (c *KvRpcClient) LockScan(ctx context.Context, addr string, req *kvrpcpb.DsLockScanRequest) (*kvrpcpb.DsLockScanResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := conn.LockScan(ctx, req)
	return resp, err
}

func (c *KvRpcClient) KvSet(ctx context.Context, addr string, req *kvrpcpb.DsKvSetRequest) (*kvrpcpb.DsKvSetResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := conn.KvSet(ctx, req)
	return resp, err
}
func (c *KvRpcClient) KvGet(ctx context.Context, addr string, req *kvrpcpb.DsKvGetRequest) (*kvrpcpb.DsKvGetResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := conn.KvGet(ctx, req)
	return resp, err
}
func (c *KvRpcClient) KvBatchSet(ctx context.Context, addr string, req *kvrpcpb.DsKvBatchSetRequest) (*kvrpcpb.DsKvBatchSetResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}

	resp, err := conn.KvBatchSet(ctx, req)
	return resp, err
}
func (c *KvRpcClient) KvBatchGet(ctx context.Context, addr string, req *kvrpcpb.DsKvBatchGetRequest) (*kvrpcpb.DsKvBatchGetResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := conn.KvBatchGet(ctx, req)
	return resp, err
}
func (c *KvRpcClient) KvScan(ctx context.Context, addr string, req *kvrpcpb.DsKvScanRequest) (*kvrpcpb.DsKvScanResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := conn.KvScan(ctx, req)
	return resp, err
}
func (c *KvRpcClient) KvDelete(ctx context.Context, addr string, req *kvrpcpb.DsKvDeleteRequest) (*kvrpcpb.DsKvDeleteResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := conn.KvDel(ctx, req)
	return resp, err
}
func (c *KvRpcClient) KvBatchDelete(ctx context.Context, addr string, req *kvrpcpb.DsKvBatchDeleteRequest) (*kvrpcpb.DsKvBatchDeleteResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := conn.KvBatchDel(ctx, req)
	return resp, err
}
func (c *KvRpcClient) KvRangeDelete(ctx context.Context, addr string, req *kvrpcpb.DsKvRangeDeleteRequest) (*kvrpcpb.DsKvRangeDeleteResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := conn.KvRangeDel(ctx, req)
	return resp, err
}

func (c *KvRpcClient) getConn(addr string) (RpcClient, error) {
	if len(addr) == 0 {
		return nil, errors.New("invalid address")
	}
	return c.pool.GetConn(addr)
}
