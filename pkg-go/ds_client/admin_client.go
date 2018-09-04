package client

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"model/pkg/ds_admin"

	"golang.org/x/net/context"
)

// AdminClient admin client
type AdminClient interface {
	// Close should release all data.
	Close() error

	// SetConfig set config
	SetConfig(addr string, configs []*ds_adminpb.ConfigItem) error

	// GetConfig get config
	GetConfig(addr string, keys []*ds_adminpb.ConfigKey) (*ds_adminpb.GetConfigResponse, error)

	// GetInfo get info
	GetInfo(addr, path string) (*ds_adminpb.GetInfoResponse, error)

	// ForceSplit force split
	ForceSplit(addr string, rangeID uint64, version uint64) error

	// ForceCompact force compact
	ForceCompact(addr string, rangeID uint64, transactionID int64) (*ds_adminpb.CompactionResponse, error)

	// ClearQueue clear queue
	ClearQueue(addr string, queueType ds_adminpb.ClearQueueRequest_QueueType) (*ds_adminpb.ClearQueueResponse, error)

	// GetPendingQueues get pending queues
	GetPendingQueues(addr string, pendingType ds_adminpb.GetPendingsRequest_PendingType, count uint64) (*ds_adminpb.GetPendingsResponse, error)

	// FlushDB sync db
	FlushDB(addr string, wait bool) error
}

type adminClient struct {
	token string
	pool  *ResourcePool
}

// NewAdminClient new admin client
func NewAdminClient(token string, poolSize int) AdminClient {
	return &adminClient{
		token: token,
		pool:  NewResourcePool(poolSize),
	}
}

// Close should release all data.
func (c *adminClient) Close() error {
	c.pool.Close()
	return nil
}

func (c *adminClient) signAuth(adminType ds_adminpb.AdminType) *ds_adminpb.AdminAuth {
	m := md5.New()
	m.Write([]byte(fmt.Sprintf("%d", adminType)))
	epoch := time.Now().Unix()
	m.Write([]byte(fmt.Sprintf("%d", epoch)))
	m.Write([]byte(c.token))
	return &ds_adminpb.AdminAuth{
		Epoch: epoch,
		Sign:  hex.EncodeToString(m.Sum(nil)),
	}
}

func (c *adminClient) newRequest(adminType ds_adminpb.AdminType) *ds_adminpb.AdminRequest {
	return &ds_adminpb.AdminRequest{
		Auth: c.signAuth(adminType),
		Typ:  adminType,
	}
}

func (c *adminClient) getConn(addr string) (RpcClient, error) {
	if len(addr) == 0 {
		return nil, errors.New("invalid address")
	}
	return c.pool.GetConn(addr)
}

func (c *adminClient) send(addr string, req *ds_adminpb.AdminRequest) (*ds_adminpb.AdminResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), ReadTimeoutShort)
	defer cancel()
	resp, err := conn.Admin(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.GetCode() != 0 {
		return nil, fmt.Errorf("code=<%d>: %s", resp.GetCode(), resp.GetErrorMsg())
	}
	return resp, nil
}

// SetConfig set config
func (c *adminClient) SetConfig(addr string, configs []*ds_adminpb.ConfigItem) error {
	req := c.newRequest(ds_adminpb.AdminType_SET_CONFIG)
	req.SetCfgReq = &ds_adminpb.SetConfigRequest{
		Configs: configs,
	}
	_, err := c.send(addr, req)
	return err
}

// GetConfig get config
func (c *adminClient) GetConfig(addr string, keys []*ds_adminpb.ConfigKey) (*ds_adminpb.GetConfigResponse, error) {
	req := c.newRequest(ds_adminpb.AdminType_GET_CONFIG)
	req.GetCfgReq = &ds_adminpb.GetConfigRequest{
		Key: keys,
	}
	resp, err := c.send(addr, req)
	if err != nil {
		return nil, err
	}
	return resp.GetGetCfgResp(), nil
}

// GetInfo get info
func (c *adminClient) GetInfo(addr, path string) (*ds_adminpb.GetInfoResponse, error) {
	req := c.newRequest(ds_adminpb.AdminType_GET_INFO)
	req.GetInfoReq = &ds_adminpb.GetInfoRequest{
		Path: path,
	}
	resp, err := c.send(addr, req)
	if err != nil {
		return nil, err
	}
	return resp.GetGetInfoResponse(), nil
}

// ForceSplit force split
func (c *adminClient) ForceSplit(addr string, rangeID uint64, version uint64) error {
	req := c.newRequest(ds_adminpb.AdminType_FORCE_SPLIT)
	req.ForceSplitReq = &ds_adminpb.ForceSplitRequest{
		RangeId: rangeID,
		Version: version,
	}
	_, err := c.send(addr, req)
	return err
}

// ForceCompact force compact
func (c *adminClient) ForceCompact(addr string, rangeID uint64, transID int64) (*ds_adminpb.CompactionResponse, error) {
	req := c.newRequest(ds_adminpb.AdminType_COMPACTION)
	req.CompactionReq = &ds_adminpb.CompactionRequest{
		RangeId:       rangeID,
		TransactionId: transID,
	}
	resp, err := c.send(addr, req)
	if err != nil {
		return nil, err
	}
	return resp.GetCompactionResp(), nil
}

// ClearQueue clear queue
func (c *adminClient) ClearQueue(addr string, queueType ds_adminpb.ClearQueueRequest_QueueType) (*ds_adminpb.ClearQueueResponse, error) {
	req := c.newRequest(ds_adminpb.AdminType_CLEAR_QUEUE)
	req.ClearQueueReq = &ds_adminpb.ClearQueueRequest{
		QueueType: queueType,
	}
	resp, err := c.send(addr, req)
	if err != nil {
		return nil, err
	}
	return resp.GetClearQueueResp(), nil
}

// GetPendingQueues get pending queues
func (c *adminClient) GetPendingQueues(addr string,
	pendingType ds_adminpb.GetPendingsRequest_PendingType, count uint64) (*ds_adminpb.GetPendingsResponse, error) {

	req := c.newRequest(ds_adminpb.AdminType_GET_PENDINGS)
	req.GetPendingsReq = &ds_adminpb.GetPendingsRequest{
		Ptype: pendingType,
		Count: count,
	}
	resp, err := c.send(addr, req)
	if err != nil {
		return nil, err
	}
	return resp.GetGetPendingsResp(), nil
}

// FlushDB sync db
func (c *adminClient) FlushDB(addr string, wait bool) error {
	req := c.newRequest(ds_adminpb.AdminType_FLUSH_DB)
	req.FlushDbReq = &ds_adminpb.FlushDBRequest{
		Wait: wait,
	}
	_, err := c.send(addr, req)
	return err
}
