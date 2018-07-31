package client

import (
	"bufio"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"model/pkg/funcpb"
	"model/pkg/kvrpcpb"
	"model/pkg/schpb"
	"pkg-go/util"
	"util/log"

	"runtime"

	"github.com/gogo/protobuf/proto"
	"golang.org/x/net/context"
)

var maxMsgID uint64
var clientID int64

type RpcError struct {
	err error
}

func NewRpcError(err error) error {
	return &RpcError{err: err}
}

func (re *RpcError) Error() string {
	return re.err.Error()
}

const (
	TempSendQueueLen = 20
)

type MsgTypeGroup struct {
	req  uint16
	resp uint16
}

func (m *MsgTypeGroup) GetRequestMsgType() uint16 {
	return m.req
}

func (m *MsgTypeGroup) GetResponseMsgType() uint16 {
	return m.resp
}

var msgType map[uint16]*MsgTypeGroup

func init() {
	msgType = make(map[uint16]*MsgTypeGroup)
	msgType[uint16(funcpb.FunctionID_kFuncHeartbeat)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncRawGet)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncRawPut)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncRawDelete)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncSelect)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncInsert)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncDelete)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncUpdate)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_KFuncReplace)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncKvSet)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncKvGet)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncKvBatchSet)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncKvBatchGet)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncKvBatchDel)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncKvDel)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncKvScan)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncKvRangeDel)] = &MsgTypeGroup{0x02, 0x12}

	msgType[uint16(funcpb.FunctionID_kFuncLock)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncLockUpdate)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncUnlock)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncUnlockForce)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncLockScan)] = &MsgTypeGroup{0x02, 0x12}

	msgType[uint16(funcpb.FunctionID_kFuncKvSet)] = &MsgTypeGroup{0x02, 0x12}
	msgType[uint16(funcpb.FunctionID_kFuncCreateRange)] = &MsgTypeGroup{0x01, 0x11}
	msgType[uint16(funcpb.FunctionID_kFuncDeleteRange)] = &MsgTypeGroup{0x01, 0x11}
	msgType[uint16(funcpb.FunctionID_kFuncRangeTransferLeader)] = &MsgTypeGroup{0x01, 0x11}
	msgType[uint16(funcpb.FunctionID_kFuncUpdateRange)] = &MsgTypeGroup{0x01, 0x11}
	msgType[uint16(funcpb.FunctionID_kFuncGetPeerInfo)] = &MsgTypeGroup{0x01, 0x11}
	msgType[uint16(funcpb.FunctionID_kFuncSetNodeLogLevel)] = &MsgTypeGroup{0x01, 0x11}
	msgType[uint16(funcpb.FunctionID_kFuncOfflineRange)] = &MsgTypeGroup{0x01, 0x11}
	msgType[uint16(funcpb.FunctionID_kFuncReplaceRange)] = &MsgTypeGroup{0x01, 0x11}
}

func getMsgType(funcId uint16) *MsgTypeGroup {
	if m, find := msgType[funcId]; find {
		return m
	}
	log.Panic("invalid funcId %d", funcId)
	return nil
}

// 链路状态
const (
	LINK_INIT = iota
	LINK_CONN
	LINK_CLOSED
	LINK_BAN_CONN
)

var (
	ErrClientClosed    = errors.New("client closed")
	ErrClientBusy      = errors.New("client is busy")
	ErrRequestTimeout  = errors.New("request timeout")
	ErrConnIdleTimeout = errors.New("conn idle timeout")
	ErrInvalidMessage  = errors.New("invalid message")
	ErrConnUnavailable = errors.New("the connection is unavailable")
	ErrConnClosing     = errors.New("the connection is closing")
	ErrNetworkIO       = errors.New("failed with network I/O error")
)

type RpcClient interface {
	// Raw
	KvRawGet(ctx context.Context, in *kvrpcpb.DsKvRawGetRequest) (*kvrpcpb.DsKvRawGetResponse, error)
	KvRawPut(ctx context.Context, in *kvrpcpb.DsKvRawPutRequest) (*kvrpcpb.DsKvRawPutResponse, error)
	KvRawDelete(ctx context.Context, in *kvrpcpb.DsKvRawDeleteRequest) (*kvrpcpb.DsKvRawDeleteResponse, error)
	KvRawExecute(ctx context.Context, in *kvrpcpb.DsKvRawExecuteRequest) (*kvrpcpb.DsKvRawExecuteResponse, error)

	// Sql
	Select(ctx context.Context, in *kvrpcpb.DsSelectRequest) (*kvrpcpb.DsSelectResponse, error)
	Insert(ctx context.Context, in *kvrpcpb.DsInsertRequest) (*kvrpcpb.DsInsertResponse, error)
	Delete(ctx context.Context, in *kvrpcpb.DsDeleteRequest) (*kvrpcpb.DsDeleteResponse, error)

	// lock
	Lock(ctx context.Context, in *kvrpcpb.DsLockRequest) (*kvrpcpb.DsLockResponse, error)
	LockUpdate(ctx context.Context, in *kvrpcpb.DsLockUpdateRequest) (*kvrpcpb.DsLockUpdateResponse, error)
	Unlock(ctx context.Context, in *kvrpcpb.DsUnlockRequest) (*kvrpcpb.DsUnlockResponse, error)
	UnlockForce(ctx context.Context, in *kvrpcpb.DsUnlockForceRequest) (*kvrpcpb.DsUnlockForceResponse, error)
	LockScan(ctx context.Context, in *kvrpcpb.DsLockScanRequest) (*kvrpcpb.DsLockScanResponse, error)

	// kv
	KvSet(ctx context.Context, in *kvrpcpb.DsKvSetRequest) (*kvrpcpb.DsKvSetResponse, error)
	KvGet(ctx context.Context, in *kvrpcpb.DsKvGetRequest) (*kvrpcpb.DsKvGetResponse, error)
	KvBatchSet(ctx context.Context, in *kvrpcpb.DsKvBatchSetRequest) (*kvrpcpb.DsKvBatchSetResponse, error)
	KvBatchGet(ctx context.Context, in *kvrpcpb.DsKvBatchGetRequest) (*kvrpcpb.DsKvBatchGetResponse, error)
	KvScan(ctx context.Context, in *kvrpcpb.DsKvScanRequest) (*kvrpcpb.DsKvScanResponse, error)
	KvDel(ctx context.Context, in *kvrpcpb.DsKvDeleteRequest) (*kvrpcpb.DsKvDeleteResponse, error)
	KvBatchDel(ctx context.Context, in *kvrpcpb.DsKvBatchDeleteRequest) (*kvrpcpb.DsKvBatchDeleteResponse, error)
	KvRangeDel(ctx context.Context, in *kvrpcpb.DsKvRangeDeleteRequest) (*kvrpcpb.DsKvRangeDeleteResponse, error)

	// admin
	CreateRange(ctx context.Context, in *schpb.CreateRangeRequest) (*schpb.CreateRangeResponse, error)
	DeleteRange(ctx context.Context, in *schpb.DeleteRangeRequest) (*schpb.DeleteRangeResponse, error)
	TransferLeader(ctx context.Context, in *schpb.TransferRangeLeaderRequest) (*schpb.TransferRangeLeaderResponse, error)
	UpdateRange(ctx context.Context, in *schpb.UpdateRangeRequest) (*schpb.UpdateRangeResponse, error)
	GetPeerInfo(ctx context.Context, in *schpb.GetPeerInfoRequest) (*schpb.GetPeerInfoResponse, error)
	SetNodeLogLevel(ctx context.Context, in *schpb.SetNodeLogLevelRequest) (*schpb.SetNodeLogLevelResponse, error)
	OfflineRange(ctx context.Context, in *schpb.OfflineRangeRequest) (*schpb.OfflineRangeResponse, error)
	ReplaceRange(ctx context.Context, in *schpb.ReplaceRangeRequest) (*schpb.ReplaceRangeResponse, error)
	Close()
}

type Message struct {
	msgId      uint64
	msgType    uint16
	funcId     uint16
	streamHash uint8
	protoType  uint8
	// 相对时间，单位毫秒
	timeout uint32
	data    []byte

	done chan error
	ctx  context.Context
}

func (m *Message) GetMsgType() uint16 {
	return m.msgType
}

func (m *Message) SetMsgType(msgType uint16) {
	m.msgType = msgType
}

func (m *Message) GetFuncId() uint16 {
	return m.funcId
}

func (m *Message) SetFuncId(funcId uint16) {
	m.funcId = funcId
}

func (m *Message) GetMsgId() uint64 {
	return m.msgId
}

func (m *Message) SetMsgId(msgId uint64) {
	m.msgId = msgId
}

func (m *Message) GetStreamHash() uint8 {
	return m.streamHash
}

func (m *Message) SetStreamHash(streamHash uint8) {
	m.streamHash = streamHash
}

func (m *Message) GetProtoType() uint8 {
	return m.protoType
}

func (m *Message) SetProtoType(protoType uint8) {
	m.protoType = protoType
}

func (m *Message) GetTimeout() uint32 {
	return m.timeout
}

func (m *Message) SetTimeout(timeout uint32) {
	m.timeout = timeout
}

func (m *Message) GetData() []byte {
	return m.data
}

func (m *Message) SetData(data []byte) {
	m.data = data
}

func (m *Message) Back(err error) {
	select {
	case m.done <- err:
	default:
		log.Error("invalid message!!!!!")
	}
}

type List struct {
	lock sync.RWMutex
	list map[uint64]*Message
}

func NewList() *List {
	return &List{list: make(map[uint64]*Message)}
}

func (l *List) AddElement(m *Message) error {
	l.lock.Lock()
	defer l.lock.Unlock()
	if _, find := l.list[m.msgId]; find {
		return errors.New("element exist")
	}
	l.list[m.msgId] = m
	return nil
}

func (l *List) DelElement(id uint64) (*Message, bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if m, find := l.list[id]; find {
		delete(l.list, id)
		return m, true
	}
	return nil, false
}

func (l *List) FindElement(id uint64) (*Message, bool) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if m, find := l.list[id]; find {
		return m, true
	}
	return nil, false
}

func (l *List) Cleanup(err error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	for _, e := range l.list {
		select {
		case e.done <- err:
		default:
		}
	}
	// cleanup
	l.list = make(map[uint64]*Message)
}

func (l *List) Size() int {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return len(l.list)

}

type WaitList struct {
	size     uint64
	waitList []*List
}

func NewWaitList(size int) *WaitList {
	waitList := make([]*List, size)
	for i := 0; i < size; i++ {
		waitList[i] = NewList()
	}
	return &WaitList{size: uint64(size), waitList: waitList}
}

func (wl *WaitList) AddElement(m *Message) error {
	index := m.msgId % wl.size
	return wl.waitList[index].AddElement(m)
}

func (wl *WaitList) DelElement(id uint64) (*Message, bool) {
	index := id % wl.size
	return wl.waitList[index].DelElement(id)
}

func (wl *WaitList) FindElement(id uint64) (*Message, bool) {
	index := id % wl.size
	return wl.waitList[index].FindElement(id)
}

func (wl *WaitList) Cleanup(err error) {
	for _, list := range wl.waitList {
		list.Cleanup(err)
	}
}

func (wl *WaitList) ElemSize() int {
	size := 0
	for _, list := range wl.waitList {
		size += list.Size()
	}
	return size
}

type DialFunc func(addr string) (*ConnTimeout, error)

type DSRpcClient struct {
	conn      *ConnTimeout
	connCount int
	writer    *bufio.Writer
	reader    *bufio.Reader
	// heartbeat
	//lastWriteTime time.Time
	sendQueue chan *Message
	waitList  *WaitList
	allowRecv chan bool
	addr      string
	dialFunc  DialFunc

	lock    sync.Mutex
	version uint64
	// 链路状态
	linkState int
	//
	closed bool
	// 连续心跳计数
	heartbeatCount int64
	hbSendTime     int64
	clientId       int64
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
}

func NewDSRpcClient(addr string, dialFunc DialFunc) *DSRpcClient {
	ctx, cancel := context.WithCancel(context.Background())
	cli := &DSRpcClient{
		addr:      addr,
		dialFunc:  dialFunc,
		closed:    false,
		linkState: LINK_INIT,
		sendQueue: make(chan *Message, 100000),
		allowRecv: make(chan bool, 1),
		waitList:  NewWaitList(64),
		clientId:  atomic.AddInt64(&clientID, 1),
		ctx:       ctx, cancel: cancel}
	cli.wg.Add(1)
	go cli.sendLoop()
	cli.wg.Add(1)
	go cli.recvLoop()
	go cli.heartbeat()
	return cli
}

func (c *DSRpcClient) getMsgId() uint64 {
	return atomic.AddUint64(&maxMsgID, 1)
}

func (c *DSRpcClient) GetClientId() int64 {
	return c.clientId
}

func (c *DSRpcClient) execute(funId uint16, ctx context.Context, in proto.Message, out proto.Message) (uint64, error) {
	data, err := proto.Marshal(in)
	if err != nil {
		return 0, err
	}
	msgId := c.getMsgId()
	now := time.Now()
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(ReadTimeout)
	}
	timeout := deadline.Sub(now) / time.Millisecond
	if timeout <= 0 {
		return msgId, ErrRequestTimeout
	}
	message := &Message{
		done:    make(chan error, 1),
		msgId:   msgId,
		msgType: getMsgType(funId).GetRequestMsgType(),
		funcId:  uint16(funId),
		timeout: uint32(timeout),
		ctx:     ctx,
		data:    data,
	}

	data, err = c.Send(ctx, message)
	if err != nil {
		return msgId, err
	}
	err = proto.Unmarshal(data, out)
	if err != nil {
		return msgId, err
	}
	return msgId, nil
}

func (c *DSRpcClient) KvRawGet(ctx context.Context, in *kvrpcpb.DsKvRawGetRequest) (*kvrpcpb.DsKvRawGetResponse, error) {
	out := new(kvrpcpb.DsKvRawGetResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncRawGet), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

func (c *DSRpcClient) KvRawPut(ctx context.Context, in *kvrpcpb.DsKvRawPutRequest) (*kvrpcpb.DsKvRawPutResponse, error) {
	out := new(kvrpcpb.DsKvRawPutResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncRawPut), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

func (c *DSRpcClient) KvRawDelete(ctx context.Context, in *kvrpcpb.DsKvRawDeleteRequest) (*kvrpcpb.DsKvRawDeleteResponse, error) {
	out := new(kvrpcpb.DsKvRawDeleteResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncRawDelete), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

func (c *DSRpcClient) KvRawExecute(ctx context.Context, in *kvrpcpb.DsKvRawExecuteRequest) (*kvrpcpb.DsKvRawExecuteResponse, error) {
	out := new(kvrpcpb.DsKvRawExecuteResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncRawExecute), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

func (c *DSRpcClient) Select(ctx context.Context, in *kvrpcpb.DsSelectRequest) (*kvrpcpb.DsSelectResponse, error) {
	out := new(kvrpcpb.DsSelectResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncSelect), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

func (c *DSRpcClient) Insert(ctx context.Context, in *kvrpcpb.DsInsertRequest) (*kvrpcpb.DsInsertResponse, error) {
	out := new(kvrpcpb.DsInsertResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncInsert), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

func (c *DSRpcClient) Delete(ctx context.Context, in *kvrpcpb.DsDeleteRequest) (*kvrpcpb.DsDeleteResponse, error) {
	out := new(kvrpcpb.DsDeleteResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncDelete), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

func (c *DSRpcClient) Lock(ctx context.Context, in *kvrpcpb.DsLockRequest) (*kvrpcpb.DsLockResponse, error) {
	out := new(kvrpcpb.DsLockResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncLock), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}
func (c *DSRpcClient) LockUpdate(ctx context.Context, in *kvrpcpb.DsLockUpdateRequest) (*kvrpcpb.DsLockUpdateResponse, error) {
	out := new(kvrpcpb.DsLockUpdateResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncLockUpdate), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}
func (c *DSRpcClient) Unlock(ctx context.Context, in *kvrpcpb.DsUnlockRequest) (*kvrpcpb.DsUnlockResponse, error) {
	out := new(kvrpcpb.DsUnlockResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncUnlock), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}
func (c *DSRpcClient) UnlockForce(ctx context.Context, in *kvrpcpb.DsUnlockForceRequest) (*kvrpcpb.DsUnlockForceResponse, error) {
	out := new(kvrpcpb.DsUnlockForceResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncUnlockForce), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}
func (c *DSRpcClient) LockScan(ctx context.Context, in *kvrpcpb.DsLockScanRequest) (*kvrpcpb.DsLockScanResponse, error) {
	out := new(kvrpcpb.DsLockScanResponse)
		_, err := c.execute(uint16(funcpb.FunctionID_kFuncLockScan), ctx, in, out)
	if err != nil {
		return nil, err
	} else {
		return out, nil	
	}
}

func (c *DSRpcClient) KvSet(ctx context.Context, in *kvrpcpb.DsKvSetRequest) (*kvrpcpb.DsKvSetResponse, error) {
	out := new(kvrpcpb.DsKvSetResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncKvSet), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}
func (c *DSRpcClient) KvGet(ctx context.Context, in *kvrpcpb.DsKvGetRequest) (*kvrpcpb.DsKvGetResponse, error) {
	out := new(kvrpcpb.DsKvGetResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncKvGet), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}
func (c *DSRpcClient) KvBatchSet(ctx context.Context, in *kvrpcpb.DsKvBatchSetRequest) (*kvrpcpb.DsKvBatchSetResponse, error) {
	out := new(kvrpcpb.DsKvBatchSetResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncKvBatchSet), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}
func (c *DSRpcClient) KvBatchGet(ctx context.Context, in *kvrpcpb.DsKvBatchGetRequest) (*kvrpcpb.DsKvBatchGetResponse, error) {
	out := new(kvrpcpb.DsKvBatchGetResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncKvBatchGet), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}
func (c *DSRpcClient) KvScan(ctx context.Context, in *kvrpcpb.DsKvScanRequest) (*kvrpcpb.DsKvScanResponse, error) {
	out := new(kvrpcpb.DsKvScanResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncKvScan), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}
func (c *DSRpcClient) KvDel(ctx context.Context, in *kvrpcpb.DsKvDeleteRequest) (*kvrpcpb.DsKvDeleteResponse, error) {
	out := new(kvrpcpb.DsKvDeleteResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncKvDel), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}
func (c *DSRpcClient) KvBatchDel(ctx context.Context, in *kvrpcpb.DsKvBatchDeleteRequest) (*kvrpcpb.DsKvBatchDeleteResponse, error) {
	out := new(kvrpcpb.DsKvBatchDeleteResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncKvBatchDel), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}
func (c *DSRpcClient) KvRangeDel(ctx context.Context, in *kvrpcpb.DsKvRangeDeleteRequest) (*kvrpcpb.DsKvRangeDeleteResponse, error) {
	out := new(kvrpcpb.DsKvRangeDeleteResponse)
	msgId, err := c.execute(uint16(funcpb.FunctionID_kFuncKvRangeDel), ctx, in, out)
	in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

// TODO message
func (c *DSRpcClient) CreateRange(ctx context.Context, in *schpb.CreateRangeRequest) (*schpb.CreateRangeResponse, error) {
	out := new(schpb.CreateRangeResponse)
	_, err := c.execute(uint16(funcpb.FunctionID_kFuncCreateRange), ctx, in, out)
	//in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

func (c *DSRpcClient) DeleteRange(ctx context.Context, in *schpb.DeleteRangeRequest) (*schpb.DeleteRangeResponse, error) {
	out := new(schpb.DeleteRangeResponse)
	_, err := c.execute(uint16(funcpb.FunctionID_kFuncDeleteRange), ctx, in, out)
	//in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

func (c *DSRpcClient) TransferLeader(ctx context.Context, in *schpb.TransferRangeLeaderRequest) (*schpb.TransferRangeLeaderResponse, error) {
	out := new(schpb.TransferRangeLeaderResponse)
	_, err := c.execute(uint16(funcpb.FunctionID_kFuncRangeTransferLeader), ctx, in, out)
	//in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

func (c *DSRpcClient) UpdateRange(ctx context.Context, in *schpb.UpdateRangeRequest) (*schpb.UpdateRangeResponse, error) {
	out := new(schpb.UpdateRangeResponse)
	_, err := c.execute(uint16(funcpb.FunctionID_kFuncUpdateRange), ctx, in, out)
	//in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

func (c *DSRpcClient) GetPeerInfo(ctx context.Context, in *schpb.GetPeerInfoRequest) (*schpb.GetPeerInfoResponse, error) {
	out := new(schpb.GetPeerInfoResponse)
	_, err := c.execute(uint16(funcpb.FunctionID_kFuncGetPeerInfo), ctx, in, out)
	//in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

func (c *DSRpcClient) SetNodeLogLevel(ctx context.Context, in *schpb.SetNodeLogLevelRequest) (*schpb.SetNodeLogLevelResponse, error) {
	out := new(schpb.SetNodeLogLevelResponse)
	_, err := c.execute(uint16(funcpb.FunctionID_kFuncSetNodeLogLevel), ctx, in, out)
	//in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

func (c *DSRpcClient) OfflineRange(ctx context.Context, in *schpb.OfflineRangeRequest) (*schpb.OfflineRangeResponse, error) {
	out := new(schpb.OfflineRangeResponse)
	_, err := c.execute(uint16(funcpb.FunctionID_kFuncOfflineRange), ctx, in, out)
	//in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

func (c *DSRpcClient) ReplaceRange(ctx context.Context, in *schpb.ReplaceRangeRequest) (*schpb.ReplaceRangeResponse, error) {
	out := new(schpb.ReplaceRangeResponse)
	_, err := c.execute(uint16(funcpb.FunctionID_kFuncReplaceRange), ctx, in, out)
	//in.GetHeader().TraceId = msgId
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

func (c *DSRpcClient) Close() {
	if c.closed {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.closed {
		return
	}
	c.linkState = LINK_BAN_CONN
	c.closed = true
	c.cancel()
	c.wg.Wait()
	if c.conn != nil {
		c.conn.Close()
	}
	c.waitList.Cleanup(ErrClientClosed)
}

func (c *DSRpcClient) Send(ctx context.Context, msg *Message) ([]byte, error) {
	metricSend := time.Now().UnixNano()
	// 链路没有恢复，直接返回错误
	var err error
	if c.closed {
		return nil, NewRpcError(ErrClientClosed)
	}
	if c.linkState != LINK_CONN {
		err = c.reconnect()
		if err != nil {
			return nil, err
		}
	}
	// 添加到等待队列
	select {
	case <-c.ctx.Done():
		return nil, NewRpcError(ErrClientClosed)
	case <-ctx.Done():
		return nil, NewRpcError(ErrRequestTimeout)
	case c.sendQueue <- msg:
	default:
		return nil, NewRpcError(ErrClientBusy)
	}
	sendToQ := (time.Now().UnixNano() - metricSend) / int64(time.Millisecond)
	if sendToQ > 2 {
		log.Warn("send queue to %s funId:%d  time:%d ms,msgid :%d ", c.addr, msg.funcId, sendToQ, msg.msgId)
	}

	select {
	case <-c.ctx.Done():
		err = ErrClientClosed
		log.Warn("request to %s client close funId:%d  time:%d ms,msgid :%d ", c.addr, msg.funcId, (time.Now().UnixNano()-metricSend)/int64(time.Millisecond), msg.msgId)
	case <-ctx.Done():
		err = ErrRequestTimeout
		log.Warn("request to %s timeout funId:%d  time:%d ms,msgid :%d ", c.addr, msg.funcId, (time.Now().UnixNano()-metricSend)/int64(time.Millisecond), msg.msgId)
	case err = <-msg.done:
	}
	c.waitList.DelElement(msg.msgId)
	if err != nil {
		return nil, err
	}

	sendDelay := (time.Now().UnixNano() - metricSend) / int64(time.Millisecond)
	if sendDelay <= 50 {
		// do nothing
	} else if sendDelay <= 200 {
		log.Info("clientId %d request to %s funId:%d execut time %d ms,msgid :%d", c.GetClientId(), c.addr, msg.funcId, sendDelay, msg.msgId)
	} else if sendDelay <= 500 {
		log.Warn("clientId %d request to %s funId:%d execut time %d ms,msgid :%d ", c.GetClientId(), c.addr, msg.funcId, sendDelay, msg.msgId)
	}

	return msg.GetData(), nil
}

func (c *DSRpcClient) reconnect() error {
	if c.linkState == LINK_BAN_CONN {
		return ErrClientClosed
	}
	version := c.version
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.linkState == LINK_BAN_CONN {
		return ErrClientClosed
	}
	// 已经重建链接了
	if c.version > version && c.linkState == LINK_CONN {
		return nil
	}
	if c.linkState == LINK_CONN {
		if c.conn != nil {
			c.conn.Close()
		}
		c.linkState = LINK_CLOSED
	}
	c.connCount++
	c.heartbeatCount = 0
	conn, err := c.dialFunc(c.addr)
	if err == nil {
		c.conn = conn
		c.writer = bufio.NewWriterSize(conn, DefaultWriteSize)
		c.reader = bufio.NewReaderSize(conn, DefaultReadSize)
		c.linkState = LINK_CONN
		c.version += 1
		// 启动读
		select {
		case c.allowRecv <- true:
		default:
		}
		log.Info("dial %s successed", c.addr)
		return nil
	}
	log.Warn("dial %s failed, err[%v]", c.addr, err)
	return ErrConnUnavailable
}

func (c *DSRpcClient) heartbeat() {
	log.Info("start heartbeat %s clientId:%d", c.addr, c.GetClientId())
	defer func() {
		log.Warn("exit heartbeat %d %s", c.clientId, c.addr)
		if e := recover(); e != nil {
			log.Error("sendLoop err, [%v]", e)
			if _, flag := e.(error); flag {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]
				log.Error("sendLoop Run %s", string(buf))
			}
		}

	}()
	hbTicker := time.NewTicker(HeartbeatInterval)

	for {
		select {
		case <-c.ctx.Done():
			hbTicker.Stop()
			return
		case <-hbTicker.C:
			ctx, _ := context.WithTimeout(context.Background(), HeartbeatInterval)
			message := &Message{
				msgId:   c.getMsgId(),
				msgType: uint16(0x12),
				funcId:  uint16(funcpb.FunctionID_kFuncHeartbeat),

				done: make(chan error, 1),
				ctx:  ctx,
			}

			curTime := time.Now().UnixNano()
			delay := (curTime - c.hbSendTime) / int64(HeartbeatInterval)
			if delay < 2 {
				//
			} else if delay < 3 {
				log.Warn("%s hb clientId %d delay too long %d ms", c.addr, c.GetClientId(), delay)
			} else if delay < 5 {
				log.Error("%s hb clientId %d delay too long %d ms", c.addr, c.GetClientId(), delay)
			}

			c.hbSendTime = curTime
			select {
			case c.sendQueue <- message:
			default:
			}
		}
	}

}

func (c *DSRpcClient) closeLink(err error) {
	c.waitList.Cleanup(err)
	if c.linkState != LINK_CONN {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.linkState != LINK_CONN {
		return
	}
	c.linkState = LINK_CLOSED
	if c.conn != nil {
		if err != nil {
			log.Warn("clientId %d close conn:%s err:%s", c.GetClientId(), c.addr, err.Error())
		} else {
			log.Warn("clientId %d  close conn:%s err: nil", c.GetClientId(), c.addr)
		}

		c.conn.Close()
	}
}

//func (c *DSRpcClient) sendLoop_old() {
//	var message *Message
//	var ok bool
//	var err error
//	defer c.wg.Done()
//	for {
//		select {
//		case <-c.ctx.Done():
//		// 清理所有在发送队列中的请求
//			for {
//				select {
//				case message, ok = <-c.sendQueue:
//				    if !ok {
//					    break
//				    }
//					message.Back(ErrClientClosed)
//				default:
//					break
//				}
//			}
//			return
//		case message, ok = <-c.sendQueue:
//			if !ok {
//				// 队列closed
//				log.Warn("send queue closed!!!!")
//				return
//			}
//		    if c.closed {
//			    message.Back(ErrClientClosed)
//			    return
//		    }
//		// 重新建立链接
//			if c.linkState != LINK_CONN {
//				err = c.reconnect()
//				if err != nil {
//					message.Back(err)
//					continue
//				}
//			}
//
//		// 已经超时，直接返回timeout
//			select {
//			case <-message.ctx.Done():
//				log.Warn("request timeout before send!!!")
//				message.Back(ErrRequestTimeout)
//				continue
//			default:
//			}
//			//c.lastWriteTime = time.Now()
//		// 发送失败，关闭链路
//			c.waitList.AddElement(message)
//			err = util.WriteMessage(c.writer, message)
//			if err != nil {
//				log.Warn("write message failed, err[%v]", err)
//				if err == io.EOF {
//					err = ErrConnClosing
//				} else {
//					err = ErrNetworkIO
//				}
//				message.Back(err)
//				// 已经发送的需要清理
//				c.closeLink(err)
//				continue
//			}
//			err = c.writer.Flush()
//			if err != nil {
//				log.Warn("write message failed, err[%v]", err)
//				// 已经发送的需要清理
//				if err == io.EOF {
//					err = ErrConnClosing
//				} else {
//					err = ErrNetworkIO
//				}
//				c.closeLink(err)
//				message.Back(err)
//			}
//		}
//	}
//}

func (c *DSRpcClient) sendLoop() {
	var message *Message
	var ok bool
	var err error
	//var messageQueue [TempSendQueueLen]*Message
	var queueCount int
	log.Info("start sendLoop %s clientId:%d", c.addr, c.GetClientId())
	defer func() {
		log.Warn("exit sendLoop %d %s", c.clientId, c.addr)
		if e := recover(); e != nil {
			log.Error("sendLoop err, [%v]", e)
			if _, flag := e.(error); flag {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]
				log.Error("sendLoop Run %s", string(buf))
			}
		}

	}()
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			// 清理所有在发送队列中的请求
			for {
				select {
				case message, ok = <-c.sendQueue:
					if !ok {
						return
					}
					message.Back(NewRpcError(ErrClientClosed))
				default:
					return
				}
			}
			return
		case message, ok = <-c.sendQueue:
			if !ok {
				// 队列closed
				log.Warn("%s %d send queue closed!!!!", c.addr, c.clientId)
				return
			}
			if c.closed {
				message.Back(NewRpcError(ErrClientClosed))
				return
			}
			//messageQueue[queueCount] = message
			c.sendMessage(message)
			queueCount++
			// 尽量收集一批请求批量写
		TryMore:
			for {
				select {
				case message, ok := <-c.sendQueue:
					if !ok {
						break TryMore
					}
					//messageQueue[queueCount] = message
					c.sendMessage(message)
					queueCount++
					if queueCount == TempSendQueueLen {
						break TryMore
					}
				default:
					break TryMore
				}
			}

			queueCount = 0
			// 有可能链接没建立成功，writer还是nil
			if c.writer != nil {
				err = c.writer.Flush()
				if err != nil {
					log.Warn("%d %s write message failed, err[%v]", c.clientId, c.addr, err)
					// 已经发送的需要清理
					if err == io.EOF {
						err = ErrConnClosing
					} else {
						err = ErrNetworkIO
					}
					c.closeLink(err)
					//message.Back(err)
				}
			}
		}
	}
}

func (c *DSRpcClient) sendMessage(message *Message) {
	if c.linkState != LINK_CONN {
		err := c.reconnect()
		if err != nil {
			message.Back(NewRpcError(err))
			return
		}
	}
	// 重新建立链接
	/**
	select {
	// 已经超时，直接返回timeout
		case <-message.ctx.Done():
			log.Warn("request timeout before send!!!")
			message.Back(NewRpcError(ErrRequestTimeout))
		default:
	}
	*/
	//c.lastWriteTime = time.Now()
	// 发送失败，关闭链路
	c.waitList.AddElement(message)
	err := util.WriteMessage(c.writer, message)
	if err != nil {
		log.Warn("write message failed, err[%v]", err)
		if err == io.EOF {
			err = ErrConnClosing
		} else {
			err = ErrNetworkIO
		}
		message.Back(NewRpcError(err))
		// 已经发送的需要清理
		c.closeLink(err)
	}
}

func (c *DSRpcClient) recvWork() {
	var err error
	msg := &Message{}
	log.Info("start rpc client recv work %s clientId:%d", c.addr, c.GetClientId())

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		if c.linkState != LINK_CONN {
			err := c.reconnect()
			if err != nil {
				log.Error("reconn error clientId: %d %s", c.GetClientId(), c.addr)
				return
			}
		}
		err = util.ReadMessage(c.reader, msg)
		if err != nil {
			log.Warn("conn[%d] recv error %v", c.connCount, err)
			if err == io.EOF {
				err = ErrConnClosing
			} else if err == io.ErrUnexpectedEOF {
				err = ErrConnClosing
			} else if _e, ok := err.(net.Error); ok {
				if _e.Timeout() {
					log.Warn("must bug for read IO timeout")
					err = ErrRequestTimeout
				}
				// TODO 暂时先不关闭链接
				//continue
			} else {
				err = ErrNetworkIO
			}
			c.closeLink(err)
			return
		}
		// 心跳响应直接删除
		if msg.GetFuncId() != uint16(funcpb.FunctionID_kFuncHeartbeat) {
			c.heartbeatCount = 0
			message, find := c.waitList.FindElement(msg.GetMsgId())
			if find {
				message.SetData(msg.GetData())
				message.Back(nil)
			} else {
				log.Warn("message[%d] timeout", msg.GetMsgId())
			}
		} else {
			// 心跳响应
			c.heartbeatCount++
			c.waitList.DelElement(msg.GetMsgId())
			delay := (time.Now().UnixNano() - c.hbSendTime) / int64(time.Millisecond)
			if delay < 2 {
				//
			} else if delay < 5 {
				log.Warn("%s hb clientId %d response too long %d ms", c.addr, c.GetClientId(), delay)
			} else if delay < 10 {
				log.Error("%s hb clientId %d response too long %d ms", c.addr, c.GetClientId(), delay)
			}
			//临时打开：
			log.Debug("%s clientId %d delay %d ,wait msgId size :%d", c.addr, c.GetClientId(), delay, c.waitList.ElemSize())
			// 空闲超时,主动断开链接
			if time.Duration(c.heartbeatCount)*HeartbeatInterval > DefaultIdleTimeout {
				c.closeLink(ErrConnIdleTimeout)
				return
			}

		}
	}
}

func (c *DSRpcClient) recvLoop() {
	defer func() {
		log.Warn("exit recvWork %d %s", c.clientId, c.addr)
		if e := recover(); e != nil {
			log.Error("recvWork err, [%v]", e)
			if _, flag := e.(error); flag {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]
				log.Error("recvWork Run %s", string(buf))
			}
		}

	}()
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.allowRecv:
			c.recvWork()
		}
	}
}
