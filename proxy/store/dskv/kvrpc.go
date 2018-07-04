package dskv

import (
	"fmt"
	"strconv"

	"model/pkg/kvrpcpb"
	"model/pkg/errorpb"
)

func EnumName(m map[int32]string, v int32) string {
	s, ok := m[v]
	if ok {
		return s
	}
	return strconv.Itoa(int(v))
}

type Type int32

const (
	Type_InvalidType Type = 0
	Type_RawGet      Type = 1
	Type_RawPut      Type = 2
	Type_RawDelete   Type = 3
	Type_RawExecute  Type = 4
	Type_Insert      Type = 5
	Type_Select      Type = 6
	Type_Delete      Type = 7
	Type_KvSet       Type = 8
	Type_KvBatchSet  Type = 9
	Type_KvGet       Type = 10
	Type_KvBatchGet  Type = 11
	Type_KvScan      Type = 12
	Type_KvDelete    Type = 13
	Type_KvBatchDel  Type = 14
	Type_KvRangeDel  Type = 15
	Type_Lock  		 	Type = 20
	Type_LockUpdate 	Type = 21
	Type_Unlock 		Type = 22
	Type_UnlockForce 	Type = 23
	Type_LockScan 		Type = 24
)

var Type_name = map[int32]string{
	0: "InvalidType",
	1: "RawGet",
	2: "RawPut",
	3: "RawDelete",
	4: "RawExecute",
	5: "Insert",
	6: "Select",
	7: "Delete",
}
var Type_value = map[string]int32{
	"InvalidType": 0,
	"RawGet":      1,
	"RawPut":      2,
	"RawDelete":   3,
	"RawExecute":  4,
	"Insert":      5,
	"Select":      6,
	"Delete":      7,
}

func (x Type) String() string {
	return EnumName(Type_name, int32(x))
}

type Request struct {
	Type          Type
	RawGetReq     *kvrpcpb.DsKvRawGetRequest
	RawPutReq     *kvrpcpb.DsKvRawPutRequest
	RawDeleteReq  *kvrpcpb.DsKvRawDeleteRequest
	RawExecuteReq *kvrpcpb.DsKvRawExecuteRequest

	SelectReq     *kvrpcpb.DsSelectRequest
	InsertReq     *kvrpcpb.DsInsertRequest
	DeleteReq     *kvrpcpb.DsDeleteRequest

	LockReq 	*kvrpcpb.DsLockRequest
	LockUpdateReq 	*kvrpcpb.DsLockUpdateRequest
	UnlockReq 	*kvrpcpb.DsUnlockRequest
	UnlockForceReq 	*kvrpcpb.DsUnlockForceRequest
	LockScanReq 	*kvrpcpb.DsLockScanRequest

	KvSetReq      *kvrpcpb.DsKvSetRequest
	KvBatchSetReq *kvrpcpb.DsKvBatchSetRequest
	KvGetReq      *kvrpcpb.DsKvGetRequest
	KvBatchGetReq *kvrpcpb.DsKvBatchGetRequest
	KvScanReq     *kvrpcpb.DsKvScanRequest
	KvDeleteReq   *kvrpcpb.DsKvDeleteRequest
	KvBatchDelReq *kvrpcpb.DsKvBatchDeleteRequest
	KvRangeDelReq *kvrpcpb.DsKvRangeDeleteRequest
}

func (m *Request) Reset() {
	*m = Request{}
}

func (m *Request) GetType() Type {
	if m != nil {
		return m.Type
	}
	return Type_InvalidType
}

func (m *Request) GetRawGetReq() *kvrpcpb.DsKvRawGetRequest {
	if m != nil {
		return m.RawGetReq
	}
	return nil
}

func (m *Request) GetLockReq() *kvrpcpb.DsLockRequest {
	if m != nil {
		return m.LockReq
	}
	return nil
}
func (m *Request) GetLockUpdateReq() *kvrpcpb.DsLockUpdateRequest {
	if m != nil {
		return m.LockUpdateReq
	}
	return nil
}
func (m *Request) GetUnlockReq() *kvrpcpb.DsUnlockRequest {
	if m != nil {
		return m.UnlockReq
	}
	return nil
}
func (m *Request) GetUnlockForceReq() *kvrpcpb.DsUnlockForceRequest {
	if m != nil {
		return m.UnlockForceReq
	}
	return nil
}
func (m *Request) GetLockScanReq() *kvrpcpb.DsLockScanRequest {
	if m != nil {
		return m.LockScanReq
	}
	return nil
}

func (m *Request) GetRawPutReq() *kvrpcpb.DsKvRawPutRequest {
	if m != nil {
		return m.RawPutReq
	}
	return nil
}

func (m *Request) GetRawDeleteReq() *kvrpcpb.DsKvRawDeleteRequest {
	if m != nil {
		return m.RawDeleteReq
	}
	return nil
}

func (m *Request) GetRawExecuteReq() *kvrpcpb.DsKvRawExecuteRequest {
	if m != nil {
		return m.RawExecuteReq
	}
	return nil
}

func (m *Request) GetSelectReq() *kvrpcpb.DsSelectRequest {
	if m != nil {
		return m.SelectReq
	}
	return nil
}

func (m *Request) GetInsertReq() *kvrpcpb.DsInsertRequest {
	if m != nil {
		return m.InsertReq
	}
	return nil
}

func (m *Request) GetDeleteReq() *kvrpcpb.DsDeleteRequest {
	if m != nil {
		return m.DeleteReq
	}
	return nil
}


func (m *Request) GetKvSetReq() *kvrpcpb.DsKvSetRequest {
	if m != nil {
		return m.KvSetReq
	}
	return nil
}

func (m *Request) GetKvBatchSetReq() *kvrpcpb.DsKvBatchSetRequest {
	if m != nil {
		return m.KvBatchSetReq
	}
	return nil
}

func (m *Request) GetKvGetReq() *kvrpcpb.DsKvGetRequest {
	if m != nil {
		return m.KvGetReq
	}
	return nil
}

func (m *Request) GetKvBatchGetReq() *kvrpcpb.DsKvBatchGetRequest {
	if m != nil {
		return m.KvBatchGetReq
	}
	return nil
}

func (m *Request) GetKvScanReq() *kvrpcpb.DsKvScanRequest {
	if m != nil {
		return m.KvScanReq
	}
	return nil
}

func (m *Request) GetKvDeleteReq() *kvrpcpb.DsKvDeleteRequest {
	if m != nil {
		return m.KvDeleteReq
	}
	return nil
}

func (m *Request) GetKvBatchDelReq() *kvrpcpb.DsKvBatchDeleteRequest {
	if m != nil {
		return m.KvBatchDelReq
	}
	return nil
}

func (m *Request) GetKvRangeDelReq() *kvrpcpb.DsKvRangeDeleteRequest {
	if m != nil {
		return m.KvRangeDelReq
	}
	return nil
}

type Response struct {
	Type           Type
	RawGetResp     *kvrpcpb.DsKvRawGetResponse
	RawPutResp     *kvrpcpb.DsKvRawPutResponse
	RawDeleteResp  *kvrpcpb.DsKvRawDeleteResponse
	RawExecuteResp *kvrpcpb.DsKvRawExecuteResponse

	SelectResp     *kvrpcpb.DsSelectResponse
	InsertResp     *kvrpcpb.DsInsertResponse
	DeleteResp     *kvrpcpb.DsDeleteResponse

	LockResp      	*kvrpcpb.DsLockResponse
	LockUpdateResp  *kvrpcpb.DsLockUpdateResponse
	UnlockResp      *kvrpcpb.DsUnlockResponse
	UnlockForceResp *kvrpcpb.DsUnlockForceResponse
	LockScanResp 	*kvrpcpb.DsLockScanResponse

	KvSetResp      *kvrpcpb.DsKvSetResponse
	KvBatchSetResp *kvrpcpb.DsKvBatchSetResponse
	KvGetResp      *kvrpcpb.DsKvGetResponse
	KvBatchGetResp *kvrpcpb.DsKvBatchGetResponse
	KvScanResp     *kvrpcpb.DsKvScanResponse
	KvDeleteResp   *kvrpcpb.DsKvDeleteResponse
	KvBatchDelResp *kvrpcpb.DsKvBatchDeleteResponse
	KvRangeDelResp *kvrpcpb.DsKvRangeDeleteResponse
}

func (m *Response) GetType() Type {
	if m != nil {
		return m.Type
	}
	return Type_InvalidType
}

func (m *Response) GetRawGetResp() *kvrpcpb.DsKvRawGetResponse {
	if m != nil {
		return m.RawGetResp
	}
	return nil
}

func (m *Response) GetRawPutResp() *kvrpcpb.DsKvRawPutResponse {
	if m != nil {
		return m.RawPutResp
	}
	return nil
}

func (m *Response) GetRawDeleteResp() *kvrpcpb.DsKvRawDeleteResponse {
	if m != nil {
		return m.RawDeleteResp
	}
	return nil
}

func (m *Response) GetRawExecuteResp() *kvrpcpb.DsKvRawExecuteResponse {
	if m != nil {
		return m.RawExecuteResp
	}
	return nil
}

func (m *Response) GetSelectResp() *kvrpcpb.DsSelectResponse {
	if m != nil {
		return m.SelectResp
	}
	return nil
}

func (m *Response) GetInsertResp() *kvrpcpb.DsInsertResponse {
	if m != nil {
		return m.InsertResp
	}
	return nil
}

func (m *Response) GetDeleteResp() *kvrpcpb.DsDeleteResponse {
	if m != nil {
		return m.DeleteResp
	}
	return nil
}

func (m *Response) GetLockResp() *kvrpcpb.DsLockResponse {
	if m != nil {
		return m.LockResp
	}
	return nil
}
func (m *Response) GetLockUpdateResp() *kvrpcpb.DsLockUpdateResponse {
	if m != nil {
		return m.LockUpdateResp
	}
	return nil
}
func (m *Response) GetUnlockResp() *kvrpcpb.DsUnlockResponse {
	if m != nil {
		return m.UnlockResp
	}
	return nil
}
func (m *Response) GetUnlockForceResp() *kvrpcpb.DsUnlockForceResponse {
	if m != nil {
		return m.UnlockForceResp
	}
	return nil
}
func (m *Response) GetLockScanResp() *kvrpcpb.DsLockScanResponse {
	if m != nil {
		return m.LockScanResp
	}
	return nil
}
func (m *Response) GetKvSetResp() *kvrpcpb.DsKvSetResponse {
	if m != nil {
		return m.KvSetResp
	}
	return nil
}

func (m *Response) GetKvBatchSetResp() *kvrpcpb.DsKvBatchSetResponse {
	if m != nil {
		return m.KvBatchSetResp
	}
	return nil
}

func (m *Response) GetKvGetResp() *kvrpcpb.DsKvGetResponse {
	if m != nil {
		return m.KvGetResp
	}
	return nil
}

func (m *Response) GetKvBatchGetResp() *kvrpcpb.DsKvBatchGetResponse {
	if m != nil {
		return m.KvBatchGetResp
	}
	return nil
}

func (m *Response) GetKvScanResp() *kvrpcpb.DsKvScanResponse {
	if m != nil {
		return m.KvScanResp
	}
	return nil
}

func (m *Response) GetKvDeleteResp() *kvrpcpb.DsKvDeleteResponse {
	if m != nil {
		return m.KvDeleteResp
	}
	return nil
}

func (m *Response) GetKvBatchDelResp() *kvrpcpb.DsKvBatchDeleteResponse {
	if m != nil {
		return m.KvBatchDelResp
	}
	return nil
}

func (m *Response) GetKvRangeDelResp() *kvrpcpb.DsKvRangeDeleteResponse {
	if m != nil {
		return m.KvRangeDelResp
	}
	return nil
}

func (resp *Response) GetErr() (pErr *errorpb.Error, err error) {
	switch resp.Type {
	case Type_RawPut:
		pErr = resp.RawPutResp.GetHeader().GetError()
	case Type_RawGet:
		pErr = resp.RawGetResp.GetHeader().GetError()
	case Type_RawDelete:
		pErr = resp.RawDeleteResp.GetHeader().GetError()
	case Type_Insert:
		pErr = resp.InsertResp.GetHeader().GetError()
	case Type_Select:
		pErr = resp.SelectResp.GetHeader().GetError()
	case Type_Delete:
		pErr = resp.DeleteResp.GetHeader().GetError()
	case Type_Lock:
		pErr = resp.LockResp.GetHeader().GetError()
	case Type_LockUpdate:
		pErr = resp.LockUpdateResp.GetHeader().GetError()
	case Type_Unlock:
		pErr = resp.UnlockResp.GetHeader().GetError()
	case Type_UnlockForce:
		pErr = resp.UnlockForceResp.GetHeader().GetError()
	case Type_LockScan:
		pErr = resp.LockScanResp.GetHeader().GetError()
	case Type_KvSet:
		pErr = resp.KvSetResp.GetHeader().GetError()
	case Type_KvBatchSet:
		pErr = resp.KvBatchSetResp.GetHeader().GetError()
	case Type_KvGet:
		pErr = resp.KvGetResp.GetHeader().GetError()
	case Type_KvBatchGet:
		pErr = resp.KvBatchGetResp.GetHeader().GetError()
	case Type_KvScan:
		pErr = resp.KvScanResp.GetHeader().GetError()
	case Type_KvDelete:
		pErr = resp.KvDeleteResp.GetHeader().GetError()
	case Type_KvBatchDel:
		pErr = resp.KvBatchDelResp.GetHeader().GetError()
	case Type_KvRangeDel:
		pErr = resp.KvRangeDelResp.GetHeader().GetError()
	default:
		err = fmt.Errorf("invalid response type %s", resp.Type.String())
	}
	return
}
