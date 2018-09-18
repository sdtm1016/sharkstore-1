package dskv

import (
	"fmt"
	"golang.org/x/net/context"
	"model/pkg/kvrpcpb"
	"time"
)

type Context struct {
	VID           RangeVerID
	RequestHeader *kvrpcpb.RequestHeader
	NodeId        uint64
	NodeAddr      string
	Timeout       time.Duration
	ReqContext    *ReqContext
}

func (context *Context) String() string  {
	if context == nil {
		return ""
	}
	return fmt.Sprintf("context: nodeId[%d], nodeAddr[%s], %s", context.NodeId, context.NodeAddr, context.ReqContext.String() )
}

//proxy request context
type ReqContext struct {
	//maybe ""
	reqId string
	reqBo *Backoffer
}

func NewPRConext(sleep int) *ReqContext {
	reqBo := NewBackoffer(sleep, context.Background())
	//todo: need a better good uuid generate method
	//var reqUuid string
	//id, err := uuid.NewV4()
	//if err != nil {
	//	reqUuid = fmt.Sprintf("%s", id)
	//}
	return &ReqContext{
		//reqId: reqUuid,
		reqBo: reqBo,
	}
}

//func (context *ReqContext) GetReqId() string {
//	return context.reqId
//}

func (context *ReqContext) GetBackOff() *Backoffer {
	if context == nil {
		return nil
	}
	return context.reqBo
}

func (context *ReqContext) String() string {
	if context == nil {
		return "request context: [<nil>]"
	}
	//return fmt.Sprintf("request context: id[%s], %s", context.reqId, context.reqBo)
	return fmt.Sprintf("request context: [%s]", context.reqBo)
}

func (context *ReqContext) Clone() *ReqContext {
	return &ReqContext{
		reqId: context.reqId,
		reqBo: context.reqBo.Clone(),
	}
}
