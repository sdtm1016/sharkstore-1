package mock_ds

import (
	"fmt"
	"net"
	"sync"
	dsClient "pkg-go/ds_client"
	"pkg-go/util"
	"github.com/gogo/protobuf/proto"
	"model/pkg/kvrpcpb"
	"bufio"
	"time"
	"model/pkg/metapb"
	"model/pkg/schpb"
	"model/pkg/errorpb"
	"proxy/store/localstore/engine"
	"proxy/store/localstore/goleveldb"
	commonUtil "util"
	"os"
	"util/log"
	"util/encoding"
	"model/pkg/funcpb"
)

//启动参数： 端口 CPU数
//func main() {
//	address := "127.0.0.1:6680"
//	cpu := 4
//	var err error
//	if len(os.Args) == 3 {
//		address = os.Args[1]
//		cpu, _ = strconv.Atoi(os.Args[2])
//	} else {
//		log.Fatal("please input port", err)
//	}
//	s := fmt.Sprintf("program run param: host: %s, cpu:%d", address, cpu)
//	fmt.Println(s)
//	runtime.GOMAXPROCS(cpu)
//	ds := NewDsRpcServer(address)
//	ds.Start()
//	c := make(chan os.Signal, 1)
//	signal.Notify(c) //ctrl+c : os.Interrupt, kill pid: terminated
//	go func() {
//		s:= <-c
//		fmt.Println("exit signal:", s)
//		ds.Stop()
//		os.Exit(1)
//	}()
//}

type WorkFunc func(msg *dsClient.Message)

type DsRpcServer struct {
	rpcAddr     string
	wg          sync.WaitGroup
	lock        sync.RWMutex
	count       uint64
	conns       map[uint64]net.Conn
	l           net.Listener
	rLock       sync.RWMutex
	rngs        map[uint64]*metapb.Range
	childRngs   map[uint64]*metapb.Range //store childRange after old range was splited key:old range id; value:new child range

	store       engine.Driver
	//msAddr      []string
	//cli         client.Client
}

func NewDsRpcServer(addr string, path string) *DsRpcServer {

	store, err := goleveldb.NewLevelDBDriver(path)
	if err != nil {
		os.Exit(-1)
		return nil
	}

	return &DsRpcServer{rpcAddr: addr, conns: make(map[uint64]net.Conn), rngs: make(map[uint64]*metapb.Range),
			childRngs: make(map[uint64]*metapb.Range),
			store:store}
}

func (svr *DsRpcServer) SetRange(r *metapb.Range) {
	svr.rLock.Lock()
	defer svr.rLock.Unlock()
	if _, find := svr.rngs[r.GetId()]; !find {
		svr.rngs[r.GetId()] = r
	}else{
		svr.rngs[r.GetId()] = r
	}
}

func (svr *DsRpcServer) GetRange(id uint64)(*metapb.Range) {
	svr.rLock.Lock()
	defer svr.rLock.Unlock()
	if r, find := svr.rngs[id]; find {
		return r
	}else{
		return nil
	}
}

func (svr *DsRpcServer) DelRange(id uint64){
	svr.rLock.Lock()
	defer svr.rLock.Unlock()
	if _, find := svr.rngs[id]; find {
		delete(svr.rngs, id)
	}
}

func (svr *DsRpcServer) Start() {
	//cli, err := client.NewClient(svr.msAddr)
	//if err != nil {
	//	fmt.Println(err)
     //   os.Exit(-1)
	//}
	//svr.cli = cli
	//// get nodeId
	//_, sPort, err := net.SplitHostPort(svr.rpcAddr)
	//if err != nil {
	//	fmt.Println(err)
	//	os.Exit(-1)
	//}
	//port, err:= strconv.ParseUint(sPort, 10, 64)
	//if err != nil {
	//	fmt.Println(err)
	//	os.Exit(-1)
	//}
	//_, err = cli.GetNodeId(&mspb.GetNodeIdRequest{Header: mspb.RequestHeader{}, ServerPort: port})
	//if err != nil {
	//	fmt.Println(err)
	//	os.Exit(-1)
	//}
	// login
	//cli.NodeLogin(&mspb.NodeLoginRequest{Header: mspb.RequestHeader{}, NodeId: gResp.GetNodeId()})

	// heartbeat

	l, err := net.Listen("tcp", svr.rpcAddr)
	if err != nil {
		fmt.Println("listener port error:", err)
		return
	}
	fmt.Println("start to listener port :", svr.rpcAddr)
	svr.l = l
	svr.wg.Add(1)
	// A common pattern is to start a loop to continously accept connections
	for {
		//accept connections using Listener.Accept()
		c, err := l.Accept()
		if err != nil {
			fmt.Println("tcp accept error" +  err.Error())
			svr.wg.Done()
			return
		}
		//It's common to handle accepted connection on different goroutines
		svr.wg.Add(1)
		svr.count++
		svr.lock.Lock()
		svr.conns[svr.count] = c
		svr.lock.Unlock()
		fmt.Println("===============new connect,", svr.count)
		go svr.handleConnection(svr.count, c)
	}
}

func (svr *DsRpcServer) RangeSplit(oldRng  *metapb.Range,newRng *metapb.Range){

	svr.SetRange(oldRng)
	svr.SetRange(newRng)
	svr.childRngs[oldRng.GetId()] = newRng
}

func (svr *DsRpcServer) Stop() {
	if svr == nil{
		return
	}
	if svr.l != nil {
		svr.l.Close()
	}
	svr.lock.RLock()
	for _, c := range svr.conns {
		c.Close()
	}
	svr.lock.RUnlock()
	svr.conns = make(map[uint64]net.Conn)
	svr.wg.Wait()

	// TODO clear data
	svr.store.Close()

}

const (
	dialTimeout = 5 * time.Second
	// 128 KB
	DefaultInitialWindowSize int32 = 1024 * 64
	DefaultPoolSize          int   = 1

	// 40 KB
	DefaultWriteSize = 40960
	// 4 MB
	DefaultReadSize = 1024 * 1024 * 4
)

func (svr *DsRpcServer) handleConnection(id uint64, c net.Conn) {
	defer func () {
		fmt.Println("================connect closed")
		c.Close()
		svr.lock.Lock()
		delete(svr.conns, id)
		svr.lock.Unlock()
		svr.wg.Done()
	}()
	writer := bufio.NewWriterSize(c, DefaultWriteSize)
	reader := bufio.NewReaderSize(c, DefaultReadSize)

	message := &dsClient.Message{}
	for {
		err := util.ReadMessage(reader, message)
		if err != nil {
			fmt.Println("+++++++++++ read message failed ", err)
			return
		}
		//fmt.Println("===========read message ", message)
		svr.do(message)
		err = util.WriteMessage(writer, message)
		if err != nil {
			fmt.Println("+++++++++++ write message failed ", err)
			return
		}
		writer.Flush()
	}
}

func (svr *DsRpcServer) createRange(msg *dsClient.Message) {
	var resp *schpb.CreateRangeResponse
	req := new(schpb.CreateRangeRequest)
	err := proto.Unmarshal(msg.GetData(), req)
	if err != nil {
		resp = &schpb.CreateRangeResponse{Header: &schpb.ResponseHeader{Error: &errorpb.Error{Message: "create range failed"}}}
	} else {
		svr.SetRange(req.GetRange())
		resp = &schpb.CreateRangeResponse{Header: &schpb.ResponseHeader{}}
	}
	data, _ := proto.Marshal(resp)
	msg.SetMsgType(0x11)
	msg.SetData(data)
}

func (svr *DsRpcServer) insert(msg *dsClient.Message) {
	var resp *kvrpcpb.DsInsertResponse
	req := new(kvrpcpb.DsInsertRequest)
	err := proto.Unmarshal(msg.GetData(), req)
	if err != nil {
		resp = &kvrpcpb.DsInsertResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: "decode insert failed"}}}
	} else {
		rangeId := req.Header.GetRangeId()
		rng :=svr.GetRange(rangeId)
		if rng == nil {
			resp = &kvrpcpb.DsInsertResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message:"no exist range", NotLeader: &errorpb.NotLeader{RangeId: rangeId}}}}
		} else {
			rngEpoch := req.Header.GetRangeEpoch()

			if rng.RangeEpoch.Version == rngEpoch.Version && rng.RangeEpoch.ConfVer == rngEpoch.ConfVer {
				num := 0
				for _,row := range req.GetReq().Rows {
					err := svr.store.Put(row.Key,row.Value)
					if err == nil {
						num++
					}

				}

				resp = &kvrpcpb.DsInsertResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.InsertResponse{Code: 0, AffectedKeys: uint64(num)}}
			}else{
				resp = &kvrpcpb.DsInsertResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.InsertResponse{Code: 1, AffectedKeys: uint64(0)}}

				staleErr := &errorpb.Error{
					StaleEpoch: &errorpb.StaleEpoch{OldRange:rng,NewRange:svr.childRngs[rangeId]},
				}
				resp.Header.Error = staleErr
			}
		}
	}
	data, _ := proto.Marshal(resp)
	msg.SetMsgType(0x12)
	msg.SetData(data)
}

/**
select all
 */
func (svr *DsRpcServer) query(msg *dsClient.Message) {
	var resp *kvrpcpb.DsSelectResponse
	req := new(kvrpcpb.DsSelectRequest)
	err := proto.Unmarshal(msg.GetData(), req)

	if err != nil {
		resp = &kvrpcpb.DsSelectResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: "decode select failed"}}}
	}else{
		rngEpoch := req.Header.GetRangeEpoch()
		rangeId := req.Header.GetRangeId()
		rng := svr.GetRange(rangeId)
		log.Info("query range:%d,startKey:%v,endKey:%v",rangeId,rng.StartKey,rng.EndKey)
		if rng.RangeEpoch.Version == rngEpoch.Version && rng.RangeEpoch.ConfVer == rngEpoch.ConfVer {
			
			it := svr.store.NewIterator(rng.StartKey, rng.EndKey)
			rows := make([]*kvrpcpb.Row, 0)

			for it.Next() {
				fields := make([]byte,0)
				row := new(kvrpcpb.Row)
				row.Key = it.Key()
				pks := rng.PrimaryKeys
				buf := it.Key()[9:]//drop table prefix
				var v []byte

				for _,pk := range pks {

					buf,v,_ =commonUtil.DecodePrimaryKey(buf,pk)

					log.Debug("pk v:%v %v ",v,row.Key)

					fields,err =commonUtil.EncodeColumnValue(fields,pk,v)
					if err != nil{
						log.Error("encode pk err:%s",err.Error())
					}
				}
				//value可能无序
				fieldBuf := it.Value()
				var colId uint32
				var typ encoding.Type
				var val []byte
				var max uint32 = 0
				filedMap:=make(map[uint32][]byte)
				for len(fieldBuf) > 0 {
					filedCodeBuf := make([]byte,0)
					fieldBuf,colId,val,typ,err =commonUtil.DecodeValue2(fieldBuf)
					log.Debug("decode field colId:%d,val:%v,type:%d",colId,val,typ)
					filedCodeBuf,_ = commonUtil.EncodeValue2(filedCodeBuf,colId,typ,val)
					filedMap[colId]=filedCodeBuf
					if colId > max {
						max = colId
					}
				}

				for i:=uint32(0);i<=max;i++{
					if v,ok := filedMap[i];ok{
						fields = append(fields,v...)
					}
				}

				row.Fields = fields
				rows = append(rows, row)
			}

			defer it.Release()
			resp = &kvrpcpb.DsSelectResponse{Header: &kvrpcpb.ResponseHeader{}}
			resp.Resp = &kvrpcpb.SelectResponse{}
			resp.Resp.Rows = rows
			resp.Resp.Offset = uint64(len(rows))
		}else{
			resp = &kvrpcpb.DsSelectResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.SelectResponse{Code: 1,}}

			staleErr := &errorpb.Error{
				StaleEpoch: &errorpb.StaleEpoch{OldRange:rng,NewRange:svr.childRngs[rangeId]},
			}
			resp.Header.Error = staleErr
		}

	}

	data, _ := proto.Marshal(resp)
	msg.SetMsgType(0x12)
	msg.SetData(data)
}

/**
delete one key or delete all
 */
func (svr *DsRpcServer) delete(msg *dsClient.Message) {
	var resp *kvrpcpb.DsDeleteResponse
	req := new(kvrpcpb.DsDeleteRequest)
	err := proto.Unmarshal(msg.GetData(), req)

	if err != nil {
		resp = &kvrpcpb.DsDeleteResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: "decode delete failed"}}}
	}else{
		rngEpoch := req.Header.GetRangeEpoch();
		rangeId := req.Header.GetRangeId()
		rng := svr.GetRange(rangeId)
		if rng.RangeEpoch.Version == rngEpoch.Version && rng.RangeEpoch.ConfVer == rngEpoch.ConfVer {

			key := req.Req.GetKey()
			if key != nil {
				num :=0
				err:=svr.store.Delete(key)
				if err != nil {
					log.Error("delete key:%v err %s",key,err.Error())
				}else{
					num++
				}
				resp = &kvrpcpb.DsDeleteResponse{Header: &kvrpcpb.ResponseHeader{},Resp:&kvrpcpb.DeleteResponse{AffectedKeys:uint64(num)}}
			}else{
				it := svr.store.NewIterator(rng.StartKey, rng.EndKey)

				num :=0
				for it.Next() {
					err := svr.store.Delete(it.Key())
					if err != nil {
						log.Error("delete key:%v err %s",it.Key(),err.Error())
					}else{
						num++
					}

				}
				resp = &kvrpcpb.DsDeleteResponse{Header: &kvrpcpb.ResponseHeader{},Resp:&kvrpcpb.DeleteResponse{AffectedKeys:uint64(num)}}
			}



		}else{
			resp = &kvrpcpb.DsDeleteResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.DeleteResponse{Code: 1,}}

			staleErr := &errorpb.Error{
				StaleEpoch: &errorpb.StaleEpoch{OldRange:rng,NewRange:svr.childRngs[rangeId]},
			}
			resp.Header.Error = staleErr
		}

	}

	data, _ := proto.Marshal(resp)
	msg.SetMsgType(0x12)
	msg.SetData(data)

}

func (svr *DsRpcServer) kvSet(msg *dsClient.Message) {
	var resp *kvrpcpb.DsKvSetResponse
	req := new(kvrpcpb.DsKvSetRequest)
	err := proto.Unmarshal(msg.GetData(), req)
	if err != nil {
		resp = &kvrpcpb.DsKvSetResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: "insert failed"}}}
	} else {
		// TODO set
		switch req.GetReq().GetCase() {
		case kvrpcpb.ExistCase_EC_NotExists:
			kv := req.GetReq().GetKv()
			value, err := svr.store.Get(kv.GetKey())
			if err != nil || value == nil {
				err = svr.store.Put(kv.GetKey(), kv.GetValue())
				if err != nil {
					resp = &kvrpcpb.DsKvSetResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: err.Error()}}}
					goto end
				}
				resp = &kvrpcpb.DsKvSetResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.KvSetResponse{Code: 0, AffectedKeys: 0}}
			} else {
				// 已经存在
				resp = &kvrpcpb.DsKvSetResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.KvSetResponse{Code: -1, AffectedKeys: 0}}
			}
		case kvrpcpb.ExistCase_EC_AnyCase:
			var affectedKeys uint64
			kv := req.GetReq().GetKv()
			value, err := svr.store.Get(kv.GetKey())
			if err != nil || value == nil {
				affectedKeys = 0
			} else {
				// 已经存在
				affectedKeys = 1
			}
			err = svr.store.Put(kv.GetKey(), kv.GetValue())
			if err != nil {
				resp = &kvrpcpb.DsKvSetResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: err.Error()}}}
				goto end
			}
			resp = &kvrpcpb.DsKvSetResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.KvSetResponse{Code: 0, AffectedKeys: affectedKeys}}
		case kvrpcpb.ExistCase_EC_Force:
			kv := req.GetReq().GetKv()
			err = svr.store.Put(kv.GetKey(), kv.GetValue())
			if err != nil {
				resp = &kvrpcpb.DsKvSetResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: err.Error()}}}
				goto end
			}
			resp = &kvrpcpb.DsKvSetResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.KvSetResponse{Code: 0, AffectedKeys: 0}}
		default:
			resp = &kvrpcpb.DsKvSetResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: "insert case"}}}
		}
	}

	end:
	data, _ := proto.Marshal(resp)
	msg.SetMsgType(0x12)
	msg.SetData(data)
}

func (svr *DsRpcServer) kvGet(msg *dsClient.Message) {
	var resp *kvrpcpb.DsKvGetResponse
	req := new(kvrpcpb.DsKvGetRequest)
	err := proto.Unmarshal(msg.GetData(), req)
	if err != nil {
		resp = &kvrpcpb.DsKvGetResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: "insert failed"}}}
	} else {
		value, _ := svr.store.Get(req.GetReq().GetKey())
		//if err != nil {
		//	resp = &kvrpcpb.DsKvGetResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.KvGetResponse{Code: -1}}
		//} else {
		//	resp = &kvrpcpb.DsKvGetResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.KvGetResponse{Value: value}}
		//}
		resp = &kvrpcpb.DsKvGetResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.KvGetResponse{Value: value}}
	}
	data, _ := proto.Marshal(resp)
	msg.SetMsgType(0x12)
	msg.SetData(data)
}

func (svr *DsRpcServer) kvBatchSet(msg *dsClient.Message) {
	var resp *kvrpcpb.DsKvBatchSetResponse
	req := new(kvrpcpb.DsKvBatchSetRequest)
	err := proto.Unmarshal(msg.GetData(), req)
	if err != nil {
		resp = &kvrpcpb.DsKvBatchSetResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: "insert failed"}}}
	} else {
		// TODO set
		switch req.GetReq().GetCase() {
		case kvrpcpb.ExistCase_EC_NotExists:
			var flag bool
			batch := svr.store.NewBatch()
			for _, kv := range req.GetReq().GetKvs() {
				value, err := svr.store.Get(kv.GetKey())
				if err != nil || value == nil {
					batch.Put(kv.GetKey(), kv.GetValue())
				} else {
					// 已经存在
					flag = true
				}
			}
			if flag {
				resp = &kvrpcpb.DsKvBatchSetResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.KvBatchSetResponse{Code: -1, AffectedKeys: 0}}
			} else {
				err = batch.Commit()
				if err != nil {
					resp = &kvrpcpb.DsKvBatchSetResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: err.Error()}}}
					goto end
				}
				resp = &kvrpcpb.DsKvBatchSetResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.KvBatchSetResponse{Code: 0, AffectedKeys: 0}}
			}
		case kvrpcpb.ExistCase_EC_AnyCase:
			var affectedKeys uint64
			batch := svr.store.NewBatch()
			for _, kv := range req.GetReq().GetKvs() {
				value, _ := svr.store.Get(kv.GetKey())
				if value != nil {
					// 已经存在
					affectedKeys++
				}
				batch.Put(kv.GetKey(), kv.GetValue())
			}
			err = batch.Commit()
			if err != nil {
				resp = &kvrpcpb.DsKvBatchSetResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: err.Error()}}}
				goto end
			}

			resp = &kvrpcpb.DsKvBatchSetResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.KvBatchSetResponse{Code: 0, AffectedKeys: affectedKeys}}
		case kvrpcpb.ExistCase_EC_Force:
			batch := svr.store.NewBatch()
			for _, kv := range req.GetReq().GetKvs() {
				batch.Put(kv.GetKey(), kv.GetValue())
			}
			err = batch.Commit()
			if err != nil {
				resp = &kvrpcpb.DsKvBatchSetResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: err.Error()}}}
				goto end
			}

			resp = &kvrpcpb.DsKvBatchSetResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.KvBatchSetResponse{Code: 0, AffectedKeys: 0}}
		default:
			resp = &kvrpcpb.DsKvBatchSetResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: "insert case"}}}
		}
	}

	end:
	data, _ := proto.Marshal(resp)
	msg.SetMsgType(0x12)
	msg.SetData(data)
}

func (svr *DsRpcServer) kvBatchGet(msg *dsClient.Message) {
	var resp *kvrpcpb.DsKvBatchGetResponse
	req := new(kvrpcpb.DsKvBatchGetRequest)
	err := proto.Unmarshal(msg.GetData(), req)
	if err != nil {
		resp = &kvrpcpb.DsKvBatchGetResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: "insert failed"}}}
	} else {
		var kvs []*kvrpcpb.RedisKeyValue
		for _, key := range req.GetReq().GetKeys() {
			value, _ := svr.store.Get(key)
			kvs = append(kvs, &kvrpcpb.RedisKeyValue{Key: key, Value: value})
		}
		resp = &kvrpcpb.DsKvBatchGetResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.KvBatchGetResponse{Kvs: kvs}}
	}
	data, _ := proto.Marshal(resp)
	msg.SetMsgType(0x12)
	msg.SetData(data)
}

func (svr *DsRpcServer) kvScan(msg *dsClient.Message) {
	var resp *kvrpcpb.DsKvScanResponse
	req := new(kvrpcpb.DsKvScanRequest)
	err := proto.Unmarshal(msg.GetData(), req)
	if err != nil {
		resp = &kvrpcpb.DsKvScanResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: "insert failed"}}}
	} else {
		var count int64
		var kvs []*kvrpcpb.RedisKeyValue
		var lastKey []byte
		maxCount := req.GetReq().GetMaxCount()
		iter := svr.store.NewIterator(req.GetReq().GetStart(), req.GetReq().GetLimit())
		defer iter.Release()
		for iter.Next() {
			count++
			if req.GetReq().GetKeyOnly() {
				key := make([]byte, len(iter.Key()))
				copy(key, iter.Key())
				kvs = append(kvs, &kvrpcpb.RedisKeyValue{Key: key})
			} else if req.GetReq().GetCountOnly() {
				// do nothing
			} else {
				key := make([]byte, len(iter.Key()))
				copy(key, iter.Key())
				value := make([]byte, len(iter.Value()))
				copy(value, iter.Value())
				kvs = append(kvs, &kvrpcpb.RedisKeyValue{Key: key, Value: value})
			}
			if maxCount > 0 && count == maxCount {
				lastKey = make([]byte, len(iter.Key()))
				copy(lastKey, iter.Key())
				break
			}
		}
		if maxCount > 0 && kvs != nil {
			if int64(len(kvs)) < maxCount {
				lastKey = kvs[len(kvs)-1].GetKey()
			}
		}
		resp = &kvrpcpb.DsKvScanResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.KvScanResponse{Count: count, Kvs: kvs, LastKey: lastKey}}
	}
	data, _ := proto.Marshal(resp)
	msg.SetMsgType(0x12)
	msg.SetData(data)
}

func (svr *DsRpcServer) kvDel(msg *dsClient.Message) {
	var resp *kvrpcpb.DsKvDeleteResponse
	req := new(kvrpcpb.DsKvDeleteRequest)
	err := proto.Unmarshal(msg.GetData(), req)
	if err != nil {
		resp = &kvrpcpb.DsKvDeleteResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: "insert failed"}}}
	} else {
		key := req.GetReq().GetKey()
		value, err := svr.store.Get(key)
		if err != nil || value == nil {
			resp = &kvrpcpb.DsKvDeleteResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.KvDeleteResponse{Code: -1, AffectedKeys: 0}}
		} else {
			// 已经存在
			err = svr.store.Delete(key)
			if err != nil {
				resp = &kvrpcpb.DsKvDeleteResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: err.Error()}}}
				goto end
			}
			resp = &kvrpcpb.DsKvDeleteResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.KvDeleteResponse{Code: 0, AffectedKeys: 1}}
		}
	}

	end:
	data, _ := proto.Marshal(resp)
	msg.SetMsgType(0x12)
	msg.SetData(data)
}

func (svr *DsRpcServer) kvBatchDel(msg *dsClient.Message) {
	var resp *kvrpcpb.DsKvBatchDeleteResponse
	req := new(kvrpcpb.DsKvBatchDeleteRequest)
	err := proto.Unmarshal(msg.GetData(), req)
	if err != nil {
		resp = &kvrpcpb.DsKvBatchDeleteResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: "insert failed"}}}
	} else {
		batch := svr.store.NewBatch()
		var affectedKeys uint64
		for _, key := range req.GetReq().GetKeys() {
			value, _ := svr.store.Get(key)
			if value != nil {
				// 已经存在
				affectedKeys++
				batch.Delete(key)
			}
		}
		err = batch.Commit()
		if err != nil {
			resp = &kvrpcpb.DsKvBatchDeleteResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: err.Error()}}}
			goto end
		}
		resp = &kvrpcpb.DsKvBatchDeleteResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.KvBatchDeleteResponse{Code: 0, AffectedKeys: affectedKeys}}
	}

	end:
	data, _ := proto.Marshal(resp)
	msg.SetMsgType(0x12)
	msg.SetData(data)
}

func (svr *DsRpcServer) kvRangeDel(msg *dsClient.Message) {
	var resp *kvrpcpb.DsKvRangeDeleteResponse
	req := new(kvrpcpb.DsKvRangeDeleteRequest)
	err := proto.Unmarshal(msg.GetData(), req)
	if err != nil {
		resp = &kvrpcpb.DsKvRangeDeleteResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: "insert failed"}}}
	} else {
		var count int64
		var lastKey []byte
		maxCount := req.GetReq().GetMaxCount()
		batch := svr.store.NewBatch()
		iter := svr.store.NewIterator(req.GetReq().GetStart(), req.GetReq().GetLimit())
		defer iter.Release()
		for iter.Next() {
			batch.Delete(iter.Key())
			count++
			lastKey = make([]byte, len(iter.Key()))
			copy(lastKey, iter.Key())
			if maxCount > 0 && count == maxCount {
				break
			}
		}
		err = batch.Commit()
		if err != nil {
			resp = &kvrpcpb.DsKvRangeDeleteResponse{Header: &kvrpcpb.ResponseHeader{Error: &errorpb.Error{Message: err.Error()}}}
		} else {
			resp = &kvrpcpb.DsKvRangeDeleteResponse{Header: &kvrpcpb.ResponseHeader{}, Resp: &kvrpcpb.KvRangeDeleteResponse{Code: 0, AffectedKeys: uint64(count), LastKey: lastKey}}
		}
	}
	data, _ := proto.Marshal(resp)
	msg.SetMsgType(0x12)
	msg.SetData(data)
}

func (svr *DsRpcServer)do(msg *dsClient.Message) {
	switch funcpb.FunctionID(msg.GetFuncId()) {
	case funcpb.FunctionID_kFuncCreateRange:
		svr.createRange(msg)
	case funcpb.FunctionID_kFuncInsert:
		svr.insert(msg)
	case funcpb.FunctionID_kFuncSelect:
		svr.query(msg)
	case funcpb.FunctionID_kFuncDelete:
		svr.delete(msg)
	case funcpb.FunctionID_kFuncKvSet:
		svr.kvSet(msg)
	case funcpb.FunctionID_kFuncKvGet:
		svr.kvGet(msg)
	case funcpb.FunctionID_kFuncKvBatchSet:
		svr.kvBatchSet(msg)
	case funcpb.FunctionID_kFuncKvBatchGet:
		svr.kvBatchGet(msg)
	case funcpb.FunctionID_kFuncKvDel:
		svr.kvDel(msg)
	case funcpb.FunctionID_kFuncKvBatchDel:
		svr.kvBatchDel(msg)
	case funcpb.FunctionID_kFuncKvRangeDel:
		svr.kvRangeDel(msg)
	case funcpb.FunctionID_kFuncKvScan:
		svr.kvScan(msg)
	case funcpb.FunctionID_kFuncHeartbeat:
		msg.SetMsgType(0x12)
	}
	return
}
