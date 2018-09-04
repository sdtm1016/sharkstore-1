package client

import (
	"google.golang.org/grpc"
	"model/pkg/mspb"
	"util/assert"
	"testing"
	"context"
	"time"
	"util/log"
	"sync"
	"sync/atomic"
)

//func TestNewClient(t *testing.T) {
//	client,err := NewClient([]string{"192.168.61.136:8887"})
//	if err != nil {
//		fmt.Errorf("%v",err)
//	}
//
//	resp,err := client.AddNode(&mspb.AddNodeRequest{
//		Header:&mspb.RequestHeader{
//			ClusterId:1,
//		},
//		ServerPort:8887,
//		HeartbeatPort:8877,
//		ReplicaPort:8867,
//	})
//
//	fmt.Println(resp)
//}

func TestRPCClient_GetRoute(t *testing.T) {
	conn, err := grpc.Dial("127.0.0.1:18887", grpc.WithInsecure(), grpc.WithTimeout(ConnectMSTimeout))
	if err != nil {
		t.Errorf("did not connect addr[%s], err[%v]", "127.0.0.1:18887", err)
		return
	}
	cli := mspb.NewMsServerClient(conn)
	req := &mspb.GetRouteRequest{
		Header:  &mspb.RequestHeader{},
		DbId:    1,
		TableId: 1,
		Key:     []byte{1},
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	response, err := cli.GetRoute(ctx, req)
	if err != nil {
		t.Logf("error : %v", err)
		return
	}
	t.Logf("response: %v", response)
}

func TestRPCClient_GetAutoIncId(t *testing.T) {
	log.InitFileLog("D:/logs", "proxy_rpc", "debug")
	conn, err := grpc.Dial("192.168.150.122:18887", grpc.WithInsecure(), grpc.WithTimeout(ConnectMSTimeout))
	if err != nil {
		t.Errorf("did not connect addr[%s], err[%v]", "192.168.150.122:18887", err)
		return
	}
	cli := mspb.NewMsServerClient(conn)

	type Stat struct {
		lastCount int64
		preCount  int64
		errCount  int64
	}
	stat := &Stat{}
	time.Sleep(1000)
	go func() {
		defer func() {
			log.Error("cal err ")
			if err := recover(); err != nil {
				log.Error("%v", err)
			}
		}()
		ticker := time.Tick(1 * time.Second)

		preTime := time.Now().UnixNano()
		for now := range ticker {
			total := atomic.LoadInt64(&stat.lastCount)
			log.Error("%s: total:%d, ops:%d, err:%d \n",
				time.Now().Format("2006-01-02T15:04:05"),
				total, (total-stat.preCount)*1000*1000*1000/(now.UnixNano()-preTime), stat.errCount)
			stat.preCount = total
			preTime = now.UnixNano()
		}
	}()

	req := &mspb.GetAutoIncIdRequest{
		Header:  &mspb.RequestHeader{},
		DbId:    161,
		TableId: 1705,
		Size_:   uint32(1),
	}
	maxGoRoutine := make(chan int, 200)
	for loop:=0;;loop++{
		maxGoRoutine <- 1
		go func(loop int) {
			startT := time.Now()
			defer func() {
				<-maxGoRoutine
			}()
			ctx, _ := context.WithTimeout(context.Background(), time.Second)
			response, err := cli.GetAutoIncId(ctx, req)
			if err != nil {
				t.Logf("loop: %v error : %v", loop, err)
				atomic.AddInt64(&stat.errCount, 1)
				return
			}
			log.Info("auto_increment time: %v", time.Since(startT).Nanoseconds()/1000000)
			assert.Equal(t, uint32(len(response.GetIds())), req.Size_, "return size error")
			atomic.AddInt64(&stat.lastCount, 1)
		}(loop)
	}

}

var wg sync.WaitGroup  //定义一个同步等待的组
func TestRPCClient_GetMsLeader(t *testing.T) {
	conn, err := grpc.Dial("192.168.183.66:18887", grpc.WithInsecure(), grpc.WithTimeout(ConnectMSTimeout))
	if err != nil {
		t.Errorf("did not connect addr[%s], err[%v]", "192.168.183.66:18887", err)
		return
	}
	cli := mspb.NewMsServerClient(conn)
	req := &mspb.GetMSLeaderRequest{
		Header:  &mspb.RequestHeader{},
	}
	for loop:=0;;loop++{
		if loop %300 == 0 {
			time.Sleep(time.Duration(time.Second))
		}
		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		wg.Add(1)
		go func(loop int) {
			defer wg.Done()
			response, err := cli.GetMSLeader(ctx, req)
			if err != nil {
				t.Logf("error : %v", err)
				return
			}
			if response.GetLeader().GetId() == 0 {
				t.Logf("error : %v", err)
				return
			}
			log.Info("loop %v info %v", loop, response)

		}(loop)
	}
	wg.Wait()
}