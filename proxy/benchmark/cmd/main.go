package main

import (
	"net"
	"strings"
	"flag"
	"runtime"
	"math/rand"
	"hash/fnv"
	"sync/atomic"
	"time"
	"fmt"

	"util/log"
	"util/gogc"
	"proxy/gateway-server/server"
)

var (
	configFileName = flag.String("config", "", "Usage : -config conf/config.toml")
)

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU() - 1)

	// load bench mark config file
	conf := new(server.Config)
	conf.LoadConfig(configFileName)
	log.InitFileLog(conf.Log.Dir, conf.Log.Module, conf.Log.Level)
	srv, err := server.NewServer(conf)
	if err != nil {
		log.Fatal("init server failed, err[%v]", err)
		return
	}
	if conf.BenchConfig.Type > 0 {
		go benchmark(srv)
	}

	go gogc.TickerPrintGCSummary(log.GetFileLogger(), "info")
	srv.Run()
}

/**

*for benchmark
*/

//=================================================================

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func randomString(length int) string {
	str := "!@#$^&*()_+<>?:{}|;.,/][-=0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < length; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}

type Stat struct {
	lastCount int64
	preCount  int64
	errCount  int64

	lastSelCount int64
	preSelCount  int64
	errSelCount  int64
}

var api *server.SharkStoreApi

var stat *Stat

func benchmark(s *server.Server) {
	api = &server.SharkStoreApi{}
	stat = &Stat{}
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
			selTotal := atomic.LoadInt64(&stat.lastSelCount)
			log.Error("%s:total:%d,ops:%d ,err:%d \n sel:total:%d,ops:%d ,err:%d \n", time.Now().Format("2006-01-02T15:04:05"),
				total, (total-stat.preCount)*1000*1000*1000/(now.UnixNano()-preTime), stat.errCount, selTotal,
				(selTotal-stat.preSelCount)*1000*1000*1000/(now.UnixNano()-preTime), stat.errSelCount)
			stat.preCount = total
			stat.preSelCount = selTotal
			preTime = now.UnixNano()
		}
	}()
	ip := getIp()
	for concur := 0; concur < s.GetCfg().BenchConfig.Threads; concur++ {
		if s.GetCfg().BenchConfig.Type&1 == 1 {
			go insertTestData(s, concur, 10000, ip)
		}
		if s.GetCfg().BenchConfig.Type&2 == 2 {
			go selectTest(s, concur, 10000, ip)
		}
	}
}

func getIp() string {
	ip := "127.0.0.1"
	adders, err := net.InterfaceAddrs()
	if err == nil {
		for _, addr := range adders {
			curIp := addr.String()
			if strings.HasPrefix(curIp, "172") || strings.HasPrefix(curIp, "10") || strings.HasPrefix(curIp, "11") {
				ip = curIp
				break
			}
		}
	}
	return ip
}

func selectTest(s *server.Server, threadNo, total int, ip string) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	fields := []string{"h", "user_name", "pass_word", "real_name"}
	pks := make(map[string]interface{})
	for {
		no := r.Intn(s.GetCfg().BenchConfig.SendNum)
		user_name := getUserName(no, ip, threadNo)
		h := hash(user_name) % 16384
		pks["user_name"] = user_name
		pks["h"] = h
		reply := api.Select(s, s.GetCfg().BenchConfig.DB, s.GetCfg().BenchConfig.Table, fields, pks)
		log.Debug("%v", reply)
		if reply.Code == 0 && len(reply.Values) > 0 {
			atomic.AddInt64(&stat.lastSelCount, 1)
		} else {
			atomic.AddInt64(&stat.errSelCount, 1)
			if reply.Code == 0 {
				log.Debug("%v", reply)
			} else {
				log.Warn("%v", reply)
			}

		}
	}

}

func getUserName(no int, ip string, threadNo int) string {
	return fmt.Sprintf("%d-%s-%d", no, ip, threadNo)
}

func insertTestData(s *server.Server, threadNo, total int, ip string) {

	fields := []string{"h", "user_name", "pass_word", "real_name"}

	real_name := randomString(s.GetCfg().BenchConfig.DataLen)
	pass_word := "pw"
	var i int

	for i = 0; i < s.GetCfg().BenchConfig.SendNum; i++ {
		rows := make([][]interface{}, 0)

		user_name := getUserName(i, ip, threadNo)
		h := hash(user_name) % 16384
		row := make([]interface{}, 0)
		row = append(row, h)
		row = append(row, user_name)
		row = append(row, pass_word)
		row = append(row, real_name)

		rows = append(rows, row)

		for b := 1; b < s.GetCfg().BenchConfig.Batch; b++ {
			row := make([]interface{}, 0)
			row = append(row, h)
			row = append(row, fmt.Sprintf("%s-%d", user_name, b))
			row = append(row, pass_word)
			row = append(row, real_name)
			rows = append(rows, row)
		}

		reply := api.Insert(s, s.GetCfg().BenchConfig.DB, s.GetCfg().BenchConfig.Table, fields, rows)
		log.Debug("%v", reply)
		if reply.Code == 0 {
			atomic.AddInt64(&stat.lastCount, 1)
		} else {
			atomic.AddInt64(&stat.errCount, 1)
			i = i - 1
			log.Warn("%v", reply)
		}
	}

}
