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
	"errors"

	"util/log"
	"util/gogc"
	"proxy/gateway-server/server"
	"model/pkg/metapb"
	"util"
	"proxy/benchmark/blob_store"
	"sync"
)

var (
	configFileName = flag.String("config", "", "Usage : -config conf/config.toml")

	TableFields = []string{"h", "user_name", "pass_word", "real_name"}
	HField      = []string{"h"}
	UField      = []string{"user_name"}
	PField      = []string{"pass_word"}

	tableId uint64 = 3
	//tableId uint64 = 209

	//blob db leavel
	path = "/export/home/admin/source/fbase/src/data-server/build/db/data/blob_dir"
	//path := "/Users/wumeijun/Documents"
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

	benchType := conf.BenchConfig.Type
	if benchType > 0 {
		go benchmark(srv)
	} else if benchType < 0 {
		go checkSelectWhenDsRestarting(srv)
	} else {
		go rangeChecking(srv)
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

	lastUpdCount int64
	preUpdCount  int64
	errUpdCount  int64

	delCount int64
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
			updTotal := atomic.LoadInt64(&stat.lastUpdCount)
			log.Error("%s: total:%d, ops:%d, err:%d \n sel: total:%d, ops:%d,err:%d \n upd: total:%d, ops:%d, err:%d \n  del: total:%d",
				time.Now().Format("2006-01-02T15:04:05"),
				total, (total-stat.preCount)*1000*1000*1000/(now.UnixNano()-preTime), stat.errCount,
				selTotal, (selTotal-stat.preSelCount)*1000*1000*1000/(now.UnixNano()-preTime), stat.errSelCount,
				updTotal, (updTotal-stat.preUpdCount)*1000*1000*1000/(now.UnixNano()-preTime), stat.errUpdCount,
				stat.delCount)
			stat.preCount = total
			stat.preSelCount = selTotal
			stat.preUpdCount = updTotal
			preTime = now.UnixNano()
		}
	}()
	ip := getIp()

	//insert data
	if s.GetCfg().BenchConfig.Type == 1 {
		for concur := 0; concur < s.GetCfg().BenchConfig.Threads; concur++ {
			go insertTestData(s, concur, 10000, ip)
		}
		log.Info("insert completed.")
	}
	//select data
	if s.GetCfg().BenchConfig.Type == 2 {
		for concur := 0; concur < s.GetCfg().BenchConfig.Threads; concur++ {
			go selectTest(s, concur, 10000, ip)
		}
	}

	//correct and concurrent check
	if s.GetCfg().BenchConfig.Type == 3 {
		// when select, check elapsed time and correctness after updating
		go correctCheck4Update(s)
	}

	if s.GetCfg().BenchConfig.Type == 4 {
		// when select, check elapsed time and correctness after deleting
		// when select, check elapsed time and correctness after inserting
		go correctCheck4DelAndInsert(s)
	}

	//check the elapsed time and correctness after deleting at blob_db level
	if s.GetCfg().BenchConfig.Type == 5 {
		go correct4BatchDelete(s)
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
	pks := make(map[string]interface{})
	for {
		no := r.Intn(s.GetCfg().BenchConfig.SendNum)
		user_name := getUserName(no, ip, threadNo)
		h := hash(user_name) % 16384
		pks["user_name"] = user_name
		pks["h"] = h
		reply := api.Select(s, s.GetCfg().BenchConfig.DB, s.GetCfg().BenchConfig.Table, TableFields, pks, nil)
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

// check correct when select after update
func correctCheck4Update(s *server.Server) {
	if s.GetCfg().BenchConfig.Scope < 1 || s.GetCfg().BenchConfig.Scope > 16384 {
		log.Fatal("bench config scope should be between 1 and 16384")
	}
	updateMsg := fmt.Sprintf("update message, %v", time.Now().Format("2006-01-02 15:04:05.000"))
	for i := 1; i <= s.GetCfg().BenchConfig.Scope; i++ {
		h := uint32(i - 1)
		userNames, err := selectSource(s, h)
		if err != nil || len(userNames) == 0 {
			log.Fatal("h %v no user", h)
		}
		log.Info("h: %v source data length: %v", h, len(userNames))
		threadNum := s.GetCfg().BenchConfig.Threads
		increase := len(userNames)/threadNum + 1
		for concur := 0; concur < threadNum; concur++ {
			start := concur * increase
			end := start + increase
			if start >= len(userNames) {
				break
			}
			if end > len(userNames) {
				end = len(userNames)
			}
			go func() {
				log.Debug("h: %v update data scope between %v and %v", h, start, end)
				subUserNames := userNames[start:end]
				for i := 0; i < len(subUserNames); i++ {
					reply := api.Insert(s, s.GetCfg().BenchConfig.DB, s.GetCfg().BenchConfig.Table, TableFields,
						createRows(h, subUserNames[i], updateMsg, updateMsg))
					if reply.Code == 0 {
						atomic.AddInt64(&stat.lastUpdCount, 1)
						checkElapsedTime(s, h, subUserNames[i], updateMsg)
					} else {
						atomic.AddInt64(&stat.errUpdCount, 1)
						i = i - 1
						log.Warn("h: %v update reply: %v, retry", h, reply)
					}
				}
			}()
		}
	}
}

// check correct when select after delete, update
func correctCheck4DelAndInsert(s *server.Server) {
	if s.GetCfg().BenchConfig.Scope < 1 || s.GetCfg().BenchConfig.Scope > 16384 {
		log.Fatal("bench config scope should be between 1 and 16384")
	}
	insertMsg := fmt.Sprintf("insert message, %v", time.Now().Format("2006-01-02 15:04:05.000"))
	for i := 1; i <= s.GetCfg().BenchConfig.Scope; i++ {
		h := uint32(i - 1)
		userNames, err := selectSource(s, h)
		if err != nil || len(userNames) == 0 {
			log.Fatal("h %v no user", h)
		}

		log.Info("h:%v source data length: %v", h, len(userNames))

		pks := make(map[string]interface{})
		pks["h"] = h
		reply := api.Delete(s, s.GetCfg().BenchConfig.DB, s.GetCfg().BenchConfig.Table, HField, pks)
		if reply.Code != 0 {
			log.Fatal("h: %v delete failed, %v", h, reply.Message)
		}

		t1 := time.Now()
		for {
			leavedData, err := selectSource(s, h)
			if err != nil {
				log.Fatal("h: %v select leave data after delete error, %v", h, err)
			}
			if len(leavedData) == 0 {
				break
			}
		}
		log.Info("h: %v, delete success, elapsed time: %v", h, time.Since(t1))
		atomic.AddInt64(&stat.delCount, int64(len(userNames)))

		threadNum := s.GetCfg().BenchConfig.Threads
		increase := len(userNames)/threadNum + 1
		for concur := 0; concur < threadNum; concur++ {
			start := concur * increase
			end := start + increase
			if start >= len(userNames) {
				break
			}
			if end > len(userNames) {
				end = len(userNames)
			}
			go func() {
				subUserNames := userNames[start:end]
				for i := 0; i < len(subUserNames); i++ {
					reply := api.Insert(s, s.GetCfg().BenchConfig.DB, s.GetCfg().BenchConfig.Table, TableFields,
						createRows(h, subUserNames[i], insertMsg, insertMsg))
					if reply.Code == 0 {
						atomic.AddInt64(&stat.lastUpdCount, 1)
						checkElapsedTime(s, h, subUserNames[i], insertMsg)
					} else {
						atomic.AddInt64(&stat.errUpdCount, 1)
						i = i - 1
						log.Warn("h %v update reply: %v, retry", h, reply)
					}
				}
			}()
		}
	}
}


//delete most data, and check the db data at rocksdb level
func correct4BatchDelete(s *server.Server) {
	if s.GetCfg().BenchConfig.Scope < 1 || s.GetCfg().BenchConfig.Scope > 16384 {
		log.Fatal("bench config scope should be between 1 and 16384")
	}
	temp := uint32(0)
	concurrentC := &temp
	var pkMap map[uint32][]string
	pkMap = make(map[uint32][]string, s.GetCfg().BenchConfig.Scope)

	var lock sync.Mutex

	threadNum := s.GetCfg().BenchConfig.Threads
	scope := s.GetCfg().BenchConfig.Scope // 1 ~ 16384
	increase := scope/threadNum + 1
	for concur := 0; concur < threadNum; concur++ {
		start := concur * increase
		end := start + increase
		if start >= scope {
			break
		}
		if end > scope {
			end = scope
		}
		log.Info("handle h scope: start: %v, end: %v", start, end)
		go func(cc *uint32) {
			for i := start; i < end; i++ {
				h := uint32(i)
				userNames, err := selectSource(s, h)
				if err != nil {
					log.Error("h: %v select user error %v", h, err)
					atomic.AddUint32(cc, 1)
					continue
				}
				if len(userNames) == 0 {
					atomic.AddUint32(cc, 1)
					log.Warn("h: %v  no user data", h)
					continue
				}
				lock.Lock()
				pkMap[h] = userNames
				lock.Unlock()
				log.Info("h: %v source data length: %v", h, len(userNames))

				time.Sleep(time.Second)
				go func(cc2 *uint32) {
					defer atomic.AddUint32(cc2, 1)
					deleteByH(s, h, len(userNames))
				}(cc)
			}
		}(concurrentC)
	}

	log.Info("start to wait concurrent finish")

	for {
		log.Info("loop ============, %v", atomic.LoadUint32(concurrentC))
		if int(atomic.LoadUint32(concurrentC)) == s.GetCfg().BenchConfig.Scope {
			break
		}
		time.Sleep(time.Second)
	}

	log.Info("concurrent %v", atomic.LoadUint32(concurrentC))

	time.Sleep(10 * time.Minute)

	keyMap := make(map[string]uint8, 0)
	value := uint8(0)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 5; i++ {
		tempH := uint32(r.Intn(s.GetCfg().BenchConfig.Scope))
		userNames, ok := pkMap[tempH]
		if !ok || len(userNames) == 0 {
			i--
			continue
		}
		for _, u := range userNames {
			key, err := getKey(tempH, u)
			if err != nil {
				continue
			}
			log.Debug("pk: h:%d, user:%s, key:%v", tempH, u, string(key))
			keyMap[string(key)] = value
		}
	}

	currentTime := time.Now()
	for {
		if err := blob_store.CheckKey(path, keyMap); err != nil {
			log.Error("check key after delete error: %v", err)
		} else {
			break
		}
		time.Sleep(2 * time.Minute)
	}
	log.Info("delete from blob_dir elapsed time %v", time.Since(currentTime))
}

func getKey(h uint32, userName string) ([]byte, error) {
	type Match struct {
		column    string
		sqlValue  []byte
		matchType int
	}
	bytes := []byte(fmt.Sprintf("%v", h))
	var prefix []byte
	var err error
	if prefix, err = util.EncodePrimaryKey(prefix,
		&metapb.Column{Name: "h", Id: 1, DataType: metapb.DataType_BigInt, PrimaryKey: 1, Index: true},
		bytes); err != nil {
		return nil, err
	}

	if prefix, err = util.EncodePrimaryKey(prefix,
		&metapb.Column{Name: "user_name", Id: 2, DataType: metapb.DataType_Varchar, PrimaryKey: 1, Index: true},
		[]byte(userName)); err != nil {
		return nil, err
	}
	prefix = append(util.EncodeStorePrefix(util.Store_Prefix_KV, tableId), prefix...)
	return prefix, nil
}

func deleteByH(s *server.Server, h uint32, sourceLength int) {
	pks := make(map[string]interface{})
	pks["h"] = h
	reply := api.Delete(s, s.GetCfg().BenchConfig.DB, s.GetCfg().BenchConfig.Table, HField, pks)
	if reply.Code != 0 {
		log.Fatal("h: %v delete failed, %v", h, reply)
	}

	t1 := time.Now()
	for {
		leavedData, err := selectSource(s, h)
		if err != nil {
			log.Warn("h: %v  select leave data after delete error, %v", h, reply)
			continue
		}
		if len(leavedData) == 0 {
			break
		}
	}
	log.Info("h: %v delete success, elapsed time: %v", h, time.Since(t1))
	atomic.AddInt64(&stat.delCount, int64(sourceLength))
}

func selectSource(s *server.Server, h uint32) ([]string, error) {
	pks := make(map[string]interface{})
	pks["h"] = h
	var userNames []string

	for i := 0; ; i++ {
		limit_ := &server.Limit_{
			Offset:   uint64(i * 10000),
			RowCount: 10000,
		}
		reply := api.Select(s, s.GetCfg().BenchConfig.DB, s.GetCfg().BenchConfig.Table, UField, pks, limit_)
		if reply.Code != 0 {
			return nil, errors.New(fmt.Sprintf("select h %v source data: %s", h, reply.Message))
		}
		if len(reply.Values) == 0 {
			break
		}
		for _, row := range reply.Values {
			for _, r := range row {
				userName := r.(string)
				userNames = append(userNames, userName)
			}
		}
	}
	return userNames, nil
}

func checkElapsedTime(s *server.Server, h uint32, userName, firstUpdate string) {
	currentTime := time.Now()
	flag := 0
	loop := 0
	for {
		loop++
		pks := make(map[string]interface{})
		pks["h"] = h
		pks["user_name"] = userName
		selectReply := api.Select(s, s.GetCfg().BenchConfig.DB, s.GetCfg().BenchConfig.Table, PField, pks, nil)
		if selectReply.Code == 0 && len(selectReply.Values) > 0 {
			var passWords []string
			for _, row := range selectReply.Values {
				for _, r := range row {
					passWord := r.(string)
					passWords = append(passWords, passWord)
				}
			}
			if len(passWords) > 1 {
				flag = len(passWords)
				break
			}

			if passWords[0] != firstUpdate {
				continue
			}
			break
		}
	}

	if flag > 0 {
		log.Error("select after update:  %v values size: %v, should be 1, loop: %v", userName, flag, loop)
	} else {
		log.Warn("select after update::   %v update correct , elapsed time: %v, loop: %v", userName, time.Since(currentTime), loop)
	}
}

func createRows(h uint32, userName, passWord, realName string) [][]interface{} {
	rows := make([][]interface{}, 0)
	row := createRow(h, userName, passWord, realName)
	rows = append(rows, row)
	return rows
}

func createRow(h uint32, userName, passWord, realName string) []interface{} {
	row := make([]interface{}, 0)
	row = append(row, h)
	row = append(row, userName)
	row = append(row, passWord)
	row = append(row, realName)
	return row
}

func getUserName(no int, ip string, threadNo int) string {
	return fmt.Sprintf("%d-%s-%d", no, ip, threadNo)
}

func insertTestData(s *server.Server, threadNo, total int, ip string) {
	real_name := randomString(s.GetCfg().BenchConfig.DataLen)
	pass_word := "pw"
	for i := 0; i < s.GetCfg().BenchConfig.SendNum; i++ {
		user_name := getUserName(i, ip, threadNo)
		h := hash(user_name) % 16384
		rows := createRows(h, user_name, pass_word, real_name)
		for b := 1; b < s.GetCfg().BenchConfig.Batch; b++ {
			row := createRow(h, fmt.Sprintf("%s-%d", user_name, b), pass_word, real_name)
			rows = append(rows, row)
		}

		for {
			reply := api.Insert(s, s.GetCfg().BenchConfig.DB, s.GetCfg().BenchConfig.Table, TableFields, rows)
			log.Debug("%v", reply)

			if reply != nil && reply.Code == 0 {
				atomic.AddInt64(&stat.lastCount, 1)
				break
			} else {
				atomic.AddInt64(&stat.errCount, 1)
				i = i - 1
				log.Warn("insert err: %v, will retry in 2s...", reply)
				time.Sleep(time.Duration(2000) * time.Millisecond)
			}
		}

	}

}


func rangeChecking(s *server.Server) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	no := r.Intn(s.GetCfg().BenchConfig.SendNum)

	pks := make(map[string]interface{})

	var threadNo = 1
	ip := getIp()
	user_name := getUserName(no, ip, threadNo)
	h := hash(user_name) % 16384
	pks["user_name"] = user_name
	pks["h"] = h

	reply := api.Select(s, s.GetCfg().BenchConfig.DB, s.GetCfg().BenchConfig.Table, TableFields, pks, nil)
	log.Debug("%v", reply)
	if reply.Code == 0 && len(reply.Values) > 0 {
		log.Debug("found data.")
	} else {
		log.Error("no such data.")
		return
	}

	api.DoMigration(s, s.GetCfg().BenchConfig.DB, s.GetCfg().BenchConfig.Table, s.GetCfg().Cluster, TableFields, pks)

	rows := make([][]interface{}, 0)
	row := make([]interface{}, 0)
	row = append(row, h)
	row = append(row, user_name)
	row = append(row, "pass_word")
	row = append(row, "real_name")
	rows = append(rows, row)

	time.Sleep(time.Duration(2000) * time.Millisecond)

	api.DoMigrationAfterUpdating(s, s.GetCfg().BenchConfig.DB, s.GetCfg().BenchConfig.Table, s.GetCfg().Cluster, TableFields, pks, rows)

}

func checkSelectWhenDsRestarting(s *server.Server) {

	total := s.GetCfg().BenchConfig.SendNum

	pks := make(map[string]interface{})
	ip := getIp()
	user_name := getUserName(50451, ip, 1)
	h := hash(user_name) % 16384 // h=10666, to fix rangeId=24
	pks["user_name"] = user_name
	pks["h"] = h

	runType := s.GetCfg().BenchConfig.Type

	if runType == -1 {
		for i := 0; i < total; i++ {
			reply := api.Select(s, s.GetCfg().BenchConfig.DB, s.GetCfg().BenchConfig.Table, TableFields, pks, nil)
			if reply != nil {
				log.Info("select from api, Reply.Message: %s", reply.Message)
			}
		}
	}
	if runType == -2 {
		for i := 0; i < total; i++ {
			api.DoDsSelect(s, s.GetCfg().BenchConfig.DB, s.GetCfg().BenchConfig.Table, TableFields, pks)
		}
	}
	if runType == -3 {
		var errNo = 0
		// check all
		for threadNo := 0; threadNo < s.GetCfg().BenchConfig.Threads; threadNo++ {
			for sendNo := 0; sendNo < s.GetCfg().BenchConfig.SendNum; sendNo++ {
				user_name := getUserName(sendNo, ip, threadNo)
				h := hash(user_name) % 16384
				pks["user_name"] = user_name
				pks["h"] = h

				var isErr = true
				for i := 0; i < 10; i++ {
					reply := api.Select(s, s.GetCfg().BenchConfig.DB, s.GetCfg().BenchConfig.Table, TableFields, pks, nil)
					if reply != nil && reply.Code == 0 && len(reply.Values) > 0 {
						isErr = false
						break
					}
					log.Warn("select [%v] err, reply=[%v].", pks, reply)
					time.Sleep(time.Duration(5) * time.Second)
				}
				if isErr {
					errNo++
				}
			}
		}
		log.Info("select completed, error times: %d", errNo)
	}

}