package metric

import (
	"golang.org/x/net/context"
	"github.com/olivere/elastic"

	"fmt"
	"errors"
	"time"
	"sync"
	"reflect"

	"util/log"
)

var errInfo = "elasticsearch client is not available"

/**
	standard:
	range_stats: range_stats-yyyy-MM
 */
func GetIndexRule(metric string) string {
	switch metric {
	case "range_stats":
		return "2006-01"
	default:
		return ""
	}
}


type EsStore struct {
	//es client http:port
	nodes   []string
	esClient *elastic.Client
	// 工作线程数量
	worker_num        int
	message chan *Message
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	lock sync.RWMutex
}

func NewEsStore(nodes []string, worker_num int) Store {
	ctx, cancel := context.WithCancel(context.Background())
	if worker_num <= 0 {
		worker_num = 10
	}
	if worker_num > 1000 {
		worker_num = 1000
	}
	p := &EsStore{
		nodes: nodes,
		worker_num: worker_num,
		message: make(chan *Message, 100000),
		ctx: ctx,
		cancel: cancel,
	}
	return p
}

func (s *EsStore) work() {
	log.Info("start pusher metric to elasticsearch")
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			log.Info("pusher stopped to elasticsearch: %v", s.ctx.Err())
			return
		case msg, ok := <-s.message:
			if !ok {
				continue
			}
			err := s.push(msg)
			if err != nil {
				log.Warn("push message to es store failed, err[%v]", err)
			}
		}
	}
}

func (s *EsStore) open_() error {
	s.lock.RLock()
	if s.esClient != nil {
		s.lock.RUnlock()
		return nil
	}
	s.lock.RUnlock()

	s.lock.Lock()
	if s.esClient != nil {
		s.lock.Unlock()
		return nil
	}

	client, err := elastic.NewClient(elastic.SetURL(s.nodes...), elastic.SetSniff(false))
	if err != nil {
		log.Warn("elasticsearch client[%s] open failed, err[%v]", s.nodes, err)
		s.lock.Unlock()
		return fmt.Errorf("elastic NewClient error: %v", err)
	}
	s.esClient = client
	s.lock.Unlock()
	return nil
}

func (s *EsStore) open() error {
	err := s.open_()
	if err != nil {
		return fmt.Errorf("EsStore open error: %v", err)
	}
	return nil
}

func (s *EsStore) Open() error {
	s.wg.Add(1)
	go func () {
		defer s.wg.Done()
		timer := time.NewTicker(time.Second * 30)
		for {
			select {
			case <-s.ctx.Done():
				log.Info("pusher stopped: %v", s.ctx.Err())
				return
			case <-timer.C:
				if err := s.ping(); err != nil {
					log.Warn("elasticsearch unhealthy, err [%v]!!!!", err)
					// 重新建立链接
					_esclient, _err := elastic.NewClient(elastic.SetURL(s.nodes...), elastic.SetSniff(false))
					if _err != nil {
						log.Warn("retry connect to elastic search  [%s] failed, err[%v]", s.nodes, _err)
						continue
					}
					s.lock.Lock()
					s.esClient.Stop()
					s.esClient = _esclient
					s.lock.Unlock()
					log.Info("elastic client reconnect success")
				}
			}
		}
	}()

	for i := 0; i < s.worker_num; i++ {
		s.wg.Add(1)
		go s.work()
	}
	return nil
}

func (s *EsStore) ping() error{
	s.lock.RLock()
	if s.esClient == nil {
		s.lock.RUnlock()
		return nil
	}
	s.lock.RUnlock()
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second)
	defer cancel1()
	var err error
	for _, node := range s.nodes {
		_, code, err := s.esClient.Ping(node).Do(ctx1)
		if err == nil && code == 200 {
			return nil
		}
		log.Warn(" ping elasticsearch [%s] code[%d], err  [%v]!!!!", node,  code, err)
	}
	return err
}

func (s *EsStore) push(msg *Message)  error {
	if err := s.open(); err != nil {
		return fmt.Errorf("EsStore push error: %v", err)
	}

	index := msg.Subsystem
	if format := GetIndexRule(msg.Subsystem); len(format) != 0 {
		index = fmt.Sprintf("%s-%s", index, time.Now().Format(format))
	}

	items := msg.Items
	switch reflect.TypeOf(items).Kind() {
	case reflect.Slice:
		values := items.([]interface{})
		num := len(values)
		if num == 0 {
			return errors.New("empty items")
		}
		for _, value := range values{
			num := len(value.(map[string]interface{}))
			if num == 0 {
				return errors.New("empty items")
			}
		}
		err := s.bulkInsert(index, msg.Subsystem, values...)
		if err != nil{
			log.Warn("bulk push metric[%s] to elasticsearch failed, err[%v]", msg.Subsystem , err)
			return err
		}
	case reflect.Map:
		value := items.(map[string]interface{})
		num := len(value)
		if num == 0 {
			return errors.New("empty items")
		}
		err := s.insert(index, msg.Subsystem, msg.MsgId, value)
		if err != nil{
			log.Warn("push metric[%s] to elasticsearch failed, err[%v]", msg.Subsystem , err)
		}
	default:
		return errors.New("not support")
	}
	return nil
}

func (s *EsStore) insert(index string, indexType string, id string, items interface{} ) error{
	if s.esClient == nil {
		return errors.New(errInfo)
	}
	log.Debug("start to push metric[%s] to elasticsearch.", index)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 10)
	defer cancel()

	indexService := s.esClient.Index().Index(index).Type(indexType).BodyJson(items)
	if len(id) != 0 {
		indexService = indexService.Id(id)
	}
	var err error
	_, err = indexService.Do(ctx)
	return err
}

func (s *EsStore) bulkInsert(index string, indexType string, items ...interface{} ) error{
	if s.esClient == nil {
		return errors.New(errInfo)
	}
	log.Debug("start to bulk push metric[%s] to elasticsearch.", index)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 50)
	defer cancel()

	bulkRequest := s.esClient.Bulk()
	for _, item := range items {
		r := elastic.NewBulkIndexRequest().Index(index).Type(indexType).Doc(item)
		bulkRequest = bulkRequest.Add(r)
	}
	resp, err := bulkRequest.Do(ctx)
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.New("excepted to be != nil; got nil")
	}
	if resp.Took == 0 {
		return errors.New(fmt.Sprintf("excepted to be > 0; got %d ", resp.Took))
	}

	if resp.Errors {
		return errors.New(fmt.Sprintf("excepted errors to be false; got %v ", resp.Errors))
	}
	if len(resp.Items) != len(items) {
		return errors.New(fmt.Sprintf("excepted %d result; got %v ", len(items), resp.Items))
	}
	return nil
}

func (s *EsStore)Put(message *Message) error  {
	select {
	case s.message <- message:
		log.Debug("put data to es")
	default:
		log.Error("metric message queue is full!!!")
		return errors.New("message queue is full")
	}
	return nil
}

func (s *EsStore) Close() {
	s.cancel()
	s.wg.Wait()
	if s.esClient != nil {
		s.esClient.Stop()
	}
	return
}