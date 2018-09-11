package server

import (
	"errors"
	"fmt"
	"model/pkg/metapb"
	"runtime"
	"time"

	"model/pkg/kvrpcpb"
	"util/log"
	"proxy/store/dskv"
)

type Task interface {
	Do()
	Wait() error
	Reset()
}

func (p *Proxy) Submit(t Task) error {
	index := time.Now().UnixNano() % int64(len(p.taskQueues))
	select {
	case <-p.ctx.Done():
		return errors.New("server closed")
	case p.taskQueues[index] <- t:
		return nil
	}
}

func (p *Proxy) workMonitor() {
	p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case index := <-p.workRecover:
			// chan closed
			if index <= 0 {
				return
			}
			p.wg.Add(1)
			go p.work(index, p.taskQueues[index])
		}
	}
}

func (p *Proxy) work(index int, queue chan Task) {
	defer func() {
		p.wg.Done()
		if r := recover(); r != nil {
			fn := func() string {
				n := 10000
				var trace []byte
				for i := 0; i < 5; i++ {
					trace = make([]byte, n)
					nbytes := runtime.Stack(trace, false)
					if nbytes < len(trace) {
						return string(trace[:nbytes])
					}
					n *= 2
				}
				return string(trace)
			}
			select {
			case <-p.ctx.Done():
				return
			case p.workRecover <- index:
			}

			log.Error("panic:%v", r)
			log.Error("Stack: %s", fn())
			return
		}
	}()
	for {
		select {
		case <-p.ctx.Done():
			return
		case t := <-queue:
			// chan closed
			if t == nil {
				return
			}
			t.Do()
		}
	}
}

type InsertResult struct {
	affected     uint64
	duplicateKey []byte
}

func (r *InsertResult) GetAffected() uint64 {
	if r == nil {
		return 0
	}
	return r.affected
}

func (r *InsertResult) GetDuplicateKey() []byte {
	if r == nil {
		return nil
	}
	return r.duplicateKey
}

type InsertTask struct {
	do      bool
	p       *Proxy
	table   *Table
	rows    []*kvrpcpb.KeyValue
	done    chan error
	rest    *InsertResult
	context *dskv.ReqContext
}

func (it *InsertTask) init(rContext *dskv.ReqContext, proxy *Proxy, table *Table, rows []*kvrpcpb.KeyValue) *InsertTask {
	if it == nil {
		return it
	}
	it.context = rContext
	it.p = proxy
	it.table = table
	it.rows = rows
	return it
}

func (it *InsertTask) Do() {
	it.do = true
	affected, duplicateKey, err := it.p.insert(it.context, it.table, it.rows)
	if err != nil {
		it.done <- err
		return
	}

	it.rest = &InsertResult{affected, duplicateKey}
	it.done <- nil
	return
}

func (it *InsertTask) Wait() error {
	select {
	case <-it.p.ctx.Done():
		return errors.New("proxy already closed")
	case err := <-it.done:
		return err
	}
	return nil
}

func (it *InsertTask) Reset() {
	if it == nil {
		return
	}
	*it = InsertTask{done: make(chan error, 1)}
}

type SelectResult struct {
	rows [][]*Row
}

type SelectTask struct {
	p         *Proxy
	table     *Table
	fieldList []*kvrpcpb.SelectField
	matches   []Match
	done      chan error
	rest      *SelectResult
}

func (it *SelectTask) init(proxy *Proxy, table *Table, fieldList []*kvrpcpb.SelectField, matches []Match) *SelectTask {
	if it == nil {
		return it
	}
	it.p = proxy
	it.table = table
	it.fieldList = fieldList
	it.matches = matches
	return it
}

func (it *SelectTask) Do() {
	rows, err := it.p.doSelect(it.table, it.fieldList, it.matches, nil, nil)
	if err != nil {
		log.Error("getcommand doselect error: %v", err)
		it.done <- err
		return
	}
	it.rest = &SelectResult{rows}
	it.done <- nil
	return
}

func (it *SelectTask) Wait() error {
	select {
	case <-it.p.ctx.Done():
		return errors.New("proxy already closed")
	case err := <-it.done:
		return err
	}
	return nil
}

func (it *SelectTask) Reset() {
	if it == nil {
		return
	}
	*it = SelectTask{done: make(chan error, 1)}
}

func newAggreTask(p *Proxy, kvproxy *dskv.KvProxy, key []byte, req *kvrpcpb.SelectRequest, readFrom metapb.ReadFromNode) *aggreTask {
	return &aggreTask{
		p:       	p,
		kvproxy: 	kvproxy,
		key:     	key,
		req:     	req,
		done:    	make(chan error, 1),
		readFrom:	readFrom,
	}
}

type aggreTask struct {
	p       	*Proxy
	kvproxy 	*dskv.KvProxy
	key     	[]byte
	req     	*kvrpcpb.SelectRequest
	done    	chan error
	result  	[]*kvrpcpb.Row
	readFrom	metapb.ReadFromNode
}

func (t *aggreTask) Do() {
	resp, _, err := t.kvproxy.SqlQuery(t.req, t.key, t.readFrom)
	if err == nil && resp.GetCode() != 0 {
		log.Error("select aggre: remote server return code: %v", resp.GetCode())
		err = fmt.Errorf("remote server return code: %v", resp.GetCode())
	}
	if err != nil {
		t.done <- err
		return
	}
	t.result = resp.GetRows()
	t.done <- nil
}

func (t *aggreTask) Wait() error {
	select {
	case <-t.p.ctx.Done():
		return errors.New("proxy already closed")
	case err := <-t.done:
		return err
	}
}

func (t *aggreTask) Reset() {
}
