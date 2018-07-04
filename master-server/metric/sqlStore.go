package metric

import (
	"time"
	"fmt"
	"database/sql"
	"sync"
	"util/log"
	"errors"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/net/context"
	"reflect"
)

/*
    监控表
    集群监控
    cluster_meta: cluster_id, total_capacity, used_capacity, range_count, db_count, table_count, ds_count, gs_count, fault_list, update_time
    cluster_net: cluster_id, tps, min, max, avg, tp50, tp90, tp99, tp999, net_in_per_sec, net_out_per_sec, clients_count, open_clients_per_sec, update_time

    MAC监控
    mac_meta: cluster_id, type, ip, cpu_rate, load1, load5, load15, process_num, thread_num, handle_num, update_time
    mac_net: cluster_id, type, ip, net_io_in_byte_per_sec, net_io_out_byte_per_sec, net_io_in_package_per_sec, net_io_out_package_per_sec,
             net_tcp_connections, net_tcp_active_opens_per_sec, net_ip_recv_package_per_sec, net_ip_send_package_per_sec,net_ip_drop_package_per_sec,
             net_tcp_recv_package_per_sec, net_tcp_send_package_per_sec, net_tcp_err_package_per_sec, net_tcp_retransfer_package_per_sec, update_time
    mac_mem: cluster_id, type, ip, memory_total, memory_used_rss, memory_used, memory_free, memory_used_percent, swap_memory_total, swap_memory_used, swap_memory_free,
             swap_memory_used_percent, update_time
    max_disk: cluster_id, type, ip, disk_path, disk_total, disk_used, disk_free, disk_proc_rate, disk_read_byte_per_sec, disk_write_byte_per_sec,
              disk_read_count_per_sec, disk_write_count_per_sec, update_time

    Process监控
    process_meta: cluster_id, type, addr, cpu_rate, load1, load5, load15, thread_num, handle_num, memory_used, start_time, update_time
    process_disk: cluster_id, type, addr, disk_path, disk_total, disk_used, disk_free, disk_proc_rate, disk_read_byte_per_sec, disk_write_byte_per_sec,
              disk_read_count_per_sec, disk_write_count_per_sec, update_time
    process_net: cluster_id, type, addr, tps, min_tp, max_tp, avg_tp, tp50, tp90, tp99, tp999, net_io_in_byte_per_sec, net_io_out_byte_per_sec, net_io_in_package_per_sec,
                 net_io_out_package_per_sec, net_tcp_connections, net_tcp_active_opens_per_sec, update_time
    process_ds: cluster_id, type, addr, range_count, range_split_count, sending_snap_count, receiving_snap_count,
                applying_snap_count, range_leader_count, version, update_time

    DB 监控
    db_meta: cluster_id, db_name, table_num, range_size, update_time

    Table　监控
    table_meta: cluster_id, db_name, table_name, range_count, range_size, update_time

    Task 监控
    task_meta: cluster_id, finish_time, used_time, state, describe, update_time
*/

var (
	ClusterMetaSQL = `insert into cluster_meta (cluster_id, total_capacity, used_capacity, range_count, db_count, table_count, ds_count, task_count, gs_count,
	    fault_list, update_time) values (%d,%d,%d,%d,%d,%d,%d,%d,%d,"%s",%d)`
	ClusterNetSQL = `insert into cluster_net (cluster_id, tps, min_tp, max_tp, avg_tp, tp50, tp90, tp99, tp999, net_in_per_sec, net_out_per_sec, clients_count,
	    open_clients_per_sec, update_time) values (%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)`

	MacMetaSQL = `insert into mac_meta (cluster_id, type, ip, cpu_rate, load1, load5, load15, process_num, thread_num, handle_num, update_time)
	    values (%d,"%s","%s",%f,%f,%f,%f,%d,%d,%d,%d)`
	MacNetSQL = `insert into mac_net (cluster_id, type, ip, net_io_in_byte_per_sec, net_io_out_byte_per_sec, net_io_in_package_per_sec, net_io_out_package_per_sec,
        net_tcp_connections, net_tcp_active_opens_per_sec, net_ip_recv_package_per_sec, net_ip_send_package_per_sec,net_ip_drop_package_per_sec,
        net_tcp_recv_package_per_sec, net_tcp_send_package_per_sec, net_tcp_err_package_per_sec, net_tcp_retransfer_package_per_sec, update_time)
        values (%d,"%s","%s",%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)`
	MacMemSQL = `insert into mac_mem (cluster_id, type, ip, memory_total, memory_used_rss, memory_used, memory_free, memory_used_percent, swap_memory_total, swap_memory_used,
	    swap_memory_free, swap_memory_used_percent, update_time) values (%d,"%s","%s",%d,%d,%d,%d,%f,%d,%d,%d,%f,%d)`
	MacDiskSQL = `insert into max_disk (cluster_id, type, ip, disk_path, disk_total, disk_used, disk_free, disk_proc_rate, disk_read_byte_per_sec, disk_write_byte_per_sec,
        disk_read_count_per_sec, disk_write_count_per_sec, update_time) values (%d,"%s","%s","%s",%d,%d,%d,%d,%d,%d,%d,%d,%d)`

	ProcessMetaSQL = `insert into process_meta (cluster_id, type, addr, cpu_rate, load1, load5, load15, thread_num, handle_num, memory_used, start_time, update_time)
	    values (%d,"%s","%s",%f,%f,%f,%f,%d,%d,%d,%d,%d)`
    	ProcessDiskSQL = `insert into process_disk (cluster_id, type, addr, disk_path, disk_total, disk_used, disk_free, disk_proc_rate, disk_read_byte_per_sec, disk_write_byte_per_sec,
        disk_read_count_per_sec, disk_write_count_per_sec, update_time) values (%d,"%s","%s","%s",%d,%d,%d,%f,%d,%d,%d,%d,%d)`
	ProcessNetSQL = `insert into process_net (cluster_id, type, addr, tps, min_tp, max_tp, avg_tp, tp50, tp90, tp99, tp999, net_io_in_byte_per_sec, net_io_out_byte_per_sec, net_io_in_package_per_sec,
        net_io_out_package_per_sec, net_tcp_connections, net_tcp_active_opens_per_sec, update_time) values (%d,"%s","%s",%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)`
	ProcessDsSQL = `insert into process_ds (cluster_id, type, addr, range_count, range_split_count, sending_snap_count, receiving_snap_count,
        applying_snap_count, range_leader_count, version, update_time) values (%d,"%s","%s",%d,%d,%d,%d,%d,%d,"%s",%d)`

	DbMetaSQL = `insert into db_meta (cluster_id, db_name, table_num, range_size, update_time) values (%d,"%s",%d,%d,%d)`
	TableMetaSQL = `insert into table_meta (cluster_id, db_name, table_name, range_count, range_size, update_time) values (%d,"%s","%s",%d,%d,%d)`
	TaskMetaSQL = `insert into task_meta (cluster_id, finish_time, used_time, state, describe, update_time) values (%d,%d,%d,"%s","%s",%d)`

	RangeStatsSQL = `insert into range_stats (cluster_id, addr, range_id, bytes_written, bytes_read, keys_written, keys_read, approximate_size, update_time) values (%d,"%s",%d,%d,%d,%d,%d,%d,%d)`
)

var metricMap map[string]string

func init() {
	metricMap = make(map[string]string)
	metricMap["cluster_meta"] = ClusterMetaSQL
	metricMap["cluster_net"] = ClusterNetSQL
	metricMap["mac_meta"] = MacMetaSQL
	metricMap["mac_net"] = MacNetSQL
	metricMap["mac_mem"] = MacMemSQL
	metricMap["mac_disk"] = MacDiskSQL
	metricMap["process_meta"] = ProcessMetaSQL
	metricMap["process_disk"] = ProcessDiskSQL
	metricMap["process_net"] = ProcessNetSQL
	metricMap["process_ds"] = ProcessDsSQL
	metricMap["db_meta"] = DbMetaSQL
	metricMap["table_meta"] = TableMetaSQL
	metricMap["task_meta"] = TaskMetaSQL
	metricMap["range_stats"] = RangeStatsSQL
}

type SqlStore struct {
	dns      string
	lock     sync.Mutex
	// 工作线程数量
	worker_num        int
	db       *sql.DB

	message chan *Message

	wg               sync.WaitGroup
	ctx              context.Context
	cancel           context.CancelFunc
}

func NewSqlStore(dns string, worker_num int) Store {
	ctx, cancel := context.WithCancel(context.Background())
	if worker_num <= 0 {
		worker_num = 10
	}
	if worker_num > 1000 {
		worker_num = 1000
	}
	p := &SqlStore{
		dns:   dns,
		worker_num: worker_num,
		message: make(chan *Message, 100000),
		ctx: ctx,
		cancel: cancel,
	}
	return p
}


func (s *SqlStore) work() {
	log.Info("start pusher metric to msyql")
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			log.Info("pusher stopped: %v", s.ctx.Err())
			return
		case msg, ok := <-s.message:
		    if !ok {
			    continue
		    }
			if err := s.push(msg); err != nil {
				log.Warn("push message to sql store failed, err[%v]", err)
			}
		}
	}
}

func (s *SqlStore) push(msg *Message)  error {
	items := msg.Items
	switch reflect.TypeOf(items).Kind() {
	case reflect.Slice:
		values := items.([]interface{})
		num := len(values)
		if num == 0 {
			return errors.New("empty items")
		}
		for _, value := range values{
			item := value.(map[string]interface{})
			num := len(item)
			if num == 0 {
				return errors.New("empty items")
			}
			format, args, err := prepare(msg.Subsystem, item)
			if err != nil {
				log.Warn("prepare message failed, err[%v]", err)
				continue
			}
			err = s.insert(format, args...)
			if err != nil {
				log.Warn("push message failed, err[%v]", err)
			}
		}
	case reflect.Map:
		value := items.(map[string]interface{})
		num := len(value)
		if num == 0 {
			return errors.New("empty items")
		}
		format, args, err := prepare(msg.Subsystem, value)
		if err != nil {
			log.Warn("prepare message failed, err[%v]", err)
			return err
		}
		err = s.insert(format, args...)
		if err != nil {
			log.Warn("push message failed, err[%v]", err)
			return err
		}
	default:
		return errors.New("not support")
	}
	return nil
}


func (s *SqlStore) Open() error {
	db, err := sql.Open("mysql", s.dns)
	if err != nil {
		log.Error("mysql server[%s] open failed, err[%v]", s.dns, err)
		return err
	}
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(10)
	s.db = db
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
				err = s.ping()
				if err != nil {
					log.Warn("mysql server unhealthy, err [%v]!!!!", err)
					// 重新建立链接
					_db, _err := sql.Open("mysql", s.dns)
					if _err != nil {
						log.Warn("connect mysql server %s failed, err[%v]", s.dns, _err)
						continue
					}
					_db.SetMaxOpenConns(50)
					_db.SetMaxIdleConns(10)
					temp := s.db
					s.db = _db
					// 关闭旧链接
					temp.Close()
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

func (s *SqlStore) Close() {
	s.cancel()
	s.wg.Wait()
	if s.db != nil {
		s.db.Close()
	}
	return
}

func (s *SqlStore) ping() error {
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second)
	defer cancel1()
	return s.db.PingContext(ctx1)
}

// 串行执行，因此不需要锁
func (s *SqlStore) insert(format string, args ...interface{}) error {
	SQL := fmt.Sprintf(format, args...)
	log.Debug("sql %s", SQL)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := s.db.ExecContext(ctx, SQL)
	if err != nil {
		log.Warn("push[%s] failed, err[%v]", SQL, err)
	}

	return err
}

func (s *SqlStore) Put(message *Message) error {
	select {
	case s.message <- message:
	default:
		log.Error("metric message queue is full!!!")
		return errors.New("message queue is full")
	}
	return nil
}

func prepare(tableName string, data map[string]interface{}) (format string, args []interface{}, err error){
	var ok bool
	_, ok = metricMap[tableName]
	if !ok {
		err = fmt.Errorf("invalid metric item[%s]", tableName)
		return
	}

	var items, values string
	count := 0
	num := len(data)
	for item, value := range data {
		count++
		if count < num {
			items += fmt.Sprintf(`%s,`, item)
		} else {
			items += fmt.Sprintf(`%s`, item)
		}

		switch reflect.TypeOf(value).Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if count  < num {
				values += `%d,`
			} else {
				values += `%d`
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if count  < num {
				values += `%d,`
			} else {
				values += `%d`
			}
		case reflect.String:
			if count  < num {
				values += `"%s",`
			} else {
				values += `"%s"`
			}
		case reflect.Float32, reflect.Float64:
			if count  < num {
				values += `%f,`
			} else {
				values += `%f`
			}
		default:
			err = errors.New("invalid item type")
			return
		}
		args = append(args, value)
	}
	format = fmt.Sprintf(`insert into %s (%s) values (%s)`, tableName, items, values)
	return
}
