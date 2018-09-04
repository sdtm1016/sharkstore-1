package metric

import (
	"net/http"
	"strconv"
	"time"
	"fmt"
	"strings"

	"model/pkg/statspb"
	"model/pkg/mspb"
	"util/bufalloc"
	"encoding/json"
	"util/log"
	"util/deepcopy"
	"util/alarm"
)

func (m *Metric) doProcessMetric(ctx *Context, data []byte) error {
	procStats := new(statspb.ProcessStats)
	err := json.Unmarshal(data, procStats)
	if err != nil {
		log.Warn("encode process stats[%s] failed, err[%v]", string(data), err)
		return err
	}
	cluster := m.getCluster(ctx.clusterId)
	if cluster == nil {
		log.Warn("ProcessMetric: invalid cluster")
		return nil
	}
	item := cluster.GetProcessItem(ctx.subsystem)
	if item == nil {
		item = &ProcessItem{Type: strings.ToUpper(ctx.namespace), Addr: ctx.subsystem, Item: procStats, UpdateTime: time.Now().Unix()}
		cluster.SetProcessItem(item)
	} else {
		item.Item = procStats
		item.UpdateTime = time.Now().Unix()
	}
	m.pushProcess(ctx.clusterId, item)
	return nil
}

func (m *Metric) doSlowLogMetric(ctx *Context, data []byte) error {
	slowlogStats := new(statspb.SlowLogStats)
	err := json.Unmarshal(data, slowlogStats)
	if err != nil {
		log.Warn("encode slow log[%s] failed, err[%v]", string(data), err)
		return err
	}
	cluster := m.getCluster(ctx.clusterId)
	if cluster == nil {
		log.Warn("invalid cluster")
		return nil
	}
	item := cluster.GetProcessItem(ctx.subsystem)
	if item == nil {
		log.Warn("SlowLog: process %s not found", ctx.subsystem)
		return nil
	}
	item.SlowLog = slowlogStats
	m.pushSlowLog(ctx.clusterId, &SlowLogItem{
		Type:    item.Type,
		Addr:    item.Addr,
		UpdateTime: time.Now().Unix(),
		Item: slowlogStats,
	})
	return nil
}

func (m *Metric) doMacMetric(ctx *Context, data []byte) error {
	macStats := new(statspb.MacStats)
	err := json.Unmarshal(data, macStats)
	if err != nil {
		log.Warn("encode mac stats[%s] failed, err[%v]", string(data), err)
		return err
	}
	cluster := m.getCluster(ctx.clusterId)
	if cluster == nil {
		log.Warn("invalid cluster")
		return nil
	}
	item := cluster.GetMacItem(ctx.subsystem)
	if item == nil {
		item = &MacItem{Type: ctx.namespace, Ip: ctx.subsystem, Item: macStats, UpdateTime: time.Now().Unix()}
		cluster.SetMacItem(item)
	} else {
		item.Item = macStats
		item.UpdateTime = time.Now().Unix()
	}
	m.pushMac(ctx.clusterId, item)
	return nil
}

func (m *Metric) doDbMetric(ctx *Context, data []byte) error {
	dbStats := new(statspb.DatabaseStats)
	err := json.Unmarshal(data, dbStats)
	if err != nil {
		log.Warn("encode db stats[%s] failed, err[%v]", string(data), err)
		return err
	}
	cluster := m.getCluster(ctx.clusterId)
	if cluster == nil {
		log.Warn("invalid cluster")
		return nil
	}
	item := cluster.GetDbItem(ctx.namespace)
	if item == nil {
		item = NewDbItem(ctx.namespace)
		item.DbName = ctx.namespace
		item.TableNum = dbStats.TableNum
		item.UpdateTime = time.Now().Unix()
		cluster.SetDbItem(item)
	} else {
		item.TableNum = dbStats.TableNum
		item.UpdateTime = time.Now().Unix()
	}
	m.pushDb(ctx.clusterId, item)
	return nil
}

func (m *Metric) doTableMetric(ctx *Context, data []byte) error {
	tableStats := new(statspb.TableStats)
	err := json.Unmarshal(data, tableStats)
	if err != nil {
		log.Warn("encode table stats[%s] failed, err[%v]", string(data), err)
		return err
	}
	cluster := m.getCluster(ctx.clusterId)
	if cluster == nil {
		log.Warn("invalid cluster")
		return nil
	}
	item := cluster.GetDbItem(ctx.namespace)
	if item != nil {
		tableItem := item.GetTableItem(ctx.subsystem)
		if tableItem == nil {
			tableItem = &TableItem{
				DbName: ctx.namespace,
				TableName: ctx.subsystem,
				RangeNum: tableStats.RangeNum,
				Size: tableStats.Size_,
				UpdateTime: time.Now().Unix()}
			item.SetTableTtem(tableItem)
		} else {
			tableItem.RangeNum = tableStats.RangeNum
			tableItem.Size = tableStats.Size_
			tableItem.UpdateTime = time.Now().Unix()
		}
		m.pushTable(ctx.clusterId, tableItem)
	}
	return nil
}

func (m *Metric) doClusterMetric(ctx *Context, data []byte) error {
	clusterStats := new(statspb.ClusterStats)
	err := json.Unmarshal(data, clusterStats)
	if err != nil {
		log.Warn("encode cluster stats[%s] failed, err[%v]", string(data), err)
		return err
	}

	cluster := m.getCluster(ctx.clusterId)
	if cluster == nil {
		cluster = NewCluster(ctx.clusterId)
		m.setCluster(cluster)
	}
	cluster.Item.CapacityTotal = clusterStats.CapacityTotal
	cluster.Item.SizeUsed = clusterStats.SizeUsed
	cluster.Item.RangeNum = clusterStats.RangeNum
	cluster.Item.DbNum = clusterStats.DbNum
	cluster.Item.TableNum = clusterStats.TableNum
	cluster.Item.TaskNum = clusterStats.TaskNum
	cluster.Item.NodeDownCount = clusterStats.NodeDownCount
	cluster.Item.NodeUpCount = clusterStats.NodeUpCount
	cluster.Item.NodeTombstoneCount = clusterStats.NodeTombstoneCount
	cluster.Item.NodeOfflineCount = clusterStats.NodeOfflineCount
	cluster.Item.LeaderBalanceRatio = clusterStats.LeaderBalanceRatio
	cluster.Item.RegionBalanceRatio = clusterStats.RegionBalanceRatio
	cluster.Item.UpdateTime = time.Now().Unix()
	m.pushCluster(ctx.clusterId, cluster)
	return nil
}

func (m *Metric) doEventMetric(ctx *Context, data []byte) error {
	taskInfo := new(statspb.TaskInfo)
	err := json.Unmarshal(data, taskInfo)
	if err != nil {
		log.Warn("encode task info[%s] failed, err[%v]", string(data), err)
		return err
	}

	cluster := m.getCluster(ctx.clusterId)
	if cluster == nil {
		log.Warn("invalid cluster")
		return nil
	}
	m.pushTask(ctx.clusterId, taskInfo)
	return nil
}

func (m *Metric) doScheduleMetric(ctx *Context, data []byte) error {
	scheduleCounter := new(statspb.ScheduleCount)
	err := json.Unmarshal(data, scheduleCounter)
	if err != nil {
		log.Warn("encode cluster stats[%s] failed, err[%v]", string(data), err)
		return err
	}

	cluster := m.getCluster(ctx.clusterId)
	if cluster == nil {
		log.Warn("invalid cluster")
		return nil
	}
	m.pushSchedule(ctx.clusterId, scheduleCounter)
	return nil
}

func (m *Metric) doHotspotMetric(ctx *Context, data []byte) error {
	hotspot := new(statspb.HotSpotStats)
	err := json.Unmarshal(data, hotspot)
	if err != nil {
		log.Warn("encode cluster stats[%s] failed, err[%v]", string(data), err)
		return err
	}

	cluster := m.getCluster(ctx.clusterId)
	if cluster == nil {
		log.Warn("invalid cluster")
		return nil
	}
	m.pushHotspot(ctx.clusterId, hotspot)
	return nil
}

func (m *Metric) nodeThresholdAlarm(clusterId, nodeId uint64, nodeAddr string, node *mspb.NodeStats) (err error) {
	var msg []byte
	var sample *alarm.Sample
	ip := strings.Split(nodeAddr, ":")[0]
	port, _ := strconv.ParseInt(strings.Split(nodeAddr, ":")[1], 10, 64)
	info := make(map[string]interface{})
	usedSize := node.GetUsedSize()
	capacity := node.GetCapacity()+1
	if usedSize/capacity > m.Threshold.Node.CapacityUsedRate {
		msg = append(msg, []byte(fmt.Sprintf("cluster id[%v] node[%v] addr[%v] cacapcity used rate %v > %v <br>",
			clusterId, nodeId, nodeAddr, usedSize/capacity, m.Threshold.Node.CapacityUsedRate))...)
		info["node_capacity_used_rate"] = usedSize/capacity
	}
	writeBps := node.GetBytesWritten()
	if writeBps > m.Threshold.Node.WriteBps {
		msg = append(msg, []byte(fmt.Sprintf("cluster id[%v] node[%v] addr[%v] write bps %v > %v <br>",
			clusterId, nodeId, nodeAddr, writeBps, m.Threshold.Node.WriteBps))...)
		info["node_write_bps"] = writeBps
	}
	writeOps := node.GetKeysWritten()
	if writeOps > m.Threshold.Node.WriteOps {
		msg = append(msg, []byte(fmt.Sprintf("cluster id[%v] node[%v] addr[%v] write ops %v > %v <br>",
			clusterId, nodeId, nodeAddr, writeOps, m.Threshold.Node.WriteOps))...)
		info["node_write_ops"] = writeOps
	}
	readBps := node.GetBytesRead()
	if readBps > m.Threshold.Node.ReadBps {
		msg = append(msg, []byte(fmt.Sprintf("cluster id[%v] node[%v] addr[%v] read bps %v > %v <br>",
			clusterId, nodeId, nodeAddr, readBps, m.Threshold.Node.ReadBps))...)
		info["node_read_bps"] = readBps
	}
	readOps := node.GetKeysRead()
	if readOps > m.Threshold.Node.ReadOps {
		msg = append(msg, []byte(fmt.Sprintf("cluster id[%v] node[%v] addr[%v] read ops %v > %v <br>",
			clusterId, nodeId, nodeAddr, readOps, m.Threshold.Node.ReadOps))...)
		info["node_read_ops"] = readOps
	}
	if len(msg) == 0 {
		return nil
	}
	if len(info) != 0 {
		info["ip"] = ip
		info["port"] = port
		info["spaceId"] = clusterId
		info["nodeId"] = fmt.Sprint(nodeId)
		log.Info("sample info: %+v", info)
		sample = alarm.NewSample(ip, int(port), int(clusterId), info)
	}

	return m.AlarmCli.SimpleAlarm(clusterId, "node stats alarm", string(msg), []*alarm.Sample{sample})
}

func (m *Metric) doNodeMetric(ctx *Context, data []byte) error {
	nodeId, err := strconv.ParseUint(ctx.namespace, 10, 64)
	if err != nil {
		log.Warn("invalid param nodeId, err[%v]", err)
		return err
	}

	nodeStats := new(mspb.NodeStats)
	err = json.Unmarshal(data, nodeStats)
	if err != nil {
		log.Warn("encode cluster node stats[%s] failed, err[%v]", string(data), err)
		return err
	}
	err = m.nodeThresholdAlarm(ctx.clusterId, nodeId, ctx.subsystem, nodeStats)
	if err != nil {
		log.Warn("node threshold alarm failed, err[%v]", err)
	}

	cluster := m.getCluster(ctx.clusterId)
	if cluster == nil {
		log.Warn("invalid cluster")
		return nil
	}
	m.pushNodeStats(ctx.clusterId, nodeId, ctx.subsystem, nodeStats)
	return nil
}

func (m *Metric) rangeThresholdAlarm(clusterId uint64, rangeStats []*statspb.RangeInfo) (err error) {
	var msg []byte
	var samples []*alarm.Sample
	for _, rang := range rangeStats {
		ip := strings.Split(rang.GetNodeAdder(), ":")[0]
		port, _ := strconv.ParseInt(strings.Split(rang.GetNodeAdder(), ":")[1], 10, 64)
		info := make(map[string]interface{})

		writeBps := rang.GetStats().GetBytesWritten()
		if writeBps > m.Threshold.Range.WriteBps {
			msg = append(msg, []byte(fmt.Sprintf("cluster id[%v] range[%v] addr[%v] write bps %v > %v <br>",
				clusterId, rang.GetRangeId(), rang.GetNodeAdder(), writeBps, m.Threshold.Range.WriteBps))...)
			info["range_write_bps"] = writeBps
		}
		writeOps := rang.GetStats().GetKeysWritten()
		if writeOps > m.Threshold.Range.WriteOps {
			msg = append(msg, []byte(fmt.Sprintf("cluster id[%v] range[%v] addr[%v] write ops %v > %v <br>",
				clusterId, rang.GetRangeId(), rang.GetNodeAdder(), writeOps, m.Threshold.Range.WriteOps))...)
			info["range_write_ops"] = writeOps
		}
		readBps := rang.GetStats().GetBytesRead()
		if readBps > m.Threshold.Range.ReadBps {
			msg = append(msg, []byte(fmt.Sprintf("cluster id[%v] range[%v] addr[%v] read bps %v > %v <br>",
				clusterId, rang.GetRangeId(), rang.GetNodeAdder(), readBps, m.Threshold.Range.ReadBps))...)
			info["range_read_bps"] = readBps
		}
		readOps := rang.GetStats().GetKeysRead()
		if readOps > m.Threshold.Range.ReadOps {
			msg = append(msg, []byte(fmt.Sprintf("cluster id[%v] range[%v] addr[%v] read ops %v > %v <br>",
				clusterId, rang.GetRangeId(), rang.GetNodeAdder(), readOps, m.Threshold.Range.ReadOps))...)
			info["range_read_ops"] = readOps
		}

		if len(info) != 0 {
			info["ip"] = ip
			info["port"] = port
			info["spaceId"] = clusterId
			info["rangeId"] = rang.GetRangeId()
			log.Info("sample info: %+v", info)
			samples = append(samples, alarm.NewSample(ip, int(port), int(clusterId), info))
		}
	}
	if len(msg) == 0 {
		return nil
	}


	return m.AlarmCli.SimpleAlarm(clusterId, "range stats alarm", string(msg), samples)
}

func (m *Metric) doRangeMetric(ctx *Context, data []byte) error {
	var rangeStats []*statspb.RangeInfo
	err := json.Unmarshal(data, &rangeStats)
	if err != nil {
		log.Warn("range metric: encode range stats[%s] failed, err[%v]", string(data), err)
		return err
	}
	err = m.rangeThresholdAlarm(ctx.clusterId, rangeStats)
	if err != nil {
		log.Warn("range threshold alarm failed, err[%v]", err)
	}

	cluster := m.getCluster(ctx.clusterId)
	if cluster == nil {
		log.Warn("range metric: invalid cluster")
		return nil
	}
	m.pushRangeStats(ctx.clusterId, rangeStats)
	return nil
}

func (m *Metric) handleMacMetric(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		reply.Code = -1
		reply.Message = "invalid param"
		log.Warn("invalid param clusterId, err[%v]", err)
		return
	}
	log.Debug("recv cluster[%d] mac metric", clusterId)
	// MS, DS, GS
	namespace := r.FormValue("namespace")
	// ip
	ip := r.FormValue("subsystem")
	bufferLen := int(r.ContentLength)
	if bufferLen <= 0 || bufferLen > 1024*1024*10 {
		bufferLen = 512
	}
	buffer := bufalloc.AllocBuffer(bufferLen)
	defer bufalloc.FreeBuffer(buffer)
	if _, err = buffer.ReadFrom(r.Body); err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("read request failed, err[%v]", err)
		return
	}
	ctx := &Context{
		clusterId: clusterId,
		namespace: namespace,
		subsystem: ip,
	}
	err = m.doMacMetric(ctx, buffer.Bytes())
	if err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("do mac metric[%s] failed, err[%v]", string(buffer.Bytes()), err)
		return
	}

	return
}

func (m *Metric) handleEventMetric(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		reply.Code = -1
		reply.Message = "invalid param"
		log.Warn("invalid param clusterId, err[%v]", err)
		return
	}
	log.Debug("recv cluster[%d] event metric", clusterId)
	// MS, DS, GS
	namespace := r.FormValue("namespace")
	// ip
	ip := r.FormValue("subsystem")
	bufferLen := int(r.ContentLength)
	if bufferLen <= 0 || bufferLen > 1024*1024*10 {
		bufferLen = 512
	}
	buffer := bufalloc.AllocBuffer(bufferLen)
	defer bufalloc.FreeBuffer(buffer)
	if _, err = buffer.ReadFrom(r.Body); err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("read request failed, err[%v]", err)
		return
	}
	ctx := &Context{
		clusterId: clusterId,
		namespace: namespace,
		subsystem: ip,
	}
	err = m.doEventMetric(ctx, buffer.Bytes())
	if err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("do event metric[%s] failed, err[%v]", string(buffer.Bytes()), err)
		return
	}

	return
}

func (m *Metric) handleProcessMetric(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		reply.Code = -1
		reply.Message = "invalid param"
		log.Warn("invalid param clusterId, err[%v]", err)
		return
	}
	// MS, DS, GS
	namespace := r.FormValue("namespace")
	// addr string
	addr := r.FormValue("subsystem")
	log.Debug("recv cluster[%d] process metric, type[%s], addr[%s]", clusterId, namespace, addr)
	bufferLen := int(r.ContentLength)
	if bufferLen <= 0 || bufferLen > 1024*1024*10 {
		bufferLen = 512
	}
	buffer := bufalloc.AllocBuffer(bufferLen)
	defer bufalloc.FreeBuffer(buffer)
	if _, err = buffer.ReadFrom(r.Body); err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("read request failed, err[%v]", err)
		return
	}
	ctx := &Context{
		clusterId: clusterId,
		namespace: namespace,
		subsystem: addr,
	}
	err = m.doProcessMetric(ctx, buffer.Bytes())
	if err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("do process metric[%s] failed, err[%v]", string(buffer.Bytes()), err)
		return
	}
	return
}

func (m *Metric) handleSlowLogMetric(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		reply.Code = -1
		reply.Message = "invalid param"
		log.Warn("invalid param clusterId, err[%v]", err)
		return
	}
	// MS, DS, GS
	namespace := r.FormValue("namespace")
	// addr string
	addr := r.FormValue("subsystem")
	log.Debug("recv cluster[%d] slow log metric, type[%s], addr[%s]", clusterId, namespace, addr)
	bufferLen := int(r.ContentLength)
	if bufferLen <= 0 || bufferLen > 1024*1024*10 {
		bufferLen = 512
	}
	buffer := bufalloc.AllocBuffer(bufferLen)
	defer bufalloc.FreeBuffer(buffer)
	if _, err = buffer.ReadFrom(r.Body); err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("read request failed, err[%v]", err)
		return
	}
	ctx := &Context{
		clusterId: clusterId,
		namespace: namespace,
		subsystem: addr,
	}
	err = m.doSlowLogMetric(ctx, buffer.Bytes())
	if err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("do slowlog metric[%s] failed, err[%v]", string(buffer.Bytes()), err)
		return
	}
	return
}

func (m *Metric) handleDbMetric(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		reply.Code = -1
		reply.Message = "invalid param"
		log.Warn("invalid param clusterId, err[%v]", err)
		return
	}
	log.Debug("recv cluster[%d] db metric", clusterId)
	namespace := r.FormValue("namespace")
	//subsystem := r.FormValue("subsystem")
	bufferLen := int(r.ContentLength)
	if bufferLen <= 0 || bufferLen > 1024*1024*10 {
		bufferLen = 512
	}
	buffer := bufalloc.AllocBuffer(bufferLen)
	defer bufalloc.FreeBuffer(buffer)
	if _, err = buffer.ReadFrom(r.Body); err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("read request failed, err[%v]", err)
		return
	}
	ctx := &Context{
		clusterId: clusterId,
		namespace: namespace,
	}
	err = m.doDbMetric(ctx, buffer.Bytes())
	if err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("do db metric[%s] failed, err[%v]", string(buffer.Bytes()), err)
	}
	return
}

func (m *Metric) handleTableMetric(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		reply.Code = -1
		reply.Message = "invalid param"
		log.Warn("invalid param clusterId, err[%v]", err)
		return
	}
	log.Debug("recv cluster[%d] table metric", clusterId)
	namespace := r.FormValue("namespace")
	subsystem := r.FormValue("subsystem")
	bufferLen := int(r.ContentLength)
	if bufferLen <= 0 || bufferLen > 1024*1024*10 {
		bufferLen = 512
	}
	buffer := bufalloc.AllocBuffer(bufferLen)
	defer bufalloc.FreeBuffer(buffer)
	if _, err = buffer.ReadFrom(r.Body); err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("read request failed, err[%v]", err)
		return
	}
	ctx := &Context{
		clusterId: clusterId,
		namespace: namespace,
		subsystem: subsystem,
	}
	err = m.doTableMetric(ctx, buffer.Bytes())
	if err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("do table metric[%s] failed, err[%v]", string(buffer.Bytes()), err)
	}

	return
}

func (m *Metric) handleClusterMetric(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		reply.Code = -1
		reply.Message = "invalid param"
		log.Warn("invalid param clusterId, err[%v]", err)
		return
	}
	//namespace := r.FormValue("namespace")
	//subsystem := r.FormValue("subsystem")
	bufferLen := int(r.ContentLength)
	if bufferLen <= 0 || bufferLen > 1024*1024*10 {
		bufferLen = 512
	}
	buffer := bufalloc.AllocBuffer(bufferLen)
	defer bufalloc.FreeBuffer(buffer)
	if _, err = buffer.ReadFrom(r.Body); err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("read request failed, err[%v]", err)
		return
	}
	ctx := &Context{clusterId: clusterId}
	err = m.doClusterMetric(ctx, buffer.Bytes())
	if err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("do cluster metric[%s] failed, err[%v]", string(buffer.Bytes()), err)
	}
	log.Info("recv cluster[%d] cluster metric success", clusterId)
	return
}

/*
	&Context{
		clusterId: clusterId,
		namespace: namespace,   //string of nodeId
		subsystem: subsystem,	//string of nodeAddr
	}
 */
func (m *Metric) handleNodeMetric(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		reply.Code = -1
		reply.Message = "invalid param"
		log.Warn("invalid param clusterId, err[%v]", err)
		return
	}
	namespace := r.FormValue("namespace")
	subsystem := r.FormValue("subsystem")
	bufferLen := int(r.ContentLength)
	if bufferLen <= 0 || bufferLen > 1024*1024*10 {
		bufferLen = 512
	}
	buffer := bufalloc.AllocBuffer(bufferLen)
	defer bufalloc.FreeBuffer(buffer)
	if _, err = buffer.ReadFrom(r.Body); err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("read request failed, err[%v]", err)
		return
	}
	ctx := &Context{
		clusterId: clusterId,
		namespace: namespace,
		subsystem: subsystem,
	}
	err = m.doNodeMetric(ctx, buffer.Bytes())
	if err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("do cluster node metric[%s] failed, err[%v]", string(buffer.Bytes()), err)
	}
	log.Info("recv cluster[%d] node[%v] metric", clusterId, namespace)
	return
}

/*
	&Context{
		clusterId: clusterId,
		namespace: namespace,   //string of rangeId
		subsystem: subsystem,	//string of nodeAddr
	}
 */
func (m *Metric) handleRangeMetric(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		reply.Code = -1
		reply.Message = "invalid param"
		log.Warn("invalid param clusterId, err[%v]", err)
		return
	}
	bufferLen := int(r.ContentLength)
	if bufferLen <= 0 || bufferLen > 1024*1024*10 {
		bufferLen = 512
	}
	buffer := bufalloc.AllocBuffer(bufferLen)
	defer bufalloc.FreeBuffer(buffer)
	if _, err = buffer.ReadFrom(r.Body); err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("read request failed, err[%v]", err)
		return
	}
	ctx := &Context{
		clusterId: clusterId,
	}
	err = m.doRangeMetric(ctx, buffer.Bytes())
	if err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("do cluster range metric[%s] failed, err[%v]", string(buffer.Bytes()), err)
	}
	log.Info("recv cluster[%d] range metric", clusterId)
	return
}

func (m *Metric) handleScheduleMetric(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		reply.Code = -1
		reply.Message = "invalid param"
		log.Warn("invalid param clusterId, err[%v]", err)
		return
	}
	//namespace := r.FormValue("namespace")
	//subsystem := r.FormValue("subsystem")
	bufferLen := int(r.ContentLength)
	if bufferLen <= 0 || bufferLen > 1024*1024*10 {
		bufferLen = 512
	}
	buffer := bufalloc.AllocBuffer(bufferLen)
	defer bufalloc.FreeBuffer(buffer)
	if _, err = buffer.ReadFrom(r.Body); err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("read request failed, err[%v]", err)
		return
	}
	ctx := &Context{clusterId: clusterId}
	err = m.doScheduleMetric(ctx, buffer.Bytes())
	if err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("do schedule metric[%s] failed, err[%v]", string(buffer.Bytes()), err)
	}
	log.Info("recv cluster[%d] schedule metric", clusterId)
	return
}

func (m *Metric) handleHotspotMetric(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		reply.Code = -1
		reply.Message = "invalid param"
		log.Warn("invalid param clusterId, err[%v]", err)
		return
	}
	//namespace := r.FormValue("namespace")
	//subsystem := r.FormValue("subsystem")
	bufferLen := int(r.ContentLength)
	if bufferLen <= 0 || bufferLen > 1024*1024*10 {
		bufferLen = 512
	}
	buffer := bufalloc.AllocBuffer(bufferLen)
	defer bufalloc.FreeBuffer(buffer)
	if _, err = buffer.ReadFrom(r.Body); err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("read request failed, err[%v]", err)
		return
	}
	ctx := &Context{clusterId: clusterId}
	err = m.doHotspotMetric(ctx, buffer.Bytes())
	if err != nil {
		reply.Code = -1
		reply.Message = err.Error()
		log.Warn("do hotspot metric[%s] failed, err[%v]", string(buffer.Bytes()), err)
	}
	log.Info("recv cluster[%d] hotspot metric", clusterId)
	return
}

func (m *Metric) handleTcpProcessMetric(w http.ResponseWriter, r *http.Request) {
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		log.Warn("bad request %v, err[%v]", r, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	// MS, DS, GS
	namespace := r.FormValue("namespace")
	// addr string
	subsystem := r.FormValue("subsystem")
	if len(namespace) == 0 || len(subsystem) == 0 {
		log.Warn("bad request %v", r)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		log.Error("server doesn't support hijacking: conn %v", w)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	_conn, bufrw, err := hj.Hijack()
	if err != nil {
		log.Error("http hijack failed, err[%v]", err)
		return
	}
	c := &conn{
		ctx: &Context{
			clusterId: clusterId,
			namespace: namespace,
			subsystem: subsystem,
		},
		item: Type_Process,
		rb:   bufrw.Reader,
		wb:   bufrw.Writer,
		conn: _conn,
	}
   	m.connsLock.Lock()
	m.conns[c] = struct {}{}
	m.connsLock.Unlock()
	m.wg.Add(1)
	go m.work(c)
}

func (m *Metric) handleTcpMacMetric(w http.ResponseWriter, r *http.Request) {
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		log.Warn("bad request %v, err[%v]", r, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	// MS, DS, GS
	namespace := r.FormValue("namespace")
	// addr string
	subsystem := r.FormValue("subsystem")
	if len(namespace) == 0 || len(subsystem) == 0 {
		log.Warn("bad request %v", r)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		log.Error("server doesn't support hijacking: conn %v", w)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	_conn, bufrw, err := hj.Hijack()
	if err != nil {
		log.Error("http hijack failed, err[%v]", err)
		return
	}
	c := &conn{
		ctx: &Context{
			clusterId: clusterId,
			namespace: namespace,
			subsystem: subsystem,
		},
		item: Type_Mac,
		rb:   bufrw.Reader,
		wb:   bufrw.Writer,
		conn: _conn,
	}
	m.connsLock.Lock()
	m.conns[c] = struct {}{}
	m.connsLock.Unlock()
	m.wg.Add(1)
	go m.work(c)
}

func (m *Metric) handleTcpClusterMetric(w http.ResponseWriter, r *http.Request) {
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		log.Warn("bad request %v, err[%v]", r, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		log.Error("server doesn't support hijacking: conn %v", w)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	_conn, bufrw, err := hj.Hijack()
	if err != nil {
		log.Error("http hijack failed, err[%v]", err)
		return
	}
	c := &conn{
		ctx: &Context{
			clusterId: clusterId,
		},
		item: Type_Cluster,
		rb:   bufrw.Reader,
		wb:   bufrw.Writer,
		conn: _conn,
	}
	m.connsLock.Lock()
	m.conns[c] = struct {}{}
	m.connsLock.Unlock()
	m.wg.Add(1)
	go m.work(c)
}

func (m *Metric) handleTcpDbMetric(w http.ResponseWriter, r *http.Request) {
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		log.Warn("bad request %v, err[%v]", r, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	// MS, DS, GS
	namespace := r.FormValue("namespace")
	if len(namespace) == 0 {
		log.Warn("bad request %v", r)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		log.Error("server doesn't support hijacking: conn %v", w)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	_conn, bufrw, err := hj.Hijack()
	if err != nil {
		log.Error("http hijack failed, err[%v]", err)
		return
	}
	c := &conn{
		ctx: &Context{
			clusterId: clusterId,
			namespace: namespace,
		},
		item: Type_DB,
		rb:   bufrw.Reader,
		wb:   bufrw.Writer,
		conn: _conn,
	}
	m.connsLock.Lock()
	m.conns[c] = struct {}{}
	m.connsLock.Unlock()
	m.wg.Add(1)
	go m.work(c)
}

func (m *Metric) handleAppPing(w http.ResponseWriter, r *http.Request) {

}

func (m *Metric) handleTcpTableMetric(w http.ResponseWriter, r *http.Request) {
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		log.Warn("bad request %v, err[%v]", r, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	// MS, DS, GS
	namespace := r.FormValue("namespace")
	// addr string
	subsystem := r.FormValue("subsystem")
	if len(namespace) == 0 || len(subsystem) == 0 {
		log.Warn("bad request %v", r)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		log.Error("server doesn't support hijacking: conn %v", w)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	_conn, bufrw, err := hj.Hijack()
	if err != nil {
		log.Error("http hijack failed, err[%v]", err)
		return
	}
	c := &conn{
		ctx: &Context{
			clusterId: clusterId,
			namespace: namespace,
			subsystem: subsystem,
		},
		item: Type_Table,
		rb:   bufrw.Reader,
		wb:   bufrw.Writer,
		conn: _conn,
	}
	m.connsLock.Lock()
	m.conns[c] = struct {}{}
	m.connsLock.Unlock()
	m.wg.Add(1)
	go m.work(c)
}

func (m *Metric) handleGetMacMetric(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		reply.Code = -1
		reply.Message = "invalid param"
		log.Warn("invalid param clusterId, err[%v]", err)
		return
	}
	// MS, DS, GS
	namespace := r.FormValue("namespace")
	// ip
	ip := r.FormValue("subsystem")

	ctx := &Context{
		clusterId: clusterId,
		namespace: namespace,
		subsystem: ip,
	}
	cluster := m.getCluster(ctx.clusterId)
	if cluster == nil {
		msg := fmt.Sprintf("invalid cluster %d", ctx.clusterId)
		log.Warn("%s", msg)
		reply.Code = -1
		reply.Message = msg
		return
	}
	item := cluster.GetMacItem(ctx.subsystem)
	if item == nil {
		msg := fmt.Sprintf("mac %s not found", ctx.subsystem)
		reply.Code = -1
		reply.Message = msg
		log.Warn("%s", msg)
		return
	}
	reply.Code = 0
	reply.Data = deepcopy.Iface(item.Item)

	return
}

func (m *Metric) handleGetProcessMetric(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		reply.Code = -1
		reply.Message = "invalid param"
		log.Warn("invalid param clusterId, err[%v]", err)
		return
	}
	// MS, DS, GS
	namespace := r.FormValue("namespace")
	// addr string
	addr := r.FormValue("subsystem")
	ctx := &Context{
		clusterId: clusterId,
		namespace: namespace,
		subsystem: addr,
	}
	cluster := m.getCluster(ctx.clusterId)
	if cluster == nil {
		msg := fmt.Sprintf("invalid cluster %d", ctx.clusterId)
		log.Warn("%s", msg)
		reply.Code = -1
		reply.Message = msg
		return
	}
	item := cluster.GetProcessItem(ctx.subsystem)
	if item == nil {
		msg := fmt.Sprintf("process %s not found", ctx.subsystem)
		reply.Code = -1
		reply.Message = msg
		log.Warn("%s", msg)
		return
	}
	reply.Code = 0
	reply.Data = deepcopy.Iface(item.Item)
}

func (m *Metric) handleGetDbMetric(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		reply.Code = -1
		reply.Message = "invalid param"
		log.Warn("invalid param clusterId, err[%v]", err)
		return
	}
	namespace := r.FormValue("namespace")
	//subsystem := r.FormValue("subsystem")
	ctx := &Context{
		clusterId: clusterId,
		namespace: namespace,
	}
	cluster := m.getCluster(ctx.clusterId)
	if cluster == nil {
		msg := fmt.Sprintf("invalid cluster %d", ctx.clusterId)
		log.Warn("%s", msg)
		reply.Code = -1
		reply.Message = msg
		return
	}
	item := cluster.GetDbItem(ctx.namespace)
	if item == nil {
		msg := fmt.Sprintf("db %s not found", ctx.namespace)
		reply.Code = -1
		reply.Message = msg
		log.Warn("%s", msg)
		return
	}
	reply.Code = 0
	reply.Data = item
	return
}

func (m *Metric) handleGetTableMetric(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		reply.Code = -1
		reply.Message = "invalid param"
		log.Warn("invalid param clusterId, err[%v]", err)
		return
	}
	namespace := r.FormValue("namespace")
	subsystem := r.FormValue("subsystem")
	ctx := &Context{
		clusterId: clusterId,
		namespace: namespace,
		subsystem: subsystem,
	}
	cluster := m.getCluster(ctx.clusterId)
	if cluster == nil {
		msg := fmt.Sprintf("invalid cluster %d", ctx.clusterId)
		log.Warn("%s", msg)
		reply.Code = -1
		reply.Message = msg
		return
	}
	item := cluster.GetDbItem(ctx.namespace)
	if item != nil {
		tableItem := item.GetTableItem(ctx.subsystem)
		if tableItem == nil {
			msg := fmt.Sprintf("table %s not found", ctx.subsystem)
			log.Warn("%s", msg)
			reply.Code = -1
			reply.Message = msg
			return
		} else {
			reply.Code = 0
			reply.Data = tableItem
		}
	} else {
		msg := fmt.Sprintf("db %s not found", ctx.namespace)
		reply.Code = -1
		reply.Message = msg
		log.Warn("%s", msg)
		return
	}

	return
}

func (m *Metric) handleGetClusterMetric(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	clusterId, err := strconv.ParseUint(r.FormValue("clusterId"), 10, 64)
	if err != nil {
		reply.Code = -1
		reply.Message = "invalid param"
		log.Warn("invalid param clusterId, err[%v]", err)
		return
	}
	//namespace := r.FormValue("namespace")
	//subsystem := r.FormValue("subsystem")
	ctx := &Context{clusterId: clusterId}
	cluster := m.getCluster(ctx.clusterId)
	if cluster == nil {
		msg := fmt.Sprintf("invalid cluster %d", ctx.clusterId)
		log.Warn("%s", msg)
		reply.Code = -1
		reply.Message = msg
		return
	}
	reply.Code = 0
	reply.Data = cluster.Item
	return
}


type httpReply struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

func sendReply(w http.ResponseWriter, r *httpReply) {
	reply, err := json.Marshal(r)
	if err != nil {
		log.Error("http reply marshal error: %s", err)
		w.WriteHeader(500)
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.Error("http reply[%s] len[%d] write error: %v", string(reply), len(reply), err)
	}
}
