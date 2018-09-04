package alarm

import (
	"context"
	"google.golang.org/grpc"
	"time"
	"model/pkg/alarmpb"
	"model/pkg/taskpb"
)

type AlarmClient interface {
	TaskTimeoutAlarm(clusterId int64, timeoutAlarm *alarmpb.TaskTimeout, task *taskpb.Task, desc string, samples []*Sample) error
	TaskLongTimeRunningAlarm(clusterId int64, longTimeRunningAlarm *alarmpb.TaskLongTimeRunning, task *taskpb.Task, desc string, samples []*Sample) error
	RangeNoHeartbeatAlarm(clusterId int64, rangeNoHbAlarm *alarmpb.RangeNoHeartbeatAlarm, desc string, samples []*Sample) error
	NodeNoHeartbeatAlarm(clusterId int64, nodeNoHbAlarm *alarmpb.NodeNoHeartbeatAlarm, desc string, samples []*Sample) error
	NodeDiskSizeAlarm(clusterId int64, nodeDiskSizeAlarm *alarmpb.NodeDiskSizeAlarm, desc string, samples []*Sample) error
	NodeLeaderCountAlarm(clusterId int64, nodeLeaderCountAlarm *alarmpb.NodeLeaderCountAlarm, desc string, samples []*Sample) error
	SimpleAlarm(clusterId uint64, title, content string, samples []*Sample) error
	Close()
}

type Client struct {
	conn *grpc.ClientConn
}

func NewAlarmClient(addr string) (*Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return &Client{conn: conn}, nil
}

func (c *Client) Close() {
	if c == nil {
		return
	}
	c.conn.Close()
}

func (c *Client) TaskTimeoutAlarm(clusterId int64, timeoutAlarm *alarmpb.TaskTimeout, task *taskpb.Task, desc string, samples []*Sample) error {
	if c == nil {
		return nil
	}
	//cli := alarmpb.NewAlarmClient(c.conn)
	//ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	//defer cancel()
	//_, err := cli.TaskAlarm(ctx, &alarmpb.TaskAlarmRequest{
	//	Head: &alarmpb.RequestHeader{ClusterId: clusterId},
	//	Type: alarmpb.TaskAlarmType_TIMEOUT,
	//	Task: deepcopy.Iface(task).(*taskpb.Task),
	//	TaskTimeoutAlarm: timeoutAlarm,
	//	Describe: desc,
	//	SampleJson: SamplesToJson(samples),
	//})
	return nil
}

func (c *Client) TaskLongTimeRunningAlarm(clusterId int64, longTimeRunningAlarm *alarmpb.TaskLongTimeRunning, task *taskpb.Task, desc string, samples []*Sample) error {
	if c == nil {
		return nil
	}
	//cli := alarmpb.NewAlarmClient(c.conn)
	//ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	//defer cancel()
	//_, err := cli.TaskAlarm(ctx, &alarmpb.TaskAlarmRequest{
	//	Head: &alarmpb.RequestHeader{ClusterId: clusterId},
	//	Type: alarmpb.TaskAlarmType_LONG_TIME_RUNNING,
	//	Task: deepcopy.Iface(task).(*taskpb.Task),
	//	TaskLongTimeRunningAlarm: longTimeRunningAlarm,
	//	Describe: desc,
	//	SampleJson: SamplesToJson(samples),
	//})
	return nil
}

func (c *Client) RangeNoHeartbeatAlarm(clusterId int64, rangeNoHbAlarm *alarmpb.RangeNoHeartbeatAlarm, desc string, samples []*Sample) error {
	if c == nil {
		return nil
	}
	cli := alarmpb.NewAlarmClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := cli.NodeRangeAlarm(ctx, &alarmpb.NodeRangeAlarmRequest{
		Head: &alarmpb.RequestHeader{ClusterId: clusterId},
		Type: alarmpb.NodeRangeAlarmType_RANGE_NO_HEARTBEAT,
		RangeNoHbAlarm: rangeNoHbAlarm,
		Describe: desc,
		SampleJson: SamplesToJson(samples),
	})
	return err
}

func (c *Client) NodeNoHeartbeatAlarm(clusterId int64, nodeNoHbAlarm *alarmpb.NodeNoHeartbeatAlarm, desc string, samples []*Sample) error {
	if c == nil {
		return nil
	}
	cli := alarmpb.NewAlarmClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := cli.NodeRangeAlarm(ctx, &alarmpb.NodeRangeAlarmRequest{
		Head: &alarmpb.RequestHeader{ClusterId: clusterId},
		Type: alarmpb.NodeRangeAlarmType_NODE_NO_HEARTBEAT,
		NodeNoHbAlarm: nodeNoHbAlarm,
		Describe: desc,
		SampleJson: SamplesToJson(samples),
	})
	return err
}

func (c *Client) NodeDiskSizeAlarm(clusterId int64, nodeDiskSizeAlarm *alarmpb.NodeDiskSizeAlarm, desc string, samples []*Sample) error {
	if c == nil {
		return nil
	}
	cli := alarmpb.NewAlarmClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := cli.NodeRangeAlarm(ctx, &alarmpb.NodeRangeAlarmRequest{
		Head: &alarmpb.RequestHeader{ClusterId: clusterId},
		Type: alarmpb.NodeRangeAlarmType_NODE_DISK_SIZE,
		NodeDiskSizeAlarm: nodeDiskSizeAlarm,
		Describe: desc,
		SampleJson: SamplesToJson(samples),
	})
	return err
}

func (c *Client) NodeLeaderCountAlarm(clusterId int64, nodeLeaderCountAlarm *alarmpb.NodeLeaderCountAlarm, desc string, samples []*Sample) error {
	if c == nil {
		return nil
	}
	cli := alarmpb.NewAlarmClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := cli.NodeRangeAlarm(ctx, &alarmpb.NodeRangeAlarmRequest{
		Head: &alarmpb.RequestHeader{ClusterId: clusterId},
		Type: alarmpb.NodeRangeAlarmType_NODE_LEADER_COUNT,
		NodeLeaderCountAlarm: nodeLeaderCountAlarm,
		Describe: desc,
		SampleJson: SamplesToJson(samples),
	})
	return err
}

func (c *Client) SimpleAlarm(clusterId uint64, title, content string, samples []*Sample) error {
	if c == nil {
		return nil
	}
	cli := alarmpb.NewAlarmClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := cli.SimpleAlarm(ctx, &alarmpb.SimpleRequest{
		Head: &alarmpb.RequestHeader{ClusterId: int64(clusterId)},
		Title: title,
		Content: content,
		SampleJson: SamplesToJson(samples),
	})
	return err
}

