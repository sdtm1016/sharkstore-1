package server

import (
	"fmt"
	"time"

	"model/pkg/ds_admin"
	"model/pkg/metapb"
	"util/log"
)

func (c *Cluster) NodeLogin(nodeId uint64) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	node := c.FindNodeById(nodeId)
	if node == nil {
		return ErrNotExistNode
	}

	// TODO version update
	if err := c.UpdateNodeState(node, metapb.NodeState_N_Login); err != nil {
		return err
	}
	node.LastHeartbeatTS = time.Now()
	return nil
}

/**
  get nodeId, and clean up command
*/
func (c *Cluster) GetNodeId(serverAddr, raftAddr, adminAddr, version string) (*Node, bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if node := c.FindNodeByAddr(serverAddr); node != nil {
		log.Debug("node:[%v] start or find", node)
		//需要clean up, 然后上线
		if node.IsLogout() {
			return node, true, nil
		}
		return node, false, nil
	} else {
		nodeId, err := c.GenId()
		if err != nil {
			return nil, false, err
		}
		n := &metapb.Node{
			Id:         nodeId,
			ServerAddr: serverAddr,
			RaftAddr:   raftAddr,
			AdminAddr:  adminAddr,
			State:      metapb.NodeState_N_Initial,
			Version:    version,
		}
		node = NewNode(n)

		err = c.AddNode(node)
		if err != nil {
			log.Error("node add err %v", node)
			return nil, false, err
		}
		log.Debug("node add %v", node)
		return node, false, nil
	}
}

func (c *Cluster) AddNode(node *Node) error {
	err := c.storeNode(node.Node)
	if err != nil {
		log.Error("store node:[%v] failed, err:[%v]", node.Node, err)
		return err
	}
	c.nodes.Add(node)
	return nil
}

// TODO 非故障节点???
func (c *Cluster) DeleteNodeById(id uint64) error {
	if _, find := c.nodes.FindNodeById(id); find {
		key := []byte(fmt.Sprintf("%s%d", PREFIX_NODE, id))
		if err := c.store.Delete(key); err != nil {
			return err
		}
		c.nodes.DeleteById(id)
		return nil
	}
	return ErrNotExistNode
}

func (c *Cluster) DeleteNodeByAddr(addr string) error {
	if n, find := c.nodes.FindNodeByAddr(addr); find {
		key := []byte(fmt.Sprintf("%s%d", PREFIX_NODE, n.GetId()))
		if err := c.store.Delete(key); err != nil {
			return err
		}
		c.nodes.DeleteByAddr(addr)
		return nil
	}
	return ErrNotExistNode
}

func (c *Cluster) FindNodeById(id uint64) *Node {
	if node, find := c.nodes.FindNodeById(id); find {
		return node
	}
	return nil
}

func (c *Cluster) FindNodeByAddr(addr string) *Node {
	if node, find := c.nodes.FindNodeByAddr(addr); find {
		return node
	}
	return nil
}

func (c *Cluster) GetAllActiveNode() []*Node {
	return c.nodes.GetAllActiveNode()
}

func (c *Cluster) GetAllNode() []*Node {
	return c.nodes.GetAllNode()
}

func (c *Cluster) UpdateNode(node *Node) error {
	return c.storeNode(node.Node)
}

func (c *Cluster) NodeUpgrade(nodeID uint64) error {
	node := c.FindNodeById(nodeID)
	if node == nil {
		return ErrNotExistNode
	}
	if !node.isUp() {
		return ErrNotActiveNode
	}
	if node.isBlocked() {
		return ErrNodeBlocked
	}
	return c.UpdateNodeState(node, metapb.NodeState_N_Upgrade)
}

func (c *Cluster) UpdateNodeState(n *Node, state metapb.NodeState) error {
	oldState := n.State
	n.State = state
	err := c.storeNode(n.Node)
	if err != nil {
		n.State = oldState
		log.Error("store node[%v] state failed, err[%v]", n.Node, err)
		return err
	}
	log.Info("Node id:[%v] state had changed [%v]===>[%v]", n.Id, oldState, state)
	return nil
}

func (c *Cluster) LoginNode(nodeId uint64, force bool) error {
	node := c.FindNodeById(nodeId)
	if node == nil {
		return ErrNotExistNode
	}
	if !force {
		if !node.IsLogout() {
			log.Error("Cannot login this node. current state:[%d] is not logout", node.State)
			return ErrNodeStateConfused
		}
	}

	if err := c.UpdateNodeState(node, metapb.NodeState_N_Initial); err != nil {
		return err
	}

	return nil
}

func (c *Cluster) LogoutNode(nodeId uint64) error {
	log.Warn("node %v logout", nodeId)
	node := c.FindNodeById(nodeId)
	if node == nil {
		return ErrNotExistNode
	}

	if node.IsLogout() {
		log.Info("node:[%d] has already is logout", node.Id)
		return nil
	}

	if err := c.UpdateNodeState(node, metapb.NodeState_N_Logout); err != nil {
		return err
	}

	return nil
}

func (c *Cluster) UpgradeNode(nodeId uint64) error {
	log.Warn("node %v upgrade", nodeId)
	node := c.FindNodeById(nodeId)
	if node == nil {
		return ErrNotExistNode
	}

	if node.IsLogout() {
		log.Error("Cannot upgrade this node. current state:[%d] is logout", node.State)
		return ErrNodeStateConfused
	}

	if err := c.UpdateNodeState(node, metapb.NodeState_N_Upgrade); err != nil {
		return err
	}

	return nil
}

func (c *Cluster) getRangeNodes(r *Range) []*Node {
	var nodes []*Node
	for _, peer := range r.GetPeers() {
		node := c.FindNodeById(peer.GetNodeId())
		if node != nil {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func (c *Cluster) blockNode(nodeID uint64) error {
	node := c.FindNodeById(nodeID)
	if node == nil {
		return ErrNotExistNode
	}
	if node.isBlocked() {
		return ErrNodeBlocked
	}
	node.block()
	return nil
}

func (c *Cluster) unblockNode(nodeID uint64) {
	node := c.FindNodeById(nodeID)
	if node == nil {
		log.Fatal("node %d is unblocked, but it is not found", nodeID)
	}
	node.unblock()
}

func (c *Cluster) totalWrittenBytes() uint64 {
	var totalWrittenBytes uint64
	for _, n := range c.GetAllNode() {
		if n.isUp() {
			totalWrittenBytes += n.stats.BytesWritten
		}
	}
	return totalWrittenBytes
}

func (c *Cluster) setNodeLogLevelRemote(nodeId uint64, logLevel string) error {
	node := c.FindNodeById(nodeId)
	if node == nil {
		return ErrNotExistNode
	}
	err := c.cli.SetNodeLogLevel(node.GetServerAddr(), logLevel)
	if err != nil {
		log.Warn("update node log level: node[%s] log level:[%v] failed, err[%v]", node.GetServerAddr(), logLevel, err)
		return err
	}
	return nil
}

func (c *Cluster) setConfigRemote(nodeId uint64, configs []*ds_adminpb.ConfigItem) error {
	node := c.FindNodeById(nodeId)
	if node == nil {
		return ErrNotExistNode
	}
	err := c.adminCli.SetConfig(node.GetAdminAddr(), configs)
	if err != nil {
		log.Warn("set config[%v] of node[%s] failed, error[%v]", nil, node.GetServerAddr(), err)
		return err
	}
	return nil
}

func (c *Cluster) getConfigRemote(nodeId uint64, keys []*ds_adminpb.ConfigKey) (*ds_adminpb.GetConfigResponse, error) {
	node := c.FindNodeById(nodeId)
	if node == nil {
		return nil, ErrNotExistNode
	}
	resp, err := c.adminCli.GetConfig(node.GetAdminAddr(), keys)
	if err != nil {
		log.Warn("get config of node[%s] failed, error[%v]", node.GetServerAddr(), err)
		return nil, err
	}
	return resp, nil
}

func (c *Cluster) getDsInfoRemote(nodeId uint64, path string) (*ds_adminpb.GetInfoResponse, error) {
	node := c.FindNodeById(nodeId)
	if node == nil {
		return nil, ErrNotExistNode
	}
	resp, err := c.adminCli.GetInfo(node.GetAdminAddr(), path)
	if err != nil {
		log.Warn("get ds_info of node[%s] failed, error[%v]", node.GetServerAddr(), err)
		return nil, err
	}
	return resp, nil
}

func (c *Cluster) clearQueueRemote(nodeId uint64, queueType ds_adminpb.ClearQueueRequest_QueueType) (*ds_adminpb.ClearQueueResponse, error) {
	node := c.FindNodeById(nodeId)
	if node == nil {
		return nil, ErrNotExistNode
	}
	resp, err := c.adminCli.ClearQueue(node.GetAdminAddr(), queueType)
	if err != nil {
		log.Warn("clear queueType[%v] of node[%s] failed, error[%v]", queueType, node.GetServerAddr(), err)
		return nil, err
	}
	return resp, nil
}

func (c *Cluster) getPendingQueuesRemote(nodeId uint64, pendingType ds_adminpb.GetPendingsRequest_PendingType, count uint64) (*ds_adminpb.GetPendingsResponse, error) {
	node := c.FindNodeById(nodeId)
	if node == nil {
		return nil, ErrNotExistNode
	}
	resp, err := c.adminCli.GetPendingQueues(node.GetAdminAddr(), pendingType, count)
	if err != nil {
		log.Warn("get pending[type=%d] queues of node[%s] failed, error[%v]", pendingType, node.GetServerAddr(), err)
		return nil, err
	}
	return resp, nil
}

func (c *Cluster) flushDBRemote(nodeId uint64, wait bool) error {
	node := c.FindNodeById(nodeId)
	if node == nil {
		return ErrNotExistNode
	}
	err := c.adminCli.FlushDB(node.GetAdminAddr(), wait)
	if err != nil {
		log.Warn("flush db[wait=%t] of node[%s] failed, error[%v]", wait, node.GetServerAddr(), err)
		return err
	}
	return nil
}
