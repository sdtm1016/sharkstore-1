package controllers

import (
	"github.com/gin-gonic/gin"
	"console/common"
	"console/service"
	"util/log"
	"strconv"
)

const (
	RANGE_PEERDEL              = "/range/peerDel"
	RANGE_PEERADD              = "/range/peerAdd"
	RANGE_GET_UNHEALTHY_RANGES = "/range/getUnhealthyRanges"
	RANGE_GET_UNSTABLE_RANGES  = "/range/getUnstableRanges"
	RANGE_GET_PEER_INFO        = "/range/getPeerInfo"
	RANGE_GET_RANGE_INFO_BY_ID = "/range/getRangeInfoById"
	RANGE_UPDATE_RANGE         = "/range/updateRange"
	RANGE_OFFLINE_RANGE        = "/range/offlineRange"
	RANGE_REBUILD_RANGE        = "/range/rebuildRange"
	RANGE_REPLACE_RANGE        = "/range/replaceRange"
	RANGE_FORCE_SPLIT          = "/range/forceSplitRange"
	RANGE_FORCE_COMPACT        = "/range/forceCompactRange"
	RANGE_DELETE_RANGE         = "/range/delete"
	RANGE_GET_TOPOLOGY         = "/range/getRangeTopoByRange"
	RANGE_BATCH_RECOVER_RANGE  = "/range/batchRecoverRange"
	RANGE_TRANSFER             = "/range/transfer"
	RANGE_CHANGE_LEADER        = "/range/changeLeader"
	RANGE_OPS_TOPN             = "/range/getOpsTopN"

	RANGE_DUPLICATE_GET = "/table/duplicateRange"

	TASK_GET_PRESENT = "/task/getPresentTaskById"
	TASK_OPERATION   = "/task/taskOperationById"
)

type PeerDelete struct {
}

func NewPeerDelete() *PeerDelete {
	return &PeerDelete{}
}

func (ctrl *PeerDelete) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.PostForm("clusterId")
	rangeId := c.PostForm("rangeId")
	peerId := c.PostForm("peerId")
	if "" == clusterId || "" == rangeId || "" == peerId {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("peer delete exec: clusterId: %v, rangeId: %v, peerId: %v", clusterId, rangeId, peerId)

	clusterId_, _ := strconv.ParseUint(clusterId, 10, 64)
	resp, err := service.NewService().DeletePeer(int(clusterId_), rangeId, peerId)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

type PeerAdd struct {
}

func NewPeerAdd() *PeerAdd {
	return &PeerAdd{}
}

func (ctrl *PeerAdd) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.PostForm("clusterId")
	rangeId := c.PostForm("rangeId")
	if "" == clusterId || "" == rangeId {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("peer add exec: clusterId: %v, rangeId: %v", clusterId, rangeId)

	clusterId_, _ := strconv.ParseUint(clusterId, 10, 64)
	err := service.NewService().AddPeer(int(clusterId_), rangeId)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

type RangeInfoView struct {
}

func NewRangeInfoView() *RangeInfoView {
	return &RangeInfoView{}
}

func (ctrl *RangeInfoView) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.Query("clusterId")
	dbName := c.Query("dbName")
	tableName := c.Query("tableName")
	rangeId := c.Query("rangeId")
	peerId := c.Query("peerId")
	if "" == clusterId || "" == dbName || "" == tableName || "" == rangeId || "" == peerId {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("query range info: clusterId: %v, dbName: %v, tableName: %v, rangeId: %v, peerId: %v", clusterId, dbName, tableName, rangeId, peerId)

	cId, err := strconv.Atoi(clusterId)
	rngId, err2 := strconv.Atoi(rangeId)
	if err != nil || err2 != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	resp, err := service.NewService().GetPeerInfo(cId, dbName, tableName, rngId)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

type PeerInfoView struct {
}

func NewPeerInfoView() *PeerInfoView {
	return &PeerInfoView{}
}

func (ctrl *PeerInfoView) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.Query("clusterId")
	dbName := c.Query("dbName")
	tableName := c.Query("tableName")
	rangeId := c.Query("rangeId")
	if "" == clusterId || "" == dbName || "" == tableName || "" == rangeId {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("query peer info: clusterId: %v, dbName: %v, tableName: %v, rangeId: %v", clusterId, dbName, tableName, rangeId)

	cId, err := strconv.Atoi(clusterId)
	rngId, err2 := strconv.Atoi(rangeId)
	if err != nil || err2 != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	resp, err := service.NewService().GetPeerInfo(cId, dbName, tableName, rngId)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

type GetUnhealthyRanges struct {
}

func NewGetUnhealthyRanges() *GetUnhealthyRanges {
	return &GetUnhealthyRanges{}
}

func (ctrl *GetUnhealthyRanges) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.Query("clusterId")
	dbName := c.Query("dbName")
	tableName := c.Query("tableName")
	rangeId := c.Query("rangeId")
	if "" == clusterId || "" == dbName || "" == tableName {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("query unhealthy ranges: clusterId: %v, dbName: %v, tableName: %v,rangeId:%v", clusterId, dbName, tableName, rangeId)

	cId, err := strconv.Atoi(clusterId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	resp, err := service.NewService().GetUnhealthyRanges(cId, dbName, tableName, rangeId)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

type GetUnstableRanges struct {
}

func NewGetUnstableRanges() *GetUnstableRanges {
	return &GetUnstableRanges{}
}

func (ctrl *GetUnstableRanges) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.Query("clusterId")
	dbName := c.Query("dbName")
	tableName := c.Query("tableName")
	if "" == clusterId || "" == dbName || "" == tableName {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("query unstable ranges: clusterId: %v, dbName: %v, tableName: %v", clusterId, dbName, tableName)

	cId, err := strconv.Atoi(clusterId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	resp, err := service.NewService().GetUnstableRanges(cId, dbName, tableName)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

type TaskPresent struct {
}

func NewTaskPresent() *TaskPresent {
	return &TaskPresent{}
}

func (t *TaskPresent) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.PostForm("clusterId")
	if "" == clusterId {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("get present task: clusterId: %v", clusterId)
	clusterId_, _ := strconv.ParseUint(clusterId, 10, 64)
	resp, err := service.NewService().GetPresentTask(int(clusterId_))
	if err != nil {
		return nil, err
	}
	return resp, nil
}

type TaskOperation struct {
}

func NewTaskOperation() *TaskOperation {
	return &TaskOperation{}
}

func (t *TaskOperation) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.PostForm("clusterId")
	operation := c.PostForm("type")
	taskIds := c.PostForm("taskId") // json []string
	if len(clusterId) == 0 || len(operation) == 0 || len(taskIds) == 0 {
		return nil, common.PARSE_PARAM_ERROR
	}

	clusterId_, _ := strconv.ParseUint(clusterId, 10, 64)
	log.Debug("task operation: clusterId: %v, operation: %v, taskIds: %v", clusterId, operation, taskIds)
	resp, err := service.NewService().TaskOperate(int(clusterId_), operation, taskIds)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

type RangeUpdate struct {
}

func NewRangeUpdate() *RangeUpdate {
	return &RangeUpdate{}
}

func (ctrl *RangeUpdate) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.PostForm("clusterId")
	dbName := c.PostForm("dbName")
	tableName := c.PostForm("tableName")
	rangeId := c.PostForm("rangeId")
	peerId := c.PostForm("peerId")
	if "" == clusterId || "" == dbName || "" == tableName || "" == rangeId || "" == peerId {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("update range: clusterId: %v, dbName: %v, tableName: %v, rangeId: %v", clusterId, dbName, tableName, rangeId)

	cId, err := strconv.Atoi(clusterId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	rngId, err1 := strconv.Atoi(rangeId)
	pId, err2 := strconv.Atoi(peerId)
	if err1 != nil || err2 != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	err = service.NewService().UpdateRange(cId, dbName, tableName, rngId, pId)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

type RangeOffline struct {
}

func NewRangeOffline() *RangeOffline {
	return &RangeOffline{}
}

func (ctrl *RangeOffline) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.PostForm("clusterId")
	dbName := c.PostForm("dbName")
	tableName := c.PostForm("tableName")
	rangeId := c.PostForm("rangeId")
	peerId := c.PostForm("peerId")
	if "" == clusterId || "" == dbName || "" == tableName || "" == rangeId || "" == peerId {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("offline range: clusterId: %v, dbName: %v, tableName: %v, rangeId: %v", clusterId, dbName, tableName, rangeId)

	cId, err := strconv.Atoi(clusterId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	rngId, err1 := strconv.Atoi(rangeId)
	pId, err2 := strconv.Atoi(peerId)
	if err1 != nil || err2 != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	err = service.NewService().OfflineRange(cId, dbName, tableName, rngId, pId)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

type RangeRebuild struct {
}

func NewRangeRebuild() *RangeRebuild {
	return &RangeRebuild{}
}

func (ctrl *RangeRebuild) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.PostForm("clusterId")
	dbName := c.PostForm("dbName")
	tableName := c.PostForm("tableName")
	rangeId := c.PostForm("rangeId")
	if "" == clusterId || "" == dbName || "" == tableName || "" == rangeId {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("rebuild range: clusterId: %v, dbName: %v, tableName: %v, rangeId: %v", clusterId, dbName, tableName, rangeId)

	cId, err := strconv.Atoi(clusterId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	rngId, err := strconv.Atoi(rangeId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	err = service.NewService().RebuildRange(cId, dbName, tableName, rngId)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

type RangeReplace struct {
}

func NewRangeReplace() *RangeReplace {
	return &RangeReplace{}
}

func (ctrl *RangeReplace) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.PostForm("clusterId")
	dbName := c.PostForm("dbName")
	tableName := c.PostForm("tableName")
	rangeId := c.PostForm("rangeId")
	peerId := c.PostForm("peerId")
	if "" == clusterId || "" == dbName || "" == tableName || "" == rangeId || "" == peerId {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("replace range: clusterId: %v, dbName: %v, tableName: %v, rangeId: %v", clusterId, dbName, tableName, rangeId)

	cId, err := strconv.Atoi(clusterId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	rngId, err1 := strconv.Atoi(rangeId)
	pId, err2 := strconv.Atoi(peerId)
	if err1 != nil || err2 != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	err = service.NewService().ReplaceRange(cId, dbName, tableName, rngId, pId)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

type RangeDuplicateAction struct {
}

func NewRangeDuplicateAction() *RangeDuplicateAction {
	return &RangeDuplicateAction{
	}
}
func (ctrl *RangeDuplicateAction) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	dbName := c.PostForm("dbName")
	tableName := c.PostForm("tableName")
	if dbName == "" || tableName == "" {
		return nil, common.PARSE_PARAM_ERROR
	}

	return service.NewService().GetRangeDuplicate(cId, dbName, tableName)
}

type RangeDelete struct {
}

func NewRangeDelete() *RangeDelete {
	return &RangeDelete{}
}

func (ctrl *RangeDelete) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.PostForm("clusterId")
	rangeId := c.PostForm("rangeId")
	if "" == clusterId || "" == rangeId {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("delete range: clusterId: %v,  rangeId: %v", clusterId, rangeId)

	cId, err := strconv.Atoi(clusterId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	rngId, err := strconv.Atoi(rangeId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	err = service.NewService().DeleteRange(cId, rngId)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

type RangeTopo struct {
}

func NewRangeTopo() *RangeTopo {
	return &RangeTopo{}
}

func (ctrl *RangeTopo) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.Query("clusterId")
	rangeId := c.Query("rangeId")
	if len(clusterId) == 0 || len(rangeId) == 0 {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("getting range topology: clusterId: %v,  rangeId: %v", clusterId, rangeId)

	cId, err := strconv.Atoi(clusterId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	rngId, err := strconv.Atoi(rangeId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	data, err := service.NewService().GetRangeTopoByRangeId(cId, rngId)
	if err != nil {
		return nil, err
	}
	return data, nil
}

type RangeBatchRecover struct {
}

func NewRangeBatchRecover() *RangeBatchRecover {
	return &RangeBatchRecover{}
}

func (ctrl *RangeBatchRecover) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.PostForm("clusterId")
	dbName := c.PostForm("dbName")
	tableName := c.PostForm("tableName")

	if "" == clusterId || "" == dbName || "" == tableName {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("recover range: clusterId: %v", clusterId)

	cId, err := strconv.Atoi(clusterId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	err = service.NewService().BatchRecoverRange(cId, dbName, tableName)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

type RangeTransfer struct {
}

func NewRangeTransfer() *RangeTransfer {
	return &RangeTransfer{}
}

func (ctrl *RangeTransfer) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.PostForm("clusterId")
	rangeId := c.PostForm("rangeId")
	peerId := c.PostForm("peerId")

	if "" == clusterId || "" == rangeId || "" == peerId {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("transfer range: clusterId %v, rangeId %v, peerId %v", clusterId, rangeId, peerId)

	cId, err := strconv.Atoi(clusterId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	rngId, err := strconv.Atoi(rangeId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	prId, err := strconv.Atoi(peerId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	err = service.NewService().TransferRange(cId, rngId, prId)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

//切换主
type RangeLeaderChange struct {
}

func NewRangeLeaderChange() *RangeLeaderChange {
	return &RangeLeaderChange{}
}

func (ctrl *RangeLeaderChange) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.PostForm("clusterId")
	rangeId := c.PostForm("rangeId")
	peerId := c.PostForm("peerId")

	if "" == clusterId || "" == rangeId || "" == peerId {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("change range leader: clusterId %v, rangeId %v, peerId %v", clusterId, rangeId, peerId)

	cId, err := strconv.Atoi(clusterId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	rngId, err := strconv.Atoi(rangeId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	prId, err := strconv.Atoi(peerId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	err = service.NewService().ChangeRangeLeader(cId, rngId, prId)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

//range opt topN
type RangeOpsTopN struct {
}

func NewRangeOpsTopN() *RangeOpsTopN {
	return &RangeOpsTopN{}
}

func (ctrl *RangeOpsTopN) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.Query("clusterId")
	topN := c.Query("topN")

	if "" == clusterId || "" == topN {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("query cluster range topN: clusterId %v, topN %v", clusterId, topN)

	cId, err := strconv.Atoi(clusterId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	tN, err := strconv.Atoi(topN)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	return service.NewService().GetRangeOpsTopN(cId, tN)
}

type RangeForceSplit struct {
}

func NewRangeForceSplit() *RangeForceSplit {
	return &RangeForceSplit{}
}

func (ctrl *RangeForceSplit) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.PostForm("clusterId")
	dbName := c.PostForm("dbName")
	tableName := c.PostForm("tableName")
	rangeId := c.PostForm("rangeId")
	if "" == clusterId || "" == dbName || "" == tableName || "" == rangeId {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("force split range: clusterId: %v, dbName: %v, tableName: %v, rangeId: %v", clusterId, dbName, tableName, rangeId)

	cId, err := strconv.Atoi(clusterId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	rngId, err := strconv.Atoi(rangeId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	err = service.NewService().ForceSplitRange(cId, rngId, dbName, tableName)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

type RangeForceCompact struct {
}

func NewRangeForceCompact() *RangeForceCompact {
	return &RangeForceCompact{}
}

func (ctrl *RangeForceCompact) Execute(c *gin.Context) (interface{}, error) {
	clusterId := c.PostForm("clusterId")
	dbName := c.PostForm("dbName")
	tableName := c.PostForm("tableName")
	rangeId := c.PostForm("rangeId")
	if "" == clusterId || "" == dbName || "" == tableName || "" == rangeId {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("force compact range: clusterId: %v, dbName: %v, tableName: %v, rangeId: %v", clusterId, dbName, tableName, rangeId)

	cId, err := strconv.Atoi(clusterId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	rngId, err := strconv.Atoi(rangeId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	resp, err := service.NewService().ForceCompactRange(cId, rngId, dbName, tableName)
	if err != nil {
		return nil, err
	}
	return resp, nil
}