package controllers

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"console/service"
	"console/common"
	"util/log"
)

const (
	REQURI_SCHEDULER_GETALL = "/scheduler/getAll"
	REQURI_SCHEDULER_GETDETAIL = "/scheduler/getDetail"
	REQURI_SCHEDULER_ADJUST = "/scheduler/adjust"

	REQURI_TASK_GETTASKTYPEALL="/task/getTypeAll"
)

/**
 * 查询集群正在执行的调度任务列表
 */
type SchedulerAllAction struct {
}
func NewSchedulerAllAction() *SchedulerAllAction {
	return &SchedulerAllAction{
	}
}
func (ctrl *SchedulerAllAction)Execute(c *gin.Context) (interface{}, error) {
	log.Debug("start to query scheduler")
	cIdStr := c.Query("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	return service.NewService().GetSchedulerAll(cId)
}

type SchedulerDetailAction struct {
}
func NewSchedulerDetailAction() *SchedulerDetailAction {
	return &SchedulerDetailAction{
	}
}
func (ctrl *SchedulerDetailAction)Execute(c *gin.Context) (interface{}, error) {
	log.Debug("start to query scheduler detail")
	cIdStr := c.Query("clusterId")
	schedulerName := c.Query("name")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	return service.NewService().GetSchedulerDetail(cId, schedulerName)
}

/**
 * 集群scheduler调整
 */
type SchedulerAdjustAction struct {
}
func NewSchedulerAdjustAction() *SchedulerAdjustAction {
	return &SchedulerAdjustAction{
	}
}
func (ctrl *SchedulerAdjustAction)Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	optType := c.PostForm("optType")
	scheduler := c.PostForm("scheduler")
	if optType == "" || scheduler == ""{
		return nil, common.PARSE_PARAM_ERROR
	}

	optI, err := strconv.Atoi(optType)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	log.Debug("schedule adjust. cid:[%v]", cId)

	err = service.NewService().AdjustScheduler(cId, optI, scheduler)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

type TaskTypeAllAction struct {
}
func NewTaskTypeAllAction() *TaskTypeAllAction {
	return &TaskTypeAllAction{
	}
}
func (ctrl *TaskTypeAllAction)Execute(c *gin.Context) (interface{}, error) {
	log.Debug("start to query task type")
	cIdStr := c.Query("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	return service.NewService().GetTaskType(cId)
}