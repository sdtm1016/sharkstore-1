package controllers

import (
	"github.com/gin-gonic/gin"
	"console/common"
	"console/service"
	"util/log"
	"encoding/json"
	"strconv"
	"strings"
)

const (
	REQURL_METRIC_SERVER_GETALL = "/metric/server/getAll"
	REQURL_METRIC_SERVER_ADD    = "/metric/server/add"
	REQURL_METRIC_SERVER_DEL    = "/metric/server/del"

	REQURL_METRIC_CONFIG_SET = "/metric/config/set"
	REQURL_METRIC_CONFIG_GET = "/metric/config/get"
)

/**
 * 查询监控服务实例列表
 */
type GetMetricServerAction struct {
}

func NewGetMetricServerAction() *GetMetricServerAction {
	return &GetMetricServerAction{
	}
}
func (gmc *GetMetricServerAction) Execute(c *gin.Context) (interface{}, error) {
	return service.NewService().GetAllMetricServer()
}

/**
 * 添加监控服务实例
 */
type AddMetricServerAction struct {
}

func NewAddMetricServerAction() *AddMetricServerAction {
	return &AddMetricServerAction{
	}
}
func (ams *AddMetricServerAction) Execute(c *gin.Context) (interface{}, error) {
	addr := c.PostForm("addr")
	if addr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	return nil, service.NewService().CreateMetricServer(addr)
}

/**
 * 删除监控服务实例
 */
type DelMetricServerAction struct {
}

func NewDelMetricServerAction() *DelMetricServerAction {
	return &DelMetricServerAction{
	}
}
func (dms *DelMetricServerAction) Execute(c *gin.Context) (interface{}, error) {
	addrsStr := c.PostForm("addrs")
	if addrsStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("delete metric server: %v", addrsStr)
	var addrs []string
	if err := json.Unmarshal([]byte(addrsStr), &addrs); err != nil {
		log.Error("http delete metric server: %v", err.Error())
		return nil, common.PARSE_PARAM_ERROR
	}
	return nil, service.NewService().DeleteMetricServer(addrs)
}

/**
 * 调整监控配置
 */
type SetMetricConfigAction struct {
}

func NewSetMetricConfigAction() *SetMetricConfigAction {
	return &SetMetricConfigAction{
	}
}
func (smc *SetMetricConfigAction) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	addrStr := c.PostForm("metricAddr")
	intervalStr := c.PostForm("metricInterval")
	if cIdStr == "" || addrStr == "" || intervalStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	if !strings.HasPrefix(intervalStr, "\""){
		intervalStr = "\"" + intervalStr
	}
	if !strings.HasSuffix(intervalStr, "\""){
		intervalStr = intervalStr + "\""
	}

	log.Debug("set cluster %v metric client config: %v, %v", cId, addrStr, intervalStr)
	return service.NewService().SetMetricConfig(cId, addrStr, intervalStr)
}

/**
 * 获取监控配置
 */
type GetMetricConfigAction struct {
}

func NewGetMetricConfigAction() *GetMetricConfigAction {
	return &GetMetricConfigAction{
	}
}
func (gmc *GetMetricConfigAction) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}

	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	log.Debug("get cluster %v metric server:", cId)
	return service.NewService().GetMetricConfig(cId)
}
