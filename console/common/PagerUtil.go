package common

import (
	"console/models"
	"util/log"

	"github.com/gin-gonic/gin"
	"strconv"
)

func GetPagerInfo(c *gin.Context) (*models.PagerInfo, error) {
	pageIndexStr := c.Query("page")
	pageSizeStr := c.Query("rows")
	name := c.Query("sort")
	order := c.Query("sortOrder")
	log.Debug("init param : page: %v, rows:%v, sortName:%v, sortOrder:%v", pageIndexStr, pageSizeStr, name, order)
	if pageIndexStr == "" {
		pageIndexStr = "1"
	}
	if pageSizeStr == "" {
		pageSizeStr = "10"
	}
	pageIndex, err1 := strconv.Atoi(pageIndexStr)
	pageSize, err2 := strconv.Atoi(pageSizeStr)
	if err1 != nil || err2 != nil{
		return nil, PARAM_FORMAT_ERROR
	}
 	return &models.PagerInfo{
		PageIndex: pageIndex,
		PageSize: pageSize,
		SortName: name,
		SortOrder: order,
	}, nil
}