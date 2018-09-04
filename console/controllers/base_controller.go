package controllers

import (
	"github.com/gin-gonic/gin"
)

type Response struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

type PageData struct {
	PageIndex int         `json:"pageIndex"`
	PageSize  int         `json:"pageSize"`
	Total     int         `json:"total"`
	Data      interface{} `json:"data"`
}

type Action interface {
	Execute(c *gin.Context) (interface{}, error)
}
