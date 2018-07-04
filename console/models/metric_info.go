package models

import "util"

type MetricServer struct {
	Addr string `json:"addr"`
}


type MetricConfig struct {
	Interval util.Duration `json:"interval"`
	Address  string        `json:"address"`
}