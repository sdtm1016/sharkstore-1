package server

import (
	"testing"
	"util"
	"time"
	"util/log"
	"flag"
)


var (
	configFileName = flag.String("config", "", "Usage : -config conf/config.toml")
)

func TestConfig_LoadConfig(t *testing.T) {
	conf := new(Config)
	conf.LoadConfig(configFileName)
	delay(conf, 3, t)

	t.Logf("%v", conf)


}

func delay(config *Config, class int, t *testing.T)  {
	var slowLogThreshold util.Duration
	start := time.Now()
	defer func() {
		delay := time.Now().Sub(start)
		if delay > slowLogThreshold.Duration {
			t.Logf("delay %v" , delay)
		} else {
			t.Logf("no delay, %v", delay)
		}
	}()
	switch class {
	case 1:
		slowLogThreshold = config.Performance.SelectSlowLog
		log.Debug("select %v", slowLogThreshold.Duration)
	case 2:
		slowLogThreshold = config.Performance.InsertSlowLog
		log.Debug("insert %v", slowLogThreshold.Duration)
	case 3:
		slowLogThreshold = config.Performance.DeleteSlowLog
		log.Debug("delete %v", slowLogThreshold.Duration)
	}

	time.Sleep(10 * time.Millisecond)
}