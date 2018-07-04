package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"master-server/raft/logger"
	"master-server/server"
	"util/gogc"
	"util/log"
)

func initRuntime() {
	maxProcs := runtime.NumCPU()
	if maxProcs > 1 {
		maxProcs = maxProcs - 1
	} else {
		maxProcs = 1
	}
	runtime.GOMAXPROCS(maxProcs)
}

func initLog(config *server.Config) {
	if config == nil {
		log.Panic("Config is null!")
	}
	log.InitFileLog(config.Log.Dir, config.Log.Module, config.Log.Level)
}

var (
	configFileName = flag.String("config", "", "Usage : -config conf/config.toml")
	printVersion   = flag.Bool("v", false, "Usage : -v")
)

var (
	// BuildVersion should generate from build script
	BuildVersion = "unknown"
	// BuildDate should generate from build script
	BuildDate = "unknown"
)

func main() {
	flag.Parse()

	if *printVersion {
		fmt.Println("Version:", BuildVersion)
		fmt.Println("Build Date:", BuildDate)
		return
	}

	initRuntime()

	conf := server.NewDefaultConfig()
	if *configFileName != "" {
		err := conf.LoadFromFile(*configFileName)
		if err != nil {
			log.Fatal("load config file failed, err %v", err)
		}
	}
	initLog(conf)
	// config raft logger
	logger.SetLogger(log.GetFileLogger())
	master := new(server.Server)
	fmt.Println("init server")
	master.InitServer(conf)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	go func() {
		sig := <-signalCh
		log.Warn("signal[%v] caught. server exit...", sig)
		master.Quit()
		time.Sleep(time.Second)
		os.Exit(0)
	}()

	go gogc.TickerPrintGCSummary(log.GetFileLogger(), "info")

	fmt.Println("server start")
	err := master.Start()
	if err != nil {
		log.Error("master server start failed, err[%v]", err)
		return
	}
	log.Info("master server start ")
}
