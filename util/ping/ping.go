package ping

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
	"strings"
)

func getAppInfo() (app string, ip []string) {
	app = filepath.Base(os.Args[0])

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return app, ip
	}
	for _, address := range addrs {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ip = append(ip, ipnet.IP.String())
				continue
			}
		}
	}
	return app, ip
}

func Ping(clusterId, pingBaseUrl string, ping_interval int64) {
	app, ips := getAppInfo()
	if len(ips) == 0 {
		return
	}

	url := fmt.Sprintf(`http://%s/app/ping?cluster_id=%s&app_name=%s&ip_addrs=%s&ping_interval=%d`,
		pingBaseUrl, clusterId, app, strings.Join(ips, ","), ping_interval)

	timer := time.NewTicker(time.Duration(ping_interval) * time.Second)
	for {
		select {
		case <-timer.C:
			resp, err := http.Get(url)
			if err != nil {
				fmt.Println(err)
			}
			if resp != nil {
				resp.Body.Close()
			}
		}
	}
}
