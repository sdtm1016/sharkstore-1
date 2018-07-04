package alarm

import (
	"strconv"
	"strings"
	"time"
	"util/log"
)

type Sample struct {
	ip        string
	port      int
	spaceId   int
	timestamp time.Time
	info      map[string]interface{}
	config    map[string]interface{}
	privData  []byte
}

func NewSample(ip string, port int, spaceId int, info map[string]interface{}) *Sample {
	return &Sample{
		//ip: ip,
		//port: port,
		//spaceId: spaceId,
		//timestamp: time.Now(),
		info: info,
	}
}

func SamplesToJson(samples []*Sample) []string {
	var ret []string
	for _, sample := range samples {
		log.Info("samples to json: %v", sample.ToJson())
		ret = append(ret, sample.ToJson())
	}
	return ret
}

func (sample *Sample) ToJson() string {
	info := sample.info
	var json = make([]string, 0, len(info))
	for key, element := range info {
		var val string
		switch threshold := element.(type) {
		case string:
			if strings.EqualFold("config_file", key) {
				continue
			}
			val = threshold
		case int:
			if strings.EqualFold("master_port", key) && threshold == 0 {
				val = ""
			} else {
				val = strconv.FormatInt(int64(threshold), 10)
			}
		case int8:
			val = strconv.FormatInt(int64(threshold), 10)
		case int16:
			val = strconv.FormatInt(int64(threshold), 10)
		case int32:
			val = strconv.FormatInt(int64(threshold), 10)
		case int64:
			val = strconv.FormatInt(threshold, 10)
		case uint64:
			val = strconv.FormatUint(threshold, 10)
		case float32:
			val = strconv.FormatFloat(float64(threshold), 'f', -1, 64)
		case float64:
			val = strconv.FormatFloat(threshold, 'f', -1, 64)
		case bool:
			val = strconv.FormatBool(threshold)
		}
		json = append(json, "\""+key+"\":\""+val+"\"")
	}

	ret := "{\"ip\":\"" + sample.ip + "\",\"port\":\"" + strconv.Itoa(sample.port) + "\",\"privData\":\"" + string(sample.privData) + "\"," + strings.Join(json, ",") + "}"
	log.Info("sample to json: %v", ret)
	return ret
}


