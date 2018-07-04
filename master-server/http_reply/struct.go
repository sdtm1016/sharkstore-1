package http_reply

import (
	"net/http"
	"io/ioutil"
	"fmt"
	"encoding/json"
	"model/pkg/metapb"
	"strconv"
	"util/log"
	"crypto/md5"
)

type Reply struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

type RangeLocateRequest struct {
	DbName    string `json:"db_name"`
	TableName string `json:"table_name"`
}

type Peer struct {
	Id   uint64       `json:"id,omitempty"`
	Node *metapb.Node `json:"node,omitempty"`
}
type Range struct {
	Id         uint64             `json:"id,omitempty"`
	StartKey   []byte             `json:"start_key,omitempty"`
	EndKey     []byte             `json:"end_key,omitempty"`
	RangeEpoch *metapb.RangeEpoch `json:"range_epoch,omitempty"`
	Peers      []*Peer            `json:"peers,omitempty"`
}
type Route struct {
	Range  *Range   `json:"range,omitempty"`
	Leader *Peer    `json:"leader,omitempty"`
	Downs  []uint64 `json:"downs"`
	Pends  []uint64 `json:"pends"`
}
type RangeLocateResponse struct {
	Routes []*Route `json:"routes"`
}

func GetRangeLocateRequest(r *http.Request) (*RangeLocateRequest, error) {
	//data, err := ioutil.ReadAll(r.Body)
	//if err != nil {
	//	return nil, fmt.Errorf("RangeLocate read body: %v", err)
	//}
	//req := new(RangeLocateRequest)
	//if err := json.Unmarshal(data, req); err != nil {
	//	return nil, fmt.Errorf("RangeLocate decode: %v", err)
	//}
	//return req, nil

	return &RangeLocateRequest{
		DbName:    r.FormValue("dbName"),
		TableName: r.FormValue("tableName"),
	}, nil
}

type Task struct {
	Id       uint64 `json:"id"`
	Type     string `json:"type"`
	RangeId  uint64 `json:"range_id"`
	Describe string `json:"describe"`
	State    string `json:"state"`
}

type TaskResponse []*Task

func GetResponse(resp *http.Response) (*Reply, error) {
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("get request read body: %v", err)
	}
	req := new(Reply)
	if err := json.Unmarshal(data, req); err != nil {
		return nil, fmt.Errorf("get request decode: %v", err)
	}
	return req, nil
}

func SendReply(w http.ResponseWriter, rep *Reply) {
	reply, err := json.Marshal(rep)
	if err != nil {
		log.Error("send response marshal: %s", err)
		w.WriteHeader(500)
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.Error("send response [%s] len[%d] write error: %v", string(reply), len(reply), err)
	}
}

func GenSign(clusterId uint64, d int64, token string) string {
	h := md5.New()
	h.Write([]byte(fmt.Sprintf("%d", clusterId)))
	h.Write([]byte(fmt.Sprintf("%d", d)))
	h.Write([]byte(token))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func VerifySign(clusterId uint64, d, token, s string) bool {
	h := md5.New()
	h.Write([]byte(fmt.Sprintf("%d", clusterId)))
	h.Write([]byte(d))
	h.Write([]byte(token))
	if s != fmt.Sprintf("%x", h.Sum(nil)) {
		return false
	}
	return true
}

type RangeBrief struct {
	Id         uint64   `json:"id,omitempty"`
	StartKey   string   `json:"start_key,omitempty"`
	EndKey     string   `json:"end_key,omitempty"`
	State      int32    `json:"state,omitempty"`
	LastHbTime string   `json:"last_hb_time,omitempty"`
	DownPeers  []uint64 `json:"down_peers,omitempty"`
	Peers      []uint64 `json:"peers,omitempty"`
	Leader     uint64   `json:"leader,omitempty"`
}

type RangeBriefReply struct {
	Code    int           `json:"code"`
	Message string        `json:"message"`
	Data    []*RangeBrief `json:"data"`
}

type PeerInfo struct {
	Id uint64 `json:"id,omitempty"`
	// Range key range [start_key, end_key).
	StartKey   []byte             `json:"start_key,omitempty"`
	EndKey     []byte             `json:"end_key,omitempty"`
	RangeEpoch *metapb.RangeEpoch `json:"range_epoch,omitempty"`
	Peer       *Peer              `json:"peers,omitempty"`
	// Range state
	State string `json:"state,omitempty"`
}

type PeerInfoReply struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    []*PeerInfo `json:"data"`
}

type PeerBrief struct {
	Id          uint64 `json:"id,omitempty"`
	Index       uint64 `json:"index,omitempty"`
	Term        uint64 `json:"term,omitempty"`
	Commit      uint64 `json:"commit,omitempty"`
	StartKey    string `json:"start_key,omitempty"`
	EndKey      string `json:"end_key,omitempty"`
	NodeId      uint64 `json:"node_id,omitempty"`
	NodeAddress string `json:"node_address,omitempty"`
	NodeState   int32  `json:"node_state,omitempty"`
}

type PeerBriefReply struct {
	Code    int          `json:"code"`
	Message string       `json:"message"`
	Data    []*PeerBrief `json:"data"`
}

type RangeStatsInfo struct {
	RangeId         uint64 `json:"range_id,omitempty"`
	LeaderId        uint64 `json:"leader_id,omitempty"`
	TableId        uint64 `json:"table_id,omitempty"`
	NodeAddr       string `json:"node_addr,omitempty"`
	BytesWritten    uint64 `json:"bytes_written,omitempty"`
	BytesRead       uint64 `json:"bytes_read,omitempty"`
	KeysWritten     uint64 `json:"keys_written,omitempty"`
	KeysRead        uint64 `json:"keys_read,omitempty"`
	ApproximateSize uint64 `json:"approximate_size,omitempty"`
	WriteOps        uint64 `json:"write_ops,omitempty"`
}

type RangeStatHeap []RangeStatsInfo

func (h RangeStatHeap) Len() int           { return len(h) }
func (h RangeStatHeap) Less(i, j int) bool {
	return h[i].WriteOps > h[j].WriteOps
}
func (h RangeStatHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *RangeStatHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(RangeStatsInfo))
}

func (h *RangeStatHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
