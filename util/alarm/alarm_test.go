package alarm

import (
	"testing"
	"context"
	"net/http"
	"time"
	"strconv"
)

func TestAlarm(t *testing.T) {
	//ctx, cancel:= context.WithCancel(context.Background())
	ctx, _ := context.WithCancel(context.Background())
	_, err := NewAlarmServer(ctx, 2222, "http://localhost:3333", &SimpleReceiver{
		Users: []*User{&User{
			Mail: "test mail",
			Sms: "test sms",
		}},
	})
	if err != nil {
		t.Fatalf("NewAlarmServer failed: %v", err)
	}
	go func() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		l := r.Header.Get("len")
		t.Log("body len: ", l)
		l_, _ := strconv.ParseInt(l, 10, 64)
		buf := make([]byte, l_)
		n, err := r.Body.Read(buf)
		if n != int(l_) {
			t.Errorf("n: %v, err: %v, buf: %v", n, err, string(buf))
			return
		}
		defer r.Body.Close()
		t.Logf("http body: %v", string(buf[:n]))
		w.Write([]byte("ok"))
	})
	http.ListenAndServe(":3333", nil)
	}()

	time.Sleep(time.Second)
	cli, err := NewAlarmClient(":2222")
	if err != nil {
		t.Fatalf("NewAlarmClient failed: %v", err)
	}

	if err := cli.RangeNoHeartbeatAlarm(2, nil, "this range no hb"); err != nil {
		t.Fatalf("alarm range no hb error: %v", err)
	}

	if err := cli.TaskTimeoutAlarm(1, nil, nil, "this task is timeout"); err != nil {
		t.Fatalf("alarm timeout error: %v", err)
	}

	if err := cli.TaskTimeoutAlarm(3, nil, nil, "this task is timeout"); err != nil {
		t.Fatalf("alarm timeout error: %v", err)
	}
	time.Sleep(3*time.Second)
}
