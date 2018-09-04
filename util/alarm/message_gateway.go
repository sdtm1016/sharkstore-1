package alarm

import (
	"net/http"
	"strings"
	"encoding/json"
	"context"
	"util/log"
	"time"
	"errors"
	"io/ioutil"
)


const (
	MESSAGE_LEVEL_INFO 		= iota
	MESSAGE_LEVEL_WARN
	MESSAGE_LEVEL_ERROR
)

type Message struct {
	ClusterId int64
	Title string
	Content string
	Level int
	samples []string
}

type MessageTo struct {
	Title string 		`json:"title"`
	Content string 		`json:"content"`
	MailTo string 		`json:"mailTo"`
	SmsTo string 		`json:"smsTo"`
}

type MessageGateway struct {
	client *http.Client // direct push to message gateway

	alarmServerAddress string // alarm server address
	pusher *http.Client // direct to alarm server

	messages chan Message

	ctx context.Context
}

const (
	MESSAGEGATEWAY_WAIT_QUEUE_LENGTH = 10000
)

func NewMessageGateway(ctx context.Context, alarmServerAddr string) *MessageGateway {
	gw := &MessageGateway{
		client: &http.Client{
			Timeout: time.Second*3,
		},
		alarmServerAddress: alarmServerAddr,
		pusher: &http.Client{
			Timeout: time.Second*3,
			//Transport: &http.Transport{
			//	Dial: func(netw, addr string) (net.Conn, error) {
			//		c, err := net.DialTimeout(netw, addr, connTimeout)
			//		if err != nil {
			//			return nil, err
			//		}
			//
			//		return c, nil
			//	},
			//	MaxIdleConnsPerHost:   2,
			//	ResponseHeaderTimeout: readTimeout,
			//},
		},
		messages: make(chan Message, MESSAGEGATEWAY_WAIT_QUEUE_LENGTH),
		ctx: ctx,
	}

	go gw.wait()
	return gw
}

func (gw *MessageGateway) send_(msg string) error {
	//req, err := http.NewRequest("POST", gw.address, strings.NewReader(msg))
	//if err != nil {
	//	return err
	//}
	//req.Header.Set("len", fmt.Sprint(len(msg)))
	//req.Header.Add("User-Agent", "Jimdb-Message-Sender")
	//req.Header.Set("Content-Type", "application/json;charset=utf-8")
	//req.Header.Set("Accept", "application/json,text/html,text/plain")
	//req.Header.Set("Accept-Charset", "utf-8,GBK")
	//
	//resp, err := gw.client.Do(req)
	////_, err = gw.client.Do(req)
	//if err != nil {
	//	return err
	//}
	//defer resp.Body.Close()
	return nil
}

func (gw *MessageGateway) send(title, content string, mailTo, smsTo[]string) error {
	//fmt.Printf("title: %v, content: %v, mailTo: %v, smsTo: %v\n", title, content, mailTo, smsTo)
	msgTo := &MessageTo{
		Title: title,
		Content: content,
		MailTo: strings.Join(mailTo, ","),
		SmsTo: strings.Join(smsTo, ","),
	}

	data, _ := json.Marshal(msgTo) // todo template deal with
	return gw.send_(string(data))
}

func (gw *MessageGateway) wait() {
	for {
		select {
		case msg := <-gw.messages:
			//mailTo := gw.receiver.GetMailList(msg.ClusterId, msg.Level)
			//smsTo := gw.receiver.GetSmsList(msg.ClusterId, msg.Level)
			//log.Debug("message: %v, mail to: %v, sms to: %v", msg, mailTo, smsTo)
			//if err := gw.send(msg.Title, msg.Content, mailTo, smsTo); err != nil {
			//	log.Error("alarm message send: %v", err)
			//}

			// to alarm server
			for _, sample := range msg.samples {
				if err := gw.PushSampleJson(sample); err != nil {
					log.Error("push to remote alarm server error: %v", err)
				}
			}
		case <-gw.ctx.Done():
			return
		}
	}
}

func (gw *MessageGateway) notify(message Message, timeout time.Duration) (err error) {
	t := time.NewTimer(timeout)
	select {
	case gw.messages <-message:
	case <-t.C:
		err = errors.New("message notify timeout")
	case <-gw.ctx.Done():
		err = errors.New("message notify is canceled")
	}
	return
}

func (gw *MessageGateway) PushSampleJson(sample string) error {
	if len(gw.alarmServerAddress) == 0 {
		return errors.New("alarm server address is not config")
	}
	req, _ := http.NewRequest("POST", gw.alarmServerAddress, strings.NewReader(sample))
	req.Header.Add("User-Agent", "AlarmPusher-Agent")
	req.Header.Set("Connection", "close")
	resp, err := gw.pusher.Do(req)
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	if err == nil {
		if resp.StatusCode == http.StatusOK {
			return nil
		} else {
			ret, _ := ioutil.ReadAll(resp.Body)
			return errors.New("[AlarmPusher-Push]Alarm server error,Error=[" + string(ret) + "]")
		}
	} else {
		return errors.New("[AlarmPusher-Push]Alarm request service error,Error=[" + err.Error() + "]")
	}
}
