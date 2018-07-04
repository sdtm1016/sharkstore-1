package alarm

type MessageReceiver interface {
	GetMailList(clusterId int64, messageLevel int) []string
	GetSmsList(clusterId int64, messageLevel int) []string
}

func NewSimpleReceiver(users []*User) *SimpleReceiver {
	return &SimpleReceiver{
		Users: users,
	}
}

type SimpleReceiver struct {
	Users []*User
}

type User struct {
	Name string
	Mail string
	Sms string
}


func (r *SimpleReceiver) GetMailList(id int64, level int) (ret []string) {
	// todo
	//switch level {
	//case MESSAGE_LEVEL_ERROR:
	//case MESSAGE_LEVEL_WARN:
	//case MESSAGE_LEVEL_INFO:
	//
	//}
	for _, u := range r.Users {
		ret = append(ret, u.Mail)
	}
	return
}

func (r *SimpleReceiver) GetSmsList(id int64, level int) (ret []string) {
	// todo
	//switch level {
	//case MESSAGE_LEVEL_ERROR:
	//case MESSAGE_LEVEL_WARN:
	//case MESSAGE_LEVEL_INFO:
	//
	//}
	for _, u := range r.Users {
		ret = append(ret, u.Sms)
	}
	return
}
