package metric

type Message struct {
	// 集群
	ClusterId uint64
	// 系统
	Namespace string
	// 子系统
	Subsystem string
	// MsgId
	MsgId string
	// item
	Items interface{}
}

type Store interface {
	Open() error
	Put(message *Message) error
	Close()
}
