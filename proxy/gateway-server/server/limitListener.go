package server

import (
	"net"
	"sync"
	"proxy/metric"
	"util/log"
)

// LimitListener returns a Listener that accepts at most n simultaneous
// connections from the provided Listener.
func LimitListener(l net.Listener, n int) net.Listener {
	if n > 0 {
		return &limitListener{l, make(chan struct{}, n)}
	} else {
		return &limitListener{l, nil}
	}
}

type limitListener struct {
	net.Listener
	sem chan struct{}
}

func (l *limitListener) acquire() {
	if l.sem != nil {
		l.sem <- struct{}{}
	}
	metric.GsMetric.AddConnectCount(1)
}

func (l *limitListener) release() {
	if l.sem != nil {
		<-l.sem
	}
	metric.GsMetric.AddConnectCount(-1)
}

func (l *limitListener) Accept() (net.Conn, error) {
	l.acquire()
	c, err := l.Listener.Accept()
	if err != nil {
		log.Warn("mysql connect err, %v", err)
		l.release()
		return nil, err
	}
	return &limitListenerConn{Conn: c, release: l.release}, nil
}

type limitListenerConn struct {
	net.Conn
	releaseOnce sync.Once
	release     func()
}

func (l *limitListenerConn) Close() error {
	err := l.Conn.Close()
	l.releaseOnce.Do(l.release)
	return err
}
