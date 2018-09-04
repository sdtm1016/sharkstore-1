package dskv

import "errors"

var (
	ErrRetryLater = errors.New("try again later")
	ErrServerBusy = errors.New("server is busy")
	ErrInternalError = errors.New("internal error")
	ErrRouteChange = errors.New("route change")
	ErrInvalidNode = errors.New("invalid node ID")
	ErrNotSupportParallelExec = errors.New("proxy not support parallel exec")

	ErrAffectRows = errors.New("affect rows is not equal")
)
