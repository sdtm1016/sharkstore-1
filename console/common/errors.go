package common

var (
	OK                 = &FbaseError{0, "success"}
	INTERNAL_ERROR     = &FbaseError{1, "internal error"}
	DB_ERROR           = &FbaseError{2, "db error"}
	HTTP_REQUEST_ERROR = &FbaseError{3, "send http request is failed"}

	PARSE_PARAM_ERROR  = &FbaseError{4, "parse parameters error"}
	PARAM_FORMAT_ERROR = &FbaseError{5, "parameters format error"}

	CLUSTER_NOTEXISTS_ERROR = &FbaseError{6, "cluster not exists"}
	CLUSTER_DUPCREATE_ERROR = &FbaseError{7, "create duplicated cluster"}
	TABLE_NOT_EXISTS        = &FbaseError{8, "table not exists"}
	NO_DATA                 = &FbaseError{9, "no data"}
	NO_RIGHT                = &FbaseError{10, "no right, please contact sharkstore team"}
	NO_USER                 = &FbaseError{11, "please login retry"}
)

type FbaseError struct {
	Code int
	Msg  string
	// ExtMsg	string
}

func (e *FbaseError) Error() string {
	//if e.ExtMsg == "" {
	//	return fmt.Sprintf("%s %s", e.Msg, e.ExtMsg)
	//} else {
	return e.Msg
	//}
}

//func (e *FbaseError)GetCode() int {
//	return e.Code
//}

//func (e *FbaseError)GetMsg() string {
//	return e.Msg
//}
