package server

import (
	"bytes"
	"strconv"
	"strings"

	"proxy/gateway-server/errors"
	"proxy/gateway-server/mysql"
	"proxy/gateway-server/sqlparser"

	"model/pkg/kvrpcpb"
	"proxy/store/dskv"
	"util/log"
	"util"
	"context"
	"fmt"
)

func (p *Proxy) selectAggre(t *Table, kvproxy *dskv.KvProxy, req *kvrpcpb.SelectRequest) ([][]*kvrpcpb.Row, error) {
	var key, start, limit []byte
	var allRows [][]*kvrpcpb.Row

	negativeInfi := util.EncodeStorePrefix(util.Store_Prefix_KV, t.GetId())
	_, positiveInfi := bytesPrefix(start)

	if req.Scope.Start == nil {
		start = negativeInfi
	} else {
		start = req.Scope.Start
	}
	if req.Scope.Limit == nil {
		limit = positiveInfi
	} else {
		limit = req.Scope.Limit
	}

	var tasks []*aggreTask
	for {
		if key == nil {
			key = start
		} else {
			bo := dskv.NewBackoffer(dskv.MsMaxBackoff, context.Background())
			route, err := kvproxy.RangeCache.LocateKey(bo, key)
			if err != nil {
				return nil, fmt.Errorf("locate route failed: %v", err)
			}
			key = route.EndKey

			if bytes.Compare(key, start) < 0 || bytes.Compare(key, limit) >= 0 {
				break
			}
		}

		task := newAggreTask(p, kvproxy, key, req)
		err := p.Submit(task)
		if err != nil {
			log.Error("submit aggre task failed, err[%v]", err)
			return nil, err
		}
		tasks = append(tasks, task)
	}

	for _, task := range tasks {
		err := task.Wait()
		if err != nil {
			log.Error("select aggre task do failed, err[%v]", err)
			return nil, err
		}
		rows := task.result
		if len(rows) > 0 {
			allRows = append(allRows, rows)
		}
	}

	return allRows, nil
}

func getSumFuncExprValue(rs []*mysql.Result, index int) (interface{}, error) {
	var sumf float64
	var sumi int64
	var IsInt bool
	var err error
	var result interface{}

	for _, r := range rs {
		for k := range r.Values {
			result, err = r.GetValue(k, index)
			if err != nil {
				return nil, err
			}
			if result == nil {
				continue
			}

			switch v := result.(type) {
			case int:
				sumi = sumi + int64(v)
				IsInt = true
			case int32:
				sumi = sumi + int64(v)
				IsInt = true
			case int64:
				sumi = sumi + v
				IsInt = true
			case uint:
				sumi = sumi + int64(v)
				IsInt = true
			case uint32:
				sumi = sumi + int64(v)
				IsInt = true
			case uint64:
				sumi = sumi + int64(v)
				IsInt = true

			case float32:
				sumf = sumf + float64(v)
			case float64:
				sumf = sumf + v
			case []byte:
				tmp, err := strconv.ParseFloat(string(v), 64)
				if err != nil {
					return nil, err
				}

				sumf = sumf + tmp
			default:
				return nil, errors.ErrSumColumnType
			}
		}
	}
	if IsInt {
		return sumi, nil
	} else {
		return sumf, nil
	}
}

func getMaxFuncExprValue(rs []*mysql.Result, index int) (interface{}, error) {
	var max interface{}
	var findNotNull bool
	if len(rs) == 0 {
		return nil, nil
	} else {
		for _, r := range rs {
			for k := range r.Values {
				result, err := r.GetValue(k, index)
				if err != nil {
					return nil, err
				}
				if result != nil {
					max = result
					findNotNull = true
					break
				}
			}
			if findNotNull {
				break
			}
		}
	}
	for _, r := range rs {
		for k := range r.Values {
			result, err := r.GetValue(k, index)
			if err != nil {
				return nil, err
			}
			if result == nil {
				continue
			}
			switch result.(type) {
			case int64:
				if max.(int64) < result.(int64) {
					max = result
				}
			case uint64:
				if max.(uint64) < result.(uint64) {
					max = result
				}
			case float64:
				if max.(float64) < result.(float64) {
					max = result
				}
			case string:
				if max.(string) < result.(string) {
					max = result
				}
			case []byte:
				if bytes.Compare(max.([]byte), result.([]byte)) < 0 {
					max = result
				}
			}
		}
	}
	return max, nil
}

func getMinFuncExprValue(rs []*mysql.Result, index int) (interface{}, error) {
	var min interface{}
	var findNotNull bool
	if len(rs) == 0 {
		return nil, nil
	} else {
		for _, r := range rs {
			for k := range r.Values {
				result, err := r.GetValue(k, index)
				if err != nil {
					return nil, err
				}
				if result != nil {
					min = result
					findNotNull = true
					break
				}
			}
			if findNotNull {
				break
			}
		}
	}
	for _, r := range rs {
		for k := range r.Values {
			result, err := r.GetValue(k, index)
			if err != nil {
				return nil, err
			}
			if result == nil {
				continue
			}
			switch result.(type) {
			case int64:
				if min.(int64) > result.(int64) {
					min = result
				}
			case uint64:
				if min.(uint64) > result.(uint64) {
					min = result
				}
			case float64:
				if min.(float64) > result.(float64) {
					min = result
				}
			case string:
				if min.(string) > result.(string) {
					min = result
				}
			case []byte:
				if bytes.Compare(min.([]byte), result.([]byte)) > 0 {
					min = result
				}
			}
		}
	}
	return min, nil
}

//calculate the the value funcExpr(sum or count)
func calFuncExprValue(funcName string, rs []*mysql.Result, index int) (interface{}, error) {
	var num int64
	switch strings.ToLower(funcName) {
	case CountFunc:
		if len(rs) == 0 {
			return nil, nil
		}
		for _, r := range rs {
			if r != nil {
				for k := range r.Values {
					result, err := r.GetInt(k, index)
					if err != nil {
						return nil, err
					}
					num += result
				}
			}
		}
		return num, nil
	case SumFunc:
		return getSumFuncExprValue(rs, index)
	case MaxFunc:
		return getMaxFuncExprValue(rs, index)
	case MinFunc:
		return getMinFuncExprValue(rs, index)
	default:
		if len(rs) == 0 {
			return nil, nil
		}
		//get a non-null value of funcExpr
		for _, r := range rs {
			for k := range r.Values {
				result, err := r.GetValue(k, index)
				if err != nil {
					return nil, err
				}
				if result != nil {
					return result, nil
				}
			}
		}
	}
	return nil, nil
}

func getFuncExprs(stmt *sqlparser.Select) map[int]string {
	var f *sqlparser.FuncExpr
	funcExprs := make(map[int]string)

	for i, expr := range stmt.SelectExprs {
		nonStarExpr, ok := expr.(*sqlparser.NonStarExpr)
		if !ok {
			continue
		}

		f, ok = nonStarExpr.Expr.(*sqlparser.FuncExpr)
		if !ok {
			continue
		} else {
			f = nonStarExpr.Expr.(*sqlparser.FuncExpr)
			funcName := strings.ToLower(string(f.Name))
			switch funcNameMap[funcName] {
			case FUNC_EXIST:
				funcExprs[i] = funcName
			}
		}
	}
	return funcExprs
}

//build values of resultset,only build one row
func buildFuncExprValues(rs []*mysql.Result, funcExprValues map[int]interface{}) ([][]interface{}, error) {
	values := make([][]interface{}, 0, 1)
	//build a row in one result
	for i := range rs {
		for j := range rs[i].Values {
			for k := range funcExprValues {
				rs[i].Values[j][k] = funcExprValues[k]
			}
			values = append(values, rs[i].Values[j])
			if len(values) == 1 {
				break
			}
		}
		break
	}

	//generate one row just for sum or count
	if len(values) == 0 {
		value := make([]interface{}, len(rs[0].Fields))
		for k := range funcExprValues {
			value[k] = funcExprValues[k]
		}
		values = append(values, value)
	}

	return values, nil
}

func buildFuncExprResult(stmt *sqlparser.Select, rs []*mysql.Result, funcExprs map[int]string, columns []string) (*mysql.Resultset, error) {
	var names []string
	var err error
	r := rs[0].Resultset
	funcExprValues := make(map[int]interface{})

	for index, funcName := range funcExprs {
		funcExprValue, err := calFuncExprValue(funcName, rs, index)
		if err != nil {
			return nil, err
		}
		funcExprValues[index] = funcExprValue
	}

	r.Values, err = buildFuncExprValues(rs, funcExprValues)

	if 0 < len(r.Values) {
		for _, field := range rs[0].Fields {
			names = append(names, string(field.Name))
		}
		r, err = buildResultset(rs[0].Fields, names, r.Values)
		if err != nil {
			return nil, err
		}
	} else {
		r = newEmptyResultSet(columns)
	}

	return r, nil
}
