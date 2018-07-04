package metric

import (
	"runtime"
)

func memInfo() (total uint64, used uint64, err error) {
	memStat := new(runtime.MemStats)
	runtime.ReadMemStats(memStat)
	used = memStat.Alloc

	//系统占用,仅linux/mac下有效
	//system memory usage

	// TOOD: total
	return
}
