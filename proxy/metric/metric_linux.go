package metric

import (
	"runtime"
	"syscall"
)

func memInfo() (total uint64, used uint64, err error) {
	memStat := new(runtime.MemStats)
	runtime.ReadMemStats(memStat)
	used = memStat.Alloc

	//系统占用,仅linux/mac下有效
	//system memory usage
	sysInfo := new(syscall.Sysinfo_t)
	err = syscall.Sysinfo(sysInfo)
	if err != nil {
		return
	}
	total = sysInfo.Totalram * uint64(syscall.Getpagesize())
	return
}
