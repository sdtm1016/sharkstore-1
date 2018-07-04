package alarm

type alarmFilter interface {
	FilteredByInt(int64) bool
	FilteredByFloat(float64) bool
	FilteredByString(string) bool
}

type clusterIdFilter struct {
	ids map[int64]struct{}
}

func NewClusterIdFilter(ids []int64) *clusterIdFilter {
	f := &clusterIdFilter{
		ids: make(map[int64]struct{}),
	}
	for _, id := range ids {
		f.ids[id] = struct{}{}
	}

	return f
}

func (f *clusterIdFilter) FilteredByInt(clusterId int64) bool {
	if _, ok := f.ids[clusterId]; ok {
		return true
	}
	return false
}

func (f *clusterIdFilter) FilteredByFloat(flt float64) bool {
	return false
}

func (f *clusterIdFilter) FilteredByString(str string) bool {
	return false
}
