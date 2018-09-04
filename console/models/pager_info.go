package models

type PagerInfo struct {
	PageIndex int
	PageSize  int
	SortName  string
	SortOrder string // asc、desc
}

func (p *PagerInfo) GetPageOffset() int {
	if p == nil {
		return 0
	}
	return (p.PageIndex - 1) * p.PageSize
}

func (p *PagerInfo) GetPageSize() int {
	if p == nil {
		return 0
	}
	return p.PageSize
}
