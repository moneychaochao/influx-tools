package influx

import (
	"errors"
	"fmt"
	"math"
)

type pageInfo struct {
	PageSize  uint64
	PageNo    uint64
	PageCount uint64
	TotalNums uint64
}

type QueryPageResults struct {
	PageInfo pageInfo
	PageData QueryResults
}

type QueryPageApi interface {
	QueryPageRows(influxQL string) (QueryPageResults, error)
}

type queryPageApiImpl struct {
	pageInfo     *pageInfo
	queryApiImpl *queryApiImpl
}

func (q *queryPageApiImpl) QueryPageRows(influxQL string) (QueryPageResults, error) {
	limit := q.pageInfo.PageSize
	if limit == 0 || q.pageInfo.PageNo == 0 {
		return QueryPageResults{}, errors.New("page size or page no error")
	}

	offset := (q.pageInfo.PageNo - 1) * limit

	influxQL = fmt.Sprintf("%s LIMIT %d OFFSET %d", influxQL, limit, offset)

	result, err := q.queryApiImpl.QueryRows(influxQL)
	if err != nil {
		return QueryPageResults{}, err
	}

	return QueryPageResults{PageInfo: *q.pageInfo, PageData: result}, nil
}

func NewPageInfo(pageSize, pageNo, totalNums uint64) *pageInfo {
	// count pages
	var pageCount uint64
	if pageSize > 0 {
		pageCount = uint64(math.Ceil(float64(totalNums) / float64(pageSize)))
	}
	return &pageInfo{
		PageSize:  pageSize,
		PageNo:    pageNo,
		PageCount: pageCount,
		TotalNums: totalNums,
	}
}

func NewQueryPageApi(pageInfo *pageInfo, impl *queryApiImpl) *queryPageApiImpl {
	return &queryPageApiImpl{
		pageInfo:     pageInfo,
		queryApiImpl: impl,
	}
}
