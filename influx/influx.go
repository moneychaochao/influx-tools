package influx

import (
	"encoding/json"
	"errors"
	_ "github.com/influxdata/influxdb1-client"
	"github.com/influxdata/influxdb1-client/models"
	client "github.com/influxdata/influxdb1-client/v2"
)

type QueryResult map[string]interface{}

type QueryResults []QueryResult

type QueryApi interface {
	QueryRow(influxQL string) (QueryResult, error)
	QueryRows(influxQL string) (QueryResults, error)
	QueryCount(influxQL string) (uint, error)
}

type queryApiImpl struct {
	db     string
	client client.Client
}

func (q *queryApiImpl) QueryRows(influxQL string) (QueryResults, error) {
	query := client.NewQuery(influxQL, q.db, "")

	if rsp, err := q.client.Query(query); err == nil {
		if rsp.Error() == nil {
			if len(rsp.Results) > 0 && len(rsp.Results[0].Series) > 0 {
				return rowToQueryResults(rsp.Results[0].Series[0]), nil
			}
			return nil, errors.New("response results len error")
		}
		return nil, rsp.Error()
	} else {
		return nil, err
	}
}

func (q *queryApiImpl) QueryRow(influxQL string) (QueryResult, error) {
	if result, err := q.QueryRows(influxQL); err == nil {
		return result[0], nil
	} else {
		return nil, err
	}
}

func (q *queryApiImpl) QueryCount(influxQL string) (uint64, error) {
	if result, err := q.QueryRow(influxQL); err == nil {
		if count, ok := result["count_value"]; ok {
			i, err := count.(json.Number).Int64()
			if err != nil {
				return 0, err
			}
			return uint64(i), nil
		}
		return 0, errors.New("query count error")
	} else {
		return 0, err
	}
}

func NewQueryApi(c client.Client, db string) *queryApiImpl {
	return &queryApiImpl{
		client: c,
		db:     db,
	}
}

func rowToQueryResults(result models.Row) QueryResults {
	var qrs QueryResults
	for _, row := range result.Values {
		qr := make(QueryResult)
		for i, v := range row {
			qr[result.Columns[i]] = v
		}
		qrs = append(qrs, qr)
	}
	return qrs
}
