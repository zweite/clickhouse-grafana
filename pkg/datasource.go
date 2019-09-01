package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context/ctxhttp"

	"github.com/Vertamedia/clickhouse-grafana/pkg/proto/datasource"
	hclog "github.com/hashicorp/go-hclog"
	plugin "github.com/hashicorp/go-plugin"
)

type ClickhouseDatasource struct {
	plugin.NetRPCUnsupportedPlugin
	logger hclog.Logger
}

func (ch *ClickhouseDatasource) Query(ctx context.Context, tsdbReq *datasource.DatasourceRequest) (*datasource.DatasourceResponse, error) {
	ch.logger.Debug("clickhouse databasesource ", tsdbReq.String())
	url := tsdbReq.Datasource.Url + "/query"
	response := &datasource.DatasourceResponse{}
	for _, query := range tsdbReq.Queries {
		q := &queryModel{}
		if err := json.Unmarshal([]byte(query.ModelJson), q); err != nil {
			return nil, err
		}

		ch.logger.Debug(q.RawQuery)
		r, err := ch.doQuery(ctx, url, q.GetQuery(query, tsdbReq.GetTimeRange()))
		if err != nil {
			return nil, err
		}
		r.RefId = q.RefID
		response.Results = append(response.Results, r)
	}
	return response, nil
}

func (ch *ClickhouseDatasource) doQuery(ctx context.Context, url, query string) (*datasource.QueryResult, error) {
	query = query + " FORMAT JSON "
	ch.logger.Debug(">>> ending query ", query)
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(query))
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")
	res, err := ctxhttp.Do(ctx, httpClient, req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid status code. status: %v", res.Status)
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	return ParseResponse(body)
}

func ParseResponse(body []byte) (*datasource.QueryResult, error) {
	var dto targetResponseDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		return nil, err
	}
	if dto.Rows == 0 {
		return &datasource.QueryResult{}, nil
	}
	if len(dto.Meta) < 2 {
		return nil, fmt.Errorf("response can't contain less than 2 columns")
	}
	// timeCol have to be the first column always
	if dto.Meta[0].Type != "UInt64" {
		return nil, fmt.Errorf("timeColumn must be UInt64; got %q instead", dto.Meta[0].Type)
	}
	timeCol := dto.Meta[0].Name

	intervals := make([]int64, dto.Rows)
	dataPoints := make(map[string]map[int64]float64)
	push := func(key string, ts int64, val interface{}) error {
		f, err := toFloat64(val)
		if err != nil {
			return err
		}
		if dataPoints[key] == nil {
			dataPoints[key] = make(map[int64]float64)
		}
		dataPoints[key][ts] = f
		return nil
	}

	for i, item := range dto.Data {
		tsVal, ok := item[timeCol]
		if !ok {
			return nil, fmt.Errorf("unable to find timeCol %q in response.data", timeCol)
		}
		ts, err := toInt64(tsVal)
		if err != nil {
			return nil, err
		}
		intervals[i] = ts
		delete(item, timeCol)

		for k, val := range item {
			switch v := val.(type) {
			case string, float64:
				push(k, ts, v)
			case []interface{}:
				for _, row := range v {
					rr, ok := row.([]interface{})
					if !ok {
						reportUnsupported(row)
						return nil, errUnsupportedType
					}

					switch rr[0].(type) {
					case []interface{}:
						rs, ok := rr[0].([]interface{})
						if !ok {
							reportUnsupported(v)
							continue
						}
						var rks string
						for _, sr := range rs {
							if rk, ok := sr.(string); ok {
								rks += rk + ","
							}
						}

						if len(rks) < 2 {
							reportUnsupported(rs)
							continue
						}

						rks = rks[:len(rks)-1]
						push(rks, ts, rr[1])
					case string:
						if rk, ok := rr[0].(string); ok {
							push(rk, ts, rr[1])
						}
					}
				}
			default:
				reportUnsupported(v)
				return nil, errUnsupportedType
			}
		}
	}

	var series []*datasource.TimeSeries
	for target, dp := range dataPoints {
		serie := &datasource.TimeSeries{Name: target}
		for _, i := range intervals {
			v, ok := dp[i]
			if !ok {
				v = float64(0)
			}
			serie.Points = append(serie.Points, &datasource.Point{
				Timestamp: i,
				Value:     v,
			})
		}
		series = append(series, serie)
	}

	if len(series) > 2 {
		// 去掉头尾两个时序数据.防止数据取值区间异常报警
		series = series[1:]
		series = series[:len(series)-1]
	}

	return &datasource.QueryResult{
		Series: series,
	}, nil
}

var httpClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			Renegotiation: tls.RenegotiateFreelyAsClient,
		},
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).Dial,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
	},
	Timeout: time.Duration(time.Second * 30),
}

var errUnsupportedType = errors.New("unsupported column type")

func toInt64(val interface{}) (int64, error) {
	switch v := val.(type) {
	case string:
		return strconv.ParseInt(v, 10, 64)
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	case uint64:
		return int64(v), nil
	case nil:
		return int64(0), nil
	}
	reportUnsupported(val)
	return int64(0), errUnsupportedType
}

func toFloat64(val interface{}) (float64, error) {
	switch v := val.(type) {
	case string:
		return strconv.ParseFloat(v, 64)
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case nil:
		return float64(0), nil
	}
	reportUnsupported(val)
	return float64(0), errUnsupportedType
}

func reportUnsupported(val interface{}) {
	typ := "nil"
	t := reflect.TypeOf(val)
	if t != nil {
		typ = t.Name()
	}
	log.Printf("ERROR: parameter %#v has unsupported type: %s", val, typ)
}
