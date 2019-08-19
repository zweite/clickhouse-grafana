package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Vertamedia/clickhouse-grafana/pkg/proto/datasource"
)

const durationSplitRegexp = "(\\d+)(ms|s|m|h|d|w|M|y)"

var (
	durationSplitReg = regexp.MustCompile(durationSplitRegexp)
)

type targetResponseDTO struct {
	Meta []meta                   `json:"meta"`
	Data []map[string]interface{} `json:"data"`
	Rows int                      `json:"rows"`
}

type meta struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type queryModel struct {
	Database            string `json:"database"`
	DateLoading         bool   `json:"dateLoading"`
	DateColDataType     string `json:"dateColDataType"`
	DateType            string `json:"dateType"`
	DateTimeColDataType string `json:"dateTimeColDataType"`
	DateTimeType        string `json:"dateTimeType"`
	DatetimeLoading     bool   `json:"datetimeLoading"`
	Format              string `json:"format"`
	FormattedQuery      string `json:"formattedQuery"`
	Interval            string `json:"interval"`
	IntervalFactor      int64  `json:"intervalFactor"`
	Query               string `json:"query"`
	RawQuery            bool   `json:"rawQuery"`
	RefID               string `json:"refId"`
	Round               string `json:"round"`
	Table               string `json:"table"`
	TableLoading        bool   `json:"tableLoading"`
}

func (q *queryModel) GetQuery(query *datasource.Query, timeRange *datasource.TimeRange) string {
	table := q.Table
	if strings.TrimSpace(q.Database) != "" {
		table = q.Database + "." + table
	}

	interval := q.getInterval()

	querySQL := q.Query
	querySQL = strings.Replace(querySQL, "$timeSeries", q.getTimeSeries(), -1)
	querySQL = strings.Replace(querySQL, "$timeFilter", q.getTimeFilter(false), -1)
	querySQL = strings.Replace(querySQL, "$table", table, -1)
	querySQL = strings.Replace(querySQL, "$dateCol", q.DateColDataType, -1)
	querySQL = strings.Replace(querySQL, "$dateTimeCol", q.DateTimeColDataType, -1)
	querySQL = strings.Replace(querySQL, "$interval", strconv.FormatInt(interval, 10), -1)
	querySQL = strings.Replace(querySQL, "\r", " ", -1)
	querySQL = strings.Replace(querySQL, "\n", " ", -1)

	var round int64
	if q.Round == "$step" {
		round = interval
	} else {
		round = convertInterval(q.Round)
	}

	querySQL = q.replaceTimeFilters(querySQL, timeRange, round)
	return querySQL
}

// getInterval interval by second
func (q *queryModel) getInterval() int64 {
	if q.Interval == "" {
		return q.IntervalFactor
	}

	interval := convertInterval(q.Interval)
	return q.IntervalFactor * interval
}

func (q *queryModel) replaceTimeFilters(query string, timeRange *datasource.TimeRange, round int64) string {
	query = strings.Replace(query, "$from", strconv.FormatInt(q.round(timeRange.FromEpochMs, round)/1000, 10), -1)
	query = strings.Replace(query, "$to", strconv.FormatInt(q.round(timeRange.ToEpochMs, round)/1000, 10), -1)
	return query
}

func (q *queryModel) round(timestamp int64, round int64) int64 {
	if round <= 1 {
		return timestamp
	}

	var coeff = 1000 * round
	return timestamp / coeff * coeff
}

func (q *queryModel) escapeIdentifier(identifier string) string {
	beMatch := false
	match, _ := regexp.MatchString("^[a-zA-Z_][0-9a-zA-Z_]*$", identifier)
	beMatch = beMatch || match
	match, _ = regexp.MatchString("\\(.*\\)", identifier)
	beMatch = beMatch || match

	if beMatch {
		return identifier
	}

	return "`" + strings.Replace(identifier, "`", "``", -1) + "`"
}

func (q *queryModel) getTimeSeries() string {
	if q.DateTimeType == "DATETIME" {
		return "(intDiv(toUInt32($dateTimeCol), $interval) * $interval) * 1000"
	}
	return "(intDiv($dateTimeCol, $interval) * $interval) * 1000"
}

func (q *queryModel) getDateTimeFilter(isToNow bool) string {
	var convertFn = func(t string) string {
		if q.DateTimeType == "DATETIME" {
			return "toDateTime(" + t + ")"
		}
		return t
	}

	if isToNow {
		return "$dateTimeCol >= " + convertFn("$from")
	}
	return "$dateTimeCol BETWEEN " + convertFn("$from") + " AND " + convertFn("$to")
}

func (q *queryModel) getDateFilter(isToNow bool) string {
	if isToNow {
		return "$dateCol >= toDate($from)"
	}
	return "$dateCol BETWEEN toDate($from) AND toDate($to)"
}

func (q *queryModel) getTimeFilter(isToNow bool) string {
	var timeFilter = q.getDateTimeFilter(isToNow)
	if q.DateTimeColDataType == "string" {
		timeFilter = q.getDateFilter(isToNow) + " AND " + timeFilter
	}
	return timeFilter
}

// convertInterval convert string to interval of second
func convertInterval(str string) int64 {
	if !durationSplitReg.MatchString(str) {
		return 1
	}

	fields := durationSplitReg.FindStringSubmatch(str)
	if len(fields) < 3 {
		return 1
	}

	fmt.Println(fields)
	intervalStr := fields[1]
	interval, _ := strconv.ParseInt(intervalStr, 10, 64)
	if interval <= 0 {
		return 1
	}

	unit := fields[2]
	interval = interval * int64(getUnit2MsSecond(unit)) / 1000
	if interval <= 0 {
		return 1
	}
	return interval
}

func getUnit2MsSecond(unit string) int64 {
	switch unit {
	case "ms":
		return 1
	case "s":
		return int64(time.Second / time.Millisecond)
	case "m":
		return int64(time.Minute / time.Millisecond)
	case "h":
		return int64(time.Hour / time.Millisecond)
	case "d":
		return 24 * int64(time.Hour/time.Millisecond)
	case "w":
		return 7 * 24 * int64(time.Hour/time.Millisecond)
	case "M":
		return 30 * 24 * int64(time.Hour/time.Millisecond)
	case "y":
		return 365 * 24 * int64(time.Hour/time.Millisecond)
	}
	return 1
}
