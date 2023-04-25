package plugin

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

type snellerMacroEngine struct {
	regexDateRange *regexp.Regexp
	regexMacroFunc *regexp.Regexp
	timeCandidate  string
}

const (
	reIdentifier = `([_a-zA-Z0-9]+)`
)

func newSnellerMacroEngine() *snellerMacroEngine {
	return &snellerMacroEngine{
		regexDateRange: regexp.MustCompile(`\$\{__(from|to)(?::(date(?::(?:iso|seconds))?))?}`),
		regexMacroFunc: regexp.MustCompile(`\$__` + reIdentifier + `\(` + reIdentifier + `\)`),
	}
}

func (m *snellerMacroEngine) Interpolate(query backend.DataQuery, sql string) string {
	// See https://grafana.com/docs/grafana/latest/dashboards/variables/add-template-variables/#__from-and-__to
	sql = replaceAllStringSubmatchFunc(m.regexDateRange, sql, func(groups []string) string {
		var t *time.Time
		switch groups[1] {
		case "from":
			t = &query.TimeRange.From
		case "to":
			t = &query.TimeRange.To
		}

		if t == nil {
			return groups[0]
		}

		switch groups[2] {
		case "":
			return strconv.FormatInt((*t).UnixMilli(), 10)
		case "date", "date:iso":
			return (*t).Format(time.RFC3339)
		case "date:seconds":
			return strconv.FormatInt((*t).Unix(), 10)
		}
		// TODO: support custom format

		return groups[0]
	})

	// See https://grafana.com/docs/grafana/latest/dashboards/variables/add-template-variables/#__interval_ms
	interval := strconv.FormatInt(query.Interval.Milliseconds(), 10)
	sql = strings.ReplaceAll(sql, `$__interval_ms`, interval)

	// Maximum amount of data points
	limit := strconv.FormatInt(query.MaxDataPoints, 10)
	sql = strings.ReplaceAll(sql, `$__max_data_points`, limit)

	// Macro functions
	sql = replaceAllStringSubmatchFunc(m.regexMacroFunc, sql, func(groups []string) string {
		switch groups[1] {
		case "time":
			// Custom macro to help the plugin determining the `time` field
			if m.timeCandidate == "" {
				m.timeCandidate = groups[2]
			}
			return groups[2]
		case "timeFilter":
			// See https://grafana.com/docs/grafana/latest/dashboards/variables/add-template-variables/#timefilter-or-__timefilter
			return fmt.Sprintf("%s BETWEEN `%s` AND `%s`", groups[2], query.TimeRange.From.Format(time.RFC3339), query.TimeRange.To.Format(time.RFC3339))
		}
		return groups[0]
	})

	return sql
}
