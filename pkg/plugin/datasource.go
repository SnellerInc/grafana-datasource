package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/backend/tracing"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Make sure Datasource implements required interfaces. This is important to do
// since otherwise we will only get a not implemented error response from plugin in
// runtime. In this example datasource instance implements backend.QueryDataHandler,
// backend.CheckHealthHandler interfaces. Plugin should not implement all these
// interfaces - only those which are required for a particular task.
var (
	_ backend.QueryDataHandler      = (*Datasource)(nil)
	_ backend.CheckHealthHandler    = (*Datasource)(nil)
	_ backend.CallResourceHandler   = (*Datasource)(nil)
	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
)

// NewDatasource creates a new datasource instance.
func NewDatasource(settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	var jsonData snellerJSONData
	err := json.Unmarshal(settings.JSONData, &jsonData)
	if err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}

	opts, err := settings.HTTPClientOptions()
	if err != nil {
		return nil, fmt.Errorf("http client options: %w", err)
	}

	opts.Timeouts.Timeout = 10 * time.Minute

	client, err := httpclient.New(opts)
	if err != nil {
		return nil, fmt.Errorf("httpclient new: %w", err)
	}

	ds := Datasource{
		settings: settings,
		endpoint: jsonData.Endpoint,
		client:   client,
	}

	mux := datasource.NewQueryTypeMux()
	//mux.HandleFunc("logs", ds.handleQuery)
	//mux.HandleFunc("traces", ds.handleQuery)
	mux.HandleFunc("", ds.handleQuery)
	ds.handler = mux

	return &ds, nil
}

// Datasource is an example datasource which can respond to data queries, reports
// its health and has streaming skills.
type Datasource struct {
	settings backend.DataSourceInstanceSettings
	handler  backend.QueryDataHandler
	endpoint string
	client   *http.Client
}

// Dispose here tells plugin SDK that plugin wants to clean up resources when a new instance
// created. As soon as datasource settings change detected by SDK old datasource instance will
// be disposed and a new one will be created using NewSampleDatasource factory function.
func (d *Datasource) Dispose() {
	d.client.CloseIdleConnections()
}

// QueryData handles multiple queries and returns multiple responses.
// req contains the queries []DataQuery (where each query contains RefID as a unique identifier).
// The QueryDataResponse contains a map of RefID to the response for each query, and each response
// contains Frames ([]*Frame).
func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	sctx := trace.SpanContextFromContext(ctx)
	log.DefaultLogger.Debug("QueryData", "traceID", sctx.TraceID().String(), "spanID", sctx.SpanID().String())

	return d.handler.QueryData(ctx, req)
}

// CheckHealth handles health checks sent from Grafana to the plugin.
// The main use case for these health checks is the test button on the
// datasource configuration page which allows users to verify that
// a datasource is working as expected.
func (d *Datasource) CheckHealth(ctx context.Context, _ *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	req, err := d.newRequest(ctx, http.MethodPost, "/executeQuery", strings.NewReader("SELECT 1+2"))
	if err != nil {
		return nil, err
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: fmt.Sprintf("HTTP request: %s", err),
		}, nil
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.DefaultLogger.Error("failed to close response body", "err", err)
		}
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var message string
		b, err := io.ReadAll(resp.Body)
		if err != nil && len(b) > 0 {
			message = fmt.Sprintf("HTTP error %d: %s", resp.StatusCode, string(b))
		} else {
			message = fmt.Sprintf("HTTP error %d", resp.StatusCode)
		}

		message += " - " + req.URL.String()

		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: message,
		}, nil
	}

	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "OK",
	}, nil
}

func (d *Datasource) CallResource(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	segments := strings.Split(req.Path, "/")
	switch segments[0] {
	case "databases":
		return sender.Send(d.handleCallResourceDatabases(ctx))
	case "tables":
		if len(segments) != 2 {
			return sender.Send(&backend.CallResourceResponse{
				Status: http.StatusBadRequest,
			})
		}
		return sender.Send(d.handleCallResourceTables(ctx, segments[1]))
	default:
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusNotFound,
		})
	}
}

func (d *Datasource) handleCallResourceDatabases(ctx context.Context) *backend.CallResourceResponse {
	databases, status, err := d.getDatabases(ctx)
	if err != nil {
		return &backend.CallResourceResponse{
			Status: status,
			Body:   []byte(err.Error()),
		}
	}
	result, err := json.Marshal(databases)
	if err != nil {
		return &backend.CallResourceResponse{
			Status: status,
			Body:   []byte(err.Error()),
		}
	}
	return &backend.CallResourceResponse{
		Status: http.StatusOK,
		Body:   result,
	}
}

func (d *Datasource) handleCallResourceTables(ctx context.Context, database string) *backend.CallResourceResponse {
	databases, status, err := d.getTables(ctx, database)
	if err != nil {
		return &backend.CallResourceResponse{
			Status: status,
			Body:   []byte(err.Error()),
		}
	}
	result, err := json.Marshal(databases)
	if err != nil {
		return &backend.CallResourceResponse{
			Status: status,
			Body:   []byte(err.Error()),
		}
	}
	return &backend.CallResourceResponse{
		Status: http.StatusOK,
		Body:   result,
	}
}

func (d *Datasource) handleQuery(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	response := backend.NewQueryDataResponse()

	var wg sync.WaitGroup
	wg.Add(len(req.Queries))

	var mutex sync.Mutex

	// Execute each query and store the results by query RefID
	for _, q := range req.Queries {
		go func(query backend.DataQuery) {
			resp := d.query(ctx, req.PluginContext, query)

			mutex.Lock()
			defer mutex.Unlock()
			response.Responses[query.RefID] = resp

			wg.Done()
		}(q)
	}

	wg.Wait()

	return response, nil
}

func (d *Datasource) query(ctx context.Context, _ backend.PluginContext, query backend.DataQuery) backend.DataResponse {
	ctx, span := tracing.DefaultTracer().Start(
		ctx,
		"query processing",
		trace.WithAttributes(
			attribute.String("query.ref_id", query.RefID),
			attribute.String("query.type", query.QueryType),
			attribute.Int64("query.max_data_points", query.MaxDataPoints),
			attribute.Int64("query.interval_ms", query.Interval.Milliseconds()),
			attribute.Int64("query.time_range.from", query.TimeRange.From.Unix()),
			attribute.Int64("query.time_range.to", query.TimeRange.To.Unix()),
		),
	)
	defer span.End()

	// Unmarshal the JSON into our query model
	var input snellerQuery
	err := json.Unmarshal(query.JSON, &input)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("json unmarshal: %v", err.Error()))
	}

	macros := newSnellerMacroEngine()

	database := ""
	if input.Database != nil && *input.Database != "" {
		database = *input.Database
	}
	sql := macros.Interpolate(query, input.SQL)

	resp, err := d.executeQuery(ctx, database, sql)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			// Grafana cancels the context when the same query is executed again before the
			// previous one completed
			// TODO: This workaround does not have the desired effect
			return backend.ErrDataResponse(backend.StatusOK, "OK")
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return backend.ErrDataResponse(backend.StatusTimeout, fmt.Sprintf("HTTP request: %s", err))
		}
		if resp != nil && (resp.StatusCode < 200 || resp.StatusCode >= 300) {
			switch resp.StatusCode {
			case http.StatusUnauthorized:
				return backend.ErrDataResponse(backend.StatusUnauthorized, fmt.Sprintf("unauthorized: %s", err))
			case http.StatusBadRequest:
				return backend.ErrDataResponse(backend.StatusValidationFailed, fmt.Sprintf("bad request: %s", err))
			}
		}
		return backend.ErrDataResponse(backend.StatusInternal, err.Error())
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.DefaultLogger.Error("failed to close response body", "err", err)
		}
	}()

	span.AddEvent("query done")

	frame, err := frameFromSnellerResult(query.RefID, sql, resp.Body, macros.timeCandidate)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, fmt.Sprintf("frame from rows: %s", err))
	}

	ft := frame.TimeSeriesSchema().Type
	switch ft {
	case data.TimeSeriesTypeWide:
		frame.Meta.Type = data.FrameTypeTimeSeriesWide
		frame.Meta.PreferredVisualization = data.VisTypeGraph
	case data.TimeSeriesTypeLong:
		// TODO: This SDK function is very slow and allocates a lot
		f, err := data.LongToWide(frame, &data.FillMissing{
			Mode: data.FillModeNull,
		})
		if err == nil {
			frame = f
			frame.Meta.PreferredVisualization = data.VisTypeGraph
		}
	}

	return backend.DataResponse{
		Status: backend.StatusOK,
		Frames: data.Frames{frame},
	}
}
