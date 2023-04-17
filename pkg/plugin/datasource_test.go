package plugin

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
)

func TestQueryData(t *testing.T) {
	ds := newTestDatasource(t)

	db := "statistics"
	q := snellerQuery{
		Database: &db,
		//SQL: `SELECT bucket * 1000 AS $__time(time),
		// tenant,
		// COALESCE(SUM(bytes), 0) / 3 AS bytes
		//FROM queries
		//WHERE $__timeFilter(timestamp)
		//GROUP BY TIME_BUCKET("timestamp", ($__interval_ms / 1000)) AS bucket,
		//   tenant
		//ORDER BY bucket
		//LIMIT $__max_data_points`,
		//SQL: `SELECT SNELLER_DATASHAPE(*) FROM (SELECT * FROM queries LIMIT 100)`,
		//SQL: `SELECT TIME_BUCKET(timestamp, 1000) as time FROM queries LIMIT 10`,
		SQL: `SELECT $__time(timestamp), duration FROM queries ORDER by timestamp LIMIT 10 `,
	}
	jq, _ := json.Marshal(q)

	resp, err := ds.QueryData(
		context.Background(),
		&backend.QueryDataRequest{
			Queries: []backend.DataQuery{
				{
					RefID: "A",
					JSON:  jq,
					TimeRange: backend.TimeRange{
						From: time.Now().Add(-24 * time.Hour),
						To:   time.Now(),
					},
					MaxDataPoints: 60 * 60,
					Interval:      time.Second,
				},
			},
		},
	)
	if err != nil {
		t.Error(err)
	}

	if len(resp.Responses) != 1 {
		t.Fatal("QueryData must return a response")
	}
}

func TestGetDatabases(t *testing.T) {
	ds := newTestDatasource(t)

	ds.getDatabases(context.Background())
}

func TestGetTables(t *testing.T) {
	ds := newTestDatasource(t)

	ds.getTables(context.Background(), "statistics")
}

func newTestDatasource(t *testing.T) *Datasource {
	settings := backend.DataSourceInstanceSettings{
		ID:               0,
		UID:              "",
		Type:             "",
		Name:             "",
		URL:              "",
		User:             "",
		Database:         "",
		BasicAuthEnabled: false,
		BasicAuthUser:    "",
		JSONData:         nil,
		DecryptedSecureJSONData: map[string]string{
			"token": "{censored}",
		},
		Updated: time.Time{},
	}
	opts, err := settings.HTTPClientOptions()
	if err != nil {
		t.Fatal(err)
	}
	client, err := httpclient.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	ds := Datasource{
		settings: settings,
		endpoint: "https://snellerd-master.us-east-1.sneller-dev.io",
		client:   client,
	}
	mux := datasource.NewQueryTypeMux()
	mux.HandleFunc("", ds.handleQuery)
	ds.handler = mux

	return &ds
}
