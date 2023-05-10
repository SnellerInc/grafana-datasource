package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/amazon-ion/ion-go/ion"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"golang.org/x/exp/maps"
)

// executeQuery executes a Sneller query and returns the HTTP response.
func (d *Datasource) executeQuery(ctx context.Context, database, sql string) (*http.Response, error) {
	return d.executeRequest(ctx, http.MethodPost, "/executeQuery", strings.NewReader(sql),
		map[string]string{"Accept": "application/ion"},
		map[string]string{"database": database})
}

// getDatabases returns a list of database names.
func (d *Datasource) getDatabases(ctx context.Context) ([]string, int, error) {
	key := "databases"
	cached, found := d.cache.Get(key)
	if found {
		return cached.([]string), 0, nil
	}

	resp, err := d.executeRequest(ctx, http.MethodGet, "/databases", nil,
		map[string]string{"Accept": "application/json"},
		nil)
	if err != nil {
		if resp != nil {
			return nil, resp.StatusCode, err
		}
		return nil, 500, err
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.DefaultLogger.Error("failed to close response body", "err", err)
		}
	}()

	var result []snellerDatabase

	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, 500, err
	}

	names := sliceSelect(result, func(t snellerDatabase) string {
		return t.Name
	})

	d.cache.Set(key, names, time.Minute*1)

	return names, 0, nil
}

// getTables returns a list of table names for the given database.
func (d *Datasource) getTables(ctx context.Context, database string) ([]string, int, error) {
	key := fmt.Sprintf("tables_%s", database)
	cached, found := d.cache.Get(key)
	if found {
		return cached.([]string), 0, nil
	}

	resp, err := d.executeRequest(ctx, http.MethodGet, "/tables", nil,
		map[string]string{"Accept": "application/json"},
		map[string]string{"database": database})
	if err != nil {
		if resp != nil {
			return nil, resp.StatusCode, err
		}
		return nil, 500, err
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.DefaultLogger.Error("failed to close response body", "err", err)
		}
	}()

	var result []string

	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, 500, err
	}

	d.cache.Set(key, result, time.Minute*1)

	return result, 0, nil
}

// getColumns returns a list of column names for the given database and table.
func (d *Datasource) getColumns(ctx context.Context, database, table string) ([]string, int, error) {
	key := fmt.Sprintf("columns_%s_%s", database, table)
	cached, found := d.cache.Get(key)
	if found {
		return cached.([]string), 0, nil
	}

	resp, err := d.executeQuery(ctx, database, fmt.Sprintf(`SELECT SNELLER_DATASHAPE(*) FROM (SELECT * FROM %q LIMIT 1000)`, table))
	if err != nil {
		if resp != nil {
			return nil, resp.StatusCode, err
		}
		return nil, 500, err
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.DefaultLogger.Error("failed to close response body", "err", err)
		}
	}()

	payload := map[string]any{}

	err = ion.UnmarshalFrom(ion.NewReader(resp.Body), &payload)
	if err != nil {
		return nil, 500, err
	}

	fields, ok := payload["fields"]
	if !ok {
		return []string{}, 0, nil
	}

	vals, ok := fields.(map[string]any)
	if !ok {
		return []string{}, 0, nil
	}

	cols := maps.Keys(vals)

	d.cache.Set(key, cols, time.Minute*1)

	return cols, 0, nil
}

// newRequest creates a new HTTP request and initializes the 'Authentication' header from the
// configured Sneller authentication token in the 'Authentication' header.
func (d *Datasource) newRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, d.endpoint+path, body)
	if err != nil {
		return nil, err
	}

	if token, ok := d.settings.DecryptedSecureJSONData["token"]; ok {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	return req, nil
}

// executeRequest performs an HTTP request and returns the response and/or an error with the
// message from the response body (if any).
func (d *Datasource) executeRequest(ctx context.Context, method, path string, body io.Reader, headers, args map[string]string) (*http.Response, error) {
	req, err := d.newRequest(ctx, method, path, body)
	if err != nil {
		return nil, err
	}

	if len(args) > 0 {
		q := req.URL.Query()
		for k, v := range args {
			q.Set(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}

	if headers != nil {
		for k, v := range headers {
			req.Header.Set(k, v)
		}
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		defer func() {
			if err := resp.Body.Close(); err != nil {
				log.DefaultLogger.Error("failed to close response body", "err", err)
			}
		}()

		b, err := io.ReadAll(resp.Body)
		if err == nil && len(b) > 0 {
			return resp, errors.New(string(b))
		}

		return resp, fmt.Errorf("HTTP status %d", resp.StatusCode)
	}

	return resp, nil
}
