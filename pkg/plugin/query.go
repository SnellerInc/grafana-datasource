package plugin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/amazon-ion/ion-go/ion"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// frameFromSnellerResult builds a Grafana data frame from a raw Sneller query result.
func frameFromSnellerResult(refID, sql string, input io.Reader, timeField string) (*data.Frame, error) {
	// Buffer query result in memory

	b, err := io.ReadAll(input)
	if err != nil {
		return nil, err
	}

	// Step 1: Derive schema

	schema, err := deriveSchema(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}

	if schema.FinalStatus == nil {
		return nil, errors.New("query execution failed: 'missing ::final_status annotation'")
	}
	if schema.FinalStatus.Error != "" {
		return nil, fmt.Errorf("query execution failed: '%s'", schema.FinalStatus.Error)
	}

	// Step 2: Read values

	fieldVals := make([]*fieldValues, len(schema.Columns))
	i := 0
	for _, column := range schema.Columns {
		isTimeField := (column.Name == timeField) &&
			((column.Typ == snellerTypeString) || (column.Typ == snellerTypeNumber && !column.Floating))

		values, err := grafanaFieldValues(column.Name, schema.RowCount, column, isTimeField)
		if err != nil {
			return nil, err
		}

		fieldVals[i] = values
		i++
	}

	_, err = iterateRows(bytes.NewReader(b), func(reader ion.Reader, index int) error {
		return readRowValues(reader, index, fieldVals)
	})

	// Step 3: Construct Grafana data fields

	fields := make([]*data.Field, len(fieldVals))
	for i := range fieldVals {
		fields[i] = data.NewField(fieldVals[i].Name, nil, fieldVals[i].Values)
	}

	frame := data.NewFrame(refID, fields...)
	frame.Meta = &data.FrameMeta{
		Type:                   data.FrameTypeTable,
		PreferredVisualization: data.VisTypeTable,
		ExecutedQueryString:    sql,
		Stats: []data.QueryStat{
			{
				FieldConfig: data.FieldConfig{DisplayName: "Hits"},
				Value:       float64(schema.FinalStatus.Hits),
			},
			{
				FieldConfig: data.FieldConfig{DisplayName: "Misses"},
				Value:       float64(schema.FinalStatus.Misses),
			},
			{
				FieldConfig: data.FieldConfig{DisplayName: "Scanned", Unit: "bytes"},
				Value:       float64(schema.FinalStatus.Scanned),
			},
		},
	}

	return frame, nil
}

// ---

func grafanaType(column *snellerColumn) data.FieldType {
	var result data.FieldType

	switch column.Typ {
	case snellerTypeUnknown:
		result = data.FieldTypeJSON
	case snellerTypeNull:
		result = data.FieldTypeJSON
	case snellerTypeBool:
		result = data.FieldTypeBool
	case snellerTypeNumber:
		if column.Floating {
			result = data.FieldTypeFloat64
		} else {
			result = data.FieldTypeInt64
		}
	case snellerTypeTimestamp:
		result = data.FieldTypeTime
	case snellerTypeString:
		result = data.FieldTypeString
	case snellerTypeStruct:
		result = data.FieldTypeJSON
	case snellerTypeList:
		result = data.FieldTypeJSON
	default:
		return data.FieldTypeUnknown
	}

	if column.Nullable || column.Optional {
		result = result.NullableType()
	}

	return result
}

func grafanaFieldValues(name string, rowCount int, column *snellerColumn, isTimeField bool) (*fieldValues, error) {
	typ := grafanaType(column)

	if isTimeField {
		switch typ {
		case data.FieldTypeInt64:
			return newFieldValues[time.Time](name, rowCount, readTimeFromInt64), nil
		case data.FieldTypeNullableInt64:
			return newFieldValues[*time.Time](name, rowCount, readTimeFromInt64Nullable), nil
		case data.FieldTypeString:
			return newFieldValues[time.Time](name, rowCount, readTimeFromString), nil
		case data.FieldTypeNullableString:
			return newFieldValues[*time.Time](name, rowCount, readTimeFromStringNullable), nil
		}
		return nil, fmt.Errorf("unsupported field type for time field: %s", typ)
	}

	switch typ {
	case data.FieldTypeJSON:
		return newFieldValues[json.RawMessage](name, rowCount, readJSON), nil
	case data.FieldTypeNullableJSON:
		return newFieldValues[*json.RawMessage](name, rowCount, readJSONNullable), nil
	case data.FieldTypeBool:
		return newFieldValues[bool](name, rowCount, readBool), nil
	case data.FieldTypeNullableBool:
		return newFieldValues[*bool](name, rowCount, readBoolNullable), nil
	case data.FieldTypeInt64:
		return newFieldValues[int64](name, rowCount, readInt64), nil
	case data.FieldTypeNullableInt64:
		return newFieldValues[*int64](name, rowCount, readInt64Nullable), nil
	case data.FieldTypeFloat64:
		return newFieldValues[float64](name, rowCount, readFloat64), nil
	case data.FieldTypeNullableFloat64:
		return newFieldValues[*float64](name, rowCount, readFloat64Nullable), nil
	case data.FieldTypeTime:
		return newFieldValues[time.Time](name, rowCount, readTime), nil
	case data.FieldTypeNullableTime:
		return newFieldValues[*time.Time](name, rowCount, readTimeNullable), nil
	case data.FieldTypeString:
		return newFieldValues[string](name, rowCount, readString), nil
	case data.FieldTypeNullableString:
		return newFieldValues[*string](name, rowCount, readStringNullable), nil
	}

	return nil, fmt.Errorf("unsupported field type: %s", typ)
}

// ---

func readJSON(reader ion.Reader) (json.RawMessage, error) {
	value, _ := readJSONNullable(reader)
	return *value, nil
}

func readJSONNullable(reader ion.Reader) (*json.RawMessage, error) {
	value, err := readValue(reader)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	return (*json.RawMessage)(&b), nil
}

func readBool(reader ion.Reader) (bool, error) {
	value, _ := readBoolNullable(reader)
	return *value, nil
}

func readBoolNullable(reader ion.Reader) (*bool, error) {
	return reader.BoolValue()
}

func readInt64(reader ion.Reader) (int64, error) {
	value, _ := readInt64Nullable(reader)
	return *value, nil
}

func readInt64Nullable(reader ion.Reader) (*int64, error) {
	return reader.Int64Value()
}

func readFloat64(reader ion.Reader) (float64, error) {
	value, _ := readFloat64Nullable(reader)
	return *value, nil
}

func readFloat64Nullable(reader ion.Reader) (*float64, error) {
	if reader.Type() == ion.IntType {
		value, _ := readInt64(reader)
		fvalue := float64(value)
		return &fvalue, nil
	}

	return reader.FloatValue()
}

func readString(reader ion.Reader) (string, error) {
	value, _ := readStringNullable(reader)
	return *value, nil
}

func readStringNullable(reader ion.Reader) (*string, error) {
	if reader.Type() == ion.SymbolType {
		value, _ := reader.SymbolValue()
		return convertSymbolValue(value), nil
	}

	return reader.StringValue()
}

func readTime(reader ion.Reader) (time.Time, error) {
	value, _ := readTimeNullable(reader)
	return *value, nil
}

func readTimeNullable(reader ion.Reader) (*time.Time, error) {
	value, _ := reader.TimestampValue()
	return convertTimestampValue(value), nil
}

func readTimeFromInt64(reader ion.Reader) (time.Time, error) {
	value, _ := readTimeFromInt64Nullable(reader)
	return *value, nil
}

func readTimeFromInt64Nullable(reader ion.Reader) (*time.Time, error) {
	value, _ := readInt64Nullable(reader)
	if value == nil {
		return nil, nil
	}

	result := time.UnixMilli(*value)

	return &result, nil
}

func readTimeFromString(reader ion.Reader) (time.Time, error) {
	value, _ := readTimeFromStringNullable(reader)
	return *value, nil
}

func readTimeFromStringNullable(reader ion.Reader) (*time.Time, error) {
	value, _ := readStringNullable(reader)
	if value == nil {
		return nil, nil
	}

	result, err := time.Parse(time.RFC3339, *value)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

// ---

type snellerColumnType int

const (
	snellerTypeUnknown   snellerColumnType = iota // Unknown or ambiguous type
	snellerTypeNull                               // Go: nil
	snellerTypeBool                               // Go: bool
	snellerTypeNumber                             // Go: int64 or float64 (core normalized representation)
	snellerTypeTimestamp                          // Go: time.Time
	snellerTypeString                             // Go: string
	snellerTypeStruct                             // Go: map[string]any
	snellerTypeList                               // Go: []any
)

// snellerType returns the matching Sneller column type for a given ION type.
func snellerType(typ ion.Type) snellerColumnType {
	switch typ {
	case ion.NullType:
		return snellerTypeNull
	case ion.BoolType:
		return snellerTypeBool
	case ion.IntType:
		return snellerTypeNumber
	case ion.FloatType:
		return snellerTypeNumber
	case ion.TimestampType:
		return snellerTypeTimestamp
	case ion.SymbolType:
		return snellerTypeString
	case ion.StringType:
		return snellerTypeString
	case ion.StructType:
		return snellerTypeStruct
	case ion.ListType:
		return snellerTypeList
	default:
		return snellerTypeUnknown
	}
}

// snellerColumn represents a single column in the result-set of a Sneller query.
type snellerColumn struct {
	Index    int               // The column index (or -1 if not stable)
	Name     string            // The column name
	Typ      snellerColumnType // The column type
	Nullable bool              // The column supports 'null' values
	Optional bool              // The column supports 'missing' values
	Floating bool              // The column contains at least one floating point numeric value
	Count    int               // The number of rows containing a value for this column
}

type snellerFinalStatus struct {
	Hits    int64
	Misses  int64
	Scanned int64
	Error   string
}

// snellerSchema represents the derived schema of a Sneller query result-set.
type snellerSchema struct {
	RowCount    int                       // The total number of rows returned by the query
	Columns     map[string]*snellerColumn // The individual columns indexed by their field names
	FinalStatus *snellerFinalStatus       // The final query status
}

func deriveSchema(input io.Reader) (*snellerSchema, error) {
	schema := snellerSchema{
		RowCount: 0,
		Columns:  map[string]*snellerColumn{},
	}

	status, err := iterateRows(input, func(reader ion.Reader, index int) error {
		schema.RowCount += 1
		return analyzeRow(reader, &schema)
	})
	if err != nil {
		return nil, err
	}

	schema.FinalStatus = status

	// Detect missing values
	for _, col := range schema.Columns {
		if col.Count != schema.RowCount {
			col.Optional = true
		}
	}

	return &schema, nil
}

func analyzeRow(reader ion.Reader, schema *snellerSchema) error {
	index := 0
	for reader.Next() {
		sym, err := reader.FieldName()
		if err != nil {
			return err
		}
		if sym == nil || sym.Text == nil {
			continue
		}

		name := *sym.Text
		typ := snellerType(reader.Type())

		col, ok := schema.Columns[name]
		if !ok {
			col = &snellerColumn{
				Index:    index,
				Name:     name,
				Typ:      typ,
				Nullable: typ == snellerTypeNull,
				Optional: schema.RowCount != 1,
				Count:    0,
			}
			schema.Columns[name] = col
		}

		if index != col.Index {
			col.Index = -1
		}
		col.Count++

		// Adjust column type if required
		if typ != col.Typ {
			if typ == snellerTypeNull {
				// At least one row contains a non-null value for the current field
				// -> keep type and mark row as 'nullable'
				col.Nullable = true
			} else if col.Typ == snellerTypeNull {
				// All rows contain null values for the current field
				// -> set current type as the new row type
				col.Typ = typ
			} else {
				// The column has an ambiguous type
				col.Typ = snellerTypeUnknown
			}
		}

		// Additional meta info for numeric fields
		if reader.Type() == ion.FloatType {
			col.Floating = true
		}
		// TODO: Required bits
		// TODO: Signedness

		index++
	}

	return reader.Err()
}

func iterateRows(input io.Reader, readRowFn func(reader ion.Reader, index int) error) (*snellerFinalStatus, error) {
	var status *snellerFinalStatus
	reader := ion.NewReader(input)

	index := 0
	for reader.Next() {
		if reader.Type() != ion.StructType {
			return nil, fmt.Errorf("expected 'struct' type, got '%s'", reader.Type().String())
		}

		annotations, err := reader.Annotations()
		if err != nil {
			return nil, err
		}

		// Fail on unexpected annotations
		if len(annotations) != 0 && (len(annotations) != 1 || annotations[0].Text == nil || *annotations[0].Text != "final_status") {
			labels := make([]string, len(annotations))
			for i := range annotations {
				if annotations[0].Text != nil {
					labels[i] = *annotations[i].Text
				} else {
					labels[i] = "%missing%"
				}
			}
			return nil, fmt.Errorf("unexpected annotations: [%s]", strings.Join(labels, ", "))
		}

		err = reader.StepIn()
		if err != nil {
			return nil, err
		}

		// Parse ::final_status annotation
		if len(annotations) != 0 {
			status, err = readFinalStatus(reader)
			if err != nil {
				return nil, err
			}
		}

		// Process data row
		err = readRowFn(reader, index)
		if err != nil {
			return nil, err
		}

		err = reader.StepOut()
		if err != nil {
			return nil, err
		}

		index++
	}

	return status, reader.Err()
}

func readFinalStatus(reader ion.Reader) (*snellerFinalStatus, error) {
	var status = snellerFinalStatus{}

	for reader.Next() {
		sym, err := reader.FieldName()
		if err != nil {
			return nil, err
		}
		if sym == nil || sym.Text == nil {
			continue
		}

		name := *sym.Text

		switch name {
		case "hits":
			value, err := reader.Int64Value()
			if err != nil {
				return nil, err
			}
			status.Hits = *value
		case "misses":
			value, err := reader.Int64Value()
			if err != nil {
				return nil, err
			}
			status.Misses = *value
		case "scanned":
			value, err := reader.Int64Value()
			if err != nil {
				return nil, err
			}
			status.Scanned = *value
		case "result_set":
			// Ignore for now
		case "error":
			value, err := reader.StringValue()
			if err != nil {
				return nil, err
			}
			status.Error = *value
		default:
			return nil, fmt.Errorf("unexpected ::final_status field '%s'", name)
		}

	}

	return &status, reader.Err()
}

type fieldReadFunc = func(reader ion.Reader, rowIndex int) error

type fieldValues struct {
	Name   string        // The field name
	Values any           // The field values for each row (Go: *[]T)
	ReadFn fieldReadFunc // The read function
}

func newFieldValues[T any](name string, rowCount int, fn func(reader ion.Reader) (T, error)) *fieldValues {
	values := make([]T, rowCount)
	readFn := func(reader ion.Reader, index int) error {
		value, err := fn(reader)
		if err != nil {
			return err
		}
		values[index] = value
		return nil
	}

	return &fieldValues{Name: name, Values: values, ReadFn: readFn}
}

func readRowValues(reader ion.Reader, index int, fieldValues []*fieldValues) error {
	for reader.Next() {
		sym, err := reader.FieldName()
		if err != nil {
			return err
		}
		if sym == nil || sym.Text == nil {
			continue
		}

		name := *sym.Text

		for _, field := range fieldValues {
			if name != field.Name {
				continue
			}

			err := field.ReadFn(reader, index)
			if err != nil {
				return err
			}
		}
	}

	return reader.Err()
}

// ---

func readValue(reader ion.Reader) (any, error) {
	var value any

	switch reader.Type() {
	case ion.NullType:
		return (*struct{})(nil), nil
	case ion.BoolType:
		value, _ = reader.BoolValue()
	case ion.IntType:
		value, _ = reader.Int64Value()
	case ion.FloatType:
		value, _ = reader.FloatValue()
	case ion.TimestampType:
		val, _ := reader.TimestampValue()
		value = convertTimestampValue(val)
	case ion.SymbolType:
		val, _ := reader.SymbolValue()
		value = convertSymbolValue(val)
	case ion.StringType:
		value, _ = reader.StringValue()
	case ion.StructType:
		val, err := readStruct(reader)
		if err != nil {
			return nil, err
		}
		value = val
	case ion.ListType:
		val, err := readList(reader)
		if err != nil {
			return nil, err
		}
		value = val
	default:
		return nil, fmt.Errorf("unsupported ION type '%s'", reader.Type())
	}

	return value, nil
}

func readStruct(reader ion.Reader) (map[string]any, error) {
	result := map[string]any{}

	err := reader.StepIn()
	if err != nil {
		return nil, err
	}

	for reader.Next() {
		sym, err := reader.FieldName()
		if err != nil {
			return nil, err
		}
		if sym == nil || sym.Text == nil {
			continue
		}

		name := *sym.Text

		value, err := readValue(reader)
		if err != nil {
			return nil, err
		}

		result[name] = value
	}

	if reader.Err() != nil {
		return nil, reader.Err()
	}

	err = reader.StepOut()
	if err != nil {
		return nil, err
	}

	return result, nil
}

func readList(reader ion.Reader) ([]any, error) {
	var result []any

	err := reader.StepIn()
	if err != nil {
		return nil, err
	}

	for reader.Next() {
		value, err := readValue(reader)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}

	if reader.Err() != nil {
		return nil, reader.Err()
	}

	err = reader.StepOut()
	if err != nil {
		return nil, err
	}

	return result, nil
}

func convertTimestampValue(value *ion.Timestamp) *time.Time {
	if value == nil {
		return nil
	}
	result := value.GetDateTime()
	return &result
}

func convertSymbolValue(value *ion.SymbolToken) *string {
	if value == nil || value.Text == nil {
		return nil
	}
	return value.Text
}
