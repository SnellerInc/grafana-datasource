package plugin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/SnellerInc/sneller/ion"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"golang.org/x/exp/slices"
)

// frameFromSnellerResult builds a Grafana data frame from a raw Sneller query result.
func frameFromSnellerResult(refID, sql string, input io.Reader, timeField string) (*data.Frame, error) {
	// Buffer query result in memory

	b, err := io.ReadAll(input)
	if err != nil {
		return nil, err
	}

	// Step 1: Derive schema

	schema, err := deriveSchema(b)
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

	_, err = iterateRows(b, func(reader *IonReader, index int) error {
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
			if column.Signed {
				result = data.FieldTypeInt64
			} else {
				result = data.FieldTypeUint64
			}
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
		case data.FieldTypeUint64:
			fallthrough
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
	case data.FieldTypeUint64:
		return newFieldValues[uint64](name, rowCount, readUint64), nil
	case data.FieldTypeNullableUint64:
		return newFieldValues[*uint64](name, rowCount, readUint64Nullable), nil
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

func readJSON(r *IonReader) (json.RawMessage, error) {
	value, _ := readJSONNullable(r)
	return *value, nil
}

func readJSONNullable(r *IonReader) (*json.RawMessage, error) {
	value, err := r.ReadValue()
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	return (*json.RawMessage)(&b), nil
}

func readBool(r *IonReader) (bool, error) {
	return r.ReadBool()
}

func readBoolNullable(r *IonReader) (*bool, error) {
	return r.ReadNullableBool()
}

func readUint64(r *IonReader) (uint64, error) {
	return r.ReadUint()
}

func readUint64Nullable(r *IonReader) (*uint64, error) {
	return r.ReadNullableUint()
}

func readInt64(r *IonReader) (int64, error) {
	return r.ReadInt()
}

func readInt64Nullable(r *IonReader) (*int64, error) {
	return r.ReadNullableInt()
}

func readFloat64(r *IonReader) (float64, error) {
	return r.ReadNumber()
}

func readFloat64Nullable(r *IonReader) (*float64, error) {
	return r.ReadNullableNumber()
}

func readString(r *IonReader) (string, error) {
	return r.ReadText()
}

func readStringNullable(r *IonReader) (*string, error) {
	return r.ReadNullableText()
}

func readTime(r *IonReader) (time.Time, error) {
	value, err := r.ReadTimestamp()
	if err != nil {
		return time.Time{}, err
	}
	return value.Time(), nil
}

func readTimeNullable(r *IonReader) (*time.Time, error) {
	if r.Type() == ion.NullType {
		return nil, r.ReadNull()
	}
	value, err := readTime(r)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func readTimeFromInt64(r *IonReader) (time.Time, error) {
	value, err := r.ReadInt()
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(value), nil
}

func readTimeFromInt64Nullable(r *IonReader) (*time.Time, error) {
	if r.Type() == ion.NullType {
		return nil, r.ReadNull()
	}
	result, err := readTimeFromInt64(r)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func readTimeFromString(r *IonReader) (time.Time, error) {
	value, err := r.ReadString()
	if err != nil {
		return time.Time{}, err
	}

	result, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, err
	}

	return result, nil
}

func readTimeFromStringNullable(r *IonReader) (*time.Time, error) {
	if r.Type() == ion.NullType {
		return nil, r.ReadNull()
	}
	result, err := readTimeFromString(r)
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
	case ion.UintType:
		return snellerTypeNumber
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
	Signed   bool              // The column contains at least one signed numeric value
	Count    int               // The number of rows containing a value for this column
}

type snellerFinalStatus struct {
	Hits      int64     `ion:"hits"`
	Misses    int64     `ion:"misses"`
	Scanned   int64     `ion:"scanned"`
	Error     string    `ion:"error"`
	ResultSet ion.Datum `ion:"result_set"`
}

type snellerQueryError struct {
	Error string `ion:"error"`
}

// snellerSchema represents the derived schema of a Sneller query result-set.
type snellerSchema struct {
	RowCount    int                 // The total number of rows returned by the query
	Columns     []*snellerColumn    // The individual columns
	FinalStatus *snellerFinalStatus // The final query status
}

func deriveSchema(buf []byte) (*snellerSchema, error) {
	schema := snellerSchema{
		RowCount: 0,
		Columns:  []*snellerColumn{},
	}
	lookup := map[string]*snellerColumn{}

	status, err := iterateRows(buf, func(reader *IonReader, index int) error {
		schema.RowCount += 1
		return analyzeRow(reader, &schema, lookup)
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

	if status.ResultSet.IsEmpty() {
		return &schema, nil
	}

	// Restore column order
	index := 0
	err = status.ResultSet.UnpackStruct(func(field ion.Field) error {
		for _, col := range schema.Columns {
			if col.Name == field.Label {
				col.Index = index
				break
			}
		}
		index++
		return nil
	})
	if err != nil {
		return nil, err
	}

	slices.SortFunc(schema.Columns, func(a, b *snellerColumn) bool {
		return a.Index < b.Index
	})

	return &schema, nil
}

func analyzeRow(reader *IonReader, schema *snellerSchema, lookup map[string]*snellerColumn) error {
	index := 0
	for reader.Next() {
		name, err := reader.FieldName()
		if err != nil {
			return err
		}

		ionType := reader.Type()
		snellerType := snellerType(ionType)

		col, ok := lookup[name]
		if !ok {
			col = &snellerColumn{
				Index:    index,
				Name:     name,
				Typ:      snellerType,
				Nullable: snellerType == snellerTypeNull,
				Signed:   ionType == ion.IntType || ionType == ion.FloatType,
				Optional: schema.RowCount != 1,
				Count:    0,
			}
			lookup[name] = col
			schema.Columns = append(schema.Columns, col)
		}

		if index != col.Index {
			col.Index = -1
		}
		col.Count++

		// Adjust column type if required
		if snellerType != col.Typ {
			if snellerType == snellerTypeNull {
				// At least one row contains a non-null value for the current field
				// -> keep type and mark row as 'nullable'
				col.Nullable = true
			} else if col.Typ == snellerTypeNull {
				// All rows contain null values for the current field
				// -> set current type as the new row type
				col.Typ = snellerType
			} else {
				// The column has an ambiguous type
				col.Typ = snellerTypeUnknown
			}
		}

		// Additional meta info for numeric fields
		if snellerType == snellerTypeNumber {
			if ionType == ion.FloatType {
				col.Floating = true
				col.Signed = true
			} else if ionType == ion.IntType {
				col.Signed = true
			}
			// TODO: Required bits
		}

		index++
	}

	return reader.Error()
}

func iterateRows(buf []byte, readRowFn func(reader *IonReader, index int) error) (*snellerFinalStatus, error) {
	reader := NewReader(bytes.NewReader(buf), 1024*1024*10) // 10 MiB

	var finalStatus snellerFinalStatus
	var queryError snellerQueryError
	var status *snellerFinalStatus

	index := 0
	for reader.Next() {
		if status != nil {
			return nil, errors.New("unexpected data after ::final_status annotation")
		}

		t := reader.Type()
		if t != ion.StructType {
			return nil, fmt.Errorf("expected 'struct' type, got '%s'", t)
		}

		annotations, err := reader.Annotations()
		if err != nil {
			return nil, err
		}

		if annotations != nil {
			switch annotations[0] {
			case "final_status":
				err = reader.Unmarshal(&finalStatus)
				if err != nil {
					return nil, err
				}
				status = &finalStatus
				continue
			case "query_error":
				err = reader.Unmarshal(&queryError)
				if err != nil {
					return nil, err
				}
				continue
			default:
				return nil, fmt.Errorf("unexpected annotation: [%s]", strings.Join(annotations, ", "))
			}
		}

		err = reader.StepIn()
		if err != nil {
			return nil, err
		}

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
	if status == nil {
		return nil, fmt.Errorf("missing final_status annotation (upstream query error)")
	}
	return status, reader.Error()
}

type fieldReadFunc = func(reader *IonReader, rowIndex int) error

type fieldValues struct {
	Name   string        // The field name
	Values any           // The field values for each row (Go: *[]T)
	ReadFn fieldReadFunc // The peek function
}

func newFieldValues[T any](name string, rowCount int, fn func(r *IonReader) (T, error)) *fieldValues {
	values := make([]T, rowCount)
	readFn := func(r *IonReader, index int) error {
		value, err := fn(r)
		if err != nil {
			return err
		}
		values[index] = value
		return nil
	}

	return &fieldValues{Name: name, Values: values, ReadFn: readFn}
}

func readRowValues(reader *IonReader, index int, fieldValues []*fieldValues) error {
	for reader.Next() {
		name, err := reader.FieldName()
		if err != nil {
			return err
		}

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

	return reader.Error()
}
