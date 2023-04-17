package plugin

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"runtime"
	"time"

	"github.com/amazon-ion/ion-go/ion"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/remeh/sizedwaitgroup"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

// frameFromSnellerResult builds a Grafana data frame from a raw Sneller query result.
func frameFromSnellerResult(refID, sql string, input io.Reader, timeField string) (*data.Frame, error) {
	t := time.Now()

	resultSet, err := processRows(input)
	if err != nil {
		return nil, err
	}

	fields := grafanaFields(resultSet, timeField)

	frame := data.NewFrame(refID, fields...)
	frame.Meta = &data.FrameMeta{
		Type:                   data.FrameTypeTable,
		PreferredVisualization: data.VisTypeTable,
		ExecutedQueryString:    sql,
	}

	log.Println("duration: ", time.Since(t))

	return frame, nil
}

func grafanaFields(resultSet *snellerResultSet, timeField string) []*data.Field {
	type fieldWithIndex struct {
		Index int
		Field *data.Field
	}

	fields := make(chan *fieldWithIndex, len(resultSet.Columns))

	wg := sizedwaitgroup.New(runtime.NumCPU())
	for i, col := range resultSet.Columns {
		wg.Add()
		go func(i int, column *snellerColumn) {
			var field *data.Field
			typ := grafanaType(column)

			if column.Name == timeField && (typ.NonNullableType() == data.FieldTypeInt64 || typ.NonNullableType() == data.FieldTypeString) {
				// Try to convert values to time.Time
				field = grafanaTimeField(resultSet, column, typ)
			}

			if field == nil {
				// Copy values from Sneller column to Grafana field
				field = data.NewFieldFromFieldType(typ, resultSet.RowCount)
				field.Name = column.Name
				for j := range column.Values {
					field.Set(j, grafanaValue(column, typ, j))
				}
			}

			fields <- &fieldWithIndex{Index: i, Field: field}
			wg.Done()
		}(i, col)
	}

	wg.Wait()
	close(fields)

	result := make([]*data.Field, len(resultSet.Columns))
	for field := range fields {
		result[field.Index] = field.Field
	}

	return result
}

func grafanaTimeField(resultSet *snellerResultSet, column *snellerColumn, typ data.FieldType) *data.Field {
	timeType := data.FieldTypeTime
	if column.Nullable || column.Optional {
		timeType = timeType.NullableType()
	}
	field := data.NewFieldFromFieldType(timeType, resultSet.RowCount)
	field.Name = column.Name

	failed := false
	for j := range column.Values {
		value := grafanaValue(column, typ, j)
		switch typ {
		case data.FieldTypeInt64:
			value = time.UnixMilli(value.(int64))
		case data.FieldTypeNullableInt64:
			var t time.Time
			if value != nil {
				t = time.UnixMilli(*value.(*int64))
			}
			value = &t
		case data.FieldTypeString:
			t, err := time.Parse(time.RFC3339, value.(string))
			if err != nil {
				failed = true
				break
			}
			value = t
		case data.FieldTypeNullableString:
			var t time.Time
			if value != nil {
				var err error
				t, err = time.Parse(time.RFC3339, *value.(*string))
				if err != nil {
					failed = true
					break
				}
			}
			value = &t
		}
		field.Set(j, value)
	}

	if failed {
		return nil
	}

	return field
}

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
	default:
		return data.FieldTypeUnknown
	}

	if column.Nullable || column.Optional {
		result = result.NullableType()
	}

	return result
}

func grafanaValue(column *snellerColumn, fieldType data.FieldType, index int) any {
	if fieldType == data.FieldTypeJSON || fieldType == data.FieldTypeNullableJSON {
		b, _ := json.Marshal(column.Values[index])
		j := json.RawMessage(b)
		if fieldType == data.FieldTypeNullableJSON {
			return &j
		}
		return j
	}

	if fieldType.Nullable() {
		// Core normalized number representation might use int64 for float64 values...
		if fieldType == data.FieldTypeNullableFloat64 {
			ival, ok := column.Values[index].(*int64)
			if ok {
				fval := float64(*ival)
				return &fval
			}
			return column.Values[index]
		}
		// All deserialized ION values are nullable by default
		return column.Values[index]
	}

	switch fieldType {
	case data.FieldTypeBool:
		return *(column.Values[index]).(*bool)
	case data.FieldTypeInt64:
		return *(column.Values[index]).(*int64)
	case data.FieldTypeFloat64:
		ival, ok := column.Values[index].(*int64)
		if ok {
			return float64(*ival)
		}
		return *(column.Values[index]).(*float64)
	case data.FieldTypeTime:
		return *(column.Values[index]).(*time.Time)
	case data.FieldTypeString:
		return *(column.Values[index]).(*string)
	}

	return column.Values[index]
}

// ---

type snellerColumnType int

const (
	snellerTypeUnknown   snellerColumnType = iota // Unknown or ambiguous type
	snellerTypeNull                               // Go: *struct{} (always nil)
	snellerTypeBool                               // Go: *bool
	snellerTypeNumber                             // Go: *int64 or *float64 (core normalized representation)
	snellerTypeTimestamp                          // Go: *time.Time
	snellerTypeString                             // Go: *string
	snellerTypeStruct                             // Go: map[string]any
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
	// Values contains an entry for each row. The underlying Go types are nullable and missing
	// values are filled with 'nil'.
	// Note that the elements in this array might be of arbitrary type (e.g. numeric columns
	// might contain either *int64 or *float64 values).
	Values []any
}

// snellerResultSet represents the result-set of a Sneller query.
type snellerResultSet struct {
	RowCount int              // The total number of rows returned by the query
	Columns  []*snellerColumn // The individual columns and the corresponding values for all rows
}

func processRows(input io.Reader) (*snellerResultSet, error) {
	colIndex := map[string]*snellerColumn{}
	rowCount := 0

	reader := ion.NewReader(input)
	for reader.Next() {
		if reader.Type() != ion.StructType {
			return nil, fmt.Errorf("expected 'struct' type, got '%s'", reader.Type().String())
		}

		// Ignore the struct that follows the '::final_status' annotation
		annotations, err := reader.Annotations()
		if err != nil {
			return nil, err
		}
		if len(annotations) != 0 && annotations[0].Text != nil && *annotations[0].Text == "final_status" {
			continue
		}

		// Process data row
		err = reader.StepIn()
		if err != nil {
			return nil, err
		}

		rowCount += 1

		err = processRow(reader, colIndex, rowCount)
		if err != nil {
			return nil, err
		}

		err = reader.StepOut()
		if err != nil {
			return nil, err
		}

		// Fill missing values
		for _, col := range colIndex {
			if len(col.Values) != rowCount {
				col.Values = append(col.Values, nil)
				col.Optional = true
			}
		}
	}

	if reader.Err() != nil {
		return nil, reader.Err()
	}

	columns := maps.Values(colIndex)
	slices.SortStableFunc(columns, func(a, b *snellerColumn) bool {
		return a.Index < b.Index
	})

	return &snellerResultSet{
		RowCount: rowCount,
		Columns:  columns,
	}, nil
}

func processRow(reader ion.Reader, colIndex map[string]*snellerColumn, rowCount int) error {
	for reader.Next() {
		index := 0

		sym, err := reader.FieldName()
		if err != nil {
			return err
		}
		if sym == nil || sym.Text == nil {
			continue
		}

		name := *sym.Text
		typ := snellerType(reader.Type())

		col, ok := colIndex[name]
		if !ok {
			col = &snellerColumn{
				Index:    index,
				Name:     name,
				Typ:      typ,
				Nullable: typ == snellerTypeNull,
				Optional: rowCount != 1,
				Count:    0,
				Values:   make([]any, rowCount-1),
			}
			colIndex[name] = col
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

		value, err := readValue(reader)
		if err != nil {
			return err
		}

		col.Values = append(col.Values, value)

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
