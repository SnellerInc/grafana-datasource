package plugin

import (
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"time"

	"github.com/amazon-ion/ion-go/ion"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/remeh/sizedwaitgroup"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

// frameFromSnellerResult builds a Grafana data frame from a raw Sneller query result.
func frameFromSnellerResult2(refID, sql string, input io.Reader, timeField string) (*data.Frame, error) {
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
		go func(i int, column *snellerColumn2) {
			var field *data.Field
			typ := grafanaType2(column)

			if column.Name == timeField && (typ.NonNullableType() == data.FieldTypeInt64 || typ.NonNullableType() == data.FieldTypeString) {
				// Try to convert values to time.Time
				field = grafanaTimeField2(resultSet, column, typ)
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

func grafanaTimeField2(resultSet *snellerResultSet, column *snellerColumn2, typ data.FieldType) *data.Field {
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

func grafanaType2(column *snellerColumn2) data.FieldType {
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

func grafanaValue(column *snellerColumn2, fieldType data.FieldType, index int) any {
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

// snellerColumn represents a single column in the result-set of a Sneller query.
type snellerColumn2 struct {
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
	RowCount int               // The total number of rows returned by the query
	Columns  []*snellerColumn2 // The individual columns and the corresponding values for all rows
}

func processRows(input io.Reader) (*snellerResultSet, error) {
	colIndex := map[string]*snellerColumn2{}
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
	slices.SortStableFunc(columns, func(a, b *snellerColumn2) bool {
		return a.Index < b.Index
	})

	return &snellerResultSet{
		RowCount: rowCount,
		Columns:  columns,
	}, nil
}

func processRow(reader ion.Reader, colIndex map[string]*snellerColumn2, rowCount int) error {
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
			col = &snellerColumn2{
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
