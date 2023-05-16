package plugin

import (
	"bufio"
	"errors"
	"fmt"
	"io"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

// IonReader is a stateful ION reader.
type IonReader struct {
	// Symbols is the current symbol table.
	// Calls to IonReader.Next will update the symbol table as symbol table annotations are
	// encountered in the source data stream.
	Symbols ion.Symtab
	ctx     *ionContext
	buf     []byte
	stack   []*ionContext
}

type ionContext struct {
	src         *bufferReader
	err         error
	typ         ion.Type
	size        int
	label       *ion.Symbol
	annotations []ion.Symbol
}

type bufferReader struct {
	r   *bufio.Reader
	buf []byte
}

func (b *bufferReader) Peek(n int) ([]byte, error) {
	if b.r != nil {
		return b.r.Peek(n)
	}
	if len(b.buf) < n {
		return b.buf, io.EOF
	}
	return b.buf[:n], nil
}

func (b *bufferReader) Discard(n int) (discarded int, err error) {
	if b.r != nil {
		return b.r.Discard(n)
	}
	if n == 0 {
		return
	}
	b.buf = b.buf[n:]
	return n, nil
}

// NewReader constructs a reader that reads values from r up to the given maximum size.
func NewReader(r io.Reader, max int) *IonReader {
	ctx := ionContext{
		typ: ion.InvalidType,
	}
	b, ok := r.(*bufio.Reader)
	if ok && b.Size() >= max {
		ctx.src = &bufferReader{r: b}
	}
	ctx.src = &bufferReader{r: bufio.NewReaderSize(r, max)}

	return &IonReader{ctx: &ctx} // TODO: , buf: make([]byte, 0, max)
}

// Next moves the internal iterator to the next value. Error should be checked, if this function
// return false to determine if an error occurred or the end of the iterator is reached.
func (r *IonReader) Next() bool {
	if r.ctx.size != 0 {
		r.ctx.src.Discard(r.ctx.size)
	}

	if r.inStruct() {
		buf, err := r.ctx.src.Peek(8)
		if len(buf) == 0 {
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			r.ctx.err = err
			goto handleError
		}

		sym, rest, err := ion.ReadLabel(buf)
		if err != nil {
			r.ctx.err = err
			goto handleError
		}

		r.ctx.label = &sym
		r.ctx.src.Discard(len(buf) - len(rest))
	}

	r.ctx.annotations = nil

	for {
		r.ctx.typ, r.ctx.size, r.ctx.err = ionPeek(r.ctx.src)
		if r.ctx.err != nil {
			goto handleError
		}

		if r.ctx.typ != ion.AnnotationType {
			break
		}

		buf, err := r.ctx.src.Peek(r.ctx.size)
		if err != nil {
			r.ctx.err = err
			goto handleError
		}

		var rest []byte
		if r.isSymtab(buf) {
			rest, r.ctx.err = r.Symbols.Unmarshal(buf)
			if r.ctx.err != nil {
				goto handleError
			}
		} else {
			var sym ion.Symbol
			sym, rest, _, r.ctx.err = ion.ReadAnnotation(buf)
			if err != nil {
				goto handleError
			}
			// TODO: Sneller ION library only returns the first label at the moment...
			r.ctx.annotations = append(r.ctx.annotations, sym)
		}

		r.ctx.src.Discard(len(buf) - len(rest))
	}

	return true

handleError:
	if errors.Is(r.ctx.err, io.EOF) {
		r.ctx.err = nil
		return false
	}

	return true
}

// Error returns any error occurred in the Next function.
func (r *IonReader) Error() error {
	return r.ctx.err
}

// Type returns the type of the current value.
func (r *IonReader) Type() ion.Type {
	return r.ctx.typ
}

// StepIn steps into a struct or a list.
func (r *IonReader) StepIn() error {
	if r.ctx.typ != ion.StructType && r.ctx.typ != ion.ListType {
		return fmt.Errorf("expected 'struct' type or 'list' type, got '%s'", r.ctx.typ)
	}

	err := r.peek()
	if err != nil {
		return err
	}

	body, _ := ion.Contents(r.buf)
	if len(body) == 0 {
		return io.EOF
	}
	r.discard()

	r.stack = append(r.stack, r.ctx)
	r.ctx = &ionContext{
		src: &bufferReader{buf: body},
		typ: ion.InvalidType,
	}

	return nil
}

// StepOut steps out of a struct or a list, discarding any unconsumed inner values.
func (r *IonReader) StepOut() error {
	if len(r.stack) == 0 {
		return errors.New("invalid operation: not inside a nested struct or list")
	}

	r.ctx = r.stack[len(r.stack)-1]
	r.stack = r.stack[:len(r.stack)-1]

	return nil
}

// FieldName returns the name of the current field, when inside a struct.
func (r *IonReader) FieldName() (string, error) {
	if r.ctx.label == nil {
		return "", errors.New("invalid operation: not inside a struct")
	}

	return r.LookupSymbol(*r.ctx.label)
}

// Annotations returns the annotations of the current value, if any. Returns a nil value if no
// annotations are present.
func (r *IonReader) Annotations() ([]string, error) {
	if r.ctx.annotations == nil {
		return nil, nil
	}

	result := make([]string, len(r.ctx.annotations))
	for i := range r.ctx.annotations {
		name, err := r.LookupSymbol(r.ctx.annotations[i])
		if err != nil {
			return nil, err
		}
		result[i] = name
	}

	return result, nil
}

// ReadNull reads an ion.NullType value, by discarding the descriptor. This is effectively the
// same as calling Next.
func (r *IonReader) ReadNull() error {
	err := r.checkType(ion.NullType)
	if err != nil {
		return err
	}
	r.discard()
	return nil
}

func (r *IonReader) ReadBool() (bool, error) {
	var value bool
	err := r.checkType(ion.BoolType)
	if err != nil {
		return value, err
	}
	err = r.peek()
	if err != nil {
		return value, err
	}
	value, _, err = ion.ReadBool(r.buf)
	r.discard()
	return value, err
}

func (r *IonReader) ReadNullableBool() (*bool, error) {
	if r.ctx.typ == ion.NullType {
		r.discard()
		return nil, nil
	}
	value, err := r.ReadBool()
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func (r *IonReader) ReadUint() (uint64, error) {
	var value uint64
	err := r.checkType(ion.UintType)
	if err != nil {
		return value, err
	}
	err = r.peek()
	if err != nil {
		return value, err
	}
	value, _, err = ion.ReadUint(r.buf)
	r.discard()
	return value, err
}

func (r *IonReader) ReadNullableUint() (*uint64, error) {
	if r.ctx.typ == ion.NullType {
		r.discard()
		return nil, nil
	}
	value, err := r.ReadUint()
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func (r *IonReader) ReadInt() (int64, error) {
	var value int64
	err := r.checkType(ion.IntType)
	if err != nil {
		return value, err
	}
	err = r.peek()
	if err != nil {
		return value, err
	}
	value, _, err = ion.ReadInt(r.buf)
	r.discard()
	return value, err
}

func (r *IonReader) ReadNullableInt() (*int64, error) {
	if r.ctx.typ == ion.NullType {
		r.discard()
		return nil, nil
	}
	value, err := r.ReadInt()
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func (r *IonReader) ReadFloat() (float64, error) {
	var value float64
	err := r.checkType(ion.FloatType)
	if err != nil {
		return value, err
	}
	err = r.peek()
	if err != nil {
		return value, err
	}
	value, _, err = ion.ReadFloat64(r.buf)
	r.discard()
	return value, err
}

func (r *IonReader) ReadNullableFloat() (*float64, error) {
	if r.ctx.typ == ion.NullType {
		r.discard()
		return nil, nil
	}
	value, err := r.ReadFloat()
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func (r *IonReader) ReadTimestamp() (date.Time, error) {
	var value date.Time
	err := r.checkType(ion.TimestampType)
	if err != nil {
		return value, err
	}
	err = r.peek()
	if err != nil {
		return value, err
	}
	value, _, err = ion.ReadTime(r.buf)
	r.discard()
	return value, err
}

func (r *IonReader) ReadNullableTimestamp() (*date.Time, error) {
	if r.ctx.typ == ion.NullType {
		r.discard()
		return nil, nil
	}
	value, err := r.ReadTimestamp()
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func (r *IonReader) ReadSymbol() (ion.Symbol, error) {
	var value ion.Symbol
	err := r.checkType(ion.SymbolType)
	if err != nil {
		return value, err
	}
	err = r.peek()
	if err != nil {
		return value, err
	}
	value, _, err = ion.ReadSymbol(r.buf)
	r.discard()
	return value, err
}

func (r *IonReader) ReadNullableSymbol() (*ion.Symbol, error) {
	if r.ctx.typ == ion.NullType {
		r.discard()
		return nil, nil
	}
	value, err := r.ReadSymbol()
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func (r *IonReader) ReadString() (string, error) {
	var value string
	err := r.checkType(ion.StringType)
	if err != nil {
		return value, err
	}
	err = r.peek()
	if err != nil {
		return value, err
	}
	value, _, err = ion.ReadString(r.buf)
	r.discard()
	return value, err
}

func (r *IonReader) ReadNullableString() (*string, error) {
	if r.ctx.typ == ion.NullType {
		r.discard()
		return nil, nil
	}
	value, err := r.ReadString()
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func (r *IonReader) ReadBytes() ([]byte, error) {
	var value []byte
	err := r.checkType(ion.BlobType)
	if err != nil {
		return value, err
	}
	err = r.peek()
	if err != nil {
		return value, err
	}
	value, _, err = ion.ReadBytes(r.buf)
	r.discard()
	return value, err
}

func (r *IonReader) ReadNullableBytes() ([]byte, error) {
	if r.ctx.typ == ion.NullType {
		r.discard()
		return nil, nil
	}
	return r.ReadBytes()
}

// ReadNumber reads any numeric value and returns it as a float64. Fails, if the current value
// is not of type ion.UintType, ion.IntType or ion.FloatType.
func (r *IonReader) ReadNumber() (float64, error) {
	switch r.ctx.typ {
	case ion.UintType:
		temp, err := r.ReadUint()
		if err != nil {
			return 0, err
		}
		return float64(temp), nil
	case ion.IntType:
		temp, err := r.ReadInt()
		if err != nil {
			return 0, err
		}
		return float64(temp), nil
	case ion.FloatType:
		return r.ReadFloat()
	}

	return 0, fmt.Errorf("expected numeric type, got '%s'", r.ctx.typ)
}

// ReadNullableNumber reads any numeric value and returns it as a *float64. Fails, if the current
// value is not of type ion.NullType, ion.UintType, ion.IntType or ion.FloatType.
func (r *IonReader) ReadNullableNumber() (*float64, error) {
	if r.ctx.typ == ion.NullType {
		r.discard()
		return nil, nil
	}
	value, err := r.ReadNumber()
	if err != nil {
		return nil, err
	}
	return &value, nil
}

// ReadText reads any text value and returns it as a string. Fails, if the current value is not
// of type ion.SymbolType or ion.StringType.
func (r *IonReader) ReadText() (string, error) {
	switch r.ctx.typ {
	case ion.SymbolType:
		temp, err := r.ReadSymbol()
		if err != nil {
			return "", err
		}
		name, err := r.LookupSymbol(temp)
		if err != nil {
			return "", err
		}
		return name, nil
	case ion.StringType:
		return r.ReadString()
	}

	return "", fmt.Errorf("expected text type, got '%s'", r.ctx.typ)
}

// ReadNullableText reads any text value and returns it as a string. Fails, if the current value
// is not of type ion.NullType, ion.SymbolType or ion.StringType.
func (r *IonReader) ReadNullableText() (*string, error) {
	if r.ctx.typ == ion.NullType {
		r.discard()
		return nil, nil
	}
	value, err := r.ReadText()
	if err != nil {
		return nil, err
	}
	return &value, nil
}

// ReadValue reads an arbitrary ION value and returns it as a boxed 'any' value.
func (r *IonReader) ReadValue() (any, error) {
	var value any

	err := r.peek()
	if err != nil {
		return value, err
	}

	switch r.ctx.typ {
	case ion.NullType:
		value = (*struct{})(nil)
	case ion.BoolType:
		value, err = r.ReadBool()
	case ion.UintType:
		value, err = r.ReadUint()
	case ion.IntType:
		value, err = r.ReadInt()
	case ion.FloatType:
		value, err = r.ReadFloat()
	case ion.TimestampType:
		temp, err := r.ReadTimestamp()
		if err == nil {
			value = temp.Time()
		}
	case ion.SymbolType:
		temp, err := r.ReadSymbol()
		if err == nil {
			value, err = r.LookupSymbol(temp)
		}
	case ion.StringType:
		value, err = r.ReadString()
	case ion.BlobType:
		value, err = r.ReadBytes()
	case ion.ListType:
		value, err = r.ReadList()
	case ion.StructType:
		value, err = r.ReadStruct()
	default:
		return value, fmt.Errorf("unsupported ION type '%s'", r.ctx.typ)
	}

	r.discard()

	return value, nil
}

// ReadStruct reads an arbitrary ION struct. This is slightly more efficient than using Unmarshal
// with an any-typed map target.
func (r *IonReader) ReadStruct() (map[string]any, error) {
	err := r.checkType(ion.StructType)
	if err != nil {
		return nil, err
	}

	err = r.StepIn()
	if err != nil {
		return nil, err
	}

	result := map[string]any{}

	for r.Next() {
		name, err := r.FieldName()
		if err != nil {
			return nil, err
		}
		value, err := r.ReadValue()
		if err != nil {
			return nil, err
		}
		result[name] = value
	}

	err = r.StepOut()
	if err != nil {
		return nil, err
	}

	return result, r.Error()
}

// ReadList reads an arbitrary ION list. This is slightly more efficient than using Unmarshal
// with an any-typed list target.
func (r *IonReader) ReadList() ([]any, error) {
	err := r.checkType(ion.ListType)
	if err != nil {
		return nil, err
	}

	err = r.StepIn()
	if err != nil {
		return nil, err
	}

	var result []any

	for r.Next() {
		value, err := r.ReadValue()
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}

	err = r.StepOut()
	if err != nil {
		return nil, err
	}

	return result, r.Error()
}

// Unmarshal uses reflection to unmarshal the current value.
func (r *IonReader) Unmarshal(v any) error {
	err := r.peek()
	if err != nil {
		return err
	}
	_, err = ion.Unmarshal(&r.Symbols, r.buf, v)
	r.discard()
	return err
}

// LookupSymbol looks up a symbol in the symbol table and returns the corresponding string value.
func (r *IonReader) LookupSymbol(sym ion.Symbol) (string, error) {
	name, ok := r.Symbols.Lookup(sym)
	if !ok {
		return "", fmt.Errorf("symbol %d not in symbol table", r.ctx.label)
	}

	return name, nil
}

func (r *IonReader) peek() error {
	if r.ctx.size == 0 {
		// Return gracefully to allow subsequent reads of the same value
		return nil
	}

	var err error
	r.buf, err = r.ctx.src.Peek(r.ctx.size)
	if err != nil {
		return err
	}

	return nil
}

func (r *IonReader) discard() {
	if r.ctx.size == 0 {
		return
	}
	r.ctx.src.Discard(r.ctx.size)
	r.ctx.size = 0
}

func (r *IonReader) checkType(typ ion.Type) error {
	if r.ctx.typ != typ {
		return fmt.Errorf("expected '%s' type, got '%s'", typ, r.ctx.typ)
	}
	return nil
}

func (r *IonReader) inStruct() bool {
	return len(r.stack) != 0 && r.stack[len(r.stack)-1].typ == ion.StructType
}

func (r *IonReader) isSymtab(buf []byte) bool {
	if ion.IsBVM(buf) {
		return true
	}
	lbl, _, _, _ := ion.ReadAnnotation(buf)
	return lbl == ion.SystemSymSymbolTable
}

func ionPeek(r *bufferReader) (ion.Type, int, error) {
	p, err := r.Peek(10)
	if len(p) == 0 {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return 0, 0, err
	}
	// drop BVM
	prefix := 0
	if ion.IsBVM(p) {
		p = p[4:]
		prefix = 4
	}
	return ion.TypeOf(p), ion.SizeOf(p) + prefix, nil
}
