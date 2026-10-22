package siglog

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type decodedField struct {
	kind  Kind
	key   string
	value string
}

type decodedRecord struct {
	timestamp time.Time
	level     logLevel
	file      string
	line      uint32
	message   string
	fields    []decodedField
}

type recordDecodeError struct{ message string }

func (e *recordDecodeError) Error() string {
	if e == nil {
		return ErrMalformedRecord.Error()
	}
	return ErrMalformedRecord.Error() + ": " + e.message
}
func (e *recordDecodeError) Unwrap() error   { return ErrMalformedRecord }
func (e *recordDecodeError) Retryable() bool { return false }

func decodeRecord(data []byte) (decodedRecord, int, error) {
	if len(data) < recordHeaderSize {
		return decodedRecord{}, 0, &recordDecodeError{message: "incomplete record header"}
	}
	rawSize := uint64(binary.BigEndian.Uint32(data[:4]))
	if rawSize < uint64(recordHeaderSize+payloadHeaderSize) {
		return decodedRecord{}, 0, &recordDecodeError{message: "record length is smaller than its headers"}
	}
	if rawSize > uint64(len(data)) {
		return decodedRecord{}, 0, &recordDecodeError{message: "record length exceeds available bytes"}
	}
	size := int(rawSize)
	record := data[:size]
	level := logLevel(record[12])
	payload := record[recordHeaderSize:]
	fileLen := int(binary.BigEndian.Uint16(payload[0:2]))
	line := binary.BigEndian.Uint32(payload[2:6])
	messageLen := uint64(binary.BigEndian.Uint32(payload[6:10]))
	fieldCount := int(binary.BigEndian.Uint16(payload[10:12]))
	off := recordHeaderSize + payloadHeaderSize

	if fileLen > size-off {
		return decodedRecord{}, 0, &recordDecodeError{message: "caller path exceeds record"}
	}
	file := string(record[off : off+fileLen])
	off += fileLen
	if messageLen > uint64(size-off) {
		return decodedRecord{}, 0, &recordDecodeError{message: "message exceeds record"}
	}
	message := string(record[off : off+int(messageLen)])
	off += int(messageLen)

	fields := make([]decodedField, 0, fieldCount)
	for i := 0; i < fieldCount; i++ {
		if size-off < fieldHeaderSize {
			return decodedRecord{}, 0, &recordDecodeError{message: "incomplete field header"}
		}
		kind := Kind(record[off])
		keyLen := int(binary.BigEndian.Uint16(record[off+1 : off+3]))
		valueLen := uint64(binary.BigEndian.Uint32(record[off+3 : off+7]))
		off += fieldHeaderSize
		if keyLen > size-off {
			return decodedRecord{}, 0, &recordDecodeError{message: "field key exceeds record"}
		}
		key := string(record[off : off+keyLen])
		off += keyLen
		if valueLen > uint64(size-off) {
			return decodedRecord{}, 0, &recordDecodeError{message: "field value exceeds record"}
		}
		valueBytes := record[off : off+int(valueLen)]
		off += int(valueLen)
		value, err := decodeFieldValue(kind, valueBytes)
		if err != nil {
			return decodedRecord{}, 0, &recordDecodeError{message: err.Error()}
		}
		fields = append(fields, decodedField{kind: kind, key: key, value: value})
	}
	if off != size {
		return decodedRecord{}, 0, &recordDecodeError{message: "unexpected trailing bytes"}
	}

	return decodedRecord{
		timestamp: time.Unix(0, int64(binary.BigEndian.Uint64(record[4:12]))),
		level:     level,
		file:      file,
		line:      line,
		message:   message,
		fields:    fields,
	}, size, nil
}

func decodeFieldValue(kind Kind, value []byte) (string, error) {
	switch kind {
	case KindInt:
		if len(value) != 8 {
			return "", errors.New("int field must be 8 bytes")
		}
		return strconv.FormatInt(int64(binary.BigEndian.Uint64(value)), 10), nil
	case KindFloat:
		if len(value) != 8 {
			return "", errors.New("float field must be 8 bytes")
		}
		return strconv.FormatFloat(math.Float64frombits(binary.BigEndian.Uint64(value)), 'g', -1, 64), nil
	case KindUint:
		if len(value) != 8 {
			return "", errors.New("uint field must be 8 bytes")
		}
		return strconv.FormatUint(binary.BigEndian.Uint64(value), 10), nil
	case KindDuration:
		if len(value) != 8 {
			return "", errors.New("duration field must be 8 bytes")
		}
		return time.Duration(int64(binary.BigEndian.Uint64(value))).String(), nil
	case KindBool:
		if len(value) != 1 {
			return "", errors.New("bool field must be 1 byte")
		}
		if value[0] > 1 {
			return "", errors.New("bool field must be 0 or 1")
		}
		return strconv.FormatBool(value[0] == 1), nil
	case KindString:
		return strconv.Quote(string(value)), nil
	case KindRawBytes:
		return "0x" + hex.EncodeToString(value), nil
	case KindUUID:
		if len(value) != 16 {
			return "", errors.New("UUID field must be 16 bytes")
		}
		hexValue := hex.EncodeToString(value)
		return fmt.Sprintf("%s-%s-%s-%s-%s", hexValue[0:8], hexValue[8:12], hexValue[12:16], hexValue[16:20], hexValue[20:32]), nil
	default:
		return "", fmt.Errorf("unsupported field kind %d", kind)
	}
}

func formatConsoleBatch(data []byte, color bool) ([]byte, error) {
	var output strings.Builder
	for len(data) > 0 {
		record, size, err := decodeRecord(data)
		if err != nil {
			return nil, err
		}
		output.WriteString(record.timestamp.Format(time.RFC3339Nano))
		output.WriteByte(' ')
		level := record.level.String()
		if color {
			code := "0"
			switch record.level {
			case levelDebug:
				code = "90"
			case levelInfo:
				code = "32"
			case levelWarn:
				code = "33"
			case levelError:
				code = "31"
			case levelFatal:
				code = "1;31"
			}
			output.WriteString("\x1b[")
			output.WriteString(code)
			output.WriteByte('m')
		}
		output.WriteByte('[')
		output.WriteString(level)
		output.WriteByte(']')
		if color {
			output.WriteString("\x1b[0m")
		}
		output.WriteByte(' ')
		output.WriteString(record.message)
		for _, field := range record.fields {
			output.WriteByte(' ')
			output.WriteString(field.key)
			output.WriteByte('=')
			output.WriteString(field.value)
		}
		if record.file != "" {
			output.WriteString(" (" + record.file + ":" + strconv.FormatUint(uint64(record.line), 10) + ")")
		}
		output.WriteByte('\n')
		data = data[size:]
	}
	return []byte(output.String()), nil
}
