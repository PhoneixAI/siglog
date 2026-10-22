package siglog

import (
	"math"
)

type Kind byte

const (
	KindInt Kind = iota
	KindString
	KindFloat
	KindBool
	KindUint
	KindDuration
	KindRawBytes
	KindUUID
)

// TODO: It could take a slice and a map from the user and distribute
// everything itself, producing neat JSON.
// Similarly, iterate through the struct and collect the Field.

// Field represents a key-value pair without using interfaces.
// NumVal is always 64 bits so timestamps, durations, float bit patterns and
// uint64 values are encoded consistently on 32-bit and 64-bit Go targets.
// Use the constructors below rather than constructing numeric fields manually.
type Field struct {
	Key    string
	StrVal string
	NumVal int64
	Kind   Kind
}

func String(key, val string) Field {
	return Field{Key: key, Kind: KindString, StrVal: val}
}

func Int(key string, val int) Field {
	return Field{Key: key, Kind: KindInt, NumVal: int64(val)}
}

func Float(key string, val float64) Field {
	return Field{Key: key, Kind: KindFloat, NumVal: int64(math.Float64bits(val))}
}

func Bool(key string, val bool) Field {
	var num int64
	if val {
		num = 1
	}
	return Field{Key: key, Kind: KindBool, NumVal: num}
}
