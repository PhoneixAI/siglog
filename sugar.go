package siglog

import (
	"time"
)

const (
	_  = iota             // skip 0
	KB = 1 << (10 * iota) // 1 << 10 (1 Килобайт)KB
	MB = 1 << (10 * iota) // 1 << 20 (1 Мегабайт)MB
	GB = 1 << (10 * iota) // 1 << 30 (1 Гигабайт)GB
)

func NamedErr(key string, err error) Field {
	if err != nil {
		return Field{Key: key, Kind: KindString, StrVal: err.Error()}
	}
	return Field{Key: key, Kind: KindString, StrVal: ""}

}

func Time(key string, t time.Time) Field {
	return Field{Key: key, Kind: KindInt, NumVal: t.UnixNano()}

}

func Duration(key string, d time.Duration) Field {
	return Field{Key: key, Kind: KindDuration, NumVal: int64(d)}
}

func Uint(key string, num uint) Field {
	return Field{Key: key, Kind: KindUint, NumVal: int64(num)}

}
func Uint64(key string, num uint64) Field {
	return Field{Key: key, Kind: KindUint, NumVal: int64(num)}
}

func RawBytes(key string, b []byte) Field {
	return Field{Key: key, Kind: KindRawBytes, StrVal: string(b)}
}

func UUID(key string, id [16]byte) Field {
	// A safe copy is required because the input array is passed by value and
	// cannot back a string after this function returns without unsafe lifetime
	// assumptions. UUID is a convenience field, not the main hot-path primitive.
	return Field{Key: key, Kind: KindUUID, StrVal: string(id[:])}
}
