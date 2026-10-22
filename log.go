package siglog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Record layout (all integer values use big-endian byte order):
//
//	record header:
//	  [0:4]   total record length, including this header (uint32)
//	  [4:12]  Unix timestamp in nanoseconds (uint64 representation of int64)
//	  [12]    log level (byte)
//
//	payload header, immediately after the record header:
//	  [0:2]   caller file length (uint16)
//	  [2:6]   caller line (uint32)
//	  [6:10]  message length (uint32)
//	  [10:12] field count (uint16)
//
//	payload data:
//	  caller file bytes, message bytes, then field entries
//
//	field entry:
//	  kind (1 byte), key length (uint16), value length (uint32), key bytes, value bytes
//
// Level byte values are DEBUG=1, INFO=2, WARN=3, ERROR=4, FATAL=5.
// The leading total length lets a reader move from one record to the next without
// parsing the message or its fields. Fixed-width numeric field values occupy 8
// bytes (Bool occupies one); string, raw-byte and UUID values are stored as-is.
const (
	recordHeaderSize  = 4 + 8 + 1
	payloadHeaderSize = 2 + 4 + 4 + 2
	fieldHeaderSize   = 1 + 2 + 4
)

var (
	// ErrNoPipeline is returned when Flush is called without a configured pipeline.
	ErrNoPipeline = errors.New("siglog: logger has no pipeline")
	// ErrNoTransport is returned when the configured pipeline has no transport.
	ErrNoTransport = errors.New("siglog: pipeline has no transport")
	// ErrPipelineClosed is returned when trying to add a record after Close begins.
	ErrPipelineClosed = errors.New("siglog: pipeline is closing or closed")
	// ErrRecordTooLarge is returned when a record cannot fit in one shard.
	ErrRecordTooLarge = errors.New("siglog: record exceeds shard writable capacity")
	// ErrBufferFull is retained for explicit buffer-full reporting.
	ErrBufferFull = errors.New("siglog: buffer is full")
	// ErrTransportClosed is returned when writing to a closed transport.
	ErrTransportClosed = errors.New("siglog: transport is closed")
	// ErrMalformedRecord is returned when a binary record cannot be decoded.
	ErrMalformedRecord = errors.New("siglog: malformed binary record")
)

type logLevel byte

const (
	levelDebug logLevel = iota + 1
	levelInfo
	levelWarn
	levelError
	levelFatal
)

func (level logLevel) String() string {
	switch level {
	case levelDebug:
		return "DEBUG"
	case levelInfo:
		return "INFO"
	case levelWarn:
		return "WARN"
	case levelError:
		return "ERROR"
	case levelFatal:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// LoggerStats is a snapshot of logging activity collected while EnableStats(true)
// is active. Statistics are disabled by default to minimize high-load overhead.
// Total and per-level counters count attempted calls, including rate-limited
// or otherwise dropped calls. RateLimited is a subset of Dropped. FlushErrors
// reflects batch-write failures reported by the attached Pipeline.
// ErrorsByType is derived from the "error_type" or "error" string field on
// Error/Fatal calls; a stable value such as "db_timeout" is preferable.
type LoggerStats struct {
	Total                   uint64            `json:"total"`
	Debug                   uint64            `json:"debug"`
	Info                    uint64            `json:"info"`
	Warn                    uint64            `json:"warn"`
	Error                   uint64            `json:"error"`
	Fatal                   uint64            `json:"fatal"`
	Dropped                 uint64            `json:"dropped"`
	RateLimited             uint64            `json:"rate_limited"`
	EncodingErrors          uint64            `json:"encoding_errors"`
	FlushErrors             uint64            `json:"flush_errors"`
	FlushRecordsUnconfirmed uint64            `json:"flush_records_unconfirmed"`
	BufferErrors            uint64            `json:"buffer_errors"`
	ErrorsByType            map[string]uint64 `json:"errors_by_type,omitempty"`
}

type loggerStats struct {
	enabled        atomic.Bool
	total          atomic.Uint64
	debug          atomic.Uint64
	info           atomic.Uint64
	warn           atomic.Uint64
	errorCount     atomic.Uint64
	fatal          atomic.Uint64
	dropped        atomic.Uint64
	rateLimited    atomic.Uint64
	encodingErrors atomic.Uint64
	bufferErrors   atomic.Uint64

	trackErrorTypes atomic.Bool
	errorTypeLimit  atomic.Int64
	errorsByTypeMu  sync.Mutex
	errorsByType    map[string]uint64
}

func newLoggerStats() *loggerStats {
	// Statistics are opt-in: by default the logging hot path does not update
	// shared aggregate counters. EnableStats(true) activates them explicitly.
	return &loggerStats{}
}

func (s *loggerStats) record(level logLevel, contextFields, fields []Field) {
	if !s.enabled.Load() {
		return
	}
	s.total.Add(1)
	switch level {
	case levelDebug:
		s.debug.Add(1)
	case levelInfo:
		s.info.Add(1)
	case levelWarn:
		s.warn.Add(1)
	case levelError:
		s.errorCount.Add(1)
		s.recordErrorType(contextFields, fields)
	case levelFatal:
		s.fatal.Add(1)
		s.recordErrorType(contextFields, fields)
	}
}

func (s *loggerStats) recordErrorType(contextFields, fields []Field) {
	// Detailed string-keyed statistics are opt-in: a mutex and a map update on
	// every error would otherwise become a hot-path contention point.
	if !s.trackErrorTypes.Load() {
		return
	}
	kind := errorTypeGroups(contextFields, fields)
	s.errorsByTypeMu.Lock()
	defer s.errorsByTypeMu.Unlock()
	// Recheck after acquiring the lock: TrackErrorTypes/EnableStats may have
	// disabled detail tracking after the first atomic read above.
	if !s.enabled.Load() || !s.trackErrorTypes.Load() {
		return
	}
	limit := int(s.errorTypeLimit.Load())
	if _, exists := s.errorsByType[kind]; exists {
		s.errorsByType[kind]++
	} else if len(s.errorsByType) < limit {
		s.errorsByType[kind] = 1
	} else {
		s.errorsByType["other"]++
	}
}

func (s *loggerStats) enableErrorTypes(maxKinds int) {
	s.errorsByTypeMu.Lock()
	defer s.errorsByTypeMu.Unlock()
	if maxKinds <= 0 {
		s.trackErrorTypes.Store(false)
		s.errorTypeLimit.Store(0)
		s.errorsByType = nil
		return
	}
	s.errorsByType = make(map[string]uint64)
	s.errorTypeLimit.Store(int64(maxKinds))
	s.trackErrorTypes.Store(true)
}

func (s *loggerStats) markDropped(rateLimited bool) {
	if !s.enabled.Load() {
		return
	}
	s.dropped.Add(1)
	if rateLimited {
		s.rateLimited.Add(1)
	}
}

func (s *loggerStats) markEncodingError() {
	if !s.enabled.Load() {
		return
	}
	s.encodingErrors.Add(1)
	s.dropped.Add(1)
}

func (s *loggerStats) markBufferError() {
	if !s.enabled.Load() {
		return
	}
	s.bufferErrors.Add(1)
	s.dropped.Add(1)
}

func (s *loggerStats) snapshot() LoggerStats {
	var byType map[string]uint64
	if s.trackErrorTypes.Load() {
		s.errorsByTypeMu.Lock()
		byType = make(map[string]uint64, len(s.errorsByType))
		for kind, count := range s.errorsByType {
			byType[kind] = count
		}
		s.errorsByTypeMu.Unlock()
	}

	return LoggerStats{
		Total:          s.total.Load(),
		Debug:          s.debug.Load(),
		Info:           s.info.Load(),
		Warn:           s.warn.Load(),
		Error:          s.errorCount.Load(),
		Fatal:          s.fatal.Load(),
		Dropped:        s.dropped.Load(),
		RateLimited:    s.rateLimited.Load(),
		EncodingErrors: s.encodingErrors.Load(),
		BufferErrors:   s.bufferErrors.Load(),
		ErrorsByType:   byType,
	}
}

func errorTypeGroups(contextFields, fields []Field) string {
	// Per-call fields take precedence over context fields. error_type is
	// preferred over error regardless of where either key appears.
	for _, key := range []string{"error_type", "error"} {
		for i := len(fields) - 1; i >= 0; i-- {
			f := fields[i]
			if f.Key == key && f.Kind == KindString && f.StrVal != "" {
				return f.StrVal
			}
		}
		for i := len(contextFields) - 1; i >= 0; i-- {
			f := contextFields[i]
			if f.Key == key && f.Kind == KindString && f.StrVal != "" {
				return f.StrVal
			}
		}
	}
	return "unspecified"
}

// rateLimiter implements a fixed-window limit shared by a logger and all
// loggers derived from it with With. Configure it before a hot logging path
// when possible; its internal counter is safe for concurrent log calls.
type rateLimiter struct {
	mu       sync.Mutex
	max      int
	interval time.Duration
	window   time.Time
	used     int
}

func (r *rateLimiter) allow(now time.Time) bool {
	if r == nil || r.max <= 0 || r.interval <= 0 {
		return true
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.window.IsZero() || now.Sub(r.window) >= r.interval || now.Before(r.window) {
		r.window = now
		r.used = 0
	}
	if r.used >= r.max {
		return false
	}
	r.used++
	return true
}

type loggerRuntime struct {
	limiter atomic.Pointer[rateLimiter]
	stats   *loggerStats

	flushMu sync.Mutex // serializes explicit Flush calls on With-derived loggers
}

// BufferConfig controls the sharded ring buffer. ShardCapacity is the physical
// byte capacity of each shard, including the one reserved byte that distinguishes
// a full ring from an empty ring. A shard can therefore store ShardCapacity-1 bytes.
type BufferConfig struct {
	ShardCount    int
	ShardCapacity int
}

// BufferProfile selects a tested starting point for the ring-buffer configuration.
// Profiles are defaults to benchmark against your workload, not throughput guarantees.
type BufferProfile uint8

const (
	// ProfileBalanced favors a moderate memory footprint and general workloads.
	ProfileBalanced BufferProfile = iota + 1
	// ProfileLowMemory reduces the memory reserved for ring buffers.
	ProfileLowMemory
	// ProfileHighThroughput reserves more ring-buffer memory and uses more shards.
	ProfileHighThroughput
)

// Option configures a Logger created by New. Options are validated before any
// background goroutine is started.
type Option func(*loggerConfig) error

type loggerConfig struct {
	buffer             BufferConfig
	bufferOptionSet    bool
	profileOptionSet   bool
	flushInterval      time.Duration
	flushIntervalSet   bool
	flushSize          int
	flushSizeSet       bool
	transports         []Transport
	transportOptionSet bool
	retryPolicy        RetryPolicy
	fields             []Field
}

type bufferProfileSettings struct {
	buffer        BufferConfig
	flushInterval time.Duration
	flushSize     int
}

func profileSettings(profile BufferProfile) (bufferProfileSettings, error) {
	switch profile {
	case ProfileBalanced:
		return bufferProfileSettings{
			buffer:        BufferConfig{ShardCount: 8, ShardCapacity: 64 * KB},
			flushInterval: 250 * time.Millisecond,
			flushSize:     64 * KB,
		}, nil
	case ProfileLowMemory:
		return bufferProfileSettings{
			buffer:        BufferConfig{ShardCount: 4, ShardCapacity: 16 * KB},
			flushInterval: 100 * time.Millisecond,
			flushSize:     8 * KB,
		}, nil
	case ProfileHighThroughput:
		return bufferProfileSettings{
			buffer:        BufferConfig{ShardCount: 32, ShardCapacity: 256 * KB},
			flushInterval: 250 * time.Millisecond,
			flushSize:     256 * KB,
		}, nil
	default:
		return bufferProfileSettings{}, fmt.Errorf("siglog: unknown buffer profile %d", profile)
	}
}

func defaultLoggerConfig() loggerConfig {
	settings, _ := profileSettings(ProfileBalanced) // a package-defined constant profile
	return loggerConfig{
		buffer:        settings.buffer,
		flushInterval: settings.flushInterval,
		flushSize:     settings.flushSize,
		retryPolicy:   defaultRetryPolicy(),
	}
}

// WithBufferProfile selects a preset for shard count, per-shard capacity,
// flush interval, and flush threshold. Later WithFlushInterval/WithFlushSize
// options override the profile's corresponding values. It cannot be combined
// with WithBuffer; choose either a profile or explicit shard settings.
func WithBufferProfile(profile BufferProfile) Option {
	return func(cfg *loggerConfig) error {
		if cfg.bufferOptionSet || cfg.profileOptionSet {
			return errors.New("siglog: choose one buffer configuration: profile or explicit BufferConfig")
		}
		settings, err := profileSettings(profile)
		if err != nil {
			return err
		}
		cfg.buffer = settings.buffer
		if !cfg.flushIntervalSet {
			cfg.flushInterval = settings.flushInterval
		}
		if !cfg.flushSizeSet {
			cfg.flushSize = settings.flushSize
		}
		cfg.profileOptionSet = true
		return nil
	}
}

// WithBuffer uses an explicit shard count and physical capacity per shard.
// Flush settings remain independently configurable. If WithFlushSize is not
// supplied, the constructor chooses a conservative threshold based on capacity.
func WithBuffer(config BufferConfig) Option {
	return func(cfg *loggerConfig) error {
		if cfg.profileOptionSet || cfg.bufferOptionSet {
			return errors.New("siglog: choose one buffer configuration: profile or explicit BufferConfig")
		}
		if err := config.validate(); err != nil {
			return err
		}
		cfg.buffer = config
		if !cfg.flushSizeSet {
			cfg.flushSize = recommendedFlushSize(config)
		}
		cfg.bufferOptionSet = true
		return nil
	}
}

// WithFlushInterval sets the period for background flushing. A value of zero
// disables timer-triggered flushing; size-triggered flushes and Close still work.
func WithFlushInterval(interval time.Duration) Option {
	return func(cfg *loggerConfig) error {
		if interval < 0 {
			return errors.New("siglog: flush interval cannot be negative")
		}
		cfg.flushInterval = interval
		cfg.flushIntervalSet = true
		return nil
	}
}

// WithFlushSize requests a flush when buffered bytes reach size. Zero disables
// size-triggered flushing. A negative size is invalid.
func WithFlushSize(size int) Option {
	return func(cfg *loggerConfig) error {
		if size < 0 {
			return errors.New("siglog: flush size cannot be negative")
		}
		cfg.flushSize = size
		cfg.flushSizeSet = true
		return nil
	}
}

// WithTransport adds one or more destinations. Batches are sent to all configured
// transports through MultiTransport. If no transport option is supplied, New uses
// a human-readable console transport.
func WithTransport(transports ...Transport) Option {
	return func(cfg *loggerConfig) error {
		if len(transports) == 0 {
			return errors.New("siglog: WithTransport requires at least one transport")
		}
		for i, transport := range transports {
			if isNilTransport(transport) {
				return fmt.Errorf("siglog: transport %d is nil", i)
			}
		}
		cfg.transports = append(cfg.transports, transports...)
		cfg.transportOptionSet = true
		return nil
	}
}

// WithRetryPolicy configures the bounded retry behavior for transport writes.
// MaxAttempts includes the initial write; a value of one disables retries.
func WithRetryPolicy(policy RetryPolicy) Option {
	return func(cfg *loggerConfig) error {
		if err := policy.validate(); err != nil {
			return err
		}
		cfg.retryPolicy = policy
		return nil
	}
}

// WithFields adds persistent context fields that are included in every record
// written by the new logger. Call Logger.With to derive a contextual logger later.
func WithFields(fields ...Field) Option {
	return func(cfg *loggerConfig) error {
		cfg.fields = append(cfg.fields, fields...)
		return nil
	}
}

func (c BufferConfig) validate() error {
	if c.ShardCount < 1 {
		return errors.New("siglog: shard count must be at least 1")
	}
	if c.ShardCapacity < 2 {
		return errors.New("siglog: shard capacity must be at least 2 bytes")
	}
	maxInt := int(^uint(0) >> 1)
	if c.ShardCount > maxInt/c.ShardCapacity {
		return errors.New("siglog: total buffer capacity overflows int")
	}
	return nil
}

func recommendedFlushSize(config BufferConfig) int {
	usable := config.ShardCount * (config.ShardCapacity - 1)
	threshold := usable / 4
	if threshold < 1 {
		threshold = 1
	}
	if threshold > 64*KB {
		threshold = 64 * KB
	}
	return threshold
}

func (cfg loggerConfig) validate() error {
	if err := cfg.buffer.validate(); err != nil {
		return err
	}
	if cfg.flushInterval < 0 {
		return errors.New("siglog: flush interval cannot be negative")
	}
	if cfg.flushSize < 0 {
		return errors.New("siglog: flush size cannot be negative")
	}
	usable := cfg.buffer.ShardCount * (cfg.buffer.ShardCapacity - 1)
	if cfg.flushSize > usable {
		return fmt.Errorf("siglog: flush size %d exceeds total writable buffer capacity %d", cfg.flushSize, usable)
	}
	return nil
}

func isNilTransport(transport Transport) bool {
	if transport == nil {
		return true
	}
	v := reflect.ValueOf(transport)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// Logger writes binary records through an internal Pipeline. Applications use
// Logger's methods rather than manipulating the pipeline or its ring-buffer shards.
// Context fields are immutable after construction; use With to derive a logger.
type Logger struct {
	fields   []Field
	pipeline *Pipeline

	run atomic.Pointer[loggerRuntime]
}

// New creates a logger using the balanced default profile or the supplied options.
// With no WithTransport option, output is written as readable, color-aware console
// lines. New starts background flushing; always call Close when the logger is done.
func New(opts ...Option) (*Logger, error) {
	cfg := defaultLoggerConfig()
	for i, option := range opts {
		if option == nil {
			return nil, fmt.Errorf("siglog: option %d is nil", i)
		}
		if err := option(&cfg); err != nil {
			return nil, fmt.Errorf("siglog: option %d: %w", i, err)
		}
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	transports := cfg.transports
	if !cfg.transportOptionSet {
		transports = []Transport{NewConsoleTransport()}
	}
	multi := NewTransport(transports...)
	if err := multi.SetRetryPolicy(cfg.retryPolicy); err != nil {
		return nil, err
	}

	pipeline := NewPipeline(
		cfg.buffer.ShardCount,
		cfg.buffer.ShardCapacity,
		cfg.flushInterval,
		multi,
	).SetFlushSize(cfg.flushSize)
	logger := newLogger(pipeline, cfg.fields)
	go pipeline.Start()
	return logger, nil
}

// NewWithPipeline attaches a logger to an existing Pipeline. This is a low-level
// integration hook for tests or advanced setups; the caller is responsible for
// starting the pipeline. Most applications should use New with functional options.
func NewWithPipeline(pipeline *Pipeline, fields ...Field) *Logger {
	return newLogger(pipeline, fields)
}

func newLogger(pipeline *Pipeline, fields []Field) *Logger {
	l := &Logger{
		fields:   append([]Field(nil), fields...),
		pipeline: pipeline,
	}
	l.run.Store(&loggerRuntime{stats: newLoggerStats()})
	return l
}

func (l *Logger) runtimeState() *loggerRuntime {
	if l == nil {
		return nil
	}
	if r := l.run.Load(); r != nil {
		return r
	}
	candidate := &loggerRuntime{stats: newLoggerStats()}
	if l.run.CompareAndSwap(nil, candidate) {
		return candidate
	}
	return l.run.Load()
}

func (l *Logger) log(level logLevel, msg string, fields ...Field) {
	if l == nil {
		return
	}

	r := l.runtimeState()
	contextFields := l.fields
	r.stats.record(level, contextFields, fields)

	now := time.Now()
	limiter := r.limiter.Load()
	if !limiter.allow(now) {
		r.stats.markDropped(true)
		return
	}

	if l.pipeline == nil || l.pipeline.buffer == nil {
		r.stats.markDropped(false)
		return
	}

	// log() is one frame below Info/Infof/etc.; skip=2 resolves the user's callsite.
	callerFile, callerLine := callerLocation(2)
	record, err := encodeRecordGroups(now, level, msg, contextFields, fields, callerFile, callerLine)
	if err != nil {
		r.stats.markEncodingError()
		return
	}
	if err := l.pipeline.Push(record); err != nil {
		r.stats.markBufferError()
	}
}

func encodeRecord(at time.Time, level logLevel, msg string, fields []Field, file string, line int) ([]byte, error) {
	return encodeRecordGroups(at, level, msg, nil, fields, file, line)
}

// encodeRecordGroups merges context and call-specific fields in the encoded
// record without allocating a temporary combined []Field on the hot path.
func encodeRecordGroups(at time.Time, level logLevel, msg string, contextFields, fields []Field, file string, line int) ([]byte, error) {
	if line < 0 {
		line = 0
	}
	if len(file) > int(^uint16(0)) {
		return nil, errors.New("siglog: caller file path is too long")
	}
	if uint64(line) > uint64(^uint32(0)) {
		return nil, errors.New("siglog: caller line is out of range")
	}
	if uint64(len(msg)) > uint64(^uint32(0)) {
		return nil, errors.New("siglog: message is too large")
	}
	fieldCount := len(contextFields) + len(fields)
	if fieldCount < len(contextFields) || fieldCount > int(^uint16(0)) {
		return nil, errors.New("siglog: too many fields in record")
	}

	total := uint64(recordHeaderSize + payloadHeaderSize)
	total += uint64(len(file)) + uint64(len(msg))
	var err error
	total, err = addFieldSizes(total, contextFields)
	if err != nil {
		return nil, err
	}
	total, err = addFieldSizes(total, fields)
	if err != nil {
		return nil, err
	}
	if total > uint64(^uint32(0)) {
		return nil, errors.New("siglog: record exceeds uint32 length limit")
	}

	record := make([]byte, int(total))
	binary.BigEndian.PutUint32(record[0:4], uint32(total))
	binary.BigEndian.PutUint64(record[4:12], uint64(at.UnixNano()))
	record[12] = byte(level)

	off := recordHeaderSize
	binary.BigEndian.PutUint16(record[off:off+2], uint16(len(file)))
	binary.BigEndian.PutUint32(record[off+2:off+6], uint32(line))
	binary.BigEndian.PutUint32(record[off+6:off+10], uint32(len(msg)))
	binary.BigEndian.PutUint16(record[off+10:off+12], uint16(fieldCount))
	off += payloadHeaderSize
	off += copy(record[off:], file)
	off += copy(record[off:], msg)

	for groupIndex := 0; groupIndex < 2; groupIndex++ {
		group := contextFields
		if groupIndex == 1 {
			group = fields
		}
		for _, f := range group {
			valueLen, _ := fieldValueLen(f) // validated in the sizing pass above
			record[off] = byte(f.Kind)
			binary.BigEndian.PutUint16(record[off+1:off+3], uint16(len(f.Key)))
			binary.BigEndian.PutUint32(record[off+3:off+7], uint32(valueLen))
			off += fieldHeaderSize
			off += copy(record[off:], f.Key)
			off += encodeFieldValue(record[off:], f)
		}
	}
	return record, nil
}

func addFieldSizes(total uint64, fields []Field) (uint64, error) {
	for _, f := range fields {
		if len(f.Key) > int(^uint16(0)) {
			return 0, errors.New("siglog: field key is too long")
		}
		valueLen, err := fieldValueLen(f)
		if err != nil {
			return 0, err
		}
		if uint64(valueLen) > uint64(^uint32(0)) {
			return 0, errors.New("siglog: field value is too large")
		}
		total += uint64(fieldHeaderSize) + uint64(len(f.Key)) + uint64(valueLen)
		if total > uint64(^uint32(0)) {
			return 0, errors.New("siglog: record exceeds uint32 length limit")
		}
	}
	return total, nil
}

func fieldValueLen(f Field) (int, error) {
	switch f.Kind {
	case KindInt, KindFloat, KindUint, KindDuration:
		return 8, nil
	case KindBool:
		return 1, nil
	case KindString, KindRawBytes:
		return len(f.StrVal), nil
	case KindUUID:
		if len(f.StrVal) != 16 {
			return 0, errors.New("siglog: UUID field must contain exactly 16 bytes")
		}
		return 16, nil
	default:
		return 0, fmt.Errorf("siglog: unsupported field kind %d", f.Kind)
	}
}

// encodeFieldValue writes directly into the record to keep serialization to
// one allocation per record. Its result is the number of bytes written.
func encodeFieldValue(dst []byte, f Field) int {
	switch f.Kind {
	case KindInt:
		binary.BigEndian.PutUint64(dst[:8], uint64(f.NumVal))
		return 8
	case KindFloat, KindUint:
		binary.BigEndian.PutUint64(dst[:8], uint64(f.NumVal))
		return 8
	case KindDuration:
		binary.BigEndian.PutUint64(dst[:8], uint64(f.NumVal))
		return 8
	case KindBool:
		if f.NumVal != 0 {
			dst[0] = 1
		} else {
			dst[0] = 0
		}
		return 1
	case KindString, KindRawBytes, KindUUID:
		return copy(dst, f.StrVal)
	default:
		return 0 // rejected by fieldValueLen before this function is called
	}
}

// Info logs an informational message.
func (l *Logger) Info(msg string, fields ...Field) { l.log(levelInfo, msg, fields...) }

// Warn logs a warning message.
func (l *Logger) Warn(msg string, fields ...Field) { l.log(levelWarn, msg, fields...) }

// Error logs an error message.
func (l *Logger) Error(msg string, fields ...Field) { l.log(levelError, msg, fields...) }

// Debug logs a debug message.
func (l *Logger) Debug(msg string, fields ...Field) { l.log(levelDebug, msg, fields...) }

// Fatal logs a fatal-level record. It deliberately does not terminate the process.
func (l *Logger) Fatal(msg string, fields ...Field) { l.log(levelFatal, msg, fields...) }

// LogInfo, LogWarn, LogError, LogDebug and LogFatal are explicit-name aliases.
func (l *Logger) LogInfo(msg string, fields ...Field)  { l.log(levelInfo, msg, fields...) }
func (l *Logger) LogWarn(msg string, fields ...Field)  { l.log(levelWarn, msg, fields...) }
func (l *Logger) LogError(msg string, fields ...Field) { l.log(levelError, msg, fields...) }
func (l *Logger) LogDebug(msg string, fields ...Field) { l.log(levelDebug, msg, fields...) }
func (l *Logger) LogFatal(msg string, fields ...Field) { l.log(levelFatal, msg, fields...) }

// Infof logs a formatted informational message.
func (l *Logger) Infof(format string, args ...any) { l.log(levelInfo, fmt.Sprintf(format, args...)) }

// Warnf logs a formatted warning message.
func (l *Logger) Warnf(format string, args ...any) { l.log(levelWarn, fmt.Sprintf(format, args...)) }

// Errorf logs a formatted error message.
func (l *Logger) Errorf(format string, args ...any) { l.log(levelError, fmt.Sprintf(format, args...)) }

// Debugf logs a formatted debug message.
func (l *Logger) Debugf(format string, args ...any) { l.log(levelDebug, fmt.Sprintf(format, args...)) }

// Fatalf logs a formatted fatal-level message without terminating the process.
func (l *Logger) Fatalf(format string, args ...any) { l.log(levelFatal, fmt.Sprintf(format, args...)) }

// With returns a derived logger, preserving context fields and sharing the
// pipeline, statistics and rate-limit configuration with the parent.
func (l *Logger) With(fields ...Field) *Logger {
	if l == nil {
		created, err := New(WithFields(fields...))
		if err != nil {
			return nil
		}
		return created
	}
	combined := make([]Field, 0, len(l.fields)+len(fields))
	combined = append(combined, l.fields...)
	combined = append(combined, fields...)
	derived := &Logger{fields: combined, pipeline: l.pipeline}
	derived.run.Store(l.runtimeState())
	return derived
}

// LimitRate allows at most max messages per fixed interval. A non-positive
// max or interval disables limiting. The configuration is shared with loggers
// derived through With. Configure the limit before concurrent log calls. Fixed
// windows are efficient but can allow a burst at a window boundary.
func (l *Logger) LimitRate(max int, interval time.Duration) *Logger {
	if l == nil {
		return nil
	}
	r := l.runtimeState()
	if max <= 0 || interval <= 0 {
		r.limiter.Store(nil)
	} else {
		r.limiter.Store(&rateLimiter{max: max, interval: interval})
	}
	return l
}

// TrackErrorTypes enables bounded per-error-type counters and aggregate stats.
// maxKinds limits the number of concrete error-type keys; additional keys are
// counted under the extra "other" bucket. Passing maxKinds <= 0 disables
// detailed tracking. Aggregate counters remain enabled until EnableStats(false).
func (l *Logger) TrackErrorTypes(maxKinds int) *Logger {
	if l == nil {
		return nil
	}
	stats := l.runtimeState().stats
	if maxKinds > 0 {
		stats.enabled.Store(true)
	}
	stats.enableErrorTypes(maxKinds)
	return l
}

// EnableStats controls aggregate counters. Statistics are disabled by default
// to minimize hot-path overhead. Enabling them starts counting immediately;
// disabling them preserves already collected values without further updates.
func (l *Logger) EnableStats(enabled bool) *Logger {
	if l == nil {
		return nil
	}
	stats := l.runtimeState().stats
	if !enabled {
		stats.enableErrorTypes(0)
	}
	stats.enabled.Store(enabled)
	return l
}

// Stats returns a point-in-time snapshot of activity counters. Per-error-type
// counts are empty unless TrackErrorTypes was explicitly enabled.
func (l *Logger) Stats() LoggerStats {
	r := l.runtimeState()
	if r == nil {
		return LoggerStats{ErrorsByType: map[string]uint64{}}
	}
	stats := r.stats.snapshot()
	if r.stats.enabled.Load() && l.pipeline != nil {
		stats.FlushRecordsUnconfirmed = l.pipeline.unconfirmedRecords.Load()
		stats.FlushErrors = l.pipeline.FlushErrors()
	}
	return stats
}

// Flush sends the currently buffered batch to configured transports and returns
// the final transport error after the configured bounded retry attempts.
func (l *Logger) Flush() error {
	if l == nil || l.pipeline == nil || l.pipeline.buffer == nil {
		return ErrNoPipeline
	}
	if l.pipeline.transport == nil || len(l.pipeline.transport.transports) == 0 {
		l.pipeline.setLastError(ErrNoTransport)
		return ErrNoTransport
	}
	return l.pipeline.FlushBuffer()
}

// LastError returns the most recent asynchronous flush failure, if any.
func (l *Logger) LastError() error {
	if l == nil || l.pipeline == nil {
		return ErrNoPipeline
	}
	return l.pipeline.LastError()
}

// Close stops background flushing, performs a final flush, and closes transports.
// It is safe to call more than once. Derived loggers from With share this lifecycle.
func (l *Logger) Close() error {
	if l == nil || l.pipeline == nil {
		return ErrNoPipeline
	}
	return l.pipeline.Close()
}

func countRecords(data []byte) uint64 {
	var count uint64
	for len(data) >= 4 {
		size := uint64(binary.BigEndian.Uint32(data[:4]))
		if size < recordHeaderSize || size > uint64(len(data)) {
			break
		}
		count++
		data = data[int(size):]
	}
	return count
}

// String renders a stable, human-readable, non-colored representation of the snapshot.
func (s LoggerStats) String() string { return s.Format(false) }

// Format returns a compact statistics report. Set color=true to request ANSI
// colors; callers should disable it for files, CI logs, and non-terminal output.
func (s LoggerStats) Format(color bool) string {
	keys := make([]string, 0, len(s.ErrorsByType))
	for key := range s.ErrorsByType {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var byType strings.Builder
	for i, key := range keys {
		if i > 0 {
			byType.WriteString(", ")
		}
		fmt.Fprintf(&byType, "%s=%d", key, s.ErrorsByType[key])
	}
	var out strings.Builder
	if color {
		out.WriteString("\x1b[1;36m")
	}
	out.WriteString("siglog statistics")
	if color {
		out.WriteString("\x1b[0m")
	}
	out.WriteByte('\n')
	fmt.Fprintf(&out, "  total: %d\n", s.Total)
	if color {
		out.WriteString("  \x1b[90m")
	}
	fmt.Fprintf(&out, "debug: %d", s.Debug)
	if color {
		out.WriteString("\x1b[0m  \x1b[32m")
	} else {
		out.WriteString("  ")
	}
	fmt.Fprintf(&out, "info: %d", s.Info)
	if color {
		out.WriteString("\x1b[0m  \x1b[33m")
	} else {
		out.WriteString("  ")
	}
	fmt.Fprintf(&out, "warn: %d", s.Warn)
	if color {
		out.WriteString("\x1b[0m  \x1b[31m")
	} else {
		out.WriteString("  ")
	}
	fmt.Fprintf(&out, "error: %d", s.Error)
	if color {
		out.WriteString("\x1b[0m  \x1b[1;31m")
	} else {
		out.WriteString("  ")
	}
	fmt.Fprintf(&out, "fatal: %d", s.Fatal)
	if color {
		out.WriteString("\x1b[0m")
	}
	out.WriteByte('\n')
	fmt.Fprintf(&out, "  dropped: %d  rate-limited: %d  buffer errors: %d\n", s.Dropped, s.RateLimited, s.BufferErrors)
	fmt.Fprintf(&out, "  encoding errors: %d  flush errors: %d  unconfirmed records: %d\n", s.EncodingErrors, s.FlushErrors, s.FlushRecordsUnconfirmed)
	if len(keys) > 0 {
		fmt.Fprintf(&out, "  errors by type: %s\n", byType.String())
	}
	return out.String()
}
