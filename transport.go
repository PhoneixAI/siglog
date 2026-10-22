package siglog

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

// SyncMode controls whether FileTransport calls fsync after each write.
type SyncMode uint8

const (
	SyncNever SyncMode = iota
	SyncAlways
)

// RetryPolicy controls transport write retries. MaxAttempts includes the first
// attempt. A value of one disables retries.
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

func defaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, InitialBackoff: 5 * time.Millisecond, MaxBackoff: 100 * time.Millisecond}
}

func (p RetryPolicy) validate() error {
	if p.MaxAttempts < 1 {
		return errors.New("siglog: retry MaxAttempts must be at least 1")
	}
	if p.InitialBackoff < 0 || p.MaxBackoff < 0 {
		return errors.New("siglog: retry backoffs cannot be negative")
	}
	if p.MaxBackoff < p.InitialBackoff {
		return errors.New("siglog: retry MaxBackoff must be >= InitialBackoff")
	}
	return nil
}

// MultiTransport fans each binary batch out to every configured destination.
// Delivery is best-effort with bounded retries; if the whole batch is retried
// later after one destination succeeded and another failed, successful sinks may
// receive the same batch more than once.
type MultiTransport struct {
	transports []Transport

	writeMu   sync.Mutex
	closed    bool // protected by writeMu
	closeOnce sync.Once
	closeErr  error

	policyMu sync.RWMutex
	policy   RetryPolicy
}

// MultiTransportError reports failures from one or more destinations. It wraps
// all per-transport errors for errors.Is/errors.As and is retryable only when
// every failed destination classified its final error as retryable.
type MultiTransportError struct {
	err       error
	retryable bool
}

func (e *MultiTransportError) Error() string {
	if e == nil || e.err == nil {
		return "siglog: multiple transport write failure"
	}
	return e.err.Error()
}

func (e *MultiTransportError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

// Retryable is false if any failed destination reported a permanent error.
func (e *MultiTransportError) Retryable() bool { return e != nil && e.retryable }

// NewTransport creates a group that writes each batch to every configured
// transport. A failing transport is retried independently, so another
// transport's successful write is not repeated within the same Write call.
func NewTransport(transports ...Transport) *MultiTransport {
	return &MultiTransport{
		transports: append([]Transport(nil), transports...),
		policy:     defaultRetryPolicy(),
	}
}

// SetRetryPolicy configures retries for subsequent writes. Configure this before
// starting concurrent logging. MaxAttempts=1 disables retries.
func (t *MultiTransport) SetRetryPolicy(policy RetryPolicy) error {
	if t == nil {
		return ErrNoTransport
	}
	if err := policy.validate(); err != nil {
		return err
	}
	t.policyMu.Lock()
	t.policy = policy
	t.policyMu.Unlock()
	return nil
}

func (t *MultiTransport) retryPolicy() RetryPolicy {
	t.policyMu.RLock()
	p := t.policy
	t.policyMu.RUnlock()
	return p
}

// Write sends a batch to every transport, retrying failures according to RetryPolicy.
func (t *MultiTransport) Write(data []byte) error {
	if t == nil {
		return permanentTransportError{err: ErrNoTransport}
	}
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	if t.closed {
		return permanentTransportError{err: ErrTransportClosed}
	}
	if len(t.transports) == 0 {
		return permanentTransportError{err: ErrNoTransport}
	}

	policy := t.retryPolicy()
	var writeErrors []error
	allFailuresRetryable := true
	for i, transport := range t.transports {
		if transport == nil {
			writeErrors = append(writeErrors, fmt.Errorf("siglog: transport[%d] is nil: %w", i, ErrNoTransport))
			allFailuresRetryable = false
			continue
		}
		var err error
		attempts := 0
		backoff := policy.InitialBackoff
		for attempts = 1; attempts <= policy.MaxAttempts; attempts++ {
			err = transport.Write(data)
			if err == nil || !retryable(err) || attempts == policy.MaxAttempts {
				break
			}
			if backoff > 0 {
				time.Sleep(backoff)
			}
			if backoff < policy.MaxBackoff {
				if backoff == 0 {
					backoff = policy.InitialBackoff
				} else if backoff > policy.MaxBackoff/2 {
					backoff = policy.MaxBackoff
				} else {
					backoff *= 2
				}
			}
		}
		if err != nil {
			writeErrors = append(writeErrors, fmt.Errorf("siglog: transport[%d] (%T) write failed after %d attempt(s): %w", i, transport, attempts, err))
			if !retryable(err) {
				allFailuresRetryable = false
			}
		}
	}
	if len(writeErrors) == 0 {
		return nil
	}
	return &MultiTransportError{
		err:       errors.Join(writeErrors...),
		retryable: allFailuresRetryable,
	}
}

// RetryClassifier lets a custom transport error control whether MultiTransport
// retries it. Errors without this interface are retried using the configured
// bounded policy; errors classified as permanent are returned immediately.
type RetryClassifier interface {
	Retryable() bool
}

func retryable(err error) bool {
	if err == nil {
		return false
	}
	var policy RetryClassifier
	if errors.As(err, &policy) {
		return policy.Retryable()
	}
	// Custom transports often return transient network errors. Retry them a
	// bounded number of times unless they explicitly classify an error as final.
	return true
}

type permanentTransportError struct{ err error }

func (e permanentTransportError) Error() string   { return e.err.Error() }
func (e permanentTransportError) Unwrap() error   { return e.err }
func (e permanentTransportError) Retryable() bool { return false }

// Close closes each configured transport once and joins any errors.
func (t *MultiTransport) Close() error {
	if t == nil {
		return nil
	}
	t.closeOnce.Do(func() {
		t.writeMu.Lock()
		defer t.writeMu.Unlock()
		t.closed = true
		var closeErrors []error
		for i, transport := range t.transports {
			if transport == nil {
				continue
			}
			if err := transport.Close(); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("siglog: transport[%d] (%T) close failed: %w", i, transport, err))
			}
		}
		t.closeErr = errors.Join(closeErrors...)
	})
	return t.closeErr
}

// Transport is a destination for an encoded batch of binary log records.
type Transport interface {
	Write(data []byte) error
	Close() error
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n < 0 || n > len(data) {
			return io.ErrShortWrite
		}
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// File Transport

type FileTransport struct {
	mu       sync.Mutex
	file     *os.File
	mode     SyncMode
	closed   bool
	closeErr error
}

// NewFileTransport writes binary batches to file. SyncAlways calls Sync after each batch.
func NewFileTransport(file *os.File, mode SyncMode) *FileTransport {
	return &FileTransport{file: file, mode: mode}
}

// Write writes the complete batch and optionally syncs it to stable storage.
func (t *FileTransport) Write(data []byte) error {
	if t == nil {
		return permanentTransportError{err: errors.New("siglog: file transport is nil")}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return permanentTransportError{err: ErrTransportClosed}
	}
	if t.file == nil {
		return permanentTransportError{err: errors.New("siglog: file transport has no file")}
	}
	// File writes can be partial before an error. Retrying the whole batch could
	// duplicate a prefix, so errors from this non-framed sink are permanent and
	// poison the sink; future writes must not append after an uncertain prefix.
	if err := writeAll(t.file, data); err != nil {
		writeErr := fmt.Errorf("siglog: write file: %w", err)
		t.closed = true
		t.closeErr = t.file.Close()
		if t.closeErr != nil {
			writeErr = errors.Join(writeErr, fmt.Errorf("siglog: close failed file after write error: %w", t.closeErr))
		}
		return permanentTransportError{err: writeErr}
	}
	if t.mode == SyncAlways {
		// A Sync error means the bytes may already be present; don't replay them.
		if err := t.file.Sync(); err != nil {
			syncErr := fmt.Errorf("siglog: sync file: %w", err)
			t.closed = true
			t.closeErr = t.file.Close()
			if t.closeErr != nil {
				syncErr = errors.Join(syncErr, fmt.Errorf("siglog: close file after sync error: %w", t.closeErr))
			}
			return permanentTransportError{err: syncErr}
		}
	}
	return nil
}

// Close closes the underlying file once. The transport takes ownership of file.
func (t *FileTransport) Close() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return t.closeErr
	}
	t.closed = true
	if t.file != nil {
		t.closeErr = t.file.Close()
	}
	return t.closeErr
}

// ConsoleTransport decodes binary records and writes human-readable lines. ANSI
// colors are enabled automatically only when the target writer is a terminal.
type ConsoleTransport struct {
	mu       sync.Mutex
	writer   io.Writer
	color    bool
	broken   bool
	writeErr error
}

// NewConsoleTransport creates a color-aware, human-readable stdout transport.
// Use WithWriter to redirect output and WithColor to explicitly control colors.
func NewConsoleTransport() *ConsoleTransport {
	writer := io.Writer(os.Stdout)
	return &ConsoleTransport{writer: writer, color: isTerminal(writer)}
}

// WithColor explicitly enables or disables ANSI colors.
func (t *ConsoleTransport) WithColor(enabled bool) *ConsoleTransport {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	t.color = enabled
	t.mu.Unlock()
	return t
}

// WithWriter directs console output to a custom writer, useful for tests and
// embedding. Color detection is not repeated; call WithColor to choose it.
func (t *ConsoleTransport) WithWriter(writer io.Writer) *ConsoleTransport {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	if writer != nil {
		t.writer = writer
		t.broken = false
		t.writeErr = nil
	}
	t.mu.Unlock()
	return t
}

func isTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok || f == nil {
		return false
	}
	info, err := f.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0 && os.Getenv("NO_COLOR") == ""
}

func (t *ConsoleTransport) Write(data []byte) error {
	if t == nil {
		return permanentTransportError{err: errors.New("siglog: nil console transport")}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.broken {
		return permanentTransportError{err: fmt.Errorf("siglog: console transport is unusable after a partial write: %w", t.writeErr)}
	}
	writer := t.writer
	if writer == nil {
		writer = os.Stdout
	}
	formatted, err := formatConsoleBatch(data, t.color)
	if err != nil {
		return err // malformed binary records are classified as permanent
	}
	if err := writeAll(writer, formatted); err != nil {
		// A writer may have emitted a prefix. Replaying a formatted batch would
		// duplicate that prefix, so don't retry it automatically.
		t.broken = true
		t.writeErr = fmt.Errorf("siglog: write console output: %w", err)
		return permanentTransportError{err: t.writeErr}
	}
	return nil
}

// Close is a no-op because ConsoleTransport does not own stdout or custom writers.
func (t *ConsoleTransport) Close() error { return nil }

// TCP Transport

const (
	defaultDialTimeout  = 5 * time.Second
	defaultWriteTimeout = 2 * time.Second
)

// TCPTransport writes batches over a persistent TCP connection. Writes have a
// finite default deadline so Pipeline.Close cannot wait forever on a stalled peer.
type TCPTransport struct {
	mu           sync.Mutex
	conn         net.Conn
	writeTimeout time.Duration
	closed       bool
	closeErr     error
}

// NewTCPTransport connects to address, limiting connection establishment to five seconds.
func NewTCPTransport(address string) (*TCPTransport, error) {
	conn, err := net.DialTimeout("tcp", address, defaultDialTimeout)
	if err != nil {
		return nil, fmt.Errorf("siglog: dial TCP %q: %w", address, err)
	}
	return &TCPTransport{conn: conn, writeTimeout: defaultWriteTimeout}, nil
}

// WithWriteTimeout sets the deadline for each TCP write. A non-positive value
// disables the deadline; configure it before concurrent use.
func (t *TCPTransport) WithWriteTimeout(timeout time.Duration) *TCPTransport {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	if !t.closed {
		t.writeTimeout = timeout
	}
	t.mu.Unlock()
	return t
}

// Write sends all bytes over TCP, handling short writes until complete or error.
func (t *TCPTransport) Write(data []byte) error {
	if t == nil {
		return permanentTransportError{err: errors.New("siglog: nil TCP transport")}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return permanentTransportError{err: ErrTransportClosed}
	}
	if t.conn == nil {
		return permanentTransportError{err: errors.New("siglog: tcp transport has no connection")}
	}
	if t.writeTimeout > 0 {
		if err := t.conn.SetWriteDeadline(time.Now().Add(t.writeTimeout)); err != nil {
			return permanentTransportError{err: fmt.Errorf("siglog: set TCP write deadline: %w", err)}
		}
		defer t.conn.SetWriteDeadline(time.Time{})
	}
	if err := writeAll(t.conn, data); err != nil {
		// The peer may already have received a prefix. Retrying the complete batch
		// on this stream would corrupt framing, so poison/close this connection and
		// surface the original error. Recreate TCPTransport to reconnect.
		t.closed = true
		t.closeErr = t.conn.Close()
		return permanentTransportError{err: fmt.Errorf("siglog: write TCP batch (connection closed to protect framing): %w", err)}
	}
	return nil
}

// Close closes the TCP connection once and returns the same close error thereafter.
func (t *TCPTransport) Close() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return t.closeErr
	}
	t.closed = true
	if t.conn != nil {
		t.closeErr = t.conn.Close()
	}
	return t.closeErr
}

// UDP Transport

// UDPTransport sends each batch as one datagram. The entire batch must fit in a
// single UDP datagram; callers should use small FlushSize/record sizes.
type UDPTransport struct {
	mu           sync.Mutex
	conn         net.Conn
	writeTimeout time.Duration
	closed       bool
	closeErr     error
}

// NewUDPTransport connects a UDP socket to address with a bounded dial timeout.
func NewUDPTransport(address string) (*UDPTransport, error) {
	conn, err := net.DialTimeout("udp", address, defaultDialTimeout)
	if err != nil {
		return nil, fmt.Errorf("siglog: dial UDP %q: %w", address, err)
	}
	return &UDPTransport{conn: conn, writeTimeout: defaultWriteTimeout}, nil
}

// WithWriteTimeout sets the deadline for each UDP datagram write. A non-positive
// value disables the deadline; configure it before concurrent use.
func (t *UDPTransport) WithWriteTimeout(timeout time.Duration) *UDPTransport {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	if !t.closed {
		t.writeTimeout = timeout
	}
	t.mu.Unlock()
	return t
}

// Write sends exactly one UDP datagram; it never splits a short write into multiple packets.
func (t *UDPTransport) Write(data []byte) error {
	if t == nil {
		return permanentTransportError{err: errors.New("siglog: nil UDP transport")}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return permanentTransportError{err: ErrTransportClosed}
	}
	if t.conn == nil {
		return permanentTransportError{err: errors.New("siglog: udp transport has no connection")}
	}
	if t.writeTimeout > 0 {
		if err := t.conn.SetWriteDeadline(time.Now().Add(t.writeTimeout)); err != nil {
			return permanentTransportError{err: fmt.Errorf("siglog: set UDP write deadline: %w", err)}
		}
		defer t.conn.SetWriteDeadline(time.Time{})
	}
	n, err := t.conn.Write(data)
	if err != nil {
		return fmt.Errorf("siglog: write UDP datagram: %w", err)
	}
	if n != len(data) {
		return permanentTransportError{err: io.ErrShortWrite}
	}
	return nil
}

// Close closes the UDP connection once and returns the same close error thereafter.
func (t *UDPTransport) Close() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return t.closeErr
	}
	t.closed = true
	if t.conn != nil {
		t.closeErr = t.conn.Close()
	}
	return t.closeErr
}

// HTTP Transport

type HTTPTransport struct {
	host    string
	Client  *http.Client
	Headers http.Header
}

// HTTPStatusError represents a non-2xx response. 429 and 5xx responses are
// retryable; other status codes are treated as permanent client-side failures.
type HTTPStatusError struct {
	StatusCode int
	Status     string
	Body       string
}

func (e *HTTPStatusError) Error() string {
	if e == nil {
		return "siglog: http status error"
	}
	if e.Body == "" {
		return fmt.Sprintf("siglog: http transport returned %s", e.Status)
	}
	return fmt.Sprintf("siglog: http transport returned %s: %s", e.Status, e.Body)
}

func (e *HTTPStatusError) Retryable() bool {
	return e != nil && (e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500)
}

// NewHTTPTransport creates an HTTP POST transport. When client is nil, a client
// with a five-second timeout is used. A supplied client retains its own timeout
// settings, so callers should configure one to keep shutdown bounded.
func NewHTTPTransport(host string, client *http.Client) *HTTPTransport {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &HTTPTransport{host: host, Client: client, Headers: make(http.Header)}
}

// Write posts the complete binary batch and rejects non-2xx HTTP status codes.
func (t *HTTPTransport) Write(data []byte) error {
	if t == nil {
		return errors.New("siglog: nil http transport")
	}
	client := t.Client
	if client == nil {
		client = http.DefaultClient
	}
	parsedURL, err := url.Parse(t.host)
	if err != nil {
		return permanentTransportError{err: fmt.Errorf("siglog: parse http URL: %w", err)}
	}
	if (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
		return permanentTransportError{err: fmt.Errorf("siglog: invalid HTTP URL %q: absolute http(s) URL with host required", t.host)}
	}
	req, err := http.NewRequest(http.MethodPost, parsedURL.String(), bytes.NewReader(data))
	if err != nil {
		return permanentTransportError{err: fmt.Errorf("siglog: create http request: %w", err)}
	}
	req.Header = t.Headers.Clone()
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/octet-stream")
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("siglog: send http batch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &HTTPStatusError{StatusCode: resp.StatusCode, Status: resp.Status, Body: string(bytes.TrimSpace(body))}
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	return nil
}

// Close is a no-op; net/http manages HTTP connections through its Client.
func (t *HTTPTransport) Close() error { return nil }
