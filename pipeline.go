package siglog

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Pipeline connects the sharded ring buffer to one or more transports.
// Start periodically flushes it; Close stops Start, flushes remaining buffered bytes,
// and closes the transport group. A non-positive timeout disables periodic flushes.
const (
	pipelineClosingBit    uint64 = 1 << 63
	pipelineOperationMask uint64 = ^pipelineClosingBit
)

type Pipeline struct {
	timeout   time.Duration
	buffer    *Buffer
	transport *MultiTransport

	flushSize      atomic.Int64
	flushSignal    chan struct{}
	operationState atomic.Uint64 // high bit closes the gate; low bits count active operations
	opsMu          sync.Mutex
	opsCond        *sync.Cond

	flushMu            sync.Mutex
	mu                 sync.Mutex
	started            bool
	closing            bool
	closed             bool
	stopCh             chan struct{}
	doneCh             chan struct{}
	stopOnce           sync.Once
	closeErr           error
	lastErr            error
	flushErrors        atomic.Uint64
	unconfirmedRecords atomic.Uint64 // records in batches that exhausted their retry policy
}

// NewPipeline creates a pipeline. The shard count and capacities are normalized
// by NewBuffer. A timeout <= 0 disables timer-based flushes; Flush or Close still
// drains buffered records.
func NewPipeline(count, capacity int, timeout time.Duration, transport *MultiTransport) *Pipeline {
	p := &Pipeline{
		buffer:      NewBuffer(count, capacity),
		timeout:     timeout,
		transport:   transport,
		flushSignal: make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
	}
	p.opsCond = sync.NewCond(&p.opsMu)
	return p
}

// SetFlushSize requests a flush once the approximate buffered byte count reaches
// size. A non-positive size disables size-triggered flushes. Call before Start
// for deterministic configuration.
func (p *Pipeline) SetFlushSize(size int) *Pipeline {
	if p != nil {
		p.flushSize.Store(int64(size))
		if p.buffer != nil {
			p.buffer.SetTrackLen(size > 0)
		}
	}
	return p
}

// beginOperation uses an atomic CAS-based reference count on the producer hot
// path. The high bit closes the gate; the remaining bits count operations
// accepted before Close. CAS keeps the close gate and count update indivisible.
func (p *Pipeline) beginOperation() error {
	for {
		state := p.operationState.Load()
		if state&pipelineClosingBit != 0 {
			return ErrPipelineClosed
		}
		if state&pipelineOperationMask == pipelineOperationMask {
			return ErrBufferFull // practically unreachable; avoids counter overflow
		}
		if p.operationState.CompareAndSwap(state, state+1) {
			return nil
		}
	}
}

func (p *Pipeline) endOperation() {
	state := p.operationState.Add(^uint64(0)) // decrement active operation count
	if state&pipelineClosingBit != 0 && state&pipelineOperationMask == 0 {
		p.opsMu.Lock()
		p.opsCond.Broadcast()
		p.opsMu.Unlock()
	}
}

func (p *Pipeline) markClosing() {
	for {
		state := p.operationState.Load()
		if state&pipelineClosingBit != 0 || p.operationState.CompareAndSwap(state, state|pipelineClosingBit) {
			return
		}
	}
}

func (p *Pipeline) waitForOperations() {
	p.opsMu.Lock()
	for p.operationState.Load()&pipelineOperationMask != 0 {
		p.opsCond.Wait()
	}
	p.opsMu.Unlock()
}

// Push adds one complete encoded record to the ring buffer.
func (p *Pipeline) Push(data []byte) error {
	if p == nil || p.buffer == nil {
		return ErrNoPipeline
	}
	if err := p.beginOperation(); err != nil {
		return err
	}
	defer p.endOperation()
	if len(data) > p.buffer.shardCap-1 {
		return fmt.Errorf("%w: record is %d bytes, per-shard writable capacity is %d", ErrRecordTooLarge, len(data), p.buffer.shardCap-1)
	}
	if !p.buffer.push(data) {
		return ErrBufferFull
	}
	if threshold := p.flushSize.Load(); threshold > 0 && p.buffer.Len() >= int(threshold) {
		select {
		case p.flushSignal <- struct{}{}:
		default:
		}
	}
	return nil
}

// Flush writes an already assembled batch. It returns an error after the
// MultiTransport retry policy is exhausted; the supplied batch is not retained.
// Use FlushBuffer or Logger.Flush to drain the pipeline's ring buffer.
func (p *Pipeline) Flush(flushing []byte) error {
	if p == nil {
		return ErrNoPipeline
	}
	if err := p.beginOperation(); err != nil {
		return err
	}
	defer p.endOperation()
	p.flushMu.Lock()
	defer p.flushMu.Unlock()
	err := p.writeBatchLocked(flushing)
	if err != nil {
		p.unconfirmedRecords.Add(countRecords(flushing))
	}
	return err
}

// FlushBuffer drains records from Buffer and writes them to the configured
// transport. On failure, it returns the final transport error and discards that
// unconfirmed batch rather than retaining memory or retrying it automatically.
// It serializes periodic, size-triggered, and explicit flush operations.
func (p *Pipeline) FlushBuffer() error {
	if p == nil || p.buffer == nil {
		return ErrNoPipeline
	}
	if err := p.beginOperation(); err != nil {
		return err
	}
	defer p.endOperation()
	return p.flushBuffer()
}

// flushBuffer is the lifecycle-internal variant. The shutdown owner may call
// it after closing the operation gate and waiting for accepted operations.
func (p *Pipeline) flushBuffer() error {
	p.flushMu.Lock()
	defer p.flushMu.Unlock()

	data := p.buffer.readBytes()
	if len(data) == 0 {
		return nil
	}

	if err := p.writeBatchLocked(data); err != nil {
		// MultiTransport has already made its bounded number of attempts. Do not
		// retain the failed batch or retry it in the background: failures must be
		// surfaced to the caller/LastError, and memory usage remains bounded.
		p.unconfirmedRecords.Add(countRecords(data))
		return err
	}
	return nil
}

func (p *Pipeline) writeBatchLocked(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if p.transport == nil || len(p.transport.transports) == 0 {
		err := permanentTransportError{err: ErrNoTransport}
		p.setLastError(err)
		return err
	}
	err := p.transport.Write(data)
	if err != nil {
		p.setLastError(err)
	}
	return err
}

func (p *Pipeline) setLastError(err error) {
	p.setLastErrorValue(err)
	if err != nil {
		p.flushErrors.Add(1)
	}
}

func (p *Pipeline) setLastErrorValue(err error) {
	p.mu.Lock()
	p.lastErr = err
	p.mu.Unlock()
}

// LastError returns the most recent flush failure. Successful later flushes do
// not clear it, because a failed batch has already been reported as unconfirmed.
func (p *Pipeline) LastError() error {
	if p == nil {
		return ErrNoPipeline
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastErr
}

// FlushErrors returns the cumulative number of batch-write failures after the
// configured MultiTransport retry policy was exhausted.
func (p *Pipeline) FlushErrors() uint64 {
	if p == nil {
		return 0
	}
	return p.flushErrors.Load()
}

// Start runs the periodic/size-triggered flusher until Close is called. Calling
// Start more than once has no effect. Each failed batch is attempted according
// to MultiTransport's bounded RetryPolicy and then reported through LastError,
// statistics, and Close; Pipeline does not retry that batch in the background.
func (p *Pipeline) Start() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.started || p.closing || p.closed {
		p.mu.Unlock()
		return
	}
	p.started = true
	p.mu.Unlock()

	var ticker *time.Ticker
	var tickerC <-chan time.Time
	if p.timeout > 0 {
		ticker = time.NewTicker(p.timeout)
		tickerC = ticker.C
	}
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
		p.finishFromStart()
	}()

	for {
		select {
		case <-p.stopCh:
			return
		case <-tickerC:
			_ = p.flushBuffer() // errors remain available through LastError/Stats/Close
		case <-p.flushSignal:
			_ = p.flushBuffer() // one bounded MultiTransport attempt sequence per batch
		}
	}
}

func (p *Pipeline) finishFromStart() {
	// The state gate prevents new operations; wait for accepted pushes/flushes.
	p.waitForOperations()
	flushErr := p.flushBuffer()
	if flushErr == nil {
		flushErr = p.LastError()
	}
	closeErr := p.closeTransport()

	p.mu.Lock()
	p.closeErr = errors.Join(flushErr, closeErr)
	p.closed = true
	p.closing = false
	p.operationState.Store(pipelineClosingBit)
	close(p.doneCh)
	p.mu.Unlock()
}

func (p *Pipeline) closeTransport() error {
	if p.transport == nil || len(p.transport.transports) == 0 {
		return nil
	}
	return p.transport.Close()
}

// Close is idempotent. It prevents new pushes, waits for Start if running,
// flushes the records still in the ring buffer, closes transports, and returns
// any final flush/close error. If an earlier batch failed, Close also reports
// the most recent flush failure so asynchronous errors are not silently lost.
func (p *Pipeline) Close() error {
	if p == nil {
		return ErrNoPipeline
	}

	p.mu.Lock()
	if p.closed || p.closing {
		done := p.doneCh
		p.mu.Unlock()
		<-done
		p.mu.Lock()
		err := p.closeErr
		p.mu.Unlock()
		return err
	}
	p.closing = true
	p.markClosing()
	if p.started {
		p.stopOnce.Do(func() { close(p.stopCh) })
		done := p.doneCh
		p.mu.Unlock()
		<-done
		p.mu.Lock()
		err := p.closeErr
		p.mu.Unlock()
		return err
	}
	p.mu.Unlock()

	// No Start goroutine owns shutdown; perform it synchronously.
	p.waitForOperations()
	flushErr := p.flushBuffer()
	if flushErr == nil {
		flushErr = p.LastError()
	}
	closeErr := p.closeTransport()

	p.mu.Lock()
	p.closeErr = errors.Join(flushErr, closeErr)
	p.closed = true
	p.closing = false
	p.operationState.Store(pipelineClosingBit)
	close(p.doneCh)
	err := p.closeErr
	p.mu.Unlock()
	return err
}
