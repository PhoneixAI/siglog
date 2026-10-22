package siglog

import (
	"errors"
	"fmt"
)

// ProtocolError describes a protocol-level failure returned by a transport.
type ProtocolError struct {
	Protocol string
	Address  string
	Message  string
}

func (e *ProtocolError) Error() string {
	if e == nil {
		return "siglog: protocol error"
	}
	return fmt.Sprintf("siglog: %s transport at %s: %s", e.Protocol, e.Address, e.Message)
}

// TracedError adds context to an existing error while preserving its identity
// for errors.Is and errors.As.
type TracedError struct {
	message string
	next    error
}

func (e *TracedError) Error() string {
	if e == nil {
		return ""
	}
	if e.next == nil {
		return e.message
	}
	return fmt.Sprintf("%s: %v", e.message, e.next)
}

// Unwrap exposes the wrapped error to the standard errors package.
func (e *TracedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.next
}

// Trace adds a message to err. It returns nil when err is nil, so wrapping a
// successful operation cannot accidentally create an error.
func Trace(message string, err error) error {
	if err == nil {
		return nil
	}
	return &TracedError{message: message, next: err}
}

// IsProtocolError reports whether err or one of its wrapped errors is a
// ProtocolError. This helper is optional; errors.As can be used directly.
func IsProtocolError(err error) bool {
	var target *ProtocolError
	return errors.As(err, &target)
}
