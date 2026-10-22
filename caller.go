package siglog

import (
	"fmt"
	"runtime"
)

// CallInfo describes the source location associated with a log record.
// Its fields are private so the value can be extended without breaking callers.
type CallInfo struct {
	file string
	line int
}

// ReceiveInfo returns the file and line of the caller of ReceiveInfo.
func ReceiveInfo() *CallInfo {
	file, line := callerLocation(1)
	return &CallInfo{file: file, line: line}
}

// receiveInfo is kept as an internal convenience for code that needs CallInfo.
// skip=1 resolves ReceiveInfo's caller; skip=2 from Logger.log resolves the
// caller above the public logging method.
func receiveInfo(skip int) *CallInfo {
	file, line := callerLocation(skip)
	return &CallInfo{file: file, line: line}
}

// callerLocation returns the caller location without allocating a CallInfo.
// Logger.log uses it directly to avoid one allocation per log record.
func callerLocation(skip int) (string, int) {
	if skip < 0 {
		skip = 0
	}
	_, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return "", 0
	}
	return file, line
}

// File returns the source file path, or an empty string when unavailable.
func (c *CallInfo) File() string {
	if c == nil {
		return ""
	}
	return c.file
}

// Line returns the source line, or zero when unavailable.
func (c *CallInfo) Line() int {
	if c == nil {
		return 0
	}
	return c.line
}

// String returns the source location in file:line form.
func (c *CallInfo) String() string {
	if c == nil || c.file == "" || c.line <= 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d", c.file, c.line)
}
