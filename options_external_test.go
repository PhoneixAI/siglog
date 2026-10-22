package siglog_test

import (
	"testing"

	"siglog"
)

type externalSink struct {
	writes int
}

func (s *externalSink) Write(data []byte) error {
	s.writes++
	return nil
}

func (s *externalSink) Close() error { return nil }

func TestPublicOptionsAPI(t *testing.T) {
	sink := &externalSink{}
	logger, err := siglog.New(
		siglog.WithBufferProfile(siglog.ProfileLowMemory),
		siglog.WithFields(siglog.String("service", "external-test")),
		siglog.WithTransport(sink),
		siglog.WithFlushInterval(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("public API works", siglog.Int("answer", 42))
	if err := logger.Flush(); err != nil {
		t.Fatal(err)
	}
	if sink.writes != 1 {
		t.Fatalf("expected one transport write, got %d", sink.writes)
	}
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}
}
