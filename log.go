package rafttest

import (
	"io"
	"testing"
)

// TestingWriter returns an io.Writer that forwards the stream it receives to
// the Logf function of the given testing instance.
func TestingWriter(t testing.TB) io.Writer {
	return &testingWriter{t: t}
}

// Implement io.Writer and forward what it receives to a
// t.Testing logger.
type testingWriter struct {
	t testing.TB
}

func (w *testingWriter) Write(p []byte) (n int, err error) {
	w.t.Logf(string(p))
	return len(p), nil
}

// For compatibility with Go <1.9
type testingHelper interface {
	Helper()
}
