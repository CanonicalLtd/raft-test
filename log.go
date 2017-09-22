package rafttest

import "testing"

// Implement io.Writer and forward what it receives to a
// t.Testing logger.
type testingWriter struct {
	t *testing.T
}

func (w *testingWriter) Write(p []byte) (n int, err error) {
	w.t.Logf(string(p))
	return len(p), nil
}
