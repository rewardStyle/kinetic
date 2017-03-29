package utils

import (
	"io"
)

// ReadCloserWrapper wraps a stream (like HTTPResponse.Body) and allows us to
// intercept Read() and Close() calls.  Used to implement a timeout mechanism in
// the aws-sdk-go.
type ReadCloserWrapper struct {
	io.ReadCloser
	OnReadFn  func(io.ReadCloser, []byte) (int, error)
	OnCloseFn func()
}

// Read is called to read the wrapped stream.  The supplied OnReadFn is
// responsible for making the read to the wrapped stream.
func (r *ReadCloserWrapper) Read(b []byte) (int, error) {
	return r.OnReadFn(r.ReadCloser, b)
}

// Close is called to close the wrapped stream.  The supplied OnCloseFn is not
// responsible for closing the stream.
func (r *ReadCloserWrapper) Close() error {
	r.OnCloseFn()
	return r.ReadCloser.Close()
}
