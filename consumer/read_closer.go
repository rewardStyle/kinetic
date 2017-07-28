package consumer

import (
	"io"
)

// ReadCloserWrapper wraps a stream (like HTTPResponse.Body) and allows us to
// intercept Read() and Close() calls.  Used to implement a timeout mechanism in
// the aws-sdk-go.
type ReadCloserWrapper struct {
	io.ReadCloser
	OnCloseFn func()
}

// Close is called to close the wrapped stream.  The supplied OnCloseFn is not
// responsible for closing the stream.
func (r *ReadCloserWrapper) Close() error {
	r.OnCloseFn()
	return r.ReadCloser.Close()
}
