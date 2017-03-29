package utils

import (
	"io"
)

type TimeoutReadCloser struct {
	io.ReadCloser
	OnReadFn  func(io.ReadCloser, []byte) (int, error)
	OnCloseFn func()
}

func (r *TimeoutReadCloser) Read(b []byte) (int, error) {
	return r.OnReadFn(r.ReadCloser, b)
}

func (r *TimeoutReadCloser) Close() error {
	r.OnCloseFn()
	return r.ReadCloser.Close()
}
