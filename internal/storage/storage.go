// Package storage abstracts the backup repository's object store.
package storage

import (
	"context"
	"errors"
	"io"
)

// ErrNotFound is returned when an object does not exist.
var ErrNotFound = errors.New("object not found")

// ObjectInfo describes a stored object. Keys always use "/" separators.
type ObjectInfo struct {
	Key  string
	Size int64
}

// Storage is a minimal object store. Implementations must make Put atomic:
// a reader never observes a partially written object.
type Storage interface {
	Put(ctx context.Context, key string, r io.Reader) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
	String() string
}
