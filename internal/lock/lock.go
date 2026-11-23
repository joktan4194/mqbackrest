// Package lock provides host-local advisory locks so two mqbackrest processes
// never back up or expire the same stanza concurrently.
package lock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// Acquire takes an exclusive, non-blocking lock. Call the returned func to release.
func Acquire(dir, stanza, name string) (func(), error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	p := filepath.Join(dir, fmt.Sprintf("mqbackrest-%s-%s.lock", stanza, name))
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("another mqbackrest %s is already running for stanza %q (lock %s)", name, stanza, p)
		}
		return nil, err
	}
	_ = f.Truncate(0)
	_, _ = fmt.Fprintf(f, "%d\n", os.Getpid())
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}
