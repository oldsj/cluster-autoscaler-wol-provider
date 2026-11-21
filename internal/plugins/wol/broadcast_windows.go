//go:build windows

package wol

import (
	"syscall"
)

// broadcastControl sets SO_BROADCAST, which sending to a broadcast address needs.
func broadcastControl(_, _ string, c syscall.RawConn) error {
	var sockErr error
	err := c.Control(func(fd uintptr) {
		sockErr = syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_BROADCAST, 1)
	})
	if err != nil {
		return err
	}
	return sockErr
}
