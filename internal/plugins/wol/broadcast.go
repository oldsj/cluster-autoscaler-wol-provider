//go:build unix

package wol

import (
	"syscall"
)

// broadcastControl sets SO_BROADCAST, which sending to a broadcast address needs.
func broadcastControl(_, _ string, c syscall.RawConn) error {
	var sockErr error
	err := c.Control(func(fd uintptr) {
		sockErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_BROADCAST, 1)
	})
	if err != nil {
		return err
	}
	return sockErr
}
