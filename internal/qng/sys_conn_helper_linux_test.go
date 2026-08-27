//go:build linux

package quic

import (
	"errors"
	"fmt"
	"net"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

// TestIsGSOError pins the match that the GSO fallback rests on. The kernel's
// refusal reaches the sender wrapped in a *net.OpError, so a matcher that
// looked only at the top-level error would fail open: GSO would stay enabled,
// the one-datagram-at-a-time resend would never run, and the datagrams would
// leave no trace in any counter.
func TestIsGSOError(t *testing.T) {
	syscallErr := os.NewSyscallError("sendmsg", unix.EIO)
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"bare EIO syscall error", syscallErr, true},
		{"wrapped in net.OpError", &net.OpError{Op: "write", Net: "udp", Err: syscallErr}, true},
		{"wrapped twice", fmt.Errorf("send: %w", &net.OpError{Op: "write", Err: syscallErr}), true},
		{"different errno", os.NewSyscallError("sendmsg", unix.EPERM), false},
		{"bare errno, not a syscall error", unix.EIO, false},
		{"unrelated error", errors.New("boom"), false},
	}
	for _, tt := range tests {
		if got := isGSOError(tt.err); got != tt.want {
			t.Errorf("isGSOError(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}
