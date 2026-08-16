package bench

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// liveChildren returns pids of this process's children NOT in state Z.
func liveChildren(t *testing.T) []int {
	t.Helper()
	ents, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatalf("read /proc: %v", err)
	}
	me := os.Getpid()
	var out []int
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		s := string(b)
		i := strings.LastIndexByte(s, ')')
		if i < 0 || i+2 >= len(s) {
			continue
		}
		f := strings.Fields(s[i+2:])
		if len(f) < 2 {
			continue
		}
		if f[0] != "Z" && f[1] == strconv.Itoa(me) {
			out = append(out, pid)
		}
	}
	return out
}

// REVIEWER C: startVL's t.Cleanup(p.stop) is registered AFTER the t.Fatalf that
// fires when start() returns an error. start() returns an error when waitReady
// times out -- by which point cmd.Start() has ALREADY succeeded and the child is
// running. So a VictoriaLogs that starts but never becomes ready is never
// killed, never reaped, and outlives the test binary holding its port.
//
// realistic_test.go:138-141 and scalevl_test.go:144-147 have the same order,
// and they additionally `defer os.RemoveAll(vlDir)` -- deleting the storage
// directory out from under a live server.
func TestReviewCFailedStartLeaksTheChild(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("/proc")
	}
	if newVL(t, "127.0.0.1:19496") == nil {
		skipNoVL(t, "the leaked-child probe")
	}
	before := len(liveChildren(t))

	// The subtest is startVL's exact body. Its Fatalf ends the subtest, and its
	// cleanups run -- which is the point: p.stop was never registered.
	t.Run("inner", func(t *testing.T) {
		p := newVL(t, "127.0.0.1:19496")
		if p == nil {
			skipNoVL(t, "probe")
		}
		if err := p.start(); err != nil {
			t.Logf("start reported: %v", err)
			t.Fatalf("startVL does exactly this, and only registers "+
				"t.Cleanup(p.stop) on the line AFTER: %v", err)
		}
		t.Cleanup(p.stop)
	})

	after := liveChildren(t)
	t.Logf("live children before=%d after=%d pids=%v", before, len(after), after)
	if len(after) > before {
		for _, pid := range after {
			b, _ := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/comm")
			t.Logf("  LEAKED pid %d comm=%q", pid, strings.TrimSpace(string(b)))
		}
		t.Errorf("a VictoriaLogs that started but never became ready is still "+
			"running: %d live children, was %d. It holds its port and its "+
			"storage directory (which the caller's defer os.RemoveAll then "+
			"deletes underneath it) until something outside this process kills it",
			len(after), before)
	}
}
