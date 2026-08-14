package storage

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// A second process must not be able to open the same directory for writing.
// This is a real subprocess, not a second OpenStore in this one: flock is
// per-process, so an in-process check would prove nothing about the case that
// actually corrupts data -- two binaries pointed at one data directory, each
// allocating group ids from its own counter and overwriting the other's
// files.
func TestSecondProcessCannotOpenStore(t *testing.T) {
	if os.Getenv("SIMDLOGS_LOCK_CHILD") != "" {
		// Child: try to open the directory the parent holds.
		_, err := OpenStore(os.Getenv("SIMDLOGS_LOCK_DIR"))
		if err == nil {
			os.Stderr.WriteString("CHILD_OPENED\n")
			os.Exit(0)
		}
		os.Stderr.WriteString("CHILD_BLOCKED: " + err.Error() + "\n")
		os.Exit(3)
	}

	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Bounded: a child that blocks on the lock instead of failing would
	// otherwise hang forever, and a test binary that never exits is how a
	// sibling repository accumulated ~2038 of them and OOM-killed the machine
	// on 2026-08-14. Sixty seconds is far past the milliseconds this takes.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=TestSecondProcessCannotOpenStore", "-test.v")
	cmd.Env = append(os.Environ(), "SIMDLOGS_LOCK_CHILD=1", "SIMDLOGS_LOCK_DIR="+dir)
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("the child neither opened the store nor failed inside 60s; "+
			"it was killed rather than left running:\n%s", out)
	}
	if err == nil {
		t.Fatalf("the child opened a locked directory:\n%s", out)
	}
	if !strings.Contains(string(out), "CHILD_BLOCKED") {
		t.Fatalf("child failed for the wrong reason:\n%s", out)
	}
	if !strings.Contains(string(out), "locked") {
		t.Fatalf("child's error does not name the lock:\n%s", out)
	}
}

// The lock is released on Close, so a normal restart works.
func TestLockReleasedOnClose(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	s2.Close()
}

// A second OpenStore in the same process is refused too. flock is per
// process, so this is enforced by the store rather than by the kernel; it
// catches a server that opens the same tenant directory twice.
func TestSecondOpenInSameProcessFails(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := OpenStore(dir); err == nil {
		t.Fatal("a second OpenStore on the same directory succeeded")
	} else if !errors.Is(err, ErrLocked) {
		t.Fatalf("err %v, want ErrLocked", err)
	}
}
