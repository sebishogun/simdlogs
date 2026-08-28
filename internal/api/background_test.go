package api

import (
	"github.com/sebishogun/simdlogs/internal/config"
	"runtime"
	"testing"
	"time"
)

// goroutineCount settles the scheduler before counting, so a goroutine that
// is merely on its way out is not mistaken for a leak.
func goroutineCount() int {
	for i := 0; i < 50; i++ {
		runtime.Gosched()
		time.Sleep(2 * time.Millisecond)
	}
	runtime.GC()
	return runtime.NumGoroutine()
}

// Repeated start/stop must not accumulate goroutines. The alert and log-rule
// tickers had no stop at all -- every rule added started a loop that ran for
// the life of the process, still querying stores that had since closed.
func TestBackgroundLoopsDoNotLeak(t *testing.T) {
	base := goroutineCount()

	for i := 0; i < 5; i++ {
		srv, err := NewServer(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		stopR := srv.StartRetention(time.Hour, 10*time.Millisecond)
		stopT := srv.StartTiering(time.Hour, 10*time.Millisecond, false)
		if err := srv.AddAlertRule(config.AlertRule{Name: "a", Query: "*", Op: ">", Threshold: 1,
			Window: config.Duration(time.Hour), Interval: config.Duration(time.Second)}); err != nil {
			t.Fatal(err)
		}
		if err := srv.AddMetricRule(config.MetricRule{Name: "m", Query: "*",
			Window: config.Duration(time.Hour), Interval: config.Duration(time.Second)}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Millisecond) // let the loops tick
		stopR()
		stopT()
		if err := srv.Close(); err != nil {
			t.Fatal(err)
		}
	}

	after := goroutineCount()
	// A small allowance for runtime-internal goroutines; a leak here is five
	// servers' worth of loops, which is far above it.
	if after > base+4 {
		t.Fatalf("goroutines %d -> %d after five start/stop cycles", base, after)
	}
}

// Close must wait for a background pass that is already running. Returning
// while retention is still walking the stores is what let Close unmap under
// an in-flight pass.
func TestCloseWaitsForRunningBackgroundWork(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	finished := make(chan struct{})
	var once bool
	srv.goBackground(5*time.Millisecond, func() {
		if once {
			return
		}
		once = true
		close(started)
		time.Sleep(80 * time.Millisecond)
		close(finished)
	})

	<-started
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("Close returned while a background pass was still running")
	}
}

// After shutdown starts, a new background loop is refused rather than
// outliving the server that owns it.
func TestBackgroundRejectedAfterShutdown(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}

	ran := make(chan struct{}, 1)
	stop := srv.goBackground(time.Millisecond, func() {
		select {
		case ran <- struct{}{}:
		default:
		}
	})
	defer stop()
	time.Sleep(30 * time.Millisecond)
	select {
	case <-ran:
		t.Fatal("a background loop started after shutdown")
	default:
	}
}
