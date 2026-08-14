package bench

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/api"

	"net/http/httptest"
)

// Engine lifecycle for the head-to-heads. Both engines need the same thing
// from a repaired harness: a way to be put back to empty between ingest
// samples, so sample k measures ingest into an empty store rather than into
// one k-1 samples large.

// vlProc is a VictoriaLogs subprocess with its own storage directory.
type vlProc struct {
	bin  string
	dir  string
	addr string // host:port
	url  string // http://host:port
	cmd  *exec.Cmd
}

// newVL prepares a VL process on addr. It does not start it. Returns nil when
// the binary is not staged, which callers report as a loud skip rather than a
// silent one.
func newVL(t *testing.T, addr string) *vlProc {
	t.Helper()
	bin, err := filepath.Abs("victoria-logs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(bin); err != nil {
		return nil
	}
	return &vlProc{bin: bin, dir: t.TempDir(), addr: addr, url: "http://" + addr}
}

func (p *vlProc) start() error {
	cmd := exec.Command(p.bin,
		"-httpListenAddr="+p.addr,
		"-storageDataPath="+p.dir,
		"-retentionPeriod=10y",
	)
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start victoria-logs: %w", err)
	}
	p.cmd = cmd
	return p.waitReady(60 * time.Second)
}

// stop kills this process by its own PID and reaps it. Never a pattern kill:
// a pattern would take the operator's other victoria-logs down with it, and
// this package has run more than one VL at a time.
func (p *vlProc) stop() {
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	_, _ = p.cmd.Process.Wait() // reap before the port or the dir is reused
	p.cmd = nil
}

// reset returns the engine to empty: stop, wipe the storage directory,
// start again. The port is reused, so start's readiness wait is also what
// keeps the next sample from racing the old listener.
func (p *vlProc) reset() error {
	p.stop()
	if err := os.RemoveAll(p.dir); err != nil {
		return fmt.Errorf("wipe VL storage: %w", err)
	}
	if err := os.MkdirAll(p.dir, 0o755); err != nil {
		return fmt.Errorf("recreate VL storage: %w", err)
	}
	return p.start()
}

func (p *vlProc) waitReady(limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		resp, err := http.Get(p.url + "/insert/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 500 {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond) // bench:untimed -- readiness poll before any measurement
	}
	return fmt.Errorf("victoria-logs on %s not ready inside %v", p.addr, limit)
}

// slProc is the simdlogs side of the same interface: an in-process server
// behind httptest, resettable to an empty store.
type slProc struct {
	t   *testing.T
	srv *api.Server
	ts  *httptest.Server
	url string
}

func newSL(t *testing.T) *slProc {
	t.Helper()
	p := &slProc{t: t}
	if err := p.reset(); err != nil {
		t.Fatal(err)
	}
	return p
}

func (p *slProc) stop() {
	if p.ts != nil {
		p.ts.Close()
		p.ts = nil
	}
	if p.srv != nil {
		p.srv.Close()
		p.srv = nil
	}
}

func (p *slProc) reset() error {
	p.stop()
	srv, err := api.NewServer(p.t.TempDir())
	if err != nil {
		return err
	}
	p.srv = srv
	p.ts = httptest.NewServer(srv.Handler())
	p.url = p.ts.URL
	return nil
}
