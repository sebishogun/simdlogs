// Command simdlogs is the log-database server: ingest and query over HTTP,
// tracking VictoriaLogs' paths where they exist and adding the
// Elasticsearch search surface it lacks.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sebishogun/simd"
	"github.com/sebishogun/simdlogs/internal/api"
	"github.com/sebishogun/simdlogs/internal/config"
)

func main() {
	addr := flag.String("addr", ":9428", "listen address (VL's default port)")
	dir := flag.String("storage", "./simdlogs-data", "storage directory")
	retention := flag.Duration("retention", 0, "drop data older than this (e.g. 720h); 0 disables")
	tierDropPost := flag.Bool("recompact-drop-postings", false, "when recompacting, also drop the per-column inverted index (35% smaller total vs 8% for flate alone, but cold equality queries fall back to a scan -- what VictoriaLogs does for every query)")
	tierAfter := flag.Duration("recompact-after", 0, "re-encode groups older than this with flate dictionaries (~17% smaller, slower value reads on cold data); 0 disables")
	streamFields := flag.String("stream-fields", "", "comma-separated fields that identify a log stream (synthesizes _stream)")
	syslogAddr := flag.String("syslog", "", "also listen for syslog on this UDP/TCP address (e.g. :514)")
	backends := flag.String("select-backends", "", "comma-separated peer node URLs; when set this node is a select router (vmselect role)")
	compact := flag.Bool("compact", false, "compact mode: flate dictionaries for ~15% smaller groups, but 2-10x slower value-reading queries -- for cold archival only, not a queryable store")
	replicas := flag.Int("replicas", 1, "replication factor for -select-backends: backends group into shards of this many replicas")
	maxRows := flag.Int("search.maxRows", 0, "cap on rows a bare (no-pipe) select may return; 0 = the built-in default, -1 = unlimited. Over the cap the query errors (never silently truncates); add a `| limit N` or a stats pipe.")
	maxBody := flag.Int64("http.maxBodyBytes", 0, "maximum request body in bytes; 0 = the built-in default, -1 = unlimited")
	maxQueryDur := flag.Duration("search.maxDuration", 0, "maximum wall time for one query; 0 = the built-in default, -1ns = unlimited")
	maxTenants := flag.Int("tenants.max", 0, "maximum tenants held open; 0 = the built-in default, -1 = unlimited")
	flag.Parse()

	cfg := config.Default()
	cfg.Dir = *dir
	cfg.Compact = *compact
	if *streamFields != "" {
		cfg.StreamFields = strings.Split(*streamFields, ",")
	}
	// A flag left at zero keeps the built-in default; -1 is the explicit
	// opt-out. Zero used to mean unlimited for maxRows, which is why one
	// query could materialize a whole store by default.
	cfg.Limits.MaxQueryRows = *maxRows
	cfg.Limits.MaxBodyBytes = *maxBody
	cfg.Limits.MaxQueryDuration = *maxQueryDur
	cfg.Limits.MaxOpenTenants = *maxTenants

	srv, err := api.NewServerConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}
	if *backends != "" {
		srv.SetBackends(strings.Split(*backends, ","))
		srv.SetReplicas(*replicas)
		log.Printf("select-router mode: %s (replicas=%d)", *backends, *replicas)
	}
	if *compact {
		srv.SetCompact(true)
		log.Print("compact mode: flate dictionaries (smaller, slower queries)")
	}
	if *tierAfter > 0 {
		stopTier := srv.StartTiering(*tierAfter, time.Hour, *tierDropPost)
		defer stopTier()
		log.Printf("tiering: recompacting groups older than %s", *tierAfter)
	}
	if *retention > 0 {
		stop := srv.StartRetention(*retention, time.Hour)
		defer stop()
		log.Printf("retention: dropping data older than %s", *retention)
	}
	if *syslogAddr != "" {
		if _, _, err := srv.ListenSyslog(*syslogAddr); err != nil {
			log.Fatalf("syslog listen %s: %v", *syslogAddr, err)
		}
		log.Printf("syslog listener on %s (UDP+TCP)", *syslogAddr)
	}
	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second, // slowloris protection
	}
	go func() {
		log.Printf("simdlogs on %s, storage %s, simd tier %s", *addr, *dir, simd.Tier())
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	// Graceful shutdown: stop accepting, drain in-flight, then flush+unmap so
	// no buffered rows are lost and no mmap leaks.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Print("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	httpSrv.Shutdown(ctx)
	if err := srv.Close(); err != nil {
		log.Printf("close: %v", err)
	}
}
