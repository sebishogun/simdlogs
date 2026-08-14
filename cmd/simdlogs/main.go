// Command simdlogs is the log-database server: ingest and query over HTTP,
// tracking VictoriaLogs' paths where they exist and adding the
// Elasticsearch search surface it lacks.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"io"
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
	readinessReread := flag.Duration("readiness-reread-interval", 0,
		"how often /-/ready re-reads the store directory of a degraded tenant that is not open, "+
			"to notice that an operator has cleared the quarantine (0 = the built-in 250ms, "+
			"negative = every probe)")
	corruptionPolicy := flag.String("corruption-policy", "fail",
		"what to do with a stored group that cannot be read: fail (refuse to open the tenant) "+
			"or quarantine (move it aside, serve the rest, and report the tenant degraded and "+
			"not ready until POST /admin/acknowledge-degraded)")
	syslogAddr := flag.String("syslog", "", "also listen for syslog on this UDP/TCP address (e.g. :514)")
	backends := flag.String("select-backends", "", "comma-separated peer node URLs; when set this node is a select router (vmselect role)")
	compact := flag.Bool("compact", false, "compact mode: flate dictionaries for ~15% smaller groups, but 2-10x slower value-reading queries -- for cold archival only, not a queryable store")
	replicas := flag.Int("replicas", 1, "replication factor for -select-backends: backends group into shards of this many replicas")
	maxRows := flag.Int("search.maxRows", 0, "cap on rows a bare (no-pipe) select may return; 0 = the built-in default, -1 = unlimited. Over the cap the query errors (never silently truncates); add a `| limit N` or a stats pipe.")
	maxBody := flag.Int64("http.maxBodyBytes", 0, "maximum request body in bytes; 0 = the built-in default, -1 = unlimited")
	maxQueryDur := flag.Duration("search.maxDuration", 0, "maximum wall time for one query; 0 = the built-in default, -1ns = unlimited")
	maxQueryBytes := flag.Int64("search.maxQueryBytes", 0, "maximum bytes one query may materialize; 0 = the built-in default, -1 = unlimited. Over it the query errors rather than returning a short answer.")
	maxTenants := flag.Int("tenants.max", 0, "maximum tenants held open; 0 = the built-in default, -1 = unlimited")
	authFile := flag.String("auth.config", "", "path to the JSON auth file (bearer-token hashes, roles, tenants). Without it the server is UNAUTHENTICATED: every client can query, ingest and download backups.")
	tlsCert := flag.String("tls.certFile", "", "PEM certificate; with -tls.keyFile this serves HTTPS")
	tlsKey := flag.String("tls.keyFile", "", "PEM private key for -tls.certFile")
	tlsClientCA := flag.String("tls.clientCAFile", "", "PEM CA bundle; when set, clients must present a certificate signed by it (mTLS)")
	// Two spellings: -tls.insecure follows the dotted convention of every
	// neighbouring flag, and -insecure-http is kept because it is what the
	// first documentation said. Either turns it on.
	insecureTLS := flag.Bool("tls.insecure", false, "serve plaintext on a non-loopback address (log data travels in clear text)")
	insecureHTTP := flag.Bool("insecure-http", false, "alias for -tls.insecure")
	syslogTLS := flag.Bool("syslog.tls", false,
		"serve RFC 5425 syslog-over-TLS on the syslog TCP listener, using -tls.certFile/-tls.keyFile (UDP stays plaintext)")
	flag.Parse()

	// Validate the listener configuration before anything is acquired. It
	// depends only on flags, and log.Fatal calls os.Exit, so a failure after
	// the store is open leaves a data directory, bound sockets and running
	// goroutines behind with no deferred cleanup.
	tlsCfg := config.TLSConfig{
		CertFile:     *tlsCert,
		KeyFile:      *tlsKey,
		ClientCAFile: *tlsClientCA,
		InsecureHTTP: *insecureTLS || *insecureHTTP,
	}
	if err := tlsCfg.CheckListen(*addr); err != nil {
		log.Fatal(err)
	}
	// The syslog listener is unauthenticated and writes to the default tenant,
	// so a plaintext one on a non-loopback address is refused for the same
	// reason the HTTP listener is. -syslog.tls turns the TCP half into RFC
	// 5425 syslog-over-TLS using the same certificate, which is what makes
	// that refusal something an operator can actually satisfy rather than
	// only work around with -tls.insecure.
	//
	// UDP stays plaintext: RFC 5425 is TLS over TCP, and RFC 5426's UDP
	// transport has no TLS form at all.
	if *syslogAddr != "" && !*syslogTLS {
		// CheckPlaintextListen, not CheckListen: the syslog port is plaintext
		// whatever the HTTP listener does, so a certificate on the HTTP side
		// must not exempt it.
		if err := tlsCfg.CheckPlaintextListen(*syslogAddr); err != nil {
			log.Fatalf("syslog: %v (or pass -syslog.tls to serve RFC 5425 "+
				"syslog-over-TLS on the TCP half)", err)
		}
	}
	if *syslogTLS && !tlsCfg.Enabled() {
		log.Fatal("-syslog.tls needs -tls.certFile and -tls.keyFile")
	}
	tc, err := tlsCfg.Build()
	if err != nil {
		log.Fatal(err)
	}

	cfg := config.Default()
	cfg.CorruptionPolicy = *corruptionPolicy
	cfg.DirRereadInterval = *readinessReread
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
	cfg.Limits.MaxQueryBytes = *maxQueryBytes
	cfg.Limits.MaxOpenTenants = *maxTenants

	srv, err2 := api.NewServerConfig(cfg)
	if err2 != nil {
		log.Fatal(err2)
	}
	if *authFile != "" {
		ac, err := config.LoadAuth(*authFile)
		if err != nil {
			log.Fatal(err)
		}
		if err := srv.SetAuth(ac); err != nil {
			log.Fatal(err)
		}
		if ac.Disabled {
			log.Print("WARNING: authentication is explicitly disabled in " + *authFile)
		} else {
			// Count every credential kind, not just tokens: a certs-only file
			// logged "0 credentials".
			creds := len(ac.Tokens) + len(ac.Certs)
			if ac.TrustedProxy {
				creds++
			}
			log.Printf("authentication enabled: %d credentials from %s", creds, *authFile)
			if *tlsClientCA != "" && len(ac.Certs) == 0 {
				log.Print("WARNING: -tls.clientCAFile is set but the auth file has no certs entries; " +
					"a client certificate proves only that the CA trusts the client and grants nothing")
			}
		}
	} else {
		// Loud, once, at startup. A server that is open to everyone should
		// say so rather than leaving an operator to infer it.
		log.Print("WARNING: no -auth.config; the server is UNAUTHENTICATED and every client can query, ingest and download backups")
		if *tlsClientCA != "" {
			log.Print("WARNING: -tls.clientCAFile without -auth.config gates the transport only; " +
				"every client the CA signs gets full access, including /admin/backup")
		}
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
	var syslogClosers []io.Closer
	if *syslogAddr != "" {
		// Both closers are kept. Discarding them left the UDP and TCP
		// listeners accepting data all the way through shutdown, into writers
		// that were being flushed and stores that were being unmapped.
		syslogCfg := api.DefaultSyslogConfig()
		if *syslogTLS {
			// The same certificate the HTTP listener uses. A separate one
			// would be a second thing to rotate for no gain: both are this
			// process's identity on this host.
			syslogCfg.TLS = tc
		}
		udpC, tcpC, err := srv.ListenSyslogConfig(*syslogAddr, syslogCfg)
		if err != nil {
			log.Fatalf("syslog listen %s: %v", *syslogAddr, err)
		}
		if udpC != nil {
			syslogClosers = append(syslogClosers, udpC)
		}
		if tcpC != nil {
			syslogClosers = append(syslogClosers, tcpC)
		}
		log.Printf("syslog listener on %s (UDP+TCP)", *syslogAddr)
	}
	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second, // slowloris protection
		ReadTimeout:       5 * time.Minute,  // a large ingest body may legitimately take a while
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
		TLSConfig:         tc,
		// No WriteTimeout on purpose: it is absolute, not idle-based, and
		// would cut the live tail off mid-stream. Per-route deadlines belong
		// with the query executor (plan task 6.1), which can tell a tail from
		// a query.
	}
	go func() {
		scheme := "http"
		if tc != nil {
			scheme = "https"
			if tc.ClientAuth == tls.RequireAndVerifyClientCert {
				scheme = "https+mtls"
			}
		}
		log.Printf("simdlogs %s on %s, storage %s, simd tier %s", scheme, *addr, *dir, simd.Tier())
		var err error
		if tc != nil {
			// The certificate is already in TLSConfig; empty paths here tell
			// ListenAndServeTLS to use it rather than re-reading the files.
			err = httpSrv.ListenAndServeTLS("", "")
		} else {
			err = httpSrv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	// Graceful shutdown: stop accepting, drain in-flight, then flush+unmap so
	// no buffered rows are lost and no mmap leaks.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Print("shutting down...")

	// Order matters, and each step has a reason.
	//
	// 1. Stop the native listeners first. They are not part of the HTTP
	//    server, so Shutdown does not touch them: leaving them open fed rows
	//    into writers that were already being flushed.
	for _, c := range syslogClosers {
		if err := c.Close(); err != nil {
			log.Printf("syslog listener close: %v", err)
		}
	}
	// 2. Drain HTTP. Shutdown stops accepting and waits for in-flight
	//    requests. Its error was discarded before, so a deadline expiring with
	//    requests still running looked identical to a clean drain.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("http shutdown did not drain cleanly (requests may have been cut): %v", err)
	}
	// 3. Close the server: stops background loops and waits for them, flushes
	//    every tenant writer, then releases the stores. Only now is it safe to
	//    unmap -- steps 1 and 2 guarantee nothing is still reading.
	if err := srv.Close(); err != nil {
		log.Printf("close: %v", err)
	}
	log.Print("stopped")
}
