// Command simdlogs is the log-database server: ingest and query over HTTP,
// tracking VictoriaLogs' paths where they exist and adding the
// Elasticsearch search surface it lacks.
package main

import (
	"flag"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/sebishogun/simd"
	"github.com/sebishogun/simdlogs/internal/api"
)

func main() {
	addr := flag.String("addr", ":9428", "listen address (VL's default port)")
	dir := flag.String("storage", "./simdlogs-data", "storage directory")
	retention := flag.Duration("retention", 0, "drop data older than this (e.g. 720h); 0 disables")
	streamFields := flag.String("stream-fields", "", "comma-separated fields that identify a log stream (synthesizes _stream)")
	syslogAddr := flag.String("syslog", "", "also listen for syslog on this UDP/TCP address (e.g. :514)")
	backends := flag.String("select-backends", "", "comma-separated peer node URLs; when set this node is a select router (vmselect role)")
	flag.Parse()

	srv, err := api.NewServer(*dir)
	if err != nil {
		log.Fatal(err)
	}
	if *streamFields != "" {
		srv.SetStreamFields(strings.Split(*streamFields, ","))
	}
	if *backends != "" {
		srv.SetBackends(strings.Split(*backends, ","))
		log.Printf("select-router mode: %s", *backends)
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
	log.Printf("simdlogs on %s, storage %s, simd tier %s", *addr, *dir, simd.Tier())
	log.Fatal(http.ListenAndServe(*addr, srv.Handler()))
}
