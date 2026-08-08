// Command simdlogs is the log-database server: ingest and query over HTTP,
// tracking VictoriaLogs' paths where they exist and adding the
// Elasticsearch search surface it lacks.
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/sebishogun/simd"
	"github.com/sebishogun/simdlogs/internal/api"
)

func main() {
	addr := flag.String("addr", ":9428", "listen address (VL's default port)")
	dir := flag.String("storage", "./simdlogs-data", "storage directory")
	flag.Parse()

	srv, err := api.NewServer(*dir)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("simdlogs on %s, storage %s, simd tier %s", *addr, *dir, simd.Tier())
	log.Fatal(http.ListenAndServe(*addr, srv.Handler()))
}
