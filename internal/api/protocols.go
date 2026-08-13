package api

import (
	"io"
	"net/http"
	"strings"

	"github.com/sebishogun/simdlogs/internal/ingest"
)

// backup streams a tar of the tenant's group files: a consistent point-in-time
// snapshot for offline restore via storage.RestoreTar.
func (s *Server) backup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Content-Disposition", `attachment; filename="simdlogs-backup.tar"`)
	if err := s.tn(r).store.BackupTar(w); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

// ingestOptions reads the per-request field mappings a shipper sends as query
// args (_time_field, _msg_field, _stream_fields, ignore_fields, extra_fields).
// Read from the URL, never r.FormValue: FormValue parses the BODY for a
// form-encoded content type, and a line-protocol write then stored nothing
// while still answering 204.
func ingestOptions(r *http.Request) ingest.Options {
	q := r.URL.Query()
	return ingest.ParseOptions(func(k string) string { return q.Get(k) })
}

// ingestBody reads the request body and hands it to one of the protocol
// parsers against the request's tenant writer, then flushes. status is the
// success code the protocol's clients expect.
func (s *Server) ingestBody(w http.ResponseWriter, r *http.Request, status int,
	parse func(*ingest.Writer, []byte, func() int64, *ingest.Options) (int, int)) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	tn := s.tn(r)
	opts := ingestOptions(r)
	ing, skip := parse(tn.w, body, tn.fallbackTS(), &opts)
	s.countRows(ing, skip, len(body))
	if err := tn.w.Flush(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(status)
}

// insertLoki ingests a Grafana Loki push body (JSON); clients expect 204.
func (s *Server) insertLoki(w http.ResponseWriter, r *http.Request) {
	s.ingestBody(w, r, http.StatusNoContent, ingest.IngestLokiOpts)
}

// insertJournald ingests the systemd journal export (systemd-journal-upload).
func (s *Server) insertJournald(w http.ResponseWriter, r *http.Request) {
	s.ingestBody(w, r, http.StatusAccepted, ingest.IngestJournaldOpts)
}

// insertSyslog ingests syslog lines over HTTP (one per line). The native
// transport is UDP/TCP via ListenSyslog; this is for HTTP forwarders.
func (s *Server) insertSyslog(w http.ResponseWriter, r *http.Request) {
	s.ingestBody(w, r, http.StatusNoContent, ingest.IngestSyslogOpts)
}

// insertDatadog ingests a Datadog logs-intake body (JSON array or object);
// Datadog's intake returns 202 Accepted.
func (s *Server) insertDatadog(w http.ResponseWriter, r *http.Request) {
	s.ingestBody(w, r, http.StatusAccepted, ingest.IngestDatadogOpts)
}

// insertOTLPLogs ingests an OpenTelemetry logs export (OTLP/HTTP, JSON). The
// OTLP spec expects a 200 with an ExportLogsServiceResponse body.
func (s *Server) insertOTLPLogs(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	tn := s.tn(r)
	otlpOpts := ingestOptions(r)
	// The collector's otlphttp exporter sends protobuf unless configured
	// otherwise, so the Content-Type decides the parser. Feeding protobuf to
	// the JSON decoder stored nothing while answering 200 -- silent data loss
	// for the DEFAULT client configuration.
	proto := strings.Contains(r.Header.Get("Content-Type"), "protobuf")
	var oing, oskip int
	if proto {
		oing, oskip = ingest.IngestOTLPLogsProto(tn.w, body, tn.fallbackTS(), &otlpOpts)
	} else {
		oing, oskip = ingest.IngestOTLPLogsOpts(tn.w, body, tn.fallbackTS(), &otlpOpts)
	}
	s.countRows(oing, oskip, len(body))
	if err := tn.w.Flush(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	// The response mirrors the request's encoding, as the OTLP/HTTP spec
	// requires: an empty ExportLogsServiceResponse is zero bytes in protobuf
	// and {} in JSON, both meaning full success.
	if proto {
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("{}")) // empty ExportLogsServiceResponse == full success
}
