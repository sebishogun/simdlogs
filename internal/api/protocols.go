package api

import (
	"encoding/json"
	"net/http"
	"strconv"
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
	parse func(*ingest.Writer, []byte, func() int64, *ingest.Options) (ingest.Result, error)) {
	body, berr := s.readBody(w, r)
	if berr != nil {
		s.writeErr(w, r, ndjsonSpec(), berr.code, berr.msg)
		return
	}
	tn := s.tn(r)
	opts := ingestOptions(r)
	mark := tn.w.Mark()
	res, perr := parse(tn.w, body, tn.fallbackTS(), &opts)
	// Whatever was accepted before the failure is still counted: the rows are
	// in the writer either way, and metrics that disagree with the store are
	// worse than metrics that report a partial batch.
	if perr != nil {
		// A payload this parser could not read is a failed request. Every one
		// of these used to return zero records and success, so a
		// misconfigured agent looked healthy while nothing was stored.
		s.writeErr(w, r, ndjsonSpec(), ingest.StatusFor(perr), perr.Error())
		return
	}
	// FlushMark, not Flush: the row buffer is shared by every request and
	// every syslog connection on this tenant, so a plain Flush reports on
	// whatever batches it happened to wait for -- which is routinely another
	// request's rows. This asks about the rows THIS request added.
	if err := tn.w.FlushMark(mark); err != nil {
		s.writeErr(w, r, ndjsonSpec(), http.StatusServiceUnavailable, err.Error())
		return
	}
	// Counted after the flush, not before it with a subtraction on failure.
	// vl_rows_ingested_total is declared a Prometheus counter, and a scrape
	// landing between the add and the subtract saw a spike that the
	// correction then turned into an apparent restart for rate().
	s.countRows(res.Accepted, res.Rejected, len(body))
	// Records the parser refused are reported rather than dropped silently.
	//
	// A 204 carries no body -- Go discards anything written after it -- so a
	// route whose success code is 204 reports the rejects in headers instead.
	// Writing a JSON body after WriteHeader(204) looked like reporting and
	// dropped the counts exactly as before.
	if res.Rejected > 0 {
		if status == http.StatusNoContent {
			w.Header().Set("X-Simdlogs-Accepted", strconv.Itoa(res.Accepted))
			w.Header().Set("X-Simdlogs-Rejected", strconv.Itoa(res.Rejected))
			if ws := warningStrings(res.Warnings); len(ws) > 0 {
				w.Header().Set("X-Simdlogs-Warning", ws[0])
			}
			w.WriteHeader(status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(map[string]any{
			"accepted": res.Accepted,
			"rejected": res.Rejected,
			"warnings": warningStrings(res.Warnings),
		})
		return
	}
	w.WriteHeader(status)
}

// warningStrings flattens the parser's warnings for a response body.
func warningStrings(ws []ingest.Warning) []string {
	if len(ws) == 0 {
		return nil
	}
	out := make([]string, 0, len(ws))
	for _, w := range ws {
		out = append(out, w.Msg)
	}
	return out
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
	body, berr := s.readBody(w, r)
	if berr != nil {
		s.writeErr(w, r, ndjsonSpec(), berr.code, berr.msg)
		return
	}
	tn := s.tn(r)
	otlpOpts := ingestOptions(r)
	// The collector's otlphttp exporter sends protobuf unless configured
	// otherwise, so the Content-Type decides the parser. Feeding protobuf to
	// the JSON decoder stored nothing while answering 200 -- silent data loss
	// for the DEFAULT client configuration.
	proto := strings.Contains(r.Header.Get("Content-Type"), "protobuf")
	mark := tn.w.Mark()
	var ores ingest.Result
	var operr error
	if proto {
		ores, operr = ingest.IngestOTLPLogsProto(tn.w, body, tn.fallbackTS(), &otlpOpts)
	} else {
		ores, operr = ingest.IngestOTLPLogsOpts(tn.w, body, tn.fallbackTS(), &otlpOpts)
	}
	if operr != nil {
		// OTLP exporters retry 5xx and give up on 4xx; answering 200 for an
		// undecodable body told them the data was delivered.
		s.writeErr(w, r, otlpSpec(), ingest.StatusFor(operr), operr.Error())
		return
	}
	if err := tn.w.FlushMark(mark); err != nil {
		s.writeErr(w, r, otlpSpec(), http.StatusServiceUnavailable, err.Error())
		return
	}
	// After the flush, like every sibling path: a counter must not go
	// backwards, and counting first meant a scrape could see a spike the
	// correction then read as a restart.
	s.countRows(ores.Accepted, ores.Rejected, len(body))
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
