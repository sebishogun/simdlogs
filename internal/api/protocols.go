package api

import (
	"encoding/binary"
	"encoding/json"
	obs "github.com/sebishogun/simdlogs/internal/observability"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sebishogun/simdlogs/internal/ingest"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// backupFlushTimeout bounds the pre-snapshot flush. Long enough that an
// ordinary flush finishes, short enough that a stalled writer does not turn
// "take a backup" into "wait forever".
//
// Declared above the handler's own doc comment rather than between it and the
// func: inserted there, it took that whole rationale as its own godoc and left
// `backup` undocumented.
const backupFlushTimeout = 10 * time.Second

// backup streams a tar of the tenant's group files: a consistent point-in-time
// snapshot for offline restore via storage.Restore (the `simdlogs restore`
// command); storage.RestoreTar is the older unstaged path.
//
// A backup that fails partway CANNOT be reported with a status code, because
// the 200 and the first bytes are already on the wire. It used to call
// http.Error anyway, which logged "superfluous WriteHeader" and appended the
// error text to the archive -- so a truncated backup arrived as a 200 with a
// plausible-looking tar that had an error message glued to the end of it. For
// a disaster-recovery artifact that is the worst possible failure mode: it is
// discovered at restore time.
//
// So the response is failed the only way HTTP allows once bytes are out:
// http.ErrAbortHandler, which drops the connection without the terminating
// chunk. Every HTTP client reports that as an unexpected EOF, which is a
// truthful "this transfer did not complete". Before any byte is written a
// clean 500 is still possible, and that path is taken.
func (s *Server) backup(w http.ResponseWriter, r *http.Request) {
	if s.refuseInRouterMode(w, r, "backup",
		"a router's store is empty, so this would stream a valid backup of no "+
			"data and restore as an empty cluster; the coordinated cluster backup "+
			"is not in this build") {
		return
	}
	tn := s.tn(r)
	// One backup per tenant at a time. Each one holds a Snapshot for its whole
	// duration, which pins every group it captured against unmapping, so N
	// concurrent streams hold N copies of the store's full mapping set and
	// retention frees nothing while any of them runs. 429 rather than a queue:
	// a second backup is a duplicate request, not work to serialize, and a
	// queued one would sit on its own snapshot while it waited.
	if !tn.backupBusy.CompareAndSwap(false, true) {
		w.Header().Set("Retry-After", "60")
		s.writeErr(w, r, opsSpec(), http.StatusTooManyRequests,
			"a backup of this tenant is already in progress")
		return
	}
	defer tn.backupBusy.Store(false)

	// Audited. A backup is a full copy of a tenant's data leaving the server,
	// so "who took one, when" is the question a security review asks first --
	// and the answer has to exist before the review, not be reconstructed from
	// an access log that may have rolled.
	obs.Audit(r.Context(), obs.EventBackupTaken, subjectOf(r), obs.OutcomeOK,
		obs.FieldTenant, tn.key, obs.FieldRoute, r.URL.Path)

	// Flush before the snapshot, so the archive holds what this tenant has
	// been told is stored. Rows still in the writer's buffer are in no group
	// yet, and a backup taken without this is missing every row since the last
	// flush trigger -- silently, because the archive is consistent with the
	// store and the store is simply behind its clients.
	//
	// A flush failure is not fatal to the backup. Whatever is already durable
	// is still worth capturing, and this endpoint has no way to report a
	// failure once bytes are out; the consequence of ignoring it is that the
	// archive stops at the last durable group rather than the last
	// acknowledged row, which is the pre-flush behaviour and no worse.
	//
	// BOUNDED, because Flush waits on every live batch -- including one pinned
	// by a stalled fsync, which is the scenario the writer's own history
	// ceiling exists for. Unbounded, it would hold backupBusy forever and make
	// a stalled writer disable that tenant's backups entirely, which is the
	// opposite of what a backup is for. On timeout the archive is taken
	// anyway: that is exactly the "stops at the last durable group" case the
	// paragraph above already accepts.
	if tn.preFlushing.CompareAndSwap(false, true) {
		flushed := make(chan struct{})
		go func() {
			defer tn.preFlushing.Store(false)
			defer close(flushed)
			_ = tn.w.Flush()
		}()
		select {
		case <-flushed:
		case <-time.After(backupFlushTimeout):
		}
	}
	// If one is already parked, this backup skips its own: a second would wait
	// on the same batches, and spawning it is how polling this endpoint
	// against a stalled writer accumulated goroutines nothing counted.

	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Content-Disposition", `attachment; filename="simdlogs-backup.tar"`)
	cw := &countingWriter{w: w}
	if err := tn.store.BackupTarWith(cw, storage.BackupOptions{Tenant: tn.key}); err != nil {
		if cw.n == 0 {
			s.writeErr(w, r, opsSpec(), http.StatusInternalServerError,
				"backup failed before any data was written: "+err.Error())
			return
		}
		// Bytes are already out. Abort rather than append.
		panic(http.ErrAbortHandler)
	}
}

// countingWriter records whether anything reached the client yet, which is
// what decides whether a failure can still be a status code.
type countingWriter struct {
	w http.ResponseWriter
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
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
		s.writeFlushErr(w, r, ndjsonSpec(), err)
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

// insertLoki ingests a Grafana Loki push body; clients expect 204.
//
// Promtail, Grafana Alloy and the Grafana Agent send snappy-compressed
// protobuf BY DEFAULT, so the Content-Type picks the parser exactly as it does
// for OTLP. Before this, a default-configured agent's body went to the JSON
// decoder, which is not JSON, so a correctly-formed push was answered 400 --
// the whole default configuration was unusable.
func (s *Server) insertLoki(w http.ResponseWriter, r *http.Request) {
	// JSON is the exception, protobuf is the default -- the way round Loki's
	// own API defines it ("the default behavior is for the POST body to be a
	// Snappy-compressed Protocol Buffer message") and the way VictoriaLogs
	// routes it. Matching on "protobuf" instead sent everything else to the
	// JSON decoder, so `application/protobuf` (the IANA spelling), an absent
	// Content-Type, and application/octet-stream all reached a JSON parser
	// holding a snappy blob and answered 400.
	if strings.Contains(r.Header.Get("Content-Type"), "json") {
		s.ingestBody(w, r, http.StatusNoContent, ingest.IngestLokiOpts)
		return
	}
	s.ingestBody(w, r, http.StatusNoContent, ingest.IngestLokiProto)
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
		s.writeFlushErr(w, r, otlpSpec(), err)
		return
	}
	// After the flush, like every sibling path: a counter must not go
	// backwards, and counting first meant a scrape could see a spike the
	// correction then read as a restart.
	s.countRows(ores.Accepted, ores.Rejected, len(body))
	// The response mirrors the request's encoding, as the OTLP/HTTP spec
	// requires: an empty ExportLogsServiceResponse is zero bytes in protobuf
	// and {} in JSON, both meaning full success.
	// partial_success, when anything was rejected. Before this the response
	// was always the empty "everything was accepted" message, so an exporter
	// whose records this store dropped -- a metrics payload posted to /v1/logs,
	// a record whose shape was refused -- was told they were all stored, and
	// had no signal at all that some of its data was gone.
	//
	// It is a 200 either way: OTLP's partial success is deliberately NOT an
	// error status, because a 4xx tells the exporter to drop the whole batch
	// including the records this store did accept, and a 5xx makes it resend
	// them.
	if proto {
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		if ores.Rejected > 0 {
			w.Write(otlpPartialSuccessProto(ores.Rejected, otlpRejectMessage(ores)))
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if ores.Rejected > 0 {
		json.NewEncoder(w).Encode(ingest.OTLPResponseFor(ores, otlpRejectMessage(ores)))
		return
	}
	w.Write([]byte("{}")) // empty ExportLogsServiceResponse == full success
}

// otlpRejectMessage renders the reasons the ingest recorded. OTLP says
// error_message is for a human and must not be parsed, so this is the
// operator's only route from "records vanished" to "why".
func otlpRejectMessage(res ingest.Result) string {
	if len(res.Warnings) == 0 {
		return ""
	}
	msgs := make([]string, 0, len(res.Warnings))
	for _, w := range res.Warnings {
		msgs = append(msgs, w.Msg)
	}
	return strings.Join(msgs, "; ")
}

// otlpPartialSuccessProto encodes ExportLogsServiceResponse{ partial_success }
// by hand, in the same style as the request decoder and for the same reason:
// this repository takes no protobuf dependency.
//
//	ExportLogsServiceResponse: partial_success = 1
//	ExportLogsPartialSuccess:  rejected_log_records = 1 (int64), error_message = 2
func otlpPartialSuccessProto(rejected int, msg string) []byte {
	var ps []byte
	ps = binary.AppendUvarint(ps, 1<<3|0) // field 1, varint
	ps = binary.AppendUvarint(ps, uint64(rejected))
	if msg != "" {
		ps = binary.AppendUvarint(ps, 2<<3|2) // field 2, length-delimited
		ps = binary.AppendUvarint(ps, uint64(len(msg)))
		ps = append(ps, msg...)
	}
	out := binary.AppendUvarint(nil, 1<<3|2) // field 1, length-delimited
	out = binary.AppendUvarint(out, uint64(len(ps)))
	return append(out, ps...)
}
