package ingest

import (
	"encoding/json"
	"strconv"
)

// otlpValue is an OTLP AnyValue: exactly one of the typed fields is set. OTLP's
// JSON encoding writes int64 as a string, so IntValue is a *string.
type otlpValue struct {
	StringValue *string  `json:"stringValue"`
	IntValue    *string  `json:"intValue"`
	DoubleValue *float64 `json:"doubleValue"`
	BoolValue   *bool    `json:"boolValue"`
}

func (v otlpValue) str() string {
	switch {
	case v.StringValue != nil:
		return *v.StringValue
	case v.IntValue != nil:
		return *v.IntValue
	case v.DoubleValue != nil:
		return strconv.FormatFloat(*v.DoubleValue, 'g', -1, 64)
	case v.BoolValue != nil:
		return strconv.FormatBool(*v.BoolValue)
	}
	return ""
}

type otlpKV struct {
	Key   string    `json:"key"`
	Value otlpValue `json:"value"`
}

// otlpLogs is the OTLP/HTTP logs export payload (ExportLogsServiceRequest) in
// its JSON encoding: resource -> scope -> log records.
type otlpLogs struct {
	ResourceLogs []struct {
		Resource struct {
			Attributes []otlpKV `json:"attributes"`
		} `json:"resource"`
		ScopeLogs []struct {
			LogRecords []struct {
				TimeUnixNano         string    `json:"timeUnixNano"`
				ObservedTimeUnixNano string    `json:"observedTimeUnixNano"`
				SeverityText         string    `json:"severityText"`
				Body                 otlpValue `json:"body"`
				Attributes           []otlpKV  `json:"attributes"`
			} `json:"logRecords"`
		} `json:"scopeLogs"`
	} `json:"resourceLogs"`
}

// IngestOTLPLogs ingests an OpenTelemetry logs export (OTLP/HTTP, JSON): each
// log record becomes a record whose fields are the resource attributes plus
// the record's own attributes, with severityText -> severity, body -> _msg,
// and timeUnixNano (or observedTimeUnixNano) -> time.
func IngestOTLPLogs(w *Writer, data []byte, fallback func() int64) (ingested, skipped int) {
	var p otlpLogs
	if err := json.Unmarshal(data, &p); err != nil {
		return 0, 0
	}
	fields := map[string]string{}
	for _, rl := range p.ResourceLogs {
		resAttrs := make(map[string]string, len(rl.Resource.Attributes))
		for _, a := range rl.Resource.Attributes {
			resAttrs[a.Key] = a.Value.str()
		}
		for _, sl := range rl.ScopeLogs {
			for _, lr := range sl.LogRecords {
				for k := range fields {
					delete(fields, k)
				}
				for k, v := range resAttrs {
					fields[k] = v
				}
				for _, a := range lr.Attributes {
					fields[a.Key] = a.Value.str()
				}
				if lr.SeverityText != "" {
					fields["severity"] = lr.SeverityText
				}
				if msg := lr.Body.str(); msg != "" {
					fields["_msg"] = msg
				}
				tsStr := lr.TimeUnixNano
				if tsStr == "" {
					tsStr = lr.ObservedTimeUnixNano
				}
				ts, ok := parseTime(tsStr)
				if !ok {
					ts = fallback()
				}
				w.Add(ts, fields)
				ingested++
			}
		}
	}
	return ingested, skipped
}
