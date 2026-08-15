package api

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"
)

// The client a router uses to talk to storage nodes.
//
// # Why not http.DefaultClient
//
// Every peer call used it. It has no timeout at all: a peer that accepts the
// connection and then never answers holds the router's goroutine, its share of
// the client's connection pool, and the caller's request, for as long as the
// caller waits -- and a router fanning out to N shards multiplies that by N.
// Its transport is shared process-wide, so peer traffic and any other outbound
// HTTP compete for the same pool. And it cannot be given a client certificate,
// so mTLS between nodes was not expressible.
//
// # Why the body is bounded
//
// `io.ReadAll(resp.Body)` on a peer response is an unbounded allocation driven
// by another machine. A peer that is compromised, misconfigured or simply
// running a version whose response shape exploded takes the router down with
// it -- and the router is the node the whole cluster's reads go through.

// clusterClient is a configured HTTP client for peer traffic.
type clusterClient struct {
	http *http.Client
	// maxBody bounds one peer response. Beyond it the response is discarded
	// as malformed rather than truncated: a truncated JSON document is not a
	// smaller answer, it is an unparseable one, and a truncated NDJSON stream
	// is a partial answer indistinguishable from a complete one.
	maxBody int64
}

// Peer client defaults.
//
// The dial and header timeouts are short because a peer is on the same
// network as the router; the overall timeout is not set on the client at all
// -- the caller's request context carries the deadline, and a client timeout
// on top of it would cut a legitimately slow query that the caller was still
// waiting for.
const (
	peerDialTimeout          = 2 * time.Second
	peerTLSHandshakeTimeout  = 3 * time.Second
	peerResponseHeaderTimeut = 10 * time.Second
	peerIdleConns            = 64
	peerIdleConnsPerHost     = 8
	peerIdleConnTimeout      = 90 * time.Second
	peerMaxBodyBytes         = 256 << 20
)

// newClusterClient builds the peer client. tlsCfg is nil for plaintext peer
// traffic; when set it carries the client certificate for mTLS.
func newClusterClient(tlsCfg *tls.Config) *clusterClient {
	return &clusterClient{
		http: &http.Client{
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   peerDialTimeout,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				TLSClientConfig:       tlsCfg,
				TLSHandshakeTimeout:   peerTLSHandshakeTimeout,
				ResponseHeaderTimeout: peerResponseHeaderTimeut,
				MaxIdleConns:          peerIdleConns,
				MaxIdleConnsPerHost:   peerIdleConnsPerHost,
				IdleConnTimeout:       peerIdleConnTimeout,
				// Peer bodies are already compressed where it matters and the
				// router re-reads every one; transparent gzip would decompress
				// into an unbounded buffer BEFORE maxBody could see it.
				DisableCompression: true,
			},
			// No client timeout: the caller's context carries the deadline.
			// A second one here would cut a query the caller was still
			// waiting for, and the caller is the one who knows how long it is
			// prepared to wait.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				// A peer does not redirect. Following one would send the
				// router's credential to whatever host the response named.
				return http.ErrUseLastResponse
			},
		},
		maxBody: peerMaxBodyBytes,
	}
}

// forwardedHeaders are copied from the client's request to the peer's,
// explicitly.
//
// Explicitly, because the alternative -- copying the whole header set --
// forwards the client's Authorization to every storage node, along with its
// cookies and any header a proxy added. The router authenticates to peers as
// ITSELF; a client credential travelling further than the node it was
// presented to is how one node's compromise becomes the cluster's.
var forwardedHeaders = []string{
	// The RESOLVED tenant, stamped by this router's own resolver. A storage
	// node normally has no -auth.config of its own, so this is the only place
	// a read's tenant is decided.
	"AccountID", "ProjectID",
	// Tracing, so one request is one trace across nodes.
	"X-Request-Id", "Traceparent", "Tracestate",
}

// do performs one peer request and returns the parsed envelope.
//
// It never returns a nil PeerResponse: every failure is a response with a
// class, because the caller's job is to decide what to do about it and "error
// was nil-checked away" is how a failed peer became a silently missing shard.
func (c *clusterClient) do(
	r *http.Request, shard, replica int, url, method, path string, body []byte,
) PeerResponse {
	out := PeerResponse{Shard: shard, Replica: replica, URL: url}

	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	target := url + path
	if method == http.MethodGet && r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(r.Context(), method, target, rdr)
	if err != nil {
		out.Class, out.Err = PeerMalformed, err
		return out
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for _, h := range forwardedHeaders {
		if v := r.Header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}
	// Marked internal, so the peer answers with the envelope rather than the
	// plain public response.
	req.Header.Set(HdrInternal, "1")
	req.Header.Set(HdrProtocolVersion, strconv.Itoa(ProtocolVersion))
	out.TraceID = r.Header.Get("X-Request-Id")
	if out.TraceID != "" {
		req.Header.Set(HdrTraceID, out.TraceID)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		// Every transport failure -- refused, timed out, TLS rejected, DNS --
		// is "this peer did not answer". They are one class because the
		// router's response to all of them is the same: try another replica.
		out.Class, out.Err = PeerUnavailable, err
		return out
	}
	defer resp.Body.Close()
	out.Status = resp.StatusCode

	// The version FIRST, before anything in the body is trusted. A peer on an
	// unknown version may have produced a body that parses and means something
	// else, which is worse than one that does not parse.
	switch v := resp.Header.Get(HdrProtocolVersion); v {
	case "":
		// No version header at all: either a node from before this protocol
		// existed, or something that is not a simdlogs node. Both are
		// unusable and neither should be merged.
		out.Class = PeerVersionMismatch
		out.Err = errors.New("peer sent no protocol version")
		return out
	default:
		n, err := strconv.Atoi(v)
		if err != nil || n != ProtocolVersion {
			out.Class = PeerVersionMismatch
			out.Err = fmt.Errorf("peer speaks protocol %q, this node speaks %d",
				v, ProtocolVersion)
			return out
		}
		out.Version = n
	}

	if cls := PeerErrorClass(resp.Header.Get(HdrErrorClass)); cls != PeerOK {
		out.Class = cls
		out.Err = fmt.Errorf("peer reported %s (HTTP %d)", cls, resp.StatusCode)
		return out
	}
	switch {
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		out.Class = PeerUnauthorized
		out.Err = fmt.Errorf("peer refused this router's credential (HTTP %d)", resp.StatusCode)
		return out
	case resp.StatusCode == http.StatusTooManyRequests ||
		resp.StatusCode == http.StatusServiceUnavailable:
		out.Class = PeerOverloaded
		out.Err = fmt.Errorf("peer refused for load (HTTP %d)", resp.StatusCode)
		return out
	case resp.StatusCode >= 500:
		out.Class = PeerUnavailable
		out.Err = fmt.Errorf("peer returned HTTP %d", resp.StatusCode)
		return out
	}

	// Bounded. One extra byte is read so a body exactly at the limit is
	// distinguishable from one that was cut.
	lr := &io.LimitedReader{R: resp.Body, N: c.maxBody + 1}
	b, err := io.ReadAll(lr)
	if err != nil {
		out.Class, out.Err = PeerUnavailable, err
		return out
	}
	if int64(len(b)) > c.maxBody {
		// Discarded, not truncated: a truncated JSON document is unparseable
		// and a truncated NDJSON stream is a partial answer that looks
		// complete.
		out.Class = PeerMalformed
		out.Err = fmt.Errorf("peer response exceeds %d bytes", c.maxBody)
		return out
	}
	out.Body = b

	// Completeness and the watermark. A missing Complete header is NOT read as
	// complete: absent means the peer did not say, and a router that assumed
	// yes would report a partial answer as whole -- which is the failure the
	// envelope exists to prevent.
	out.Complete = resp.Header.Get(HdrComplete) == "true"
	if hw := resp.Header.Get(HdrHighWatermark); hw != "" {
		out.HighWatermark, _ = strconv.ParseInt(hw, 10, 64)
	}
	if id := resp.Header.Get(HdrTraceID); id != "" {
		out.TraceID = id
	}
	return out
}
