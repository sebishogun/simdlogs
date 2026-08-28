package api

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/sebishogun/simdlogs/internal/query"
)

// Opaque, authenticated pagination cursors.
//
// # Why the cursor is signed rather than just encoded
//
// A cursor is a resume point the SERVER hands out and the CLIENT hands back.
// Everything in it is therefore attacker-controlled on the way back: the
// tenant it was issued for, the query it belongs to, the direction, and the
// row tuple. An unsigned cursor is a request parameter wearing a disguise --
// a client that base64-decodes one, edits the tenant, and re-encodes it is
// asking for another tenant's rows, and the only thing standing in the way
// would be the tenant check the cursor was supposed to make unnecessary.
//
// So it is HMAC-SHA256 over the whole payload, and the payload names what it
// is valid for. The tenant is in it, so a cursor cannot cross tenants. The
// query hash is in it, so a cursor cannot be replayed against a different
// query -- which matters because the row tuple's meaning depends on the
// filter that produced it: (time, group, row) after `level:error` and the same
// tuple after `*` name the same STORED row, but resuming the second walk from
// the first walk's position skips rows that the first walk filtered out.
//
// # Why the key is generated rather than configured
//
// A cursor is valid for the life of a process, which is the life of the
// snapshot semantics behind it. Making the key configurable would invite
// sharing it across a cluster, and a cursor that survives a restart is a
// cursor that resumes into a store that has since compacted, retired and
// re-ingested -- a resume point with no guarantee attached. A per-process
// random key makes "your cursor expired, start again" the honest answer.
//
// The exception is a select-router with several backends, where a cursor must
// mean the same thing on whichever node answers next. That is task 8.x's
// problem; single-node is what this ships, and a router that forwards a cursor
// to a node that did not issue it gets a clean rejection rather than a wrong
// page.

// ErrBadCursor is a cursor that does not verify, does not parse, or was issued
// for a different query, tenant or direction.
var ErrBadCursor = errors.New("simdlogs: invalid or expired cursor")

// cursorVersion prefixes the payload so a future format change is a clean
// rejection rather than a misread tuple.
const cursorVersion byte = 1

// cursorPayload is what a cursor carries, before signing.
type cursorPayload struct {
	tenant    string
	queryHash [32]byte
	dir       query.Direction
	key       query.RowKey
}

// cursorSigner issues and verifies cursors for one process.
type cursorSigner struct{ key []byte }

func newCursorSigner() (*cursorSigner, error) {
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		return nil, fmt.Errorf("simdlogs: no entropy for the cursor key: %w", err)
	}
	return &cursorSigner{key: k}, nil
}

// encode marshals the payload and appends its MAC.
//
// The MAC covers the version byte too. A signature over the fields alone would
// let a version-2 payload be re-labelled version 1 and reinterpreted under the
// old layout, which is the classic way a versioned format loses its version.
func (c *cursorSigner) encode(p cursorPayload) string {
	body := c.marshal(p)
	mac := hmac.New(sha256.New, c.key)
	mac.Write(body)
	return base64.RawURLEncoding.EncodeToString(append(body, mac.Sum(nil)...))
}

func (c *cursorSigner) marshal(p cursorPayload) []byte {
	b := make([]byte, 0, 1+32+1+8+8+4+2+len(p.tenant))
	b = append(b, cursorVersion)
	b = append(b, p.queryHash[:]...)
	b = append(b, byte(p.dir))
	b = binary.BigEndian.AppendUint64(b, uint64(p.key.Time))
	b = binary.BigEndian.AppendUint64(b, p.key.Group)
	b = binary.BigEndian.AppendUint32(b, p.key.Row)
	// The tenant is length-prefixed and last. Concatenating variable-length
	// fields without a length is how two different payloads produce the same
	// bytes; here only one field is variable, but the prefix is what keeps
	// that true when a second one is added.
	b = binary.BigEndian.AppendUint16(b, uint16(len(p.tenant)))
	return append(b, p.tenant...)
}

// decode verifies and unmarshals, and reports a mismatch against what the
// CURRENT request is asking for.
//
// The comparison is here rather than left to the caller on purpose: a decode
// that returned a valid-looking payload for another query would be a function
// whose result is safe only if every caller remembers a check.
func (c *cursorSigner) decode(s, tenant string, qh [32]byte, dir query.Direction) (query.RowKey, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(raw) < sha256.Size+1+32+1+8+8+4+2 {
		return query.RowKey{}, ErrBadCursor
	}
	body, sig := raw[:len(raw)-sha256.Size], raw[len(raw)-sha256.Size:]
	mac := hmac.New(sha256.New, c.key)
	mac.Write(body)
	// Constant-time, because a byte-at-a-time compare on a value an attacker
	// can resubmit is a forgery oracle.
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return query.RowKey{}, ErrBadCursor
	}
	if body[0] != cursorVersion {
		return query.RowKey{}, ErrBadCursor
	}
	var p cursorPayload
	copy(p.queryHash[:], body[1:33])
	p.dir = query.Direction(body[33])
	p.key.Time = int64(binary.BigEndian.Uint64(body[34:42]))
	p.key.Group = binary.BigEndian.Uint64(body[42:50])
	p.key.Row = binary.BigEndian.Uint32(body[50:54])
	n := int(binary.BigEndian.Uint16(body[54:56]))
	if len(body) != 56+n {
		return query.RowKey{}, ErrBadCursor
	}
	p.tenant = string(body[56:])

	if p.tenant != tenant {
		return query.RowKey{}, fmt.Errorf("%w: issued for another tenant", ErrBadCursor)
	}
	if !hmac.Equal(p.queryHash[:], qh[:]) {
		return query.RowKey{}, fmt.Errorf("%w: issued for a different query", ErrBadCursor)
	}
	if p.dir != dir {
		return query.RowKey{}, fmt.Errorf("%w: issued for the %s direction", ErrBadCursor, p.dir)
	}
	return p.key, nil
}

// queryHash identifies the query a cursor belongs to.
//
// Over the QUERY TEXT and the resolved window, not over the parsed tree: the
// window is what makes two textually identical requests different walks, and a
// relative window (`_time:5m`) resolves to a different absolute one on every
// request. Hashing the resolved values means a cursor is bound to the window
// it was issued in, so paging does not silently slide forward as the clock
// moves -- which would return rows the caller already saw and skip rows it had
// not.
func queryHash(src string, from, to int64) [32]byte {
	h := sha256.New()
	h.Write([]byte(src))
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(from))
	h.Write(b[:])
	binary.BigEndian.PutUint64(b[:], uint64(to))
	h.Write(b[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
