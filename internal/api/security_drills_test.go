package api

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// Security drills.
//
// Each of these is an attack a deployed server will actually see, written as
// the attacker would perform it rather than as an assertion about a helper.
// Where a drill is already covered elsewhere it is named here rather than
// duplicated, because a second copy of a test is a second thing to keep in step
// and no extra coverage:
//
//	oversized request bodies  -- TestGuardRejectsOversizedBody (middleware_test)
//	decompression bombs       -- TestGuardRejectsDecompressionBomb (middleware_test)
//	oversized syslog frames   -- TestSyslogOversizedFrameIsRejected (syslog_contract_test)
//	malformed tenant headers  -- TestMalformedTenantIsRejected (auth_test)
//
// The four below had no drill.

// Drill 1: tenant escape.
//
// A credential scoped to one tenant must not read another's data, by any route.
// This is the failure with the worst blast radius in a multi-tenant log store:
// logs carry credentials, session identifiers and personal data, and a cross
// tenant read is a disclosure that leaves no trace in the victim's tenant.
func TestDrillATenantCannotReadAnother(t *testing.T) {
	_, ts := authedServer(t)

	// Tenant 0 stores something identifiable.
	resp := do(t, ts, http.MethodPost, "/insert/jsonline", tokIngest, nil,
		`{"_time":"2026-06-01T12:00:00Z","_msg":"tenant zero secret","k":"v"}`+"\n")
	resp.Body.Close()

	// tokOther is scoped to tenant 7:0. Every read route it can reach must
	// answer for 7, never for 0.
	for _, path := range []string{
		"/select/logsql/query?query=%2A",
		"/select/logsql/field_values?query=%2A&field=k",
		"/select/logsql/facets?query=%2A",
		"/select/sql?query=SELECT%20%2A%20FROM%20logs",
	} {
		t.Run(path, func(t *testing.T) {
			// Asking as tenant 7 with tenant 7's credential.
			resp := do(t, ts, http.MethodGet, path, tokOther, nil, "")
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if strings.Contains(string(b), "tenant zero secret") {
				t.Fatalf("tenant 7's credential read tenant 0's data: %.300s", b)
			}

			// And explicitly claiming tenant 0 must be refused, not honoured.
			resp = do(t, ts, http.MethodGet, path, tokOther,
				map[string]string{"AccountID": "0", "ProjectID": "0"}, "")
			b, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK &&
				strings.Contains(string(b), "tenant zero secret") {
				t.Fatalf("claiming AccountID 0 with tenant 7's credential returned "+
					"tenant 0's data: %d %.300s", resp.StatusCode, b)
			}
		})
	}
}

// Drill 2: role bypass.
//
// A read credential must not reach a write or admin route, and an ingest
// credential must not read. Roles that are checked on some routes and not
// others are the usual shape: the check lives in a wrapper, and a route added
// later gets the wrong wrapper or none.
func TestDrillARoleCannotReachAnotherRolesRoutes(t *testing.T) {
	_, ts := authedServer(t)

	cases := []struct {
		name, method, path, token, body, ctype string
	}{
		{"query token on ingest", http.MethodPost, "/insert/jsonline", tokQuery,
			`{"_msg":"x"}` + "\n", "application/x-ndjson"},
		{"query token on admin backup", http.MethodGet, "/admin/backup", tokQuery, "", ""},
		{"query token on cluster repair", http.MethodPost, "/admin/cluster/repair", tokQuery, "", ""},
		{"query token on replica state", http.MethodGet, pathReplicaState, tokQuery, "", ""},
		{"query token on replica group", http.MethodPost, pathReplicaGroup + "?digest=abcd",
			tokQuery, "junk", "application/octet-stream"},
		{"ingest token on query", http.MethodGet, "/select/logsql/query?query=%2A", tokIngest, "", ""},
		{"ingest token on admin backup", http.MethodGet, "/admin/backup", tokIngest, "", ""},
		{"no token at all on admin", http.MethodGet, "/admin/backup", "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hdr := map[string]string{}
			if tc.ctype != "" {
				hdr["Content-Type"] = tc.ctype
			}
			resp := do(t, ts, tc.method, tc.path, tc.token, hdr, tc.body)
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized &&
				resp.StatusCode != http.StatusForbidden {
				t.Fatalf("%s %s answered %d, want 401 or 403: %.200s",
					tc.method, tc.path, resp.StatusCode, b)
			}
		})
	}
}

// Drill 3: header spoofing of the internal protocol.
//
// X-Simdlogs-Internal is what makes a storage node answer with the cluster
// envelope instead of the public response. A client that could set it would get
// a different response shape from the documented one, and -- more to the point
// -- the internal replica endpoints must not become reachable just because a
// header says so. Authorization is what protects them, not the header.
func TestDrillAClientCannotForgeTheInternalProtocolHeader(t *testing.T) {
	_, ts := authedServer(t)

	internal := map[string]string{
		HdrInternal:        "1",
		HdrProtocolVersion: "1",
		// A forged completeness claim: if a router ever trusted an INBOUND
		// completeness header rather than computing its own, a client could
		// declare its own partial answer complete.
		HdrComplete:      "true",
		HdrHighWatermark: "9223372036854775807",
	}

	// With no credential, the header changes nothing.
	resp := do(t, ts, http.MethodGet, pathReplicaState, "", internal, "")
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
		t.Fatalf("the internal header opened %s without a credential: %d %.200s",
			pathReplicaState, resp.StatusCode, b)
	}

	// With a QUERY credential, likewise: the endpoint is admin.
	resp = do(t, ts, http.MethodGet, pathReplicaState, tokQuery, internal, "")
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
		t.Fatalf("the internal header let a query credential reach %s: %d %.200s",
			pathReplicaState, resp.StatusCode, b)
	}

	// On a PUBLIC route the header is accepted -- it only selects the envelope
	// -- but the response must still be the caller's own tenant's data and must
	// not carry a completeness claim the caller supplied.
	resp = do(t, ts, http.MethodGet, "/select/logsql/query?query=%2A", tokQuery, internal, "")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if got := resp.Header.Get(HdrHighWatermark); got == "9223372036854775807" {
		t.Fatalf("the response echoed the caller's forged high watermark %q; a "+
			"router merging that would believe this node's data runs to the end "+
			"of time", got)
	}
}

// Drill 4: a rejected credential must not reveal how close it was.
//
// Not a timing measurement. A wall-clock timing assertion in a test suite is
// noise, and one that passes on a quiet machine and fails under load is worse
// than none.
//
// What makes prefix timing useless here is the design rather than a comparison
// primitive: a presented token is SHA-256'd and the hash is looked up in a map,
// so an attacker attacking the comparison would have to steer the digest, which
// needs the token they are trying to find. What is left to check is the
// observable channel -- that two wrong guesses are answered identically however
// close either was.
func TestDrillARejectedCredentialRevealsNothing(t *testing.T) {
	_, ts := authedServer(t)

	// Credentials that are PRESENTED and rejected. Every one of these must be
	// answered identically: a body naming the reason, or a differing status,
	// separates "not a token" from "a token that is nearly right".
	rejected := map[string]string{
		"one char":           "x",
		"right length wrong": strings.Repeat("z", len(tokQuery)),
		"long shared prefix": tokQuery[:len(tokQuery)-1] + "X",
		"one char too long":  tokQuery + "X",
		"one char too short": tokQuery[:len(tokQuery)-1],
		"case flipped":       strings.ToUpper(tokQuery),
	}

	type answer struct {
		code int
		body string
	}
	ask := func(tok string) answer {
		t.Helper()
		resp := do(t, ts, http.MethodGet, "/select/logsql/query?query=%2A", tok, nil, "")
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return answer{resp.StatusCode, strings.TrimSpace(string(b))}
	}

	var firstName string
	var first answer
	for name, tok := range rejected {
		got := ask(tok)
		if firstName == "" {
			firstName, first = name, got
			continue
		}
		if got != first {
			t.Errorf("%q answered %d %q and %q answered %d %q: the difference tells "+
				"an attacker which guess was closer",
				name, got.code, got.body, firstName, first.code, first.body)
		}
	}
	if first.code != http.StatusUnauthorized && first.code != http.StatusForbidden {
		t.Errorf("a rejected credential answered %d, want 401 or 403", first.code)
	}

	// A VALID token that lacks the role is allowed to answer differently, and
	// does: 403 naming the principal and the missing role. That is
	// authorization, not authentication -- the holder of a real credential
	// already knows which roles it has, so nothing about the token VALUE
	// leaks, and an operator debugging a misconfigured shipper needs exactly
	// this sentence.
	wrongRole := ask(tokIngest)
	if wrongRole.code != http.StatusForbidden {
		t.Errorf("a valid token on a route it lacks the role for answered %d, want 403",
			wrongRole.code)
	}
	if wrongRole == first {
		t.Error("a valid token with the wrong role is answered identically to an " +
			"invalid one; an operator cannot tell a misconfigured role from a bad token")
	}

	// Presenting NO credential is allowed to answer differently, and does:
	// "authentication required" rather than "invalid credential". That leaks
	// nothing -- the caller already knows whether it sent one -- and it is the
	// difference an operator needs when a client is misconfigured.
	none := ask("")
	if none.code != http.StatusUnauthorized {
		t.Errorf("no credential answered %d, want 401", none.code)
	}

	// Surrounding whitespace is trimmed, so " token " is accepted. That is
	// deliberate: bearerToken trims for drop-in compatibility with clients that
	// add a stray space, and the trim is by position, not by content -- it
	// cannot branch on how close a guess was. Asserted rather than left
	// unstated, because a reader finding it later cannot tell leniency from an
	// accident.
	if padded := ask(" " + tokQuery + " "); padded.code != http.StatusOK {
		t.Errorf("a whitespace-padded valid token answered %d; bearerToken's trim "+
			"is deliberate and this is where that is written down", padded.code)
	}
}
