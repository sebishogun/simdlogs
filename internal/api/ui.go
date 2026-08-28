package api

import (
	_ "embed"
	"net/http"
	"strings"
)

//go:embed ui.html
var uiHTML []byte

// ui serves the single-page LogsQL explorer -- the vmui equivalent. It is
// self-contained (inline CSS/JS, no external assets) and drives the same JSON
// endpoints the API already exposes, so it needs no server-side rendering.
func (s *Server) ui(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/vmui" && r.URL.Path != "/select/vmui" {
		http.NotFound(w, r) // "/" is a catch-all in ServeMux; only serve the UI on its own paths
		return
	}
	setUISecurityHeaders(w)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(uiHTML)
}

// setUISecurityHeaders is the browser-side hardening for the one HTML page
// this server serves.
//
// It had none. That matters more here than on a typical page, because this one
// renders LOG CONTENT -- arbitrary attacker-influenced strings that arrived
// through an ingest endpoint -- into a table. The renderer escapes them, and a
// CSP is what stands between an escaping bug and script execution with the
// operator's session.
func setUISecurityHeaders(w http.ResponseWriter) {
	h := w.Header()
	// The page is entirely self-contained: inline CSS and one inline script,
	// no external assets. So everything is `self` except the two inline
	// blocks, and nothing may be fetched, framed or connected to elsewhere.
	//
	// 'unsafe-inline' is required by the inline <style> and <script> and is
	// stated rather than worked around: the alternative is a nonce, which
	// means the page can no longer be a static embedded byte slice served
	// without templating. What the policy still buys with it: no remote
	// script, no eval, no exfiltration target, no framing.
	h.Set("Content-Security-Policy", strings.Join([]string{
		"default-src 'none'",
		"script-src 'self' 'unsafe-inline'",
		"style-src 'self' 'unsafe-inline'",
		"connect-src 'self'",
		"img-src 'self' data:",
		"form-action 'none'",
		"base-uri 'none'",
		"frame-ancestors 'none'",
	}, "; "))
	// frame-ancestors covers modern browsers; X-Frame-Options is for the ones
	// that do not implement it. Clickjacking a log explorer is a route to the
	// admin actions it can reach.
	h.Set("X-Frame-Options", "DENY")
	// No MIME sniffing: a log line that looks like HTML must not be treated as
	// HTML because a browser guessed.
	h.Set("X-Content-Type-Options", "nosniff")
	// The query is in the URL, and a query is a search over someone's logs.
	// Sending it to a third party in a Referer header is a data leak in a
	// header nobody thinks about.
	h.Set("Referrer-Policy", "no-referrer")
	// The page needs no device access at all; saying so costs nothing and
	// closes anything a future edit might accidentally reach for.
	h.Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
}
