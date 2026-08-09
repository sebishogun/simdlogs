package api

import (
	_ "embed"
	"net/http"
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
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(uiHTML)
}
