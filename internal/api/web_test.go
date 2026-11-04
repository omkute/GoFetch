package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEmbeddedWebGUI(t *testing.T) {
	server := NewServer("127.0.0.1:0")

	// 1. Test GET / serves index.html
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for GET /, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "GoFetch v2") {
		t.Errorf("expected HTML to contain 'GoFetch v2', got:\n%s", body[:min(len(body), 200)])
	}

	// 2. Test GET /style.css
	cssReq := httptest.NewRequest(http.MethodGet, "/style.css", nil)
	cssRec := httptest.NewRecorder()
	server.Router().ServeHTTP(cssRec, cssReq)

	if cssRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for GET /style.css, got %d", cssRec.Code)
	}
	if !strings.Contains(cssRec.Body.String(), "--bg-base") {
		t.Errorf("expected CSS content, got: %s", cssRec.Body.String()[:100])
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
