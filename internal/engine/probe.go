package engine

import (
	"context"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
)

// DefaultFilename is used when no valid filename can be parsed or sanitized.
const DefaultFilename = "download"

// ResourceInfo represents metadata about a remote resource discovered during probe.
type ResourceInfo struct {
	URL           string
	FinalURL      string
	Filename      string
	ContentLength int64
	ContentType   string
	AcceptRanges  bool
	ETag          string
	LastModified  string
}

// SanitizeFilename strips path traversal sequences, null bytes, slashes, and control
// characters from a proposed filename, ensuring tenant and filesystem isolation.
func SanitizeFilename(raw string) string {
	// 1. Remove null bytes and non-printable control characters
	cleaned := strings.Map(func(r rune) rune {
		if r == 0 || unicode.IsControl(r) {
			return -1
		}
		return r
	}, raw)

	// 2. Normalize Windows backslashes to forward slashes before stripping directory paths
	cleaned = strings.ReplaceAll(cleaned, `\`, `/`)

	// 3. Extract purely the base filename
	base := path.Base(cleaned)

	// 4. Strip leading/trailing whitespaces, quotes, and dots
	base = strings.Trim(base, ` "'`)
	base = strings.TrimLeft(base, ".")

	// 5. Check if resulting filename is empty, root, or traversal
	if base == "" || base == "." || base == "/" || base == ".." {
		return DefaultFilename
	}

	return base
}

// extractFilename extracts the most accurate filename from Content-Disposition header
// or falls back to the URL path.
func extractFilename(header string, rawURL string) string {
	if header != "" {
		if _, params, err := mime.ParseMediaType(header); err == nil {
			if fn, ok := params["filename*"]; ok && fn != "" {
				// Format might be UTF-8''filename or plain
				parts := strings.SplitN(fn, "''", 2)
				if len(parts) == 2 {
					if unescaped, err := url.PathUnescape(parts[1]); err == nil {
						return SanitizeFilename(unescaped)
					}
				}
				return SanitizeFilename(fn)
			}
			if fn, ok := params["filename"]; ok && fn != "" {
				return SanitizeFilename(fn)
			}
		}
	}

	if parsedURL, err := url.Parse(rawURL); err == nil {
		urlPath := strings.TrimRight(parsedURL.Path, "/")
		if urlPath != "" {
			return SanitizeFilename(path.Base(urlPath))
		}
	}

	return DefaultFilename
}

// Probe inspects a remote URL using HTTP HEAD (falling back to GET if unsupported)
// and extracts content metadata, range support, and a sanitized target filename.
func Probe(ctx context.Context, client *http.Client, targetURL string) (*ResourceInfo, error) {
	if client == nil {
		client = http.DefaultClient
	}

	// 1. First attempt HEAD request
	headReq, err := http.NewRequestWithContext(ctx, http.MethodHead, targetURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HEAD request: %w", err)
	}
	headReq.Header.Set("User-Agent", "GoFetch/1.0")

	resp, err := client.Do(headReq)
	if err == nil {
		defer func() {
			_ = resp.Body.Close()
		}()

		// If HEAD was accepted, parse metadata
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return parseResponseMetadata(resp, targetURL), nil
		}
	}

	// 2. If HEAD failed or server returned 405/501, fallback to a GET request
	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create fallback GET request: %w", err)
	}
	getReq.Header.Set("User-Agent", "GoFetch/1.0")
	// Request a single byte to avoid streaming entire body during probe if Range is supported
	getReq.Header.Set("Range", "bytes=0-0")

	getResp, err := client.Do(getReq)
	if err != nil {
		return nil, fmt.Errorf("probe fallback GET failed: %w", err)
	}
	defer func() {
		_ = getResp.Body.Close()
	}()

	if getResp.StatusCode < 200 || getResp.StatusCode >= 400 {
		return nil, fmt.Errorf("probe request returned unexpected status %d: %s", getResp.StatusCode, getResp.Status)
	}

	return parseResponseMetadata(getResp, targetURL), nil
}

func parseResponseMetadata(resp *http.Response, originalURL string) *ResourceInfo {
	finalURL := originalURL
	if resp.Request != nil && resp.Request.URL != nil {
		finalURL = resp.Request.URL.String()
	}

	acceptRanges := strings.EqualFold(strings.TrimSpace(resp.Header.Get("Accept-Ranges")), "bytes") ||
		resp.StatusCode == http.StatusPartialContent

	contentLength := resp.ContentLength
	if contentLength <= 0 {
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			if val, err := strconv.ParseInt(strings.TrimSpace(cl), 10, 64); err == nil && val >= 0 {
				contentLength = val
			}
		}
	}
	// Check Content-Range if status was 206 (e.g., Content-Range: bytes 0-0/1024)
	if resp.StatusCode == http.StatusPartialContent {
		cr := resp.Header.Get("Content-Range")
		if slashIdx := strings.LastIndex(cr, "/"); slashIdx != -1 {
			var total int64
			if _, err := fmt.Sscanf(cr[slashIdx+1:], "%d", &total); err == nil && total > 0 {
				contentLength = total
			}
		}
	}

	contentDisposition := resp.Header.Get("Content-Disposition")
	filename := extractFilename(contentDisposition, finalURL)

	return &ResourceInfo{
		URL:           originalURL,
		FinalURL:      finalURL,
		Filename:      filename,
		ContentLength: contentLength,
		ContentType:   resp.Header.Get("Content-Type"),
		AcceptRanges:  acceptRanges,
		ETag:          resp.Header.Get("ETag"),
		LastModified:  resp.Header.Get("LastModified"),
	}
}

// CleanDestinationPath resolves the destination directory to an absolute path.
func CleanDestinationPath(destDir string) (string, error) {
	if destDir == "" {
		destDir = "."
	}
	absDir, err := filepath.Abs(destDir)
	if err != nil {
		return "", fmt.Errorf("failed to resolve absolute path for %q: %w", destDir, err)
	}
	return absDir, nil
}
