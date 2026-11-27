package engine_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/engine"
	"github.com/omkute/gofetch/internal/storage"
)

func TestStagingPathFor(t *testing.T) {
	finalPath := filepath.Join("dest", "download.bin")
	legacyPath := finalPath + storage.PartFileExtension

	t.Run("empty job ID preserves legacy path", func(t *testing.T) {
		if got := engine.StagingPathFor(finalPath, ""); got != legacyPath {
			t.Fatalf("StagingPathFor(%q, \"\") = %q, want %q", finalPath, got, legacyPath)
		}
	})

	t.Run("job ID is deterministic, unique, and path-safe", func(t *testing.T) {
		expectedFor := func(jobID string) string {
			sum := sha256.Sum256([]byte(jobID))
			return finalPath + "." + hex.EncodeToString(sum[:8]) + storage.PartFileExtension
		}

		jobA, jobB := "job-a", "job-b"
		pathA, pathB := expectedFor(jobA), expectedFor(jobB)
		if pathA == pathB {
			t.Fatalf("different job IDs produced the same staging path %q", pathA)
		}
		for i := 0; i < 3; i++ {
			if got := engine.StagingPathFor(finalPath, jobA); got != pathA {
				t.Fatalf("StagingPathFor(%q, %q) = %q, want %q", finalPath, jobA, got, pathA)
			}
			if got := engine.StagingPathFor(finalPath, jobB); got != pathB {
				t.Fatalf("StagingPathFor(%q, %q) = %q, want %q", finalPath, jobB, got, pathB)
			}
		}
		if !strings.HasPrefix(pathA, finalPath+".") || !strings.HasSuffix(pathA, storage.PartFileExtension) {
			t.Fatalf("unexpected staging path layout: %q", pathA)
		}

		traversal := engine.StagingPathFor(finalPath, "../../etc/passwd")
		if filepath.Dir(traversal) != filepath.Dir(finalPath) {
			t.Fatalf("job ID leaked into the directory of staging path %q", traversal)
		}
	})
}

// Two jobs with distinct IDs writing the same filename into the same directory
// must stage into separate files. The origin holds Range responses until both
// transfers are in flight, so a shared .gofetch.part would race on WriteAt and
// one job would rename the other's staging file away before commit.
func TestDownloadWithOptions_ConcurrentSameFilename_IsolatedStaging(t *testing.T) {
	payload := bytes.Repeat([]byte("GoFetch concurrent staging collision payload! "), 4096)
	const fileName = "collision.bin"

	const userAgentA = "gofetch-staging-test-a"
	const userAgentB = "gofetch-staging-test-b"

	release := make(chan struct{})
	rangeA := make(chan struct{}, 8)
	rangeB := make(chan struct{}, 8)

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.Header.Get("Range") != "" {
			switch r.Header.Get("User-Agent") {
			case userAgentA:
				select {
				case rangeA <- struct{}{}:
				default:
				}
			case userAgentB:
				select {
				case rangeB <- struct{}{}:
				default:
				}
			}
			<-release
		}
		http.ServeContent(w, r, fileName, time.Time{}, bytes.NewReader(payload))
	}))
	defer origin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	probe, err := engine.Probe(ctx, origin.Client(), origin.URL+"/"+fileName)
	if err != nil {
		t.Fatalf("Probe failed: %v", err)
	}
	if !probe.AcceptRanges || probe.ContentLength != int64(len(payload)) {
		t.Fatalf("expected a range-capable origin with size %d, got %+v", len(payload), probe)
	}

	destDir := t.TempDir()
	finalPath := filepath.Join(destDir, fileName)
	jobs := []struct {
		id        string
		userAgent string
	}{
		{id: "collision-job-a", userAgent: userAgentA},
		{id: "collision-job-b", userAgent: userAgentB},
	}

	results := make(chan error, len(jobs))
	for _, job := range jobs {
		job := job
		coordinator := engine.NewCoordinator(origin.Client())
		go func() {
			path, dlErr := coordinator.DownloadWithOptions(ctx, probe, engine.DownloadOptions{
				DestDir:    destDir,
				JobID:      job.id,
				MaxWorkers: 4,
				UserAgent:  job.userAgent,
			})
			if dlErr == nil && path != finalPath {
				dlErr = fmt.Errorf("returned path %q, want %q", path, finalPath)
			}
			results <- dlErr
		}()
	}

	// Both transfers must be live (and blocked in the origin) before either
	// can reach its commit step.
	for _, ch := range []<-chan struct{}{rangeA, rangeB} {
		select {
		case <-ch:
		case <-ctx.Done():
			close(release)
			t.Fatal("timed out waiting for both jobs to issue Range requests")
		}
	}

	stagingA := engine.StagingPathFor(finalPath, jobs[0].id)
	stagingB := engine.StagingPathFor(finalPath, jobs[1].id)
	if stagingA == stagingB {
		close(release)
		t.Fatalf("jobs share staging path %q", stagingA)
	}
	for _, staging := range []string{stagingA, stagingB} {
		info, statErr := os.Stat(staging)
		if statErr != nil {
			close(release)
			t.Fatalf("expected distinct staged file %q while both jobs are in flight: %v", staging, statErr)
		}
		if info.Size() != int64(len(payload)) {
			close(release)
			t.Fatalf("staged file %q has size %d, want %d", staging, info.Size(), len(payload))
		}
	}

	close(release)

	for i := 0; i < len(jobs); i++ {
		select {
		case dlErr := <-results:
			if dlErr != nil {
				t.Errorf("concurrent download %d failed: %v", i, dlErr)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for concurrent downloads to finish")
		}
	}

	committed, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("failed to read committed file %q: %v", finalPath, err)
	}
	if !bytes.Equal(committed, payload) {
		t.Fatalf("committed file has %d bytes, want the exact %d-byte payload", len(committed), len(payload))
	}
	for _, staging := range []string{stagingA, stagingB} {
		if _, statErr := os.Stat(staging); !os.IsNotExist(statErr) {
			t.Errorf("staging file %q should be removed after commit: %v", staging, statErr)
		}
	}
}
