package classifier_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/omkute/gofetch/internal/classifier"
)

func TestRouter_RouteFile(t *testing.T) {
	tempDir := t.TempDir()
	sourceFile := filepath.Join(tempDir, "manual.pdf")
	pdfContent := []byte("%PDF-1.5 instruction manual content")

	if err := os.WriteFile(sourceFile, pdfContent, 0600); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}

	router := classifier.NewRouter(tempDir)
	destPath, err := router.Route(sourceFile)
	if err != nil {
		t.Fatalf("Route failed: %v", err)
	}

	expectedPath := filepath.Join(tempDir, string(classifier.CategoryDocuments), "manual.pdf")
	if destPath != expectedPath {
		t.Fatalf("expected destPath %q, got %q", expectedPath, destPath)
	}

	// Verify file was moved and exists in Documents
	if _, err := os.Stat(expectedPath); err != nil {
		t.Fatalf("dest file %q does not exist: %v", expectedPath, err)
	}

	// Verify original file in root is gone
	if _, err := os.Stat(sourceFile); !os.IsNotExist(err) {
		t.Fatalf("source file %q still exists in root folder", sourceFile)
	}
}

func TestRouter_DisguisedFileRoute(t *testing.T) {
	tempDir := t.TempDir()
	sourceFile := filepath.Join(tempDir, "trailer.bin")
	mp4Magic := []byte{0x00, 0x00, 0x00, 0x20, 0x66, 0x74, 0x79, 0x70, 0x69, 0x73, 0x6f, 0x6d}

	if err := os.WriteFile(sourceFile, mp4Magic, 0600); err != nil {
		t.Fatalf("failed to write mp4 file: %v", err)
	}

	router := classifier.NewRouter(tempDir)
	destPath, err := router.Route(sourceFile)
	if err != nil {
		t.Fatalf("Route failed: %v", err)
	}

	expectedPath := filepath.Join(tempDir, string(classifier.CategoryVideos), "trailer.bin")
	if destPath != expectedPath {
		t.Fatalf("expected destPath %q, got %q", expectedPath, destPath)
	}

	if _, err := os.Stat(expectedPath); err != nil {
		t.Fatalf("expected file to exist in Videos directory: %v", err)
	}
}

func TestRouter_NameCollisionAvoidance(t *testing.T) {
	tempDir := t.TempDir()
	docsDir := filepath.Join(tempDir, string(classifier.CategoryDocuments))
	if err := os.MkdirAll(docsDir, 0750); err != nil {
		t.Fatalf("failed to create documents dir: %v", err)
	}

	// Existing file in Documents/report.pdf
	existingFile := filepath.Join(docsDir, "report.pdf")
	if err := os.WriteFile(existingFile, []byte("existing report v1"), 0600); err != nil {
		t.Fatalf("failed to seed existing file: %v", err)
	}

	// New file incoming
	newFile := filepath.Join(tempDir, "report.pdf")
	if err := os.WriteFile(newFile, []byte("%PDF-1.4 new report v2"), 0600); err != nil {
		t.Fatalf("failed to write new file: %v", err)
	}

	router := classifier.NewRouter(tempDir)
	destPath, err := router.Route(newFile)
	if err != nil {
		t.Fatalf("Route failed: %v", err)
	}

	expectedRenamed := filepath.Join(docsDir, "report_1.pdf")
	if destPath != expectedRenamed {
		t.Fatalf("expected collision-renamed path %q, got %q", expectedRenamed, destPath)
	}

	// Original existing file remains unmodified
	origContent, err := os.ReadFile(existingFile)
	if err != nil || string(origContent) != "existing report v1" {
		t.Fatalf("original file was corrupted or overwritten!")
	}
}
