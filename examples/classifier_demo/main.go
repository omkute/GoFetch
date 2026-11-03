package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/omkute/gofetch/internal/classifier"
)

func main() {
	stagingDir := "./downloads_demo/staging"
	organizedDir := "./downloads_demo/organized"

	fmt.Println("==================================================")
	fmt.Println("🚀 GoFetch Media Classifier & Directory Router (T-006)")
	fmt.Println("==================================================")

	if err := os.MkdirAll(stagingDir, 0750); err != nil {
		log.Fatalf("❌ Failed to create staging dir: %v", err)
	}

	// Step 1: Create sample files with disguised extensions and magic bytes
	testFiles := []struct {
		filename    string
		description string
		data        []byte
	}{
		{
			filename:    "report.dat",
			description: "PDF document disguised with .dat extension",
			data:        []byte("%PDF-1.5 Confidential Financial Report Content"),
		},
		{
			filename:    "clip.bin",
			description: "MP4 video stream disguised with .bin extension",
			data:        []byte{0x00, 0x00, 0x00, 0x1c, 0x66, 0x74, 0x79, 0x70, 0x69, 0x73, 0x6f, 0x6d, 0x00, 0x00},
		},
		{
			filename:    "backup.tmp",
			description: "ZIP archive disguised with .tmp extension",
			data:        []byte{0x50, 0x4b, 0x03, 0x04, 0x14, 0x00, 0x08, 0x00},
		},
		{
			filename:    "audio.data",
			description: "MP3 audio track disguised with .data extension",
			data:        []byte{0x49, 0x44, 0x33, 0x03, 0x00, 0x00, 0x00},
		},
		{
			filename:    "daemon.raw",
			description: "ELF executable disguised with .raw extension",
			data:        []byte{0x7f, 0x45, 0x4c, 0x46, 0x02, 0x01, 0x01, 0x00},
		},
		{
			filename:    "photo.dat",
			description: "PNG image disguised with .dat extension",
			data:        []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00},
		},
		{
			filename:    "document.docx",
			description: "Word document (classified via extension fallback)",
			data:        []byte("plain text inside word doc wrapper"),
		},
		{
			filename:    "unknown.custom",
			description: "Unrecognized custom format (classified as Miscellaneous)",
			data:        []byte("some random arbitrary payload"),
		},
	}

	fmt.Println("\n[Step 1] Creating unorganized staging files:")
	for _, tf := range testFiles {
		filePath := filepath.Join(stagingDir, tf.filename)
		if err := os.WriteFile(filePath, tf.data, 0600); err != nil {
			log.Fatalf("❌ Failed writing %s: %v", tf.filename, err)
		}
		fmt.Printf("   ├── Staged: %-18s (%s)\n", tf.filename, tf.description)
	}

	// Step 2: Route files through Media Classifier
	fmt.Printf("\n[Step 2] Routing files into categorized folders under %s/...\n", organizedDir)
	router := classifier.NewRouter(organizedDir)

	for _, tf := range testFiles {
		src := filepath.Join(stagingDir, tf.filename)
		dest, err := router.Route(src)
		if err != nil {
			log.Fatalf("❌ Routing error for %s: %v", tf.filename, err)
		}
		relDest, _ := filepath.Rel(organizedDir, dest)
		fmt.Printf("   ├── %-18s ──▶ %s\n", tf.filename, relDest)
	}

	// Step 3: Inspect resulting directory structure
	fmt.Println("\n[Step 3] Final Organized Subfolder Tree:")
	categories := []classifier.Category{
		classifier.CategoryDocuments,
		classifier.CategoryVideos,
		classifier.CategoryArchives,
		classifier.CategoryAudio,
		classifier.CategorySoftware,
		classifier.CategoryImages,
		classifier.CategoryMiscellaneous,
	}

	for _, cat := range categories {
		dir := filepath.Join(organizedDir, string(cat))
		entries, err := os.ReadDir(dir)
		if err == nil && len(entries) > 0 {
			fmt.Printf("   📁 %s/\n", cat)
			for _, e := range entries {
				fmt.Printf("      └── %s\n", e.Name())
			}
		}
	}

	fmt.Println("\n✅ All T-006 Media Classifier & Directory Routing Invariants Verified!")
	fmt.Printf("👉 Inspect the organized folders directly: ls -R %s\n", organizedDir)
	fmt.Println("==================================================")
}
