package classifier_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/omkute/gofetch/internal/classifier"
)

func TestClassify_MagicBytesDetection(t *testing.T) {
	testCases := []struct {
		name             string
		filename         string
		magicBytes       []byte
		expectedCategory classifier.Category
	}{
		{
			name:             "PDF with misleading .dat extension",
			filename:         "annual_report.dat",
			magicBytes:       []byte("%PDF-1.4 test document content"),
			expectedCategory: classifier.CategoryDocuments,
		},
		{
			name:             "PNG image with .bin extension",
			filename:         "screenshot.bin",
			magicBytes:       []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00},
			expectedCategory: classifier.CategoryImages,
		},
		{
			name:             "JPEG image with .tmp extension",
			filename:         "photo.tmp",
			magicBytes:       []byte{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46},
			expectedCategory: classifier.CategoryImages,
		},
		{
			name:             "ZIP archive with .data extension",
			filename:         "archive.data",
			magicBytes:       []byte{0x50, 0x4b, 0x03, 0x04, 0x14, 0x00},
			expectedCategory: classifier.CategoryArchives,
		},
		{
			name:             "7-Zip archive with generic extension",
			filename:         "backup.dat",
			magicBytes:       []byte{0x37, 0x7a, 0xbc, 0xaf, 0x27, 0x1c},
			expectedCategory: classifier.CategoryArchives,
		},
		{
			name:             "RAR archive with generic extension",
			filename:         "pack.bin",
			magicBytes:       []byte{0x52, 0x61, 0x72, 0x21, 0x1a, 0x07, 0x00},
			expectedCategory: classifier.CategoryArchives,
		},
		{
			name:             "ELF executable with .txt extension",
			filename:         "installer.txt",
			magicBytes:       []byte{0x7f, 0x45, 0x4c, 0x46, 0x02, 0x01, 0x01},
			expectedCategory: classifier.CategorySoftware,
		},
		{
			name:             "Windows PE / EXE executable",
			filename:         "setup.dat",
			magicBytes:       []byte{0x4d, 0x5a, 0x90, 0x00, 0x03, 0x00},
			expectedCategory: classifier.CategorySoftware,
		},
		{
			name:             "MP4 video with .bin extension",
			filename:         "clip.bin",
			magicBytes:       []byte{0x00, 0x00, 0x00, 0x18, 0x66, 0x74, 0x79, 0x70, 0x69, 0x73, 0x6f, 0x6d},
			expectedCategory: classifier.CategoryVideos,
		},
		{
			name:             "MP3 audio with ID3 tag",
			filename:         "track.bin",
			magicBytes:       []byte{0x49, 0x44, 0x33, 0x03, 0x00, 0x00},
			expectedCategory: classifier.CategoryAudio,
		},
	}

	tempDir := t.TempDir()
	c := classifier.NewClassifier()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, tc.filename)
			if err := os.WriteFile(filePath, tc.magicBytes, 0600); err != nil {
				t.Fatalf("failed to write test file: %v", err)
			}

			cat, err := c.ClassifyFile(filePath)
			if err != nil {
				t.Fatalf("ClassifyFile failed: %v", err)
			}

			if cat != tc.expectedCategory {
				t.Errorf("ClassifyFile(%q) = %q; expected %q", tc.filename, cat, tc.expectedCategory)
			}
		})
	}
}

func TestClassify_ExtensionFallback(t *testing.T) {
	testCases := []struct {
		filename         string
		expectedCategory classifier.Category
	}{
		{"notes.docx", classifier.CategoryDocuments},
		{"spreadsheet.xlsx", classifier.CategoryDocuments},
		{"movie.mkv", classifier.CategoryVideos},
		{"song.flac", classifier.CategoryAudio},
		{"package.tar.gz", classifier.CategoryArchives},
		{"app.dmg", classifier.CategorySoftware},
		{"binary.deb", classifier.CategorySoftware},
		{"random.xyz123", classifier.CategoryMiscellaneous},
	}

	tempDir := t.TempDir()
	c := classifier.NewClassifier()

	for _, tc := range testCases {
		t.Run(tc.filename, func(t *testing.T) {
			filePath := filepath.Join(tempDir, tc.filename)
			// Text-only content without binary magic bytes
			if err := os.WriteFile(filePath, []byte("empty text placeholder"), 0600); err != nil {
				t.Fatalf("failed to write test file: %v", err)
			}

			cat, err := c.ClassifyFile(filePath)
			if err != nil {
				t.Fatalf("ClassifyFile failed: %v", err)
			}

			if cat != tc.expectedCategory {
				t.Errorf("ClassifyFile(%q) = %q; expected %q", tc.filename, cat, tc.expectedCategory)
			}
		})
	}
}
