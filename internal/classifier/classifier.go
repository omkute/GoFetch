package classifier

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

// Classifier detects media categories based on magic byte sniffing and extension fallback.
type Classifier struct{}

// NewClassifier creates a new Classifier instance.
func NewClassifier() *Classifier {
	return &Classifier{}
}

// ClassifyFile inspects the given file's magic bytes and extension to determine its Category.
func (c *Classifier) ClassifyFile(filePath string) (Category, error) {
	if filePath == "" {
		return CategoryMiscellaneous, errors.New("file path cannot be empty")
	}

	f, err := os.Open(filePath)
	if err != nil {
		return CategoryMiscellaneous, fmt.Errorf("failed to open file %q: %w", filePath, err)
	}
	defer func() {
		_ = f.Close()
	}()

	// Read up to 512 bytes for MIME and magic byte sniffing
	buffer := make([]byte, 512)
	n, readErr := io.ReadFull(f, buffer)
	if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
		return CategoryMiscellaneous, fmt.Errorf("failed reading file header %q: %w", filePath, readErr)
	}

	header := buffer[:n]

	// 1. First priority: Check custom binary magic bytes signatures
	for _, rule := range magicRules {
		if rule.match(header) {
			return rule.category, nil
		}
	}

	// 2. Second priority: Standard net/http sniffing
	if len(header) > 0 {
		mimeType := strings.ToLower(http.DetectContentType(header))
		switch {
		case strings.HasPrefix(mimeType, "video/"):
			return CategoryVideos, nil
		case strings.HasPrefix(mimeType, "audio/"):
			return CategoryAudio, nil
		case strings.HasPrefix(mimeType, "image/"):
			return CategoryImages, nil
		case mimeType == "application/pdf":
			return CategoryDocuments, nil
		case mimeType == "application/zip", mimeType == "application/x-gzip":
			return CategoryArchives, nil
		}
	}

	// 3. Third priority: Extension fallback
	return MatchExtension(filePath), nil
}
