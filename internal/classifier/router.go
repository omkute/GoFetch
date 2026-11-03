package classifier

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Router classifies completed downloads and moves them into categorized subfolders.
type Router struct {
	baseDir    string
	classifier *Classifier
}

// NewRouter creates a new Router with destination subfolders rooted at baseDir.
func NewRouter(baseDir string) *Router {
	if baseDir == "" {
		baseDir = "."
	}
	return &Router{
		baseDir:    baseDir,
		classifier: NewClassifier(),
	}
}

// Route inspects the file at sourcePath, determines its media Category,
// ensures the destination subfolder exists, and atomically relocates the file via os.Rename.
func (r *Router) Route(sourcePath string) (string, error) {
	if sourcePath == "" {
		return "", errors.New("source path cannot be empty")
	}

	// 1. Determine media category
	category, err := r.classifier.ClassifyFile(sourcePath)
	if err != nil {
		return "", fmt.Errorf("failed to classify file %q: %w", sourcePath, err)
	}

	// 2. Ensure category directory exists
	targetDir := filepath.Join(r.baseDir, string(category))
	if err := os.MkdirAll(targetDir, 0750); err != nil {
		return "", fmt.Errorf("failed to create destination directory %q: %w", targetDir, err)
	}

	// 3. Prevent filename collision in destination folder
	fileName := filepath.Base(sourcePath)
	initialDest := filepath.Join(targetDir, fileName)
	destPath := resolveUniqueName(initialDest)

	// 4. Atomically relocate file
	if err := os.Rename(sourcePath, destPath); err != nil {
		return "", fmt.Errorf("failed to atomically move %q to %q: %w", sourcePath, destPath, err)
	}

	return destPath, nil
}

// resolveUniqueName checks if targetPath exists and appends incrementing suffixes
// (_1, _2, ...) to prevent overwriting existing files in destination folders.
func resolveUniqueName(targetPath string) string {
	if _, err := os.Stat(targetPath); os.IsNotExist(err) {
		return targetPath
	}

	dir := filepath.Dir(targetPath)
	base := filepath.Base(targetPath)
	ext := filepath.Ext(base)
	nameWithoutExt := strings.TrimSuffix(base, ext)

	for i := 1; ; i++ {
		candidate := filepath.Join(dir, fmt.Sprintf("%s_%d%s", nameWithoutExt, i, ext))
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate
		}
	}
}
