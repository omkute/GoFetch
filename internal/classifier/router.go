package classifier

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// Router classifies completed downloads and moves them into categorized subfolders.
type Router struct {
	baseDir       string
	classifier    *Classifier
	categoryPaths map[string]string
}

// NewRouter creates a new Router with destination subfolders rooted at baseDir.
// Categories use their built-in folder names.
func NewRouter(baseDir string) *Router {
	if baseDir == "" {
		baseDir = "."
	}
	return &Router{
		baseDir:    baseDir,
		classifier: NewClassifier(),
	}
}

// NewRouterWithPaths creates a Router rooted at baseDir with optional per-category
// folder names. Keys are category names (e.g. "Videos"); values are folder names
// relative to baseDir. Categories without an override keep their built-in folder
// name. Every configured value is validated before the Router is returned, so an
// unsafe mapping can never cause a directory to be created outside baseDir.
func NewRouterWithPaths(baseDir string, categoryPaths map[string]string) (*Router, error) {
	if err := ValidateCategoryPaths(categoryPaths); err != nil {
		return nil, err
	}

	router := NewRouter(baseDir)
	if len(categoryPaths) > 0 {
		router.categoryPaths = make(map[string]string, len(categoryPaths))
		for category, folder := range categoryPaths {
			if folder != "" {
				router.categoryPaths[category] = folder
			}
		}
	}
	return router, nil
}

// ValidateCategoryPaths checks that every configured folder name is a safe
// relative path: absolute paths, ".." segments, and null bytes are rejected.
func ValidateCategoryPaths(categoryPaths map[string]string) error {
	for category, folder := range categoryPaths {
		if folder == "" {
			continue // empty value means "no override"
		}
		if strings.ContainsRune(folder, 0) {
			return fmt.Errorf("category %q path %q contains a null byte", category, folder)
		}
		normalized := strings.ReplaceAll(folder, `\`, "/")
		if filepath.IsAbs(folder) || path.IsAbs(normalized) || hasWindowsDrive(normalized) {
			return fmt.Errorf("category %q path %q must be relative, not absolute", category, folder)
		}
		for _, segment := range strings.Split(normalized, "/") {
			if segment == ".." {
				return fmt.Errorf("category %q path %q contains a parent-directory segment", category, folder)
			}
		}
	}
	return nil
}

func hasWindowsDrive(value string) bool {
	return len(value) >= 2 && value[1] == ':' &&
		((value[0] >= 'a' && value[0] <= 'z') || (value[0] >= 'A' && value[0] <= 'Z'))
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

	// 2. Ensure category directory exists (honoring a configured folder override)
	folderName := string(category)
	if custom, ok := r.categoryPaths[string(category)]; ok && custom != "" {
		folderName = custom
	}
	targetDir := filepath.Join(r.baseDir, folderName)
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
