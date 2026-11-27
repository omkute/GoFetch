package engine

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/omkute/gofetch/internal/storage"
)

// StagingPathFor returns the staging part path used for finalPath.
//
// An empty jobID preserves the legacy layout (finalPath + PartFileExtension)
// for callers that do not participate in resumable job tracking. A non-empty
// jobID inserts a deterministic hash prefix before the extension so that jobs
// writing the same filename into the same directory never share a staging
// file. The hash is derived from the job ID alone and contains no path
// separators, so it cannot introduce a path-traversal component.
func StagingPathFor(finalPath, jobID string) string {
	if jobID == "" {
		return finalPath + storage.PartFileExtension
	}

	sum := sha256.Sum256([]byte(jobID))
	return finalPath + "." + hex.EncodeToString(sum[:8]) + storage.PartFileExtension
}
