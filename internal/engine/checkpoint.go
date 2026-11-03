package engine

import "time"

// Checkpoint captures the complete persistent state of a download job.
type Checkpoint struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	FinalPath string    `json:"final_path"`
	TotalSize int64     `json:"total_size"`
	ETag      string    `json:"etag"`
	Chunks    []Chunk   `json:"chunks"`
	UpdatedAt time.Time `json:"updated_at"`
}

// CheckpointStore defines the interface for persisting and managing download checkpoints.
type CheckpointStore interface {
	SaveCheckpoint(cp *Checkpoint) error
	GetCheckpoint(id string) (*Checkpoint, error)
	DeleteCheckpoint(id string) error
	Close() error
}
