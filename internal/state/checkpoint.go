package state

import (
	"errors"

	"github.com/omkute/gofetch/internal/engine"
)

// ErrCheckpointNotFound indicates that the requested checkpoint ID does not exist.
var ErrCheckpointNotFound = errors.New("checkpoint not found")

// Checkpoint is an alias for engine.Checkpoint.
type Checkpoint = engine.Checkpoint

// Store is an alias for engine.CheckpointStore.
type Store = engine.CheckpointStore
