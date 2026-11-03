package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

const bucketCheckpoints = "checkpoints"

// BboltStore implements the Store interface using embedded BBolt key-value storage.
type BboltStore struct {
	db *bolt.DB
}

// NewBboltStore initializes or opens an existing BBolt database at dbPath.
func NewBboltStore(dbPath string) (*BboltStore, error) {
	if dbPath == "" {
		return nil, errors.New("db path cannot be empty")
	}

	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create directory for bbolt db: %w", err)
	}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt database at %q: %w", dbPath, err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, createErr := tx.CreateBucketIfNotExists([]byte(bucketCheckpoints))
		return createErr
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to initialize checkpoints bucket: %w", err)
	}

	return &BboltStore{db: db}, nil
}

// SaveCheckpoint serializes and persists a Checkpoint record atomically.
func (s *BboltStore) SaveCheckpoint(cp *Checkpoint) error {
	if cp == nil || cp.ID == "" {
		return errors.New("invalid checkpoint: ID cannot be empty")
	}

	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketCheckpoints))
		return b.Put([]byte(cp.ID), data)
	})
}

// GetCheckpoint retrieves and deserializes a Checkpoint record by ID.
func (s *BboltStore) GetCheckpoint(id string) (*Checkpoint, error) {
	if id == "" {
		return nil, nil
	}

	var cp Checkpoint
	var found bool

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketCheckpoints))
		val := b.Get([]byte(id))
		if val == nil {
			return nil
		}
		found = true
		return json.Unmarshal(val, &cp)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query checkpoint %q: %w", id, err)
	}
	if !found {
		return nil, nil
	}

	return &cp, nil
}

// DeleteCheckpoint removes a checkpoint record from storage.
func (s *BboltStore) DeleteCheckpoint(id string) error {
	if id == "" {
		return nil
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketCheckpoints))
		return b.Delete([]byte(id))
	})
}

// Close closes the underlying BBolt database file handle.
func (s *BboltStore) Close() error {
	return s.db.Close()
}
