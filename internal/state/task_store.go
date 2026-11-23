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

const bucketTasks = "daemon_tasks_v2"

// PersistedTask represents durable download task metadata saved in BBolt.
type PersistedTask struct {
	ID              string    `json:"id"`
	URL             string    `json:"url"`
	FileName        string    `json:"file_name"`
	TargetPath      string    `json:"target_path"`
	Status          string    `json:"status"`
	Priority        string    `json:"priority"`
	TotalBytes      int64     `json:"total_bytes"`
	DownloadedBytes int64     `json:"downloaded_bytes"`
	SpeedBps        int64     `json:"speed_bps"`
	ETASeconds      int64     `json:"eta_seconds"`
	Category        string    `json:"category"`
	Error           string    `json:"error,omitempty"`
	DestDir         string    `json:"dest_dir"`
	MaxChunks       int       `json:"max_chunks"`
	RateLimit       string    `json:"rate_limit,omitempty"`
	Checksum        string    `json:"checksum,omitempty"`
	ChecksumAlgo    string    `json:"checksum_algo,omitempty"`
	AutoClassify    bool      `json:"auto_classify"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// TaskStore manages durable task storage in an embedded BBolt database.
type TaskStore struct {
	db *bolt.DB
}

// NewTaskStore initializes or opens a task store at dbPath.
func NewTaskStore(dbPath string) (*TaskStore, error) {
	if dbPath == "" {
		return nil, errors.New("db path cannot be empty")
	}

	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create directory for task store: %w", err)
	}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("failed to open task store bbolt db at %q: %w", dbPath, err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, createErr := tx.CreateBucketIfNotExists([]byte(bucketTasks))
		return createErr
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to initialize tasks bucket: %w", err)
	}

	return &TaskStore{db: db}, nil
}

// SaveTask persists or updates a task record atomically.
func (s *TaskStore) SaveTask(task *PersistedTask) error {
	if task == nil || task.ID == "" {
		return errors.New("invalid task: ID cannot be empty")
	}

	if task.UpdatedAt.IsZero() {
		task.UpdatedAt = time.Now().UTC()
	}

	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketTasks))
		return b.Put([]byte(task.ID), data)
	})
}

// GetTask retrieves a task record by ID. Returns nil, nil if not found.
func (s *TaskStore) GetTask(id string) (*PersistedTask, error) {
	if id == "" {
		return nil, nil
	}

	var task PersistedTask
	var found bool

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketTasks))
		val := b.Get([]byte(id))
		if val == nil {
			return nil
		}
		found = true
		return json.Unmarshal(val, &task)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query task %q: %w", id, err)
	}
	if !found {
		return nil, nil
	}

	return &task, nil
}

// ListTasks returns all tasks saved in the store.
func (s *TaskStore) ListTasks() ([]*PersistedTask, error) {
	var tasks []*PersistedTask

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketTasks))
		return b.ForEach(func(k, v []byte) error {
			var t PersistedTask
			if err := json.Unmarshal(v, &t); err != nil {
				return err
			}
			tasks = append(tasks, &t)
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list tasks: %w", err)
	}

	return tasks, nil
}

// DeleteTask removes a task record by ID.
func (s *TaskStore) DeleteTask(id string) error {
	if id == "" {
		return nil
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketTasks))
		return b.Delete([]byte(id))
	})
}

// Close gracefully closes the underlying BBolt database.
func (s *TaskStore) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}
