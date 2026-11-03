package api

import "time"

// CreateDownloadRequest is the JSON payload submitted to enqueue a new download.
type CreateDownloadRequest struct {
	URL            string `json:"url"`
	DestinationDir string `json:"destination_dir"`
	Priority       string `json:"priority"`
	MaxChunks      int    `json:"max_chunks"`
	Checksum       string `json:"checksum"`
	ChecksumAlgo   string `json:"checksum_algo"`
	AutoClassify   bool   `json:"auto_classify"`
}

// DownloadResponse is the response returned when a download is enqueued.
type DownloadResponse struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	FileName  string    `json:"file_name"`
	Status    string    `json:"status"`
	Priority  string    `json:"priority"`
	CreatedAt time.Time `json:"created_at"`
}

// DownloadItem summarizes an active, queued, or completed download in list responses.
type DownloadItem struct {
	ID              string  `json:"id"`
	URL             string  `json:"url"`
	FileName        string  `json:"file_name"`
	Status          string  `json:"status"`
	Priority        string  `json:"priority"`
	TotalBytes      int64   `json:"total_bytes"`
	DownloadedBytes int64   `json:"downloaded_bytes"`
	ProgressPercent float64 `json:"progress_percent"`
	SpeedBps        int64   `json:"speed_bps"`
	ETASeconds      int64   `json:"eta_seconds"`
	Category        string  `json:"category"`
	Error           string  `json:"error,omitempty"`
}

// ListDownloadsResponse wraps the paginated items returned by GET /api/v1/downloads.
type ListDownloadsResponse struct {
	Items []DownloadItem `json:"items"`
	Total int            `json:"total"`
}

// ChunkDetail provides per-chunk boundary and progress information.
type ChunkDetail struct {
	ChunkIndex      int   `json:"chunk_index"`
	StartByte       int64 `json:"start_byte"`
	EndByte         int64 `json:"end_byte"`
	DownloadedBytes int64 `json:"downloaded_bytes"`
	Completed       bool  `json:"completed"`
}

// DownloadDetailResponse is the granular response returned by GET /api/v1/downloads/{id}.
type DownloadDetailResponse struct {
	ID              string        `json:"id"`
	URL             string        `json:"url"`
	FileName        string        `json:"file_name"`
	TargetPath      string        `json:"target_path"`
	Status          string        `json:"status"`
	Priority        string        `json:"priority"`
	TotalBytes      int64         `json:"total_bytes"`
	DownloadedBytes int64         `json:"downloaded_bytes"`
	SpeedBps        int64         `json:"speed_bps"`
	ETASeconds      int64         `json:"eta_seconds"`
	Category        string        `json:"category"`
	Error           string        `json:"error,omitempty"`
	Chunks          []ChunkDetail `json:"chunks"`
}

// ActionResponse confirms the state change of an administrative operation (pause, resume, delete).
type ActionResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}
