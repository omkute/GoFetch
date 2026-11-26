package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/omkute/gofetch/config"
)

func TestResolveStandaloneConfig(t *testing.T) {
	configured := &config.Config{
		Concurrency: config.ConcurrencyConfig{ChunksPerDownload: 7},
		Storage: config.StorageConfig{
			DefaultDownloadDir: "/cfg/downloads",
			AutoClassify:       false,
			CategoryPaths:      map[string]string{"Documents": "ConfiguredDocs"},
		},
		Network: config.NetworkConfig{UserAgent: "cfg-agent", ConnectionTimeout: 9},
	}

	tests := []struct {
		name  string
		flags standaloneFlagState
		cfg   *config.Config
		want  standaloneResolved
	}{
		{
			name: "nil config keeps historical defaults",
			cfg:  nil,
			want: standaloneResolved{DestDir: ".", Chunks: 4, NoOrganize: false},
		},
		{
			// A synthetic zero-valued struct means auto_classify is off; a real
			// empty config file loads DefaultConfig, where it is on.
			name: "zero-valued struct disables classification",
			cfg:  &config.Config{},
			want: standaloneResolved{DestDir: ".", Chunks: 4, NoOrganize: true},
		},
		{
			name: "config fills every unset flag",
			cfg:  configured,
			want: standaloneResolved{DestDir: "/cfg/downloads", Chunks: 7, NoOrganize: true},
		},
		{
			name: "explicit dir overrides config",
			flags: standaloneFlagState{
				destDir:        "/flag/downloads",
				destDirChanged: true,
			},
			cfg:  configured,
			want: standaloneResolved{DestDir: "/flag/downloads", Chunks: 7, NoOrganize: true},
		},
		{
			name: "explicit chunks overrides config",
			flags: standaloneFlagState{
				chunks:        2,
				chunksChanged: true,
			},
			cfg:  configured,
			want: standaloneResolved{DestDir: "/cfg/downloads", Chunks: 2, NoOrganize: true},
		},
		{
			name: "explicit no-organize=false overrides config",
			flags: standaloneFlagState{
				noOrganize:        false,
				noOrganizeChanged: true,
			},
			cfg:  configured,
			want: standaloneResolved{DestDir: "/cfg/downloads", Chunks: 7, NoOrganize: false},
		},
		{
			name: "explicit no-organize=true overrides config",
			flags: standaloneFlagState{
				noOrganize:        true,
				noOrganizeChanged: true,
			},
			cfg: &config.Config{
				Storage: config.StorageConfig{AutoClassify: true},
			},
			want: standaloneResolved{DestDir: ".", Chunks: 4, NoOrganize: true},
		},
		{
			name: "invalid explicit values fall back to defaults",
			flags: standaloneFlagState{
				destDir:        "",
				destDirChanged: true,
				chunks:         0,
				chunksChanged:  true,
			},
			cfg:  &config.Config{},
			want: standaloneResolved{DestDir: ".", Chunks: 4, NoOrganize: true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveStandaloneConfig(tc.flags, tc.cfg)
			if got != tc.want {
				t.Fatalf("resolveStandaloneConfig() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestNewStandaloneHTTPClient(t *testing.T) {
	for _, timeoutSec := range []int{0, 5, 30} {
		client := newStandaloneHTTPClient(timeoutSec)
		if client.Timeout != 0 {
			t.Errorf("connection timeout %ds: Client.Timeout = %v, want 0", timeoutSec, client.Timeout)
		}
	}

	if got := newStandaloneHTTPClient(0).Transport; got != http.DefaultTransport {
		t.Errorf("zero connection timeout should keep the default transport, got %T", got)
	}
	if got := newStandaloneHTTPClient(5).Transport; got == http.DefaultTransport {
		t.Error("configured connection timeout should use a streaming transport")
	}
}

func writeStandaloneConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatalf("failed writing config file: %v", err)
	}
	return path
}

func TestRootCmd_StandaloneUsesConfig(t *testing.T) {
	baseDir := t.TempDir()
	configDest := filepath.Join(baseDir, "from-config")
	payload := bytes.Repeat([]byte("config routing payload! "), 4)

	var mu sync.Mutex
	var agents []string
	rangeRequests := 0
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		agents = append(agents, req.Header.Get("User-Agent"))
		if req.Header.Get("Range") != "" {
			rangeRequests++
		}
		mu.Unlock()
		http.ServeContent(w, req, filepath.Base(req.URL.Path), time.Time{}, bytes.NewReader(payload))
	}))
	defer origin.Close()

	const configAgent = "GoFetch-Config-Test/1.0"
	configPath := writeStandaloneConfig(t, fmt.Sprintf(`
concurrency:
  chunks_per_download: 3
storage:
  default_download_dir: %q
  auto_classify: true
  category_paths:
    Documents: ConfiguredDocs
network:
  user_agent: %q
  connection_timeout_sec: 5
`, configDest, configAgent))

	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{origin.URL + "/report.txt", "--config", configPath, "--quiet"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("standalone run with config failed: %v", err)
	}

	routed := filepath.Join(configDest, "ConfiguredDocs", "report.txt")
	downloaded, err := os.ReadFile(routed)
	if err != nil {
		t.Fatalf("expected download routed via config category_paths at %q: %v", routed, err)
	}
	if !bytes.Equal(downloaded, payload) {
		t.Errorf("downloaded content mismatch: got %d bytes, want %d", len(downloaded), len(payload))
	}
	if _, err := os.Stat(filepath.Join(configDest, "report.txt")); !os.IsNotExist(err) {
		t.Errorf("file should have been routed out of the config download dir; stat error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if rangeRequests != 3 {
		t.Errorf("config chunks_per_download: got %d ranged requests, want 3", rangeRequests)
	}
	if len(agents) == 0 {
		t.Fatal("no requests reached the test origin")
	}
	for _, agent := range agents {
		if agent != configAgent {
			t.Errorf("User-Agent = %q, want config value %q", agent, configAgent)
		}
	}
}

func TestRootCmd_StandaloneFlagsOverrideConfig(t *testing.T) {
	baseDir := t.TempDir()
	configDest := filepath.Join(baseDir, "from-config")
	flagDest := filepath.Join(baseDir, "from-flag")
	payload := bytes.Repeat([]byte("explicit flags win over config"), 3)

	var mu sync.Mutex
	rangeRequests := 0
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		if req.Header.Get("Range") != "" {
			rangeRequests++
		}
		mu.Unlock()
		http.ServeContent(w, req, filepath.Base(req.URL.Path), time.Time{}, bytes.NewReader(payload))
	}))
	defer origin.Close()

	configPath := writeStandaloneConfig(t, fmt.Sprintf(`
concurrency:
  chunks_per_download: 5
storage:
  default_download_dir: %q
  auto_classify: true
  category_paths:
    Documents: ConfiguredDocs
network:
  user_agent: GoFetch-Config-Test/1.0
  connection_timeout_sec: 5
`, configDest))

	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		origin.URL + "/override.txt",
		"--config", configPath,
		"--dir", flagDest,
		"--chunks", "2",
		"--no-organize",
		"--quiet",
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("standalone run with flag overrides failed: %v", err)
	}

	wantPath := filepath.Join(flagDest, "override.txt")
	if _, err := os.ReadFile(wantPath); err != nil {
		t.Fatalf("expected file at explicit --dir %q: %v", wantPath, err)
	}
	if _, err := os.Stat(filepath.Join(configDest, "ConfiguredDocs", "override.txt")); !os.IsNotExist(err) {
		t.Errorf("--no-organize should suppress config category routing; stat error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if rangeRequests != 2 {
		t.Errorf("explicit --chunks: got %d ranged requests, want 2", rangeRequests)
	}
}

func TestRootCmd_StandaloneConfigAutoClassifyFalse(t *testing.T) {
	configDest := t.TempDir()
	payload := []byte("classification disabled by configuration")
	origin := newStandaloneTestOrigin(t, payload)

	configPath := writeStandaloneConfig(t, fmt.Sprintf(`
storage:
  default_download_dir: %q
  auto_classify: false
  category_paths:
    Documents: ConfiguredDocs
`, configDest))

	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{origin.URL + "/note.txt", "--config", configPath, "--quiet"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("standalone run with auto_classify=false failed: %v", err)
	}

	if _, err := os.ReadFile(filepath.Join(configDest, "note.txt")); err != nil {
		t.Fatalf("expected unclassified download in config dir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(configDest, "ConfiguredDocs", "note.txt")); !os.IsNotExist(err) {
		t.Errorf("auto_classify=false must not route the file; stat error: %v", err)
	}
}

func TestRootCmd_StandaloneRejectsTraversalCategoryPaths(t *testing.T) {
	configDest := t.TempDir()
	origin := newStandaloneTestOrigin(t, []byte("traversal validation payload"))

	configPath := writeStandaloneConfig(t, fmt.Sprintf(`
storage:
  default_download_dir: %q
  auto_classify: true
  category_paths:
    Documents: ../../escaped
`, configDest))

	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{origin.URL + "/doc.txt", "--config", configPath, "--quiet"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected traversal in category_paths to be rejected")
	}
	if !strings.Contains(err.Error(), "parent-directory segment") {
		t.Errorf("unexpected traversal error: %v", err)
	}
	if entries, statErr := os.ReadDir(configDest); statErr != nil || len(entries) != 0 {
		t.Errorf("no file should be written when config paths are invalid; entries=%v err=%v", entries, statErr)
	}
}

func TestRootCmd_StandaloneEmptyConfigFile(t *testing.T) {
	tempDir := t.TempDir()
	content := []byte("empty config file still downloads")
	origin := newStandaloneTestOrigin(t, content)
	configPath := writeStandaloneConfig(t, "")

	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		origin.URL + "/empty-config.txt",
		"--config", configPath,
		"--dir", tempDir,
		"--no-organize",
		"--quiet",
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("empty config file execution failed: %v", err)
	}
	downloaded, err := os.ReadFile(filepath.Join(tempDir, "empty-config.txt"))
	if err != nil {
		t.Fatalf("expected download with an empty config file: %v", err)
	}
	if !bytes.Equal(downloaded, content) {
		t.Errorf("content mismatch: got %q, want %q", string(downloaded), string(content))
	}
}

func TestRootCmd_StandaloneInvalidConfig(t *testing.T) {
	configPath := writeStandaloneConfig(t, "storage:\n  unknown_key: true\n")

	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"http://127.0.0.1:1/file.txt", "--config", configPath, "--quiet"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected an error for an invalid config file")
	}
	if !strings.Contains(err.Error(), "configuration error") ||
		!strings.Contains(err.Error(), "invalid config file") {
		t.Errorf("unexpected config error: %v", err)
	}
}

func TestRootConfigFlagDoesNotReplaceDaemonFlag(t *testing.T) {
	root := newRootCmd()
	if root.Flag("config") == nil {
		t.Fatal("root command is missing --config")
	}
	if root.PersistentFlags().Lookup("config") != nil {
		t.Error("root --config must be local so subcommands keep their own flag")
	}

	daemon := newDaemonCmd()
	flag := daemon.Flag("config")
	if flag == nil {
		t.Fatal("daemon command lost its --config flag")
	}
	if flag.DefValue != "" {
		t.Errorf("daemon --config default = %q, want empty", flag.DefValue)
	}
}
