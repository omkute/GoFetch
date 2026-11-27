# Screenshots

This directory holds real screenshots of GoFetch used in the root `README.md`. Do not commit generated or mocked-up images as if they were captures of the application.

## Expected files

| File | What to show |
| :--- | :--- |
| `dashboard.png` | Web dashboard at `http://127.0.0.1:12701/` with several queue rows and the speed chart populated |
| `chunk-details.png` | Dashboard task detail drawer open, showing per-chunk ranges and progress |
| `tui.png` | Interactive terminal UI during an active segmented download (progress bar, speed, ETA, chunk rows) |

Add the images here, then embed them in the root README:

```markdown
![GoFetch dashboard](docs/screenshots/dashboard.png)
```

## Capturing the web dashboard

1. Build and start a daemon with the GUI enabled:

   ```bash
   make build
   ./bin/gofetch daemon
   ```

2. Enqueue a few downloads so the queue and chart have data:

   ```bash
   gofetch add https://example.com/file.bin --priority high -c 8
   gofetch add -f urls.txt
   ```

3. Open `http://127.0.0.1:12701/` in your browser, click a row for the detail drawer, then capture the window:

   ```bash
   screencapture -x -t png docs/screenshots/dashboard.png
   screencapture -x -t png docs/screenshots/chunk-details.png
   ```

   `screencapture` requires Screen Recording permission for the terminal app on macOS.

### Headless Safari alternative

If you cannot grant Screen Recording permission, enable **Safari → Settings → Developer → Allow remote automation**, start `safaridriver -p 4444`, and capture via the WebDriver screenshot endpoint:

```bash
curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"capabilities":{"alwaysMatch":{"browserName":"safari"}}}' \
  http://127.0.0.1:4444/session
# use the returned session id for /session/<id>/url and /session/<id>/screenshot
```

## Capturing the TUI

Start a real download in an interactive terminal so the Bubbletea view is visible, then capture that window:

```bash
./bin/gofetch -c 8 https://example.com/large.iso
# in another shell, while it is running:
screencapture -x -t png docs/screenshots/tui.png
```

Stop the daemon when finished (`Ctrl+C` in its terminal, or `gofetch daemon` shutdown).
