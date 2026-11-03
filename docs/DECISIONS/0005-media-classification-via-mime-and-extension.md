# ADR-0005: Two-Tier Media Classification via Extension Mapping and MIME Magic Bytes

* **Status**: Accepted
* **Date**: 2026-11-03
* **Deciders**: Principal System Architect, Principal Product Manager

---

## Context
A primary differentiator and product requirement in [`docs/PRODUCT.md`](file:///Users/omkute/Desktop/Projects/GoFetch/docs/PRODUCT.md) is the automatic sorting of completed downloads into categorized directories (`Videos/`, `Documents/`, `Audio/`, `Archives/`, `Software/`, `Others/`).

Relying solely on file extensions has known failure modes:
* URLs often use query parameters or opaque routes (e.g., `https://example.com/stream?id=98234`) without file extensions.
* Filenames can be mislabeled or omitted by servers.

Conversely, relying solely on MIME sniffing (`http.DetectContentType`) inspects only the initial 512 bytes, which cannot distinguish between specialized container formats (e.g., modern Microsoft Office `.docx` / `.xlsx` files and Android `.apk` files are internally ZIP archives).

## Decision
We implement a **Two-Tier Classification Pipeline**:
1. **Tier 1 (Extension Registry)**: Inspect the target filename derived from the URL or sanitized `Content-Disposition` header. Match against an internal curated extension map:
   * **Documents**: `.pdf`, `.docx`, `.doc`, `.xlsx`, `.pptx`, `.txt`, `.epub`, `.md`
   * **Video**: `.mp4`, `.mkv`, `.avi`, `.mov`, `.webm`, `.flv`
   * **Audio**: `.mp3`, `.flac`, `.wav`, `.aac`, `.ogg`, `.m4a`
   * **Archives**: `.zip`, `.tar.gz`, `.tar.bz2`, `.rar`, `.7z`, `.iso`, `.gz`
   * **Software**: `.exe`, `.dmg`, `.pkg`, `.deb`, `.rpm`, `.msi`, `.appimage`
2. **Tier 2 (MIME Sniffing Fallback)**: If the extension is absent, generic (`.bin`, `.dat`), or unknown:
   * Read the initial 512 bytes of the downloaded file header.
   * Classify using `http.DetectContentType` and MIME category prefixes (`video/*` -> `Video/`, `audio/*` -> `Audio/`, `application/pdf` -> `Documents/`).
3. **Fallback & User Control**: Files that match neither tier are routed safely to `Others/`. Users can disable categorization via the `--no-organize` flag or override rules in `config.yaml`.

## Consequences

### Positive
* **Robust File Placement**: Accurately classifies files with or without explicit URL extensions.
* **Deterministic Behavior**: Avoids misclassifying `.docx` as a `.zip` archive while still handling extensionless binary streams.
* **Non-Destructive**: Routing is executed via atomic file rename only after the download is 100% complete and verified.

### Negative / Trade-Offs
* Extremely rare or proprietary media types will default to `Others/` unless explicitly defined in user configuration.
