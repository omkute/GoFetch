# ADR-0002: Sparse File Preallocation and Non-Overlapping WriteAt Chunk Assembly

* **Status**: Accepted
* **Date**: 2026-11-03
* **Deciders**: Principal System Architect, Principal Product Manager

---

## Context
When downloading large files concurrently across multiple connections, download engines traditionally use one of two file assembly strategies:
1. **Separate Chunk Files**: Download each chunk to a separate file (`file.part.0`, `file.part.1`, ...), then sequentially concatenate them into the final file upon completion.
2. **Direct Random-Access Sparse File**: Pre-allocate the entire target size in a single file and write chunks directly to their calculated byte offsets in parallel.

Strategy 1 incurs severe penalties for large downloads (e.g., 5GB–50GB ISOs or video archives):
* Doubled disk write I/O (writing chunks once, then reading and rewriting during concatenation).
* Disk space requirement is doubled during the concatenation phase.
* Noticeable delay and I/O thrashing after download completes while files are stitched together.

## Decision
We adopt **Strategy 2 (Sparse File Preallocation + `os.File.WriteAt`)**:
1. **Sparse Sizing**: Once the `Content-Length` header is verified, GoFetch immediately sizes the target temporary file (`<filename>.gofetch.part`) using `os.File.Truncate(contentLength)`. Modern filesystems (APFS, ext4, NTFS) allocate sparse blocks without zeroing the entire drive space upfront.
2. **Thread-Safe Random Access Writes**: Each chunk worker is assigned a disjoint byte range `[start, end]`. Workers write incoming byte buffers directly to disk using `os.File.WriteAt(p, offset)`. Under POSIX and Windows, `WriteAt` does not modify a shared file offset pointer, enabling concurrent goroutine writes to the same file descriptor without write locks.
3. **Atomic Commit**: Once all chunk ranges are validated and checksums verified, the file is committed to its final path via a single atomic `os.Rename`.

## Consequences

### Positive
* **Zero Assembly Latency**: When the final byte finishes streaming, the file is already assembled. There is zero post-download file stitching time.
* **Halved Disk Wear & Space**: No secondary copy phase; requires only the exact file size on disk.
* **Concurrent Throughput**: Parallel workers stream directly to disk without mutex serialization on write heads.

### Negative / Trade-Offs
* Requires upstream server to provide a valid `Content-Length`. If the response uses chunked transfer encoding with unknown length, the engine dynamically falls back to sequential single-stream append mode.
* If a download is permanently abandoned, an unallocated sparse file must be explicitly deleted to reclaim metadata space.
