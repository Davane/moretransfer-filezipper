package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// ZIP constants/signatures.
//
// This file writes ZIP records manually (no archive/zip) because we need:
// - streaming output (can't seek back to fill sizes)
// - resumability via multipart upload checkpoints
// - ZIP64 structures so offsets/sizes can exceed 32-bit limits.
const (
	sigLocal          = 0x04034b50
	sigDataDescriptor = 0x08074b50
	sigCentral        = 0x02014b50
	sigZip64Eocd      = 0x06064b50
	sigZip64Locator   = 0x07064b50
	sigEocd           = 0x06054b50

	gpUtf8AndDD = 0x0808
	versionZip64 = 45
	methodStore  = 0

	maxU16 = 0xffff
	maxU32 = 0xffffffff
)

// RunChunkRequest is the input contract for POST /runChunk.
//
// The caller (typically a coordinator) supplies the current checkpoint:
// - which manifest file index we are at
// - the ZIP byte offset already written/uploaded
// - the multipart next part number.
//
// This service will continue from that checkpoint, uploading at most MaxParts
// *new* parts, but it only stops at file boundaries to keep ZIP structures intact.
type RunChunkRequest struct {
	JobID         string `json:"jobId"`
	ManifestKey   string `json:"manifestKey"`
	OutputKey     string `json:"outputKey"`
	UploadID      string `json:"uploadId"`
	PartSize      int    `json:"partSize"`
	NextPartNum   int    `json:"nextPartNumber"`
	FileIndex     int    `json:"fileIndex"`
	ZipOffset     string `json:"zipOffset"`
	MaxParts      int    `json:"maxParts"`
}

// UploadedPart is the minimal information required to complete an R2/S3-style
// multipart upload: part number, etag, and the bytes uploaded for that part.
type UploadedPart struct {
	PartNumber int    `json:"partNumber"`
	Etag       string `json:"etag"`
	SizeBytes  int    `json:"sizeBytes"`
}

// CentralEntry is the metadata needed to write a central directory record later.
//
// We persist these as we go (via JobManager DO) because the central directory is
// written only at the end, but the build is chunked across many /runChunk calls.
type CentralEntry struct {
	NameB64         string `json:"nameB64"`
	CRC32           uint32 `json:"crc32"`
	CompressedSize  string `json:"compressedSize"`
	UncompressedSize string `json:"uncompressedSize"`
	LocalHeaderOffset string `json:"localHeaderOffset"`
	ModTime         uint16 `json:"modTime"`
	ModDate         uint16 `json:"modDate"`
}

// RunChunkResponse returns an updated checkpoint that lets the caller resume:
// nextPartNumber, fileIndex, zipOffset, and the parts uploaded during this call.
type RunChunkResponse struct {
	UploadedParts    []UploadedPart `json:"uploadedParts"`
	NextPartNumber   int            `json:"nextPartNumber"`
	FileIndex        int            `json:"fileIndex"`
	ZipOffset        string         `json:"zipOffset"`
	BytesWrittenTotal string        `json:"bytesWrittenTotal"`
	FilesDone        int            `json:"filesDone"`
	Done             bool           `json:"done"`
}

// Manifest describes the work to be performed (what objects to zip, expected sizes,
// and their names inside the archive). It is fetched from output storage.
type Manifest struct {
	Version     string `json:"version"`
	JobID       string `json:"jobId"`
	TransferID  string `json:"transferId"`
	OutputKey   string `json:"outputKey"`
	CreatedAtMs int64  `json:"createdAtMs"`
	IncludeEmpty bool  `json:"includeEmpty"`
	Files       []struct {
		Key      string `json:"key"`
		NameInZip string `json:"nameInZip"`
		Size     int64  `json:"size"`
	} `json:"files"`
}


// --------------------------------------------------------------------------------
// Main
// --------------------------------------------------------------------------------
// Setup the HTTP server to handle the /runChunk endpoint from the JobManagerDO
func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/runChunk", handleRunChunk)
	addr := ":8080"
	log.Printf("zip container listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

// handleRunChunk incrementally builds a ZIP64 stream and uploads it via multipart.
//
// Important properties/assumptions:
// - Resumability is based on an external checkpoint: zipOffset, nextPartNumber, fileIndex,
//   and the JobManager DO's persisted central directory entries.
// - We stop only at file boundaries (never mid-file) so the caller can safely resume
//   without having to re-stream partial file data.
// - We always write ZIP64 central directory entries and ZIP64 EOCD structures.
func handleRunChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	start := time.Now()
	var req RunChunkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	if req.MaxParts <= 0 {
		req.MaxParts = 8
	}

	log.Printf("[runChunk] start jobId=%s startTimeMs=%d fileIndex=%d nextPart=%d maxParts=%d partSize=%d zipOffset=%s outputKey=%s uploadId=%s",
		req.JobID, start.UnixMilli(), req.FileIndex, req.NextPartNum, req.MaxParts, req.PartSize, req.ZipOffset, req.OutputKey, redact(req.UploadID))

	zipOffset, err := parseBigInt(req.ZipOffset)
	if err != nil {
		http.Error(w, "bad zipOffset", 400)
		return
	}

	manifest, err := fetchManifest(ctx, req.ManifestKey)
	if err != nil {
		http.Error(w, "manifest fetch failed: "+err.Error(), 500)
		return
	}

	// Load previously recorded central directory entries from JobManager DO.
	// These must include every file completed in prior chunks; otherwise the final
	// central directory won't match the file data already uploaded.
	entries, err := fetchEntries(ctx, req.JobID)
	if err != nil {
		http.Error(w, "entries fetch failed: "+err.Error(), 500)
		return
	}

	uploader := NewMultipartUploader(req.OutputKey, req.UploadID, req.PartSize, req.NextPartNum)
	uploader.Offset = zipOffset

	// If we're resuming, assume previous chunks already uploaded bytes through zipOffset.
	// We do not re-upload previous bytes; we resume part numbering at req.NextPartNum.
	// NOTE: This requires checkpoint correctness in JobManager DO.

	filesDone := req.FileIndex
	var partsUploadedThisRun int

	for i := req.FileIndex; i < len(manifest.Files); i++ {
		f := manifest.Files[i]

		nameBytes := []byte(normalizeZipPath(f.NameInZip))
		modTime, modDate := dosDateTimeNow()
		localHeaderOffset := uploader.Offset

		if err := writeLocalHeader(uploader, nameBytes, modTime, modDate); err != nil {
			http.Error(w, "write local header failed: "+err.Error(), 500)
			return
		}

		crc := uint32(0)
		size := int64(0)

		rc, err := getSourceObject(ctx, f.Key)
		if err != nil {
			http.Error(w, "source get failed: "+err.Error(), 500)
			return
		}

		// Stream the source object into the ZIP stream, while computing CRC32.
		// We use "store" (no compression), so compressedSize == uncompressedSize.
		buf := make([]byte, 1024*1024)
		for {
			n, readErr := rc.Read(buf)
			if n > 0 {
				chunk := buf[:n]
				crc = crc32.Update(crc, crc32.IEEETable, chunk)
				size += int64(n)
				if err := uploader.Write(chunk); err != nil {
					_ = rc.Close()
					http.Error(w, "upload write failed: "+err.Error(), 500)
					return
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				_ = rc.Close()
				http.Error(w, "source read failed: "+readErr.Error(), 500)
				return
			}
		}
		_ = rc.Close()

		if size != f.Size {
			// Manifest is treated as authoritative; a mismatch likely indicates a bad
			// manifest, a racing overwrite, or reading the wrong object version.
			http.Error(w, fmt.Sprintf("size mismatch %s expected %d got %d", f.Key, f.Size, size), 500)
			return
		}

		// Because the local header is written before we know CRC/sizes, we must emit a
		// data descriptor. We always write the ZIP64 variant with 8-byte sizes.
		if err := writeDataDescriptor(uploader, crc, uint64(size), uint64(size)); err != nil {
			http.Error(w, "write data descriptor failed: "+err.Error(), 500)
			return
		}

		entry := CentralEntry{
			NameB64: base64.StdEncoding.EncodeToString(nameBytes),
			CRC32: crc,
			CompressedSize: fmt.Sprintf("%d", size),
			UncompressedSize: fmt.Sprintf("%d", size),
			LocalHeaderOffset: localHeaderOffset.String(),
			ModTime: modTime,
			ModDate: modDate,
		}
		entries = append(entries, entry)
		if err := appendEntry(ctx, req.JobID, entry, i); err != nil {
			http.Error(w, "append entry failed: "+err.Error(), 500)
			return
		}

		filesDone = i + 1

		// Chunking: stop only at file boundary.
		//
		// Watch out: this means we will not split a single large file across multiple
		// /runChunk calls. If a single file is enormous, this chunk may exceed MaxParts
		// and runtime expectations because we finish the current file before stopping.
		//
		// We use the number of parts uploaded (not bytes) because the coordinator
		// typically budgets per-container runtime based on predictable part sizes and
		// request counts.
		partsUploadedThisRun += uploader.PartsUploadedThisRun
		uploader.PartsUploadedThisRun = 0
		// Important: never stop a chunk with a partially-filled multipart buffer.
		// R2/S3 requires all non-trailing parts to have identical length (PartSize).
		// If we were to stop while u.buf has bytes, resuming would require uploading a
		// short non-trailing part, and completeMultipart would fail.
		if partsUploadedThisRun >= req.MaxParts && uploader.buf.Len() == 0 {
			break
		}
	}

	done := filesDone >= len(manifest.Files)

	if done {
		// Finalization stage: write the central directory and end records.
		// At this point the output stream becomes immutable; after we complete the
		// multipart upload, the resulting object should be a valid ZIP64 archive.
		centralDirOffset := uploader.Offset
		for _, e := range entries {
			if err := writeCentralDirectoryEntry(uploader, e); err != nil {
				http.Error(w, "write central dir failed: "+err.Error(), 500)
				return
			}
		}
		centralDirSize := uploader.Offset.Sub(centralDirOffset)

		if err := writeZip64EndOfCentralDirectory(uploader, uint64(len(entries)), centralDirSize, centralDirOffset); err != nil {
			http.Error(w, "zip64 eocd failed: "+err.Error(), 500)
			return
		}
		zip64EocdOffset := centralDirOffset.Add(centralDirSize)
		if err := writeZip64EndOfCentralDirectoryLocator(uploader, zip64EocdOffset); err != nil {
			http.Error(w, "zip64 locator failed: "+err.Error(), 500)
			return
		}
		if err := writeEndOfCentralDirectory(uploader, len(entries)); err != nil {
			http.Error(w, "eocd failed: "+err.Error(), 500)
			return
		}

		// Flush final part and complete multipart
		// Note: final part may be smaller than PartSize; multipart APIs allow that.
		uploaded, err := uploader.FlushFinal()
		if err != nil {
			http.Error(w, "flush final failed: "+err.Error(), 500)
			return
		}
		_ = uploaded
		// Multipart completion is performed by the coordinator (JobManagerDO), which has
		// the full ordered list of uploaded parts across all /runChunk calls.
	} else {
		// Non-final chunk: do not flush partial buffer.
		// We rely on the chunking rule above to only stop on part boundaries.
	}

	resp := RunChunkResponse{
		UploadedParts: uploader.UploadedParts,
		NextPartNumber: uploader.NextPartNumber,
		FileIndex: filesDone,
		ZipOffset: uploader.Offset.String(),
		BytesWrittenTotal: uploader.Offset.String(),
		FilesDone: filesDone,
		Done: done,
	}

	log.Printf("[runChunk] done jobId=%s durationMs=%d uploadedParts=%d nextPart=%d fileIndex=%d filesDone=%d done=%t bytesWrittenTotal=%s",
		req.JobID, time.Since(start).Milliseconds(), len(resp.UploadedParts), resp.NextPartNumber, resp.FileIndex, resp.FilesDone, resp.Done, resp.BytesWrittenTotal)
	w.Header().Set("content-type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(resp)
}

func redact(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[:8] + "..."
}

// ---------------- Multipart uploader ----------------

// BigInt is a convenience wrapper used to carry byte offsets/sizes as strings.
//
// Watch out: despite the name, it is only uint64 (not arbitrary precision). If offsets
// or sizes exceed 2^64-1, these operations will overflow and corrupt the ZIP layout.
type BigInt struct{ n uint64 }

func (b BigInt) String() string { return fmt.Sprintf("%d", b.n) }
func (b BigInt) Add(o BigInt) BigInt { return BigInt{n: b.n + o.n} }
func (b BigInt) Sub(o BigInt) BigInt { return BigInt{n: b.n - o.n} }

// parseBigInt parses base-10 unsigned integers (used for JSON portability).
func parseBigInt(s string) (BigInt, error) {
	u, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return BigInt{}, err
	}
	return BigInt{n: u}, nil
}

// MultipartUploader turns sequential writes into fixed-size multipart uploads.
//
// It buffers bytes until PartSize is reached, uploads that buffer as a part, and then
// continues. Offset is the total number of bytes written into the ZIP stream.
type MultipartUploader struct {
	OutputKey string
	UploadID  string
	PartSize  int
	NextPartNumber int

	buf   *bytes.Buffer
	Offset BigInt

	UploadedParts []UploadedPart
	PartsUploadedThisRun int
}

// NewMultipartUploader constructs a new uploader with an empty part buffer.
func NewMultipartUploader(outputKey, uploadId string, partSize, nextPart int) *MultipartUploader {
	return &MultipartUploader{
		OutputKey: outputKey,
		UploadID: uploadId,
		PartSize: partSize,
		NextPartNumber: nextPart,
		buf: bytes.NewBuffer(make([]byte, 0, partSize)),
		Offset: BigInt{n: 0},
	}
}

// Write appends bytes to the current part buffer and uploads full parts as needed.
func (u *MultipartUploader) Write(p []byte) error {
	for len(p) > 0 {
		space := u.PartSize - u.buf.Len()
		if space == 0 {
			if err := u.flushFull(); err != nil {
				return err
			}
			continue
		}
		take := space
		if take > len(p) {
			take = len(p)
		}
		_, _ = u.buf.Write(p[:take])
		u.Offset = u.Offset.Add(BigInt{n: uint64(take)})
		p = p[take:]
		if u.buf.Len() == u.PartSize {
			if err := u.flushFull(); err != nil {
				return err
			}
		}
	}
	return nil
}

// flushFull uploads the current buffer as a full multipart part.
func (u *MultipartUploader) flushFull() error {
	partNum := u.NextPartNumber
	body := bytes.NewReader(u.buf.Bytes())
	// Watch out: this uses context.Background(), so request cancellation/timeouts from
	// handleRunChunk will NOT interrupt an in-flight uploadPart call. If you need strict
	// cancellation, pass the request ctx through the uploader and into uploadPart.
	etag, size, err := uploadPart(context.Background(), u.OutputKey, u.UploadID, partNum, body)
	if err != nil {
		return err
	}
	u.UploadedParts = append(u.UploadedParts, UploadedPart{PartNumber: partNum, Etag: etag, SizeBytes: size})
	u.NextPartNumber++
	u.PartsUploadedThisRun++
	u.buf.Reset()
	return nil
}

// FlushFinal uploads any remaining buffered bytes as the final multipart part.
func (u *MultipartUploader) FlushFinal() (UploadedPart, error) {
	if u.buf.Len() == 0 {
		return UploadedPart{}, nil
	}
	partNum := u.NextPartNumber
	body := bytes.NewReader(u.buf.Bytes())
	// Watch out: same as flushFull()—this ignores request context cancellation.
	etag, size, err := uploadPart(context.Background(), u.OutputKey, u.UploadID, partNum, body)
	if err != nil {
		return UploadedPart{}, err
	}
	up := UploadedPart{PartNumber: partNum, Etag: etag, SizeBytes: size}
	u.UploadedParts = append(u.UploadedParts, up)
	u.NextPartNumber++
	u.buf.Reset()
	return up, nil
}

// AllParts returns the parts uploaded so far (used to complete multipart uploads).
func (u *MultipartUploader) AllParts() []UploadedPart {
	return u.UploadedParts
}

// ---------------- Outbound helper calls ----------------

// fetchManifest loads the zip job manifest from output storage.
func fetchManifest(ctx context.Context, manifestKey string) (*Manifest, error) {
	u := "http://output.r2/object/" + url.PathEscape(manifestKey)
	req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("status %d: %s", res.StatusCode, string(b))
	}
	var m Manifest
	if err := json.NewDecoder(res.Body).Decode(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

// getSourceObject returns a streaming reader for the source object content.
func getSourceObject(ctx context.Context, key string) (io.ReadCloser, error) {
	u := "http://source.r2/object/" + url.PathEscape(key)
	req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		_ = res.Body.Close()
		return nil, fmt.Errorf("status %d: %s", res.StatusCode, string(b))
	}
	return res.Body, nil
}

// uploadPart uploads one multipart part to output storage.
// The service is expected to respond with headers containing the ETag and size.
func uploadPart(ctx context.Context, key, uploadId string, partNumber int, body io.Reader) (string, int, error) {
	u := fmt.Sprintf("http://output.r2/uploadPart?key=%s&uploadId=%s&partNumber=%d", url.QueryEscape(key), url.QueryEscape(uploadId), partNumber)
	req, _ := http.NewRequestWithContext(ctx, "POST", u, body)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		return "", 0, fmt.Errorf("status %d: %s", res.StatusCode, string(b))
	}
	etag := res.Header.Get("x-etag")
	sizeStr := res.Header.Get("x-size-bytes")
	size, _ := strconv.Atoi(sizeStr)
	return etag, size, nil
}

// completeMultipart finalizes the multipart upload by sending the full ordered part list.
func completeMultipart(ctx context.Context, key, uploadId string, parts []UploadedPart) error {
	u := fmt.Sprintf("http://output.r2/complete?key=%s&uploadId=%s", url.QueryEscape(key), url.QueryEscape(uploadId))
	body, _ := json.Marshal(parts)
	req, _ := http.NewRequestWithContext(ctx, "POST", u, bytes.NewReader(body))
	req.Header.Set("content-type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("status %d: %s", res.StatusCode, string(b))
	}
	return nil
}

// fetchEntries retrieves previously persisted central directory entries from the JobManager DO.
func fetchEntries(ctx context.Context, jobId string) ([]CentralEntry, error) {
	u := "http://job.do/entries?jobId=" + url.QueryEscape(jobId)
	req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("status %d: %s", res.StatusCode, string(b))
	}
	var out []CentralEntry
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// appendEntry persists one central directory entry into the JobManager DO.
// This is how the build can resume later without re-walking completed files.
func appendEntry(ctx context.Context, jobId string, entry CentralEntry, fileIndex int) error {
	u := "http://job.do/entries?jobId=" + url.QueryEscape(jobId) + "&fileIndex=" + strconv.Itoa(fileIndex)
	body, _ := json.Marshal(entry)
	req, _ := http.NewRequestWithContext(ctx, "POST", u, bytes.NewReader(body))
	req.Header.Set("content-type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("status %d: %s", res.StatusCode, string(b))
	}
	return nil
}

// ---------------- ZIP writer helpers ----------------

// normalizeZipPath forces forward slashes and strips any leading slash so entries are
// relative within the archive. This avoids platform-specific path separators.
func normalizeZipPath(p string) string {
	p = strings.ReplaceAll(p, "\\", "/")
	p = strings.TrimPrefix(p, "/")
	return p
}

// dosDateTimeNow returns the current time encoded in MS-DOS date/time format,
// as required by the ZIP spec for file headers.
func dosDateTimeNow() (uint16, uint16) {
	t := time.Now()
	sec := t.Second() / 2
	dt := uint16(sec) | uint16(t.Minute())<<5 | uint16(t.Hour())<<11
	dd := uint16(t.Day()) | uint16(t.Month())<<5 | uint16(t.Year()-1980)<<9
	return dt, dd
}

// Little-endian integer encoders used when constructing ZIP structures.
func u16(v uint16) []byte {
	return []byte{byte(v), byte(v >> 8)}
}
func u32(v uint32) []byte {
	return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)}
}
func u64(v uint64) []byte {
	return []byte{
		byte(v),
		byte(v >> 8),
		byte(v >> 16),
		byte(v >> 24),
		byte(v >> 32),
		byte(v >> 40),
		byte(v >> 48),
		byte(v >> 56),
	}
}

// writeLocalHeader writes a ZIP local file header for a stored (uncompressed) file.
//
// We set CRC and sizes to 0 and rely on the data descriptor that follows the file data
// because we are streaming and cannot seek back to patch the header.
func writeLocalHeader(w *MultipartUploader, name []byte, modTime, modDate uint16) error {
	var b bytes.Buffer
	b.Write(u32(sigLocal))
	b.Write(u16(versionZip64))
	b.Write(u16(gpUtf8AndDD))
	b.Write(u16(methodStore))
	b.Write(u16(modTime))
	b.Write(u16(modDate))
	b.Write(u32(0))
	b.Write(u32(0))
	b.Write(u32(0))
	b.Write(u16(uint16(len(name))))
	b.Write(u16(0))
	b.Write(name)
	return w.Write(b.Bytes())
}

// writeDataDescriptor writes the post-data descriptor that carries CRC + sizes.
// We always use the ZIP64 layout (8-byte sizes).
func writeDataDescriptor(w *MultipartUploader, crc uint32, comp, uncomp uint64) error {
	var b bytes.Buffer
	b.Write(u32(sigDataDescriptor))
	b.Write(u32(crc))
	b.Write(u64(comp))
	b.Write(u64(uncomp))
	return w.Write(b.Bytes())
}

// writeCentralDirectoryEntry writes a ZIP central directory header for one file.
//
// We always emit ZIP64 extra fields containing the true sizes and local header offset,
// and we write 0xFFFFFFFF placeholders in the 32-bit fields per the ZIP64 spec.
func writeCentralDirectoryEntry(w *MultipartUploader, e CentralEntry) error {
	name, err := base64.StdEncoding.DecodeString(e.NameB64)
	if err != nil {
		return err
	}
	comp, _ := strconv.ParseUint(e.CompressedSize, 10, 64)
	uncomp, _ := strconv.ParseUint(e.UncompressedSize, 10, 64)
	off, _ := strconv.ParseUint(e.LocalHeaderOffset, 10, 64)

	zip64Extra := new(bytes.Buffer)
	zip64Extra.Write(u16(0x0001))
	zip64Extra.Write(u16(24))
	zip64Extra.Write(u64(uncomp))
	zip64Extra.Write(u64(comp))
	zip64Extra.Write(u64(off))

	var b bytes.Buffer
	b.Write(u32(sigCentral))
	b.Write(u16(versionZip64))
	b.Write(u16(versionZip64))
	b.Write(u16(gpUtf8AndDD))
	b.Write(u16(methodStore))
	b.Write(u16(e.ModTime))
	b.Write(u16(e.ModDate))
	b.Write(u32(e.CRC32))
	b.Write(u32(maxU32))
	b.Write(u32(maxU32))
	b.Write(u16(uint16(len(name))))
	b.Write(u16(uint16(zip64Extra.Len())))
	b.Write(u16(0))
	b.Write(u16(0))
	b.Write(u16(0))
	b.Write(u32(0))
	b.Write(u32(maxU32))
	b.Write(name)
	b.Write(zip64Extra.Bytes())
	return w.Write(b.Bytes())
}

// writeZip64EndOfCentralDirectory writes the ZIP64 EOCD record summarizing the archive.
func writeZip64EndOfCentralDirectory(w *MultipartUploader, totalEntries uint64, centralSize, centralOffset BigInt) error {
	var b bytes.Buffer
	b.Write(u32(sigZip64Eocd))
	b.Write(u64(44))
	b.Write(u16(versionZip64))
	b.Write(u16(versionZip64))
	b.Write(u32(0))
	b.Write(u32(0))
	b.Write(u64(totalEntries))
	b.Write(u64(totalEntries))
	b.Write(u64(centralSize.n))
	b.Write(u64(centralOffset.n))
	return w.Write(b.Bytes())
}

// writeZip64EndOfCentralDirectoryLocator points ZIP readers to the ZIP64 EOCD record.
func writeZip64EndOfCentralDirectoryLocator(w *MultipartUploader, zip64EocdOffset BigInt) error {
	var b bytes.Buffer
	b.Write(u32(sigZip64Locator))
	b.Write(u32(0))
	b.Write(u64(zip64EocdOffset.n))
	b.Write(u32(1))
	return w.Write(b.Bytes())
}

// writeEndOfCentralDirectory writes the classic EOCD record with placeholder sizes/offsets.
// The real values are provided via ZIP64 structures; classic EOCD enables compatibility
// with readers that expect it to exist even for ZIP64 archives.
func writeEndOfCentralDirectory(w *MultipartUploader, n int) error {
	count := n
	if count > maxU16 {
		count = maxU16
	}
	var b bytes.Buffer
	b.Write(u32(sigEocd))
	b.Write(u16(0))
	b.Write(u16(0))
	b.Write(u16(uint16(count)))
	b.Write(u16(uint16(count)))
	b.Write(u32(maxU32))
	b.Write(u32(maxU32))
	b.Write(u16(0))
	return w.Write(b.Bytes())
}

// Silence unused imports in early iterations.
//
// This container file has been iterated on quickly and sometimes shares snippets with
// other implementations; keeping these avoids churn while stabilizing the build.
var _ = os.Getenv
var _ = strings.Builder{}
var _ = url.Values{}
var _ = bytes.MinRead
var _ = fmt.Sprintf
var _ = log.Printf
var _ = context.Canceled
var _ = io.Discard
var _ = time.Second
var _ = crc32.IEEETable
var _ = base64.StdEncoding
var _ = strconv.IntSize
var _ = url.PathEscape
var _ = strings.Contains
var _ = http.MethodGet
var _ = bytes.NewBuffer
var _ = bytes.NewReader
var _ = strings.NewReader
var _ = url.QueryEscape
var _ = json.RawMessage{}
var _ = bytes.Compare
var _ = strings.EqualFold

