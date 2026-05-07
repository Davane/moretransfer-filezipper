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

// ZIP constants/signatures
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

type UploadedPart struct {
	PartNumber int    `json:"partNumber"`
	Etag       string `json:"etag"`
	SizeBytes  int    `json:"sizeBytes"`
}

type CentralEntry struct {
	NameB64         string `json:"nameB64"`
	CRC32           uint32 `json:"crc32"`
	CompressedSize  string `json:"compressedSize"`
	UncompressedSize string `json:"uncompressedSize"`
	LocalHeaderOffset string `json:"localHeaderOffset"`
	ModTime         uint16 `json:"modTime"`
	ModDate         uint16 `json:"modDate"`
}

type RunChunkResponse struct {
	UploadedParts    []UploadedPart `json:"uploadedParts"`
	NextPartNumber   int            `json:"nextPartNumber"`
	FileIndex        int            `json:"fileIndex"`
	ZipOffset        string         `json:"zipOffset"`
	BytesWrittenTotal string        `json:"bytesWrittenTotal"`
	FilesDone        int            `json:"filesDone"`
	Done             bool           `json:"done"`
}

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

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/runChunk", handleRunChunk)
	addr := ":8080"
	log.Printf("zip container listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func handleRunChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
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
			http.Error(w, fmt.Sprintf("size mismatch %s expected %d got %d", f.Key, f.Size, size), 500)
			return
		}

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

		// Chunking: stop only at file boundary. If we already uploaded >= MaxParts, exit.
		partsUploadedThisRun += uploader.PartsUploadedThisRun
		uploader.PartsUploadedThisRun = 0
		if partsUploadedThisRun >= req.MaxParts {
			break
		}
	}

	done := filesDone >= len(manifest.Files)

	if done {
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
		uploaded, err := uploader.FlushFinal()
		if err != nil {
			http.Error(w, "flush final failed: "+err.Error(), 500)
			return
		}
		_ = uploaded
		if err := completeMultipart(ctx, req.OutputKey, req.UploadID, uploader.AllParts()); err != nil {
			http.Error(w, "complete multipart failed: "+err.Error(), 500)
			return
		}
	} else {
		// For non-final chunks, flush any full parts already uploaded by uploader; do not flush partial buffer.
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
	w.Header().Set("content-type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(resp)
}

// ---------------- Multipart uploader ----------------

type BigInt struct{ n uint64 }

func (b BigInt) String() string { return fmt.Sprintf("%d", b.n) }
func (b BigInt) Add(o BigInt) BigInt { return BigInt{n: b.n + o.n} }
func (b BigInt) Sub(o BigInt) BigInt { return BigInt{n: b.n - o.n} }

func parseBigInt(s string) (BigInt, error) {
	u, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return BigInt{}, err
	}
	return BigInt{n: u}, nil
}

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

func (u *MultipartUploader) flushFull() error {
	partNum := u.NextPartNumber
	body := bytes.NewReader(u.buf.Bytes())
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

func (u *MultipartUploader) FlushFinal() (UploadedPart, error) {
	if u.buf.Len() == 0 {
		return UploadedPart{}, nil
	}
	partNum := u.NextPartNumber
	body := bytes.NewReader(u.buf.Bytes())
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

func (u *MultipartUploader) AllParts() []UploadedPart {
	return u.UploadedParts
}

// ---------------- Outbound helper calls ----------------

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

func normalizeZipPath(p string) string {
	p = strings.ReplaceAll(p, "\\", "/")
	p = strings.TrimPrefix(p, "/")
	return p
}

func dosDateTimeNow() (uint16, uint16) {
	t := time.Now()
	sec := t.Second() / 2
	dt := uint16(sec) | uint16(t.Minute())<<5 | uint16(t.Hour())<<11
	dd := uint16(t.Day()) | uint16(t.Month())<<5 | uint16(t.Year()-1980)<<9
	return dt, dd
}

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

func writeDataDescriptor(w *MultipartUploader, crc uint32, comp, uncomp uint64) error {
	var b bytes.Buffer
	b.Write(u32(sigDataDescriptor))
	b.Write(u32(crc))
	b.Write(u64(comp))
	b.Write(u64(uncomp))
	return w.Write(b.Bytes())
}

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

func writeZip64EndOfCentralDirectoryLocator(w *MultipartUploader, zip64EocdOffset BigInt) error {
	var b bytes.Buffer
	b.Write(u32(sigZip64Locator))
	b.Write(u32(0))
	b.Write(u64(zip64EocdOffset.n))
	b.Write(u32(1))
	return w.Write(b.Bytes())
}

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

// silence unused imports in early iterations
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

