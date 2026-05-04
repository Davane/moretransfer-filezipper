export type MultipartSinkOptions = {
  partSize?: number;
};

const MIN_PART = 5 * 1024 * 1024;

/**
 * Buffers ZIP bytes into fixed-size R2 multipart parts. Each full part is uploaded
 * with await; R2 retains the buffer until the upload completes, so we allocate a
 * new buffer after each flush.
 */
export class R2MultipartSink {
  private buf: Uint8Array;
  private offset = 0;
  private partNumber = 1;
  private readonly etags: R2UploadedPart[] = [];
  private closed = false;

  constructor(
    private readonly multipartUpload: R2MultipartUpload,
    options: MultipartSinkOptions = {},
  ) {
    const partSize = options.partSize ?? 32 * 1024 * 1024;
    if (partSize < MIN_PART) {
      throw new Error("R2 multipart part size must be at least 5 MiB");
    }
    this.buf = new Uint8Array(partSize);
  }

  async write(chunk: Uint8Array): Promise<void> {
    if (this.closed) {
      throw new Error("Cannot write to closed multipart sink");
    }

    let i = 0;
    while (i < chunk.byteLength) {
      const take = Math.min(this.buf.byteLength - this.offset, chunk.byteLength - i);
      this.buf.set(chunk.subarray(i, i + take), this.offset);
      this.offset += take;
      i += take;

      if (this.offset === this.buf.byteLength) {
        await this.flushFullPart();
      }
    }
  }

  private async flushFullPart(): Promise<void> {
    const part = this.buf;
    const uploaded = await this.multipartUpload.uploadPart(this.partNumber++, part);
    this.etags.push(uploaded);
    this.buf = new Uint8Array(part.byteLength);
    this.offset = 0;
  }

  async close(): Promise<R2Object> {
    if (this.closed) {
      throw new Error("Multipart sink already closed");
    }

    this.closed = true;

    if (this.offset > 0) {
      const finalPart = this.buf.subarray(0, this.offset);
      const uploaded = await this.multipartUpload.uploadPart(this.partNumber++, finalPart);
      this.etags.push(uploaded);
    }

    return this.multipartUpload.complete(this.etags);
  }

  async abort(): Promise<void> {
    this.closed = true;
    try {
      await this.multipartUpload.abort();
    } catch {
      // Ignore abort cleanup errors.
    }
  }
}
