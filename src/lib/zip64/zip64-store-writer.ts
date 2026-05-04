import { concatBytes, dosDateTime, normalizeZipPath, textEncoder, u16, u32, u64 } from "./binary";
import { crc32Update } from "./crc32";
import type { R2MultipartSink } from "./r2-multipart-sink";

const SIG_LOCAL = 0x04034b50;
const SIG_DATA_DESCRIPTOR = 0x08074b50;
const SIG_CENTRAL = 0x02014b50;
const SIG_ZIP64_EOCD = 0x06064b50;
const SIG_ZIP64_LOCATOR = 0x07064b50;
const SIG_EOCD = 0x06054b50;

const GP_UTF8_AND_DATA_DESCRIPTOR = 0x0808;
const VERSION_ZIP64 = 45;
const METHOD_STORE = 0;
const MAX_U16 = 0xffff;
const MAX_U32 = 0xffffffff;

type CentralDirectoryEntry = {
  nameBytes: Uint8Array;
  crc32: number;
  compressedSize: bigint;
  uncompressedSize: bigint;
  localHeaderOffset: bigint;
  modTime: number;
  modDate: number;
};

/**
 * Streaming STORE-only ZIP writer with ZIP64 metadata (data descriptors + EOCD).
 * Local headers use zero CRC/sizes per APPNOTE 4.4.4 when bit 3 is set; sizes live
 * in the ZIP64 data descriptor and central directory ZIP64 extra.
 */
export class Zip64StoreWriter {
  private offset = 0n;
  private readonly entries: CentralDirectoryEntry[] = [];
  private closed = false;

  constructor(private readonly sink: R2MultipartSink) {}

  async addFile(
    name: string,
    body: ReadableStream<Uint8Array>,
    expectedSize?: number,
  ): Promise<void> {
    if (this.closed) {
      throw new Error("Cannot add file after ZIP has been closed");
    }

    const safeName = normalizeZipPath(name);
    const nameBytes = textEncoder.encode(safeName);
    const { time, date } = dosDateTime();
    const localHeaderOffset = this.offset;

    await this.writeLocalHeader(nameBytes, time, date);

    const reader = body.getReader();
    let crc = 0;
    let size = 0n;

    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) {
          break;
        }
        if (!value || value.byteLength === 0) {
          continue;
        }
        crc = crc32Update(crc, value);
        size += BigInt(value.byteLength);
        await this.write(value);
      }
    } finally {
      reader.releaseLock();
    }

    if (expectedSize !== undefined && BigInt(expectedSize) !== size) {
      throw new Error(
        `Size mismatch for ${safeName}. Expected ${expectedSize}, got ${size.toString()}`,
      );
    }

    await this.writeDataDescriptor(crc, size, size);

    this.entries.push({
      nameBytes,
      crc32: crc,
      compressedSize: size,
      uncompressedSize: size,
      localHeaderOffset,
      modTime: time,
      modDate: date,
    });
  }

  async close(): Promise<R2Object> {
    if (this.closed) {
      throw new Error("ZIP already closed");
    }

    this.closed = true;

    const centralDirectoryOffset = this.offset;

    for (const entry of this.entries) {
      await this.writeCentralDirectoryEntry(entry);
    }

    const centralDirectorySize = this.offset - centralDirectoryOffset;

    await this.writeZip64EndOfCentralDirectory(
      BigInt(this.entries.length),
      centralDirectorySize,
      centralDirectoryOffset,
    );

    const zip64EocdOffset = centralDirectoryOffset + centralDirectorySize;
    await this.writeZip64EndOfCentralDirectoryLocator(zip64EocdOffset);

    await this.writeEndOfCentralDirectory();

    return this.sink.close();
  }

  async abort(): Promise<void> {
    await this.sink.abort();
  }

  private async write(bytes: Uint8Array): Promise<void> {
    await this.sink.write(bytes);
    this.offset += BigInt(bytes.byteLength);
  }

  /** Local header: CRC and sizes are zero; no extra field (APPNOTE 4.4.4 + bit 3). */
  private async writeLocalHeader(
    nameBytes: Uint8Array,
    modTime: number,
    modDate: number,
  ): Promise<void> {
    const header = concatBytes([
      u32(SIG_LOCAL),
      u16(VERSION_ZIP64),
      u16(GP_UTF8_AND_DATA_DESCRIPTOR),
      u16(METHOD_STORE),
      u16(modTime),
      u16(modDate),
      u32(0),
      u32(0),
      u32(0),
      u16(nameBytes.byteLength),
      u16(0),
      nameBytes,
    ]);
    await this.write(header);
  }

  private async writeDataDescriptor(
    crc: number,
    compressedSize: bigint,
    uncompressedSize: bigint,
  ): Promise<void> {
    const descriptor = concatBytes([
      u32(SIG_DATA_DESCRIPTOR),
      u32(crc),
      u64(compressedSize),
      u64(uncompressedSize),
    ]);
    await this.write(descriptor);
  }

  private async writeCentralDirectoryEntry(entry: CentralDirectoryEntry): Promise<void> {
    const zip64Extra = concatBytes([
      u16(0x0001),
      u16(24),
      u64(entry.uncompressedSize),
      u64(entry.compressedSize),
      u64(entry.localHeaderOffset),
    ]);

    const header = concatBytes([
      u32(SIG_CENTRAL),
      u16(VERSION_ZIP64),
      u16(VERSION_ZIP64),
      u16(GP_UTF8_AND_DATA_DESCRIPTOR),
      u16(METHOD_STORE),
      u16(entry.modTime),
      u16(entry.modDate),
      u32(entry.crc32),
      u32(MAX_U32),
      u32(MAX_U32),
      u16(entry.nameBytes.byteLength),
      u16(zip64Extra.byteLength),
      u16(0),
      u16(0),
      u16(0),
      u32(0),
      u32(MAX_U32),
      entry.nameBytes,
      zip64Extra,
    ]);
    await this.write(header);
  }

  private async writeZip64EndOfCentralDirectory(
    totalEntries: bigint,
    centralDirectorySize: bigint,
    centralDirectoryOffset: bigint,
  ): Promise<void> {
    const record = concatBytes([
      u32(SIG_ZIP64_EOCD),
      u64(44),
      u16(VERSION_ZIP64),
      u16(VERSION_ZIP64),
      u32(0),
      u32(0),
      u64(totalEntries),
      u64(totalEntries),
      u64(centralDirectorySize),
      u64(centralDirectoryOffset),
    ]);
    await this.write(record);
  }

  private async writeZip64EndOfCentralDirectoryLocator(zip64EocdOffset: bigint): Promise<void> {
    const locator = concatBytes([
      u32(SIG_ZIP64_LOCATOR),
      u32(0),
      u64(zip64EocdOffset),
      u32(1),
    ]);
    await this.write(locator);
  }

  private async writeEndOfCentralDirectory(): Promise<void> {
    const n = this.entries.length;
    const countField = Math.min(n, MAX_U16);
    const eocd = concatBytes([
      u32(SIG_EOCD),
      u16(0),
      u16(0),
      u16(countField),
      u16(countField),
      u32(MAX_U32),
      u32(MAX_U32),
      u16(0),
    ]);
    await this.write(eocd);
  }
}
