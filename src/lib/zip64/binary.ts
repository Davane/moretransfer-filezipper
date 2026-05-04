/** Shared little-endian packing and path helpers for ZIP64 writer. */

export const textEncoder = new TextEncoder();

export function u16(value: number): Uint8Array {
  const b = new Uint8Array(2);
  new DataView(b.buffer).setUint16(0, value, true);
  return b;
}

export function u32(value: number): Uint8Array {
  const b = new Uint8Array(4);
  new DataView(b.buffer).setUint32(0, value >>> 0, true);
  return b;
}

export function u64(value: number | bigint): Uint8Array {
  const b = new Uint8Array(8);
  new DataView(b.buffer).setBigUint64(0, BigInt(value), true);
  return b;
}

export function concatBytes(parts: Uint8Array[]): Uint8Array {
  const size = parts.reduce((sum, p) => sum + p.byteLength, 0);
  const out = new Uint8Array(size);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.byteLength;
  }
  return out;
}

export function dosDateTime(date = new Date()): { time: number; date: number } {
  const year = Math.max(date.getFullYear(), 1980);
  const dosTime =
    (date.getHours() << 11) | (date.getMinutes() << 5) | Math.floor(date.getSeconds() / 2);
  const dosDate = ((year - 1980) << 9) | ((date.getMonth() + 1) << 5) | date.getDate();
  return { time: dosTime, date: dosDate };
}

export function normalizeZipPath(name: string): string {
  return (
    name
      .replace(/\\/g, "/")
      .replace(/^\/+/, "")
      .replace(/\.\.+/g, "_")
      .replace(/^\.+$/, "_") || "file"
  );
}
