/** IEEE CRC32 (PKZIP) — 4-byte unrolled table update for throughput on large streams. */

const CRC32_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let k = 0; k < 8; k++) {
      c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    }
    table[i] = c >>> 0;
  }
  return table;
})();

export function crc32Update(crc: number, chunk: Uint8Array): number {
  let c = crc ^ 0xffffffff;
  const n = chunk.byteLength;
  let i = 0;

  while (i + 4 <= n) {
    c = CRC32_TABLE[(c ^ chunk[i]) & 0xff] ^ (c >>> 8);
    c = CRC32_TABLE[(c ^ chunk[i + 1]) & 0xff] ^ (c >>> 8);
    c = CRC32_TABLE[(c ^ chunk[i + 2]) & 0xff] ^ (c >>> 8);
    c = CRC32_TABLE[(c ^ chunk[i + 3]) & 0xff] ^ (c >>> 8);
    i += 4;
  }

  while (i < n) {
    c = CRC32_TABLE[(c ^ chunk[i++]) & 0xff] ^ (c >>> 8);
  }

  return (c ^ 0xffffffff) >>> 0;
}
