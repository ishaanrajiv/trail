// UUIDv7 (RFC 9562): 48 bits of Unix time in ms, the version (7), 12 bits of `rand_a`, the
// variant (0b10) and 62 random bits. Within one millisecond `rand_a` counts up from a random
// start, so ids made by one process sort in the order they were made; spool names rely on that.

let lastMs = 0;
let seq = 0;

const HEX: string[] = Array.from({ length: 256 }, (_, i) => i.toString(16).padStart(2, "0"));

export function nowMs(): number {
  return Date.now();
}

export function uuid7(): string {
  const ms = Date.now();
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  if (ms > lastMs) {
    lastMs = ms;
    seq = ((bytes[6] ?? 0) << 8 | (bytes[7] ?? 0)) & 0x7ff; // leave room to count up
  } else {
    seq += 1;
    if (seq > 0xfff) {
      lastMs += 1; // borrow the next millisecond rather than repeat
      seq = 0;
    }
  }
  let time = lastMs;
  for (let i = 5; i >= 0; i--) {
    bytes[i] = time % 256;
    time = Math.floor(time / 256);
  }
  bytes[6] = 0x70 | (seq >> 8);
  bytes[7] = seq & 0xff;
  bytes[8] = 0x80 | ((bytes[8] ?? 0) & 0x3f);
  let text = "";
  for (let i = 0; i < 16; i++) {
    if (i === 4 || i === 6 || i === 8 || i === 10) text += "-";
    text += HEX[bytes[i] ?? 0];
  }
  return text;
}
