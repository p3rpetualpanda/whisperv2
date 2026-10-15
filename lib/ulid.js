'use strict';

// STEP 2 (build guide): sortable, unique message IDs.
// ULID = 48-bit millisecond timestamp + 80-bit randomness, Crockford base32.
// Lexicographic order == chronological order, unlike Date.now() strings.

const crypto = require('crypto');
const CROCKFORD = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
const ENCODED = new Array(128);
for (let i = 0; i < CROCKFORD.length; i++) ENCODED[CROCKFORD.charCodeAt(i)] = i;

/** Generate a new ULID (26 chars, sortable). */
function ulid() {
  // 128 bits = 16 bytes. First 6 bytes hold the 48-bit millisecond timestamp
  // (big-endian), the remaining 10 bytes hold 80 bits of randomness.
  const bytes = new Uint8Array(16);

  let ts = Date.now();
  for (let i = 5; i >= 0; i--) {
    bytes[i] = ts & 0xff;
    ts = Math.floor(ts / 256);
  }
  crypto.randomFillSync(bytes.subarray(6, 16));

  // Encode the 128 bits as 26 Crockford base32 chars (5-bit windows).
  // 26 * 5 = 130 bits, so the final char is zero-padded. `acc` is masked to
  // the pending bits each step so it never exceeds ~12 bits (safe integer),
  // avoiding the 32-bit coercion / precision bugs of the old implementation.
  let out = '';
  let acc = 0;
  let bits = 0;
  for (let i = 0; i < 16; i++) {
    acc = (acc << 8) | bytes[i];
    bits += 8;
    while (bits >= 5) {
      bits -= 5;
      out += CROCKFORD[(acc >>> bits) & 0x1f];
      acc &= (1 << bits) - 1;
    }
  }
  if (bits > 0) out += CROCKFORD[(acc << (5 - bits)) & 0x1f];
  return out;
}

module.exports = { ulid };
