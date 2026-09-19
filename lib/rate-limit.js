'use strict';

// STEP 12 (build guide): per-connection rate limiting with a token bucket.
// Prevents one client from flooding a room.

class TokenBucket {
  /**
   * @param {number} capacity  Max burst size (tokens).
   * @param {number} refillPerSec  Tokens added per second.
   */
  constructor(capacity, refillPerSec) {
    this.capacity = capacity;
    this.refillPerSec = refillPerSec;
    this.tokens = capacity;
    this.last = Date.now();
  }

  /** Consume one token. Returns true if allowed, false if rate-limited. */
  take() {
    const now = Date.now();
    const elapsed = (now - this.last) / 1000;
    this.last = now;
    this.tokens = Math.min(this.capacity, this.tokens + elapsed * this.refillPerSec);
    if (this.tokens >= 1) {
      this.tokens -= 1;
      return true;
    }
    return false;
  }
}

module.exports = { TokenBucket };
