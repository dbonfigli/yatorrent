use std::time::{Duration, Instant};

const BURST_WINDOW_MS: u128 = 1_500; // 1.5s

pub struct RateLimiter {
    capacity: u128,
    tokens: u128,
    refill_rate_per_sec: u128, // tokens per seconds
    last_update: Instant,
}

impl RateLimiter {
    pub fn new(refill_rate_per_sec: u128) -> Self {
        assert!(refill_rate_per_sec > 0);

        let capacity = refill_rate_per_sec * BURST_WINDOW_MS / 1_000;
        RateLimiter {
            capacity,
            tokens: capacity,
            refill_rate_per_sec,
            last_update: Instant::now(),
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed_ns = now.duration_since(self.last_update).as_nanos();
        self.last_update = now;

        // tokens added = rate * seconds
        let added = self.refill_rate_per_sec.saturating_mul(elapsed_ns) / 1_000_000_000;
        self.tokens = self.tokens.saturating_add(added).min(self.capacity);
    }

    pub fn try_acquire(&mut self, tokens: u128) -> Result<(), Duration> {
        assert!(tokens <= self.capacity);

        self.refill();

        if self.tokens >= tokens {
            self.tokens -= tokens;
            return Ok(());
        }

        let missing = tokens - self.tokens;

        // time needed in ms = missing / rate_per_sec * 1_000_000_000
        let wait_ns = missing.saturating_mul(1_000_000_000) / self.refill_rate_per_sec;

        Err(Duration::from_nanos_u128(wait_ns))
    }
}
