use std::time::{Duration, Instant};

use tokio::time::sleep;

// how much burst allowerd over the max bandwitdh
const BURST_FACTOR: f64 = 1.5;

pub struct RateLimiter {
    max_burst: f64, // bytes/s, max allowed burst over the max_bandwidth limit
    tokens: f64,
    max_bandwidth: f64, // bytes/s, refill rate
    last_update: Instant,
}

impl RateLimiter {
    pub fn new(max_bandwidth: f64) -> Self {
        let max_burst = max_bandwidth * BURST_FACTOR;
        RateLimiter {
            max_burst,
            tokens: max_burst,
            max_bandwidth,
            last_update: Instant::now(),
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let delta_time = (now - self.last_update).as_secs_f64();
        self.last_update = now;

        self.tokens = (self.tokens + delta_time * self.max_bandwidth).min(self.max_burst);
    }

    pub async fn consume(&mut self, bytes: usize) {
        self.refill();

        if self.tokens < bytes as f64 {
            let deficit = bytes as f64 - self.tokens;
            let wait_s = deficit / self.max_bandwidth;
            sleep(Duration::from_secs_f64(wait_s)).await;
        }

        self.tokens -= bytes as f64;
    }
}
