use size::{Size, Style};
use std::{
    collections::VecDeque,
    fmt::{self, Display},
    time::{Duration, SystemTime},
};

const BANDWIDTH_POLL_COUNT: usize = 3;

struct BandwidthPoll {
    poll_time: SystemTime,
    bandwidth_up: f64,     // B/s
    bandwidth_down: f64,   // B/s
    uploaded_bytes: u64,   // total uploaded bytes up to this poll
    downloaded_bytes: u64, // tot downloaded bytes up to this poll
}

pub struct BandwidthTracker {
    bandwidth_polls: VecDeque<BandwidthPoll>,
    uploaded_bytes: u64,
    downloaded_bytes: u64,
}

impl BandwidthTracker {
    pub fn new() -> Self {
        BandwidthTracker {
            bandwidth_polls: VecDeque::new(),
            uploaded_bytes: 0,
            downloaded_bytes: 0,
        }
    }

    pub fn update(&mut self) {
        if self.bandwidth_polls.is_empty() {
            self.bandwidth_polls.push_front(BandwidthPoll {
                poll_time: SystemTime::now(),
                bandwidth_up: 0.,
                bandwidth_down: 0.,
                uploaded_bytes: self.uploaded_bytes,
                downloaded_bytes: self.downloaded_bytes,
            });
            return;
        }

        let latest_bandwith_poll = self
            .bandwidth_polls
            .front()
            .expect("invariant checked above");

        let now = SystemTime::now();
        let elapsed_s = now
            .duration_since(latest_bandwith_poll.poll_time)
            .unwrap_or_default()
            .as_secs_f64();

        let bandwidth_up =
            (self.uploaded_bytes - latest_bandwith_poll.uploaded_bytes) as f64 / elapsed_s;
        let bandwidth_down =
            (self.downloaded_bytes - latest_bandwith_poll.downloaded_bytes) as f64 / elapsed_s;

        let new_bandwith_poll = BandwidthPoll {
            poll_time: now,
            bandwidth_up,
            bandwidth_down,
            uploaded_bytes: self.uploaded_bytes,
            downloaded_bytes: self.downloaded_bytes,
        };

        self.bandwidth_polls.push_front(new_bandwith_poll);
        if self.bandwidth_polls.len() > BANDWIDTH_POLL_COUNT {
            self.bandwidth_polls.pop_back();
        }
    }

    pub fn add_uploaded_bytes(&mut self, bytes: u64) {
        self.uploaded_bytes += bytes;
    }

    pub fn add_downloaded_bytes(&mut self, bytes: u64) {
        self.downloaded_bytes += bytes;
    }

    pub fn uploaded_bytes(&self) -> u64 {
        self.uploaded_bytes
    }

    pub fn downloaded_bytes(&self) -> u64 {
        self.downloaded_bytes
    }

    pub fn bandwidth_up(&self) -> f64 {
        match self.bandwidth_polls.front() {
            Some(latest_bandwidth_poll) => latest_bandwidth_poll.bandwidth_up,
            None => 0.,
        }
    }

    pub fn bandwidth_down(&self) -> f64 {
        match self.bandwidth_polls.front() {
            Some(latest_bandwidth_poll) => latest_bandwidth_poll.bandwidth_down,
            None => 0.,
        }
    }

    pub fn avg_bandwidth_down(&self) -> f64 {
        if self.bandwidth_polls.is_empty() {
            return 0.;
        } else if self.bandwidth_polls.len() == 1 {
            return self
                .bandwidth_polls
                .front()
                .expect("invariant checked above")
                .bandwidth_down;
        }

        let front = self
            .bandwidth_polls
            .front()
            .expect("invariant checked above");
        let back = self
            .bandwidth_polls
            .back()
            .expect("invariant checked above");

        let tot_down = (front.downloaded_bytes - back.downloaded_bytes) as f64;
        let poll_interval = front
            .poll_time
            .duration_since(back.poll_time)
            .unwrap_or(Duration::from_secs(1)); // avoid division by zero, but this should not be possible
        return tot_down / poll_interval.as_secs_f64();
    }

    pub fn avg_bandwidth_up(&self) -> f64 {
        if self.bandwidth_polls.is_empty() {
            return 0.;
        } else if self.bandwidth_polls.len() == 1 {
            return self
                .bandwidth_polls
                .front()
                .expect("invariant checked above")
                .bandwidth_up;
        }

        let front = self
            .bandwidth_polls
            .front()
            .expect("invariant checked above");
        let back = self
            .bandwidth_polls
            .back()
            .expect("invariant checked above");

        let tot_up = (front.uploaded_bytes - back.uploaded_bytes) as f64;
        let poll_interval = front
            .poll_time
            .duration_since(back.poll_time)
            .unwrap_or(Duration::from_secs(1)); // avoid division by zero, but this should not be possible
        return tot_up / poll_interval.as_secs_f64();
    }
}

impl Display for BandwidthTracker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Down: {down_band}/s, Up: {up_band}/s (tot.: {tot_down}, {tot_up})",
            down_band = Size::from_bytes(self.avg_bandwidth_down())
                .format()
                .with_style(Style::Abbreviated),
            up_band = Size::from_bytes(self.avg_bandwidth_up())
                .format()
                .with_style(Style::Abbreviated),
            tot_down = Size::from_bytes(self.downloaded_bytes)
                .format()
                .with_style(Style::Abbreviated),
            tot_up = Size::from_bytes(self.uploaded_bytes)
                .format()
                .with_style(Style::Abbreviated),
        )
    }
}
