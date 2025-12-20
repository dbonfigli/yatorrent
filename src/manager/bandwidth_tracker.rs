use size::{Size, Style};
use std::{
    fmt::{self, Display},
    time::SystemTime,
};

pub struct BandwidthTracker {
    last_bandwidth_poll: SystemTime,
    uploaded_bytes: u64,
    downloaded_bytes: u64,
    uploaded_bytes_previous_poll: u64,
    downloaded_bytes_previous_poll: u64,
    bandwidth_up: f64,   // B/s
    bandwidth_down: f64, // B/s
}

impl BandwidthTracker {
    pub fn new() -> Self {
        BandwidthTracker {
            last_bandwidth_poll: SystemTime::now(),
            uploaded_bytes: 0,
            downloaded_bytes: 0,
            uploaded_bytes_previous_poll: 0,
            downloaded_bytes_previous_poll: 0,
            bandwidth_up: 0f64,
            bandwidth_down: 0f64,
        }
    }

    pub fn update(&mut self) {
        let now = SystemTime::now();
        let elapsed_s = now
            .duration_since(self.last_bandwidth_poll)
            .unwrap_or_default()
            .as_millis() as f64
            / 1000f64;
        self.bandwidth_up =
            (self.uploaded_bytes - self.uploaded_bytes_previous_poll) as f64 / elapsed_s;
        self.bandwidth_down =
            (self.downloaded_bytes - self.downloaded_bytes_previous_poll) as f64 / elapsed_s;
        self.uploaded_bytes_previous_poll = self.uploaded_bytes;
        self.downloaded_bytes_previous_poll = self.downloaded_bytes;
        self.last_bandwidth_poll = now;
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
        self.bandwidth_up
    }

    pub fn bandwidth_down(&self) -> f64 {
        self.bandwidth_down
    }
}

impl Display for BandwidthTracker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Down: {down_band}/s, Up: {up_band}/s, (tot.: {tot_down}, {tot_up})",
            down_band = Size::from_bytes(self.bandwidth_down)
                .format()
                .with_style(Style::Abbreviated),
            up_band = Size::from_bytes(self.bandwidth_up)
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
