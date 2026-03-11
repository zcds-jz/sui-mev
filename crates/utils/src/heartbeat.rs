use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::info;

pub fn start<T: Into<String>>(service_id: T, interval: Duration) -> JoinHandle<()> {
    let id = service_id.into();

    tokio::spawn(worker(id, interval))
}

async fn worker(id: String, interval: Duration) {
    info!("Heartbeat worker started for {}", id);

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        info!(service = %id, interval_secs = interval.as_secs(), "heartbeat tick");
    }
}
