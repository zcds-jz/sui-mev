use crate::shio_conn::new_shio_conn;
use crate::types::ShioItem;
use async_channel::Receiver;
use burberry::{async_trait, Collector, CollectorStream};
use eyre::Result;
use tokio::time::{Duration, MissedTickBehavior};
use tracing::{info, warn};

pub struct ShioCollector {
    receiver: Receiver<ShioItem>,
}

// Only one connection to the ws server
impl ShioCollector {
    // Only one connection to the ws server
    pub async fn new_without_executor(wss_url: String, num_retries: Option<u32>) -> Self {
        warn!("only reading from shio feed, not sending any bids");
        let (_, receiver) = new_shio_conn(wss_url, num_retries.unwrap_or(3)).await;
        Self { receiver }
    }

    pub fn new(receiver: Receiver<ShioItem>) -> Self {
        Self { receiver }
    }
}

#[async_trait]
impl Collector<ShioItem> for ShioCollector {
    fn name(&self) -> &str {
        "ShioCollector"
    }

    async fn get_event_stream(&self) -> Result<CollectorStream<'_, ShioItem>> {
        let stream = async_stream::stream! {
            let receiver = self.receiver.clone();
            let mut received = 0u64;
            let mut status_tick = tokio::time::interval(Duration::from_secs(15));
            status_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    recv = receiver.recv() => {
                        match recv {
                            Ok(item) => {
                                received = received.saturating_add(1);
                                yield item;
                            }
                            Err(_) => break,
                        }
                    }
                    _ = status_tick.tick() => {
                        info!(received, queue_len = receiver.len(), "shio collector alive");
                    }
                }
            }

            panic!("ShioCollector stream ended unexpectedly");
        };

        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_shio_collector() {
        use crate::SHIO_FEED_URL;
        use futures::StreamExt;

        mev_logger::init_console_logger(None);

        // cargo test --package shio --lib -- shio_collector::tests::test_shio_collector --exact --show-output --nocapture
        let collector = ShioCollector::new_without_executor(SHIO_FEED_URL.to_string(), Some(0)).await;
        let mut stream = collector.get_event_stream().await.unwrap();

        while let Some(item) = stream.next().await {
            match item {
                ShioItem::AuctionStarted { .. } => {
                    println!("{:#?}", item.type_name());
                }
                ShioItem::AuctionEnded { .. } => {
                    println!("{:#?}", item.type_name());
                }
                ShioItem::Dummy(_) => {
                    println!("{:#?}", item);
                }
            }
        }
    }
}
