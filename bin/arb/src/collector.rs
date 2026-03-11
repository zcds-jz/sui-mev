use burberry::{async_trait, Collector, CollectorStream};
use eyre::Result;
use fastcrypto::encoding::{Base64, Encoding};
use futures::stream::StreamExt;
use interprocess::local_socket::{
    tokio::{prelude::*, Stream},
    GenericNamespaced,
};
use serde::Deserialize;
use sui_json_rpc_types::{SuiEvent, SuiTransactionBlockEffects};
use sui_types::{effects::TransactionEffects, transaction::TransactionData};
use tokio::{io::AsyncReadExt, pin, time};
use tracing::{error, info, warn};

use crate::types::Event;

/*
    是Sui MEV项目中负责交易收集的核心模块，主要实现两种交易收集器
    1. PrivateTxCollector: 私有交易收集器，用于收集来自Sui节点的私有交易
    2. PublicTxCollector: 公有交易收集器，用于收集来自Sui节点的公有交易
    该模块为MEV套利系统提供实时交易数据源，是识别套利机会的基础组件。
*/
pub struct PublicTxCollector {
    path: String,
}

impl PublicTxCollector {
    pub fn new(path: &str) -> Self {
        Self { path: path.to_string() }
    }

    async fn connect(&self) -> Result<Stream> {
        let name = self.path.as_str().to_ns_name::<GenericNamespaced>()?;
        let conn = Stream::connect(name).await?;
        info!(socket = %self.path, "public tx collector connected");
        Ok(conn)
    }
}

#[async_trait]
impl Collector<Event> for PublicTxCollector {
    fn name(&self) -> &str {
        "PublicTxCollector"
    }

    async fn get_event_stream(&self) -> Result<CollectorStream<'_, Event>> {
        let mut conn = self.connect().await?;
        let mut effects_len_buf = [0u8; 4];
        let mut events_len_buf = [0u8; 4];

        let stream = async_stream::stream! {
            let mut received = 0u64;
            let mut status_tick = time::interval(time::Duration::from_secs(15));
            status_tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    result = conn.read_exact(&mut effects_len_buf) => {
                        if result.is_err() {
                            warn!(socket = %self.path, "failed to read effects length, reconnecting");
                            conn = self.connect().await.expect("Failed to reconnect to tx socket");
                            continue;
                        }

                        let effects_len = u32::from_be_bytes(effects_len_buf);
                        let mut effects_buf = vec![0u8; effects_len as usize];
                        if conn.read_exact(&mut effects_buf).await.is_err() {
                            warn!(socket = %self.path, "failed to read effects, reconnecting");
                            conn = self.connect().await.expect("Failed to reconnect to tx socket");
                            continue;
                        }

                        if conn.read_exact(&mut events_len_buf).await.is_err() {
                            warn!(socket = %self.path, "failed to read events length, reconnecting");
                            conn = self.connect().await.expect("Failed to reconnect to tx socket");
                            continue;
                        }

                        let events_len = u32::from_be_bytes(events_len_buf);
                        let mut events_buf = vec![0u8; events_len as usize];
                        if conn.read_exact(&mut events_buf).await.is_err() {
                            warn!(socket = %self.path, "failed to read events, reconnecting");
                            conn = self.connect().await.expect("Failed to reconnect to tx socket");
                            continue;
                        }

                        let tx_effects: TransactionEffects = match bincode::deserialize(&effects_buf) {
                            Ok(tx_effects) => tx_effects,
                            Err(e) => {
                                error!("Invalid tx_effects: {:?}", e);
                                continue;
                            }
                        };

                        let events: Vec<SuiEvent> = if events_len == 0 {
                            vec![]
                        } else {
                            match serde_json::from_slice(&events_buf) {
                                Ok(events) => events,
                                Err(e) => {
                                    error!("Invalid events: {:?}", e);
                                    continue;
                                }
                            }
                        };

                        if let Ok(tx_effects) = SuiTransactionBlockEffects::try_from(tx_effects) {
                            received = received.saturating_add(1);
                            yield Event::PublicTx(tx_effects, events);
                        }

                    }
                    _ = status_tick.tick() => {
                        info!(socket = %self.path, received, "public tx collector alive");
                    }
                    else => {
                        time::sleep(time::Duration::from_millis(10)).await;
                    }
                }
            }
        };

        Ok(Box::pin(stream))
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TxMessage {
    tx_bytes: String,
}

impl TryFrom<TxMessage> for TransactionData {
    type Error = eyre::Error;

    fn try_from(tx_message: TxMessage) -> Result<Self> {
        let tx_bytes = Base64::decode(&tx_message.tx_bytes)?;
        let tx_data: TransactionData = bcs::from_bytes(&tx_bytes)?;
        Ok(tx_data)
    }
}

pub struct PrivateTxCollector {
    ws_url: String,
}

impl PrivateTxCollector {
    pub fn new(ws_url: &str) -> Self {
        Self {
            ws_url: ws_url.to_string(),
        }
    }
}

#[async_trait]
impl Collector<Event> for PrivateTxCollector {
    fn name(&self) -> &str {
        "PrivateTxCollector"
    }

    async fn get_event_stream(&self) -> Result<CollectorStream<'_, Event>> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(&self.ws_url)
            .await
            .expect("Failed to connect to relay server");
        info!(ws = %self.ws_url, "private tx collector connected");

        let (_, read) = ws_stream.split();

        let stream = async_stream::stream! {
            pin!(read);
            let mut received = 0u64;
            let mut status_tick = time::interval(time::Duration::from_secs(15));
            status_tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    message = read.next() => {
                        let Some(message) = message else {
                            warn!(ws = %self.ws_url, "relay websocket closed");
                            break;
                        };

                        let message = match message {
                            Ok(msg) => msg,
                            Err(e) => {
                                error!("Relay websocket error: {:?}", e);
                                continue;
                            }
                        };

                        let tx_message: TxMessage = serde_json::from_str(message.to_text().unwrap()).unwrap();
                        let tx_data = match TransactionData::try_from(tx_message) {
                            Ok(tx_data) => tx_data,
                            Err(e) => {
                                error!("Invalid tx_message: {:?}", e);
                                continue;
                            }
                        };

                        received = received.saturating_add(1);
                        yield Event::PrivateTx(tx_data);
                    }
                    _ = status_tick.tick() => {
                        info!(ws = %self.ws_url, received, "private tx collector alive");
                    }
                }
            }
        };

        Ok(Box::pin(stream))
    }
}
