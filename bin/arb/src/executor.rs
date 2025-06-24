use async_trait::async_trait;
use burberry::Executor;
use eyre::Result;
use fastcrypto::hash::HashFunction;
use shared_crypto::intent::{Intent, IntentMessage};
use sui_json_rpc_types::{SuiTransactionBlockResponse, SuiTransactionBlockResponseOptions};
use sui_sdk::{SuiClient, SuiClientBuilder};
use sui_types::{
    crypto::{Signer, SuiKeyPair},
    signature::GenericSignature,
    transaction::{Transaction, TransactionData},
};
use tracing::info;

/*
PublicTxExecutor 是Sui MEV项目的交易执行器，主要功能包括：

1. 核心功能 ：
   
   - 实现 Executor<TransactionData> trait
   - 负责将MEV套利交易提交到Sui区块链
   - 提供交易状态追踪和日志记录
该执行器是MEV套利流水线的最后环节，负责将模拟验证通过的交易实际提交到区块链。   
*/
pub struct PublicTxExecutor {
    sui: SuiClient,
    keypair: SuiKeyPair,
}

impl PublicTxExecutor {
    pub async fn new(rpc_url: &str, keypair: SuiKeyPair) -> Result<Self> {
        let sui = SuiClientBuilder::default().build(rpc_url).await?;
        Ok(Self { sui, keypair })
    }

    pub async fn execute_tx(&self, tx_data: TransactionData) -> Result<SuiTransactionBlockResponse> {
        let intent_msg = IntentMessage::new(Intent::sui_transaction(), tx_data);
        let raw_tx = bcs::to_bytes(&intent_msg)?;

        let digest = {
            let mut hasher = sui_types::crypto::DefaultHash::default();
            hasher.update(raw_tx.clone());
            hasher.finalize().digest
        };

        let sig = self.keypair.sign(&digest);
        let tx = Transaction::from_generic_sig_data(intent_msg.value, vec![GenericSignature::Signature(sig)]);

        let options = SuiTransactionBlockResponseOptions::default();
        let tx_resp = self
            .sui
            .quorum_driver_api()
            .execute_transaction_block(tx, options, None)
            .await?;

        Ok(tx_resp)
    }
}

#[async_trait]
impl Executor<TransactionData> for PublicTxExecutor {
    fn name(&self) -> &str {
        "PublicTxExecutor"
    }

    async fn execute(&self, action: TransactionData) -> Result<()> {
        let resp = self.execute_tx(action).await?;
        let digest = resp.digest.base58_encode();

        info!(?digest, status_ok = ?resp.status_ok(), "Executed tx");
        Ok(())
    }
}
