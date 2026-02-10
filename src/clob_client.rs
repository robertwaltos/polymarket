use crate::config::Config;
use crate::error::AgentError;
use crate::risk_engine::{TradeSignal, TradeSide};
use crate::types::Market;
use ethers_contract::Contract;
use ethers_core::abi::AbiParser;
use ethers_core::types::{Address, U256};
use ethers_providers::{Http, Provider};
use ethers_signers::{LocalWallet, Signer};
use log::{info, warn};
use reqwest::Client;
use std::str::FromStr;
use std::sync::Arc;

pub struct ClobClient {
    client: Client,
    base_url: String,
    api_key: Option<String>,
    wallet: LocalWallet,
    provider: Arc<Provider<Http>>,
    usdc_contract: Address,
    usdc_decimals: u32,
    shadow_mode: bool,
    enable_pq_signing: bool,
}

impl ClobClient {
    pub fn new(client: Client, config: Config, wallet: LocalWallet) -> Result<Self, AgentError> {
        let provider = Provider::<Http>::try_from(config.polygon_rpc_url.as_str())
            .map_err(|e| AgentError::Wallet(e.to_string()))?;
        let usdc_contract = Address::from_str(&config.usdc_contract)
            .map_err(|e| AgentError::Wallet(e.to_string()))?;

        Ok(Self {
            client,
            base_url: "https://clob.polymarket.com".to_string(),
            api_key: config.clob_api_key,
            wallet,
            provider: Arc::new(provider),
            usdc_contract,
            usdc_decimals: config.usdc_decimals,
            shadow_mode: config.shadow_mode,
            enable_pq_signing: config.enable_pq_signing,
        })
    }

    pub async fn get_usdc_balance(&self) -> Result<f64, AgentError> {
        let abi = AbiParser::default()
            .parse_str("function balanceOf(address) view returns (uint256)")
            .map_err(|e| AgentError::Wallet(e.to_string()))?;
        let contract = Contract::new(self.usdc_contract, abi, self.provider.clone());
        let balance: U256 = contract
            .method::<_, U256>("balanceOf", self.wallet.address())
            .map_err(|e| AgentError::Wallet(e.to_string()))?
            .call()
            .await
            .map_err(|e| AgentError::Wallet(e.to_string()))?;

        let raw = balance.as_u128() as f64;
        let divisor = 10f64.powi(self.usdc_decimals as i32);
        Ok(raw / divisor)
    }

    pub async fn place_limit_order(
        &self,
        market: &Market,
        signal: &TradeSignal,
        price: f64,
    ) -> Result<(), AgentError> {
        if self.shadow_mode {
            info!(
                "[SHADOW] Would place order: market={} side={:?} price={:.4} size=${:.2}",
                market.id, signal.side, price, signal.size_usdc
            );
            return Ok(());
        }

        let api_key = self
            .api_key
            .clone()
            .ok_or_else(|| AgentError::Clob("CLOB_API_KEY missing".to_string()))?;

        let side = match signal.side {
            TradeSide::BuyYes => "buy",
            TradeSide::BuyNo => "buy",
            TradeSide::None => return Ok(()),
        };

        let token_id = market.id.clone();
        let order_payload = serde_json::json!({
            "token_id": token_id,
            "side": side,
            "price": price,
            "size": signal.size_usdc,
            "order_type": "limit"
        });

        if self.enable_pq_signing {
            warn!(\"PQ signing requested, but CLOB requires ECDSA; falling back to ECDSA signer.\");
        }

        let signature = self
            .wallet
            .sign_message(order_payload.to_string())
            .await
            .map_err(|e| AgentError::Wallet(e.to_string()))?;

        let body = serde_json::json!({
            "order": order_payload,
            "signature": signature.to_string(),
        });

        let resp = self
            .client
            .post(format!("{}/order", self.base_url))
            .header("Authorization", api_key)
            .json(&body)
            .send()
            .await?
            .error_for_status();

        match resp {
            Ok(_) => {
                info!(
                    "Order placed: market={} side={:?} price={:.4} size=${:.2}",
                    market.id, signal.side, price, signal.size_usdc
                );
                Ok(())
            }
            Err(err) => {
                warn!("Order failed: {}", err);
                Err(AgentError::Clob(err.to_string()))
            }
        }
    }
}
