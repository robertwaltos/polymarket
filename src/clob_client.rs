use crate::config::Config;
use crate::error::AgentError;
use crate::risk_engine::{TradeSignal, TradeSide};
use crate::types::Market;
use ethers_contract::Contract;
use ethers_core::abi::AbiParser;
use ethers_core::types::transaction::eip712::{
    EIP712Domain, Eip712DomainType, TypedData, Types,
};
use ethers_core::types::{Address, U256};
use ethers_providers::{Http, Provider};
use ethers_signers::{LocalWallet, Signer};
use hmac::{Hmac, Mac};
use log::{info, warn};
use reqwest::Client;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use rand::Rng;
use serde_json::json;
use base64::engine::general_purpose::URL_SAFE;
use base64::Engine as _;
use chrono::Utc;

pub struct ClobClient {
    client: Client,
    base_url: String,
    api_key: Option<String>,
    api_secret: Option<String>,
    api_passphrase: Option<String>,
    wallet: LocalWallet,
    provider: Arc<Provider<Http>>,
    usdc_contract: Address,
    usdc_decimals: u32,
    shadow_mode: bool,
    enable_pq_signing: bool,
    exchange_address: Address,
    taker_address: Address,
    fee_rate_bps: u64,
    nonce_override: u64,
    expiration_secs: u64,
    order_type: String,
    post_only: bool,
    tick_size: f64,
    signature_type: u8,
    neg_risk: bool,
    funder: Option<Address>,
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
            api_secret: config.clob_api_secret,
            api_passphrase: config.clob_api_passphrase,
            wallet,
            provider: Arc::new(provider),
            usdc_contract,
            usdc_decimals: config.usdc_decimals,
            shadow_mode: config.shadow_mode,
            enable_pq_signing: config.enable_pq_signing,
            exchange_address: Address::from_str(&config.clob_exchange_address)
                .map_err(|e| AgentError::Wallet(e.to_string()))?,
            taker_address: Address::from_str(&config.clob_taker_address)
                .map_err(|e| AgentError::Wallet(e.to_string()))?,
            fee_rate_bps: config.clob_fee_rate_bps,
            nonce_override: config.clob_nonce,
            expiration_secs: config.clob_expiration_secs,
            order_type: config.clob_order_type,
            post_only: config.clob_post_only,
            tick_size: config.clob_tick_size,
            signature_type: config.clob_signature_type,
            neg_risk: config.clob_neg_risk,
            funder: config
                .clob_funder
                .as_deref()
                .and_then(|addr| Address::from_str(addr).ok()),
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
        let api_secret = self
            .api_secret
            .as_deref()
            .ok_or_else(|| AgentError::Clob("CLOB_API_SECRET missing".to_string()))?;
        let api_passphrase = self
            .api_passphrase
            .as_deref()
            .ok_or_else(|| AgentError::Clob("CLOB_API_PASSPHRASE missing".to_string()))?;

        let side = match signal.side {
            TradeSide::BuyYes | TradeSide::BuyNo => Side::Buy,
            TradeSide::None => return Ok(()),
        };

        let price = round_down_to_tick(price, self.tick_size);
        if price <= 0.0 {
            return Err(AgentError::Clob("Invalid price".to_string()));
        }

        let size_shares = (signal.size_usdc / price).max(0.0);
        if size_shares <= 0.0 {
            return Err(AgentError::Clob("Invalid size".to_string()));
        }

        let (maker_amount, taker_amount) = get_order_amounts(
            size_shares,
            price,
            side,
            self.usdc_decimals,
        );

        let token_id = parse_token_id(&market.id)?;
        let salt = generate_salt();
        let expiration = U256::from(
            (Utc::now().timestamp() as u64).saturating_add(self.expiration_secs),
        );
        let nonce = if self.nonce_override > 0 {
            U256::from(self.nonce_override)
        } else {
            U256::from(Utc::now().timestamp_millis() as u64)
        };

        let maker = self.funder.unwrap_or_else(|| self.wallet.address());
        let signer = self.wallet.address();
        let taker = self.taker_address;

        if self.enable_pq_signing {
            warn!("PQ signing requested, but CLOB requires ECDSA; falling back to ECDSA signer.");
        }
        if self.neg_risk {
            warn!("NEG_RISK enabled; ensure exchange address matches risk mode.");
        }

        let order = OrderForSignature {
            salt,
            maker,
            signer,
            taker,
            token_id,
            maker_amount,
            taker_amount,
            expiration,
            nonce,
            fee_rate_bps: U256::from(self.fee_rate_bps),
            side: side.as_u8(),
            signature_type: self.signature_type,
        };

        let typed_data = build_typed_data(
            &order,
            self.exchange_address,
            self.wallet.chain_id(),
        );
        let signature = self
            .wallet
            .sign_typed_data(&typed_data)
            .await
            .map_err(|e| AgentError::Wallet(e.to_string()))?;

        let order_payload = json!({
            "salt": order.salt.to_string(),
            "maker": format!("{:#x}", order.maker),
            "signer": format!("{:#x}", order.signer),
            "taker": format!("{:#x}", order.taker),
            "tokenId": order.token_id.to_string(),
            "makerAmount": order.maker_amount.to_string(),
            "takerAmount": order.taker_amount.to_string(),
            "expiration": order.expiration.to_string(),
            "nonce": order.nonce.to_string(),
            "feeRateBps": order.fee_rate_bps.to_string(),
            "side": side.as_str(),
            "signatureType": order.signature_type,
            "signature": signature.to_string(),
        });

        let owner = api_key.clone();
        let body = json!({
            "order": order_payload,
            "owner": owner,
            "orderType": self.order_type.as_str(),
            "postOnly": self.post_only
        });

        let body_string =
            serde_json::to_string(&body).map_err(|e| AgentError::Clob(e.to_string()))?;
        let headers = build_l2_headers(
            signer,
            api_key,
            api_secret,
            api_passphrase,
            "POST",
            "/order",
            &body_string,
        )?;

        let mut request = self.client.post(format!("{}/order", self.base_url));
        request = request.header("Content-Type", "application/json");
        for (key, value) in headers {
            request = request.header(key, value);
        }

        let resp = request
            .body(body_string)
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

#[derive(Debug, Clone, Copy)]
enum Side {
    Buy,
    Sell,
}

impl Side {
    fn as_u8(self) -> u8 {
        match self {
            Side::Buy => 0,
            Side::Sell => 1,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }
}

#[derive(Debug, Clone)]
struct OrderForSignature {
    salt: U256,
    maker: Address,
    signer: Address,
    taker: Address,
    token_id: U256,
    maker_amount: U256,
    taker_amount: U256,
    expiration: U256,
    nonce: U256,
    fee_rate_bps: U256,
    side: u8,
    signature_type: u8,
}

fn build_typed_data(
    order: &OrderForSignature,
    verifying_contract: Address,
    chain_id: u64,
) -> TypedData {
    let mut types = Types::new();
    types.insert(
        "EIP712Domain".to_string(),
        vec![
            Eip712DomainType {
                name: "name".to_string(),
                r#type: "string".to_string(),
            },
            Eip712DomainType {
                name: "version".to_string(),
                r#type: "string".to_string(),
            },
            Eip712DomainType {
                name: "chainId".to_string(),
                r#type: "uint256".to_string(),
            },
            Eip712DomainType {
                name: "verifyingContract".to_string(),
                r#type: "address".to_string(),
            },
        ],
    );
    types.insert(
        "Order".to_string(),
        vec![
            Eip712DomainType {
                name: "salt".to_string(),
                r#type: "uint256".to_string(),
            },
            Eip712DomainType {
                name: "maker".to_string(),
                r#type: "address".to_string(),
            },
            Eip712DomainType {
                name: "signer".to_string(),
                r#type: "address".to_string(),
            },
            Eip712DomainType {
                name: "taker".to_string(),
                r#type: "address".to_string(),
            },
            Eip712DomainType {
                name: "tokenId".to_string(),
                r#type: "uint256".to_string(),
            },
            Eip712DomainType {
                name: "makerAmount".to_string(),
                r#type: "uint256".to_string(),
            },
            Eip712DomainType {
                name: "takerAmount".to_string(),
                r#type: "uint256".to_string(),
            },
            Eip712DomainType {
                name: "expiration".to_string(),
                r#type: "uint256".to_string(),
            },
            Eip712DomainType {
                name: "nonce".to_string(),
                r#type: "uint256".to_string(),
            },
            Eip712DomainType {
                name: "feeRateBps".to_string(),
                r#type: "uint256".to_string(),
            },
            Eip712DomainType {
                name: "side".to_string(),
                r#type: "uint8".to_string(),
            },
            Eip712DomainType {
                name: "signatureType".to_string(),
                r#type: "uint8".to_string(),
            },
        ],
    );

    let domain = EIP712Domain {
        name: Some("Polymarket CTF Exchange".to_string()),
        version: Some("1".to_string()),
        chain_id: Some(chain_id.into()),
        verifying_contract: Some(verifying_contract),
        ..Default::default()
    };

    let mut message = BTreeMap::new();
    message.insert("salt".to_string(), json!(order.salt.to_string()));
    message.insert("maker".to_string(), json!(format!("{:#x}", order.maker)));
    message.insert("signer".to_string(), json!(format!("{:#x}", order.signer)));
    message.insert("taker".to_string(), json!(format!("{:#x}", order.taker)));
    message.insert("tokenId".to_string(), json!(order.token_id.to_string()));
    message.insert(
        "makerAmount".to_string(),
        json!(order.maker_amount.to_string()),
    );
    message.insert(
        "takerAmount".to_string(),
        json!(order.taker_amount.to_string()),
    );
    message.insert("expiration".to_string(), json!(order.expiration.to_string()));
    message.insert("nonce".to_string(), json!(order.nonce.to_string()));
    message.insert(
        "feeRateBps".to_string(),
        json!(order.fee_rate_bps.to_string()),
    );
    message.insert("side".to_string(), json!(order.side));
    message.insert("signatureType".to_string(), json!(order.signature_type));

    TypedData {
        types,
        primary_type: "Order".to_string(),
        domain,
        message,
    }
}

fn build_l2_headers(
    address: Address,
    api_key: String,
    api_secret: &str,
    api_passphrase: &str,
    method: &str,
    request_path: &str,
    request_body: &str,
) -> Result<Vec<(String, String)>, AgentError> {
    let timestamp = Utc::now().timestamp().to_string();
    let signature = sign_clob_auth_message(
        api_secret,
        &timestamp,
        method,
        request_path,
        request_body,
    )?;

    Ok(vec![
        ("poly_address".to_string(), format!("{:#x}", address)),
        ("poly_api_key".to_string(), api_key),
        ("poly_passphrase".to_string(), api_passphrase.to_string()),
        ("poly_timestamp".to_string(), timestamp),
        ("poly_signature".to_string(), signature),
    ])
}

fn sign_clob_auth_message(
    secret: &str,
    timestamp: &str,
    method: &str,
    request_path: &str,
    request_body: &str,
) -> Result<String, AgentError> {
    let secret_bytes =
        URL_SAFE.decode(secret.trim()).map_err(|e| AgentError::Clob(e.to_string()))?;
    let message = format!(
        "{}{}{}{}",
        timestamp,
        method.to_uppercase(),
        request_path,
        request_body
    );
    let mut mac =
        Hmac::<Sha256>::new_from_slice(&secret_bytes).map_err(|e| {
            AgentError::Clob(format!("HMAC init failed: {}", e))
        })?;
    mac.update(message.as_bytes());
    let result = mac.finalize().into_bytes();
    Ok(URL_SAFE.encode(result))
}

fn parse_token_id(token: &str) -> Result<U256, AgentError> {
    if let Ok(val) = U256::from_dec_str(token) {
        return Ok(val);
    }
    if let Some(stripped) = token.strip_prefix("0x") {
        return U256::from_str_radix(stripped, 16)
            .map_err(|e| AgentError::Clob(format!("tokenId parse: {}", e)));
    }
    Err(AgentError::Clob(format!(
        "tokenId not numeric: {}",
        token
    )))
}

fn round_down_to_tick(price: f64, tick_size: f64) -> f64 {
    if tick_size <= 0.0 {
        return price;
    }
    let ticks = (price / tick_size).floor();
    (ticks * tick_size).max(0.0001)
}

fn scale_amount(value: f64, decimals: u32) -> U256 {
    let factor = 10u128.pow(decimals);
    let scaled = (value * factor as f64).floor();
    U256::from(scaled as u128)
}

fn get_order_amounts(
    size_shares: f64,
    price: f64,
    side: Side,
    decimals: u32,
) -> (U256, U256) {
    match side {
        Side::Buy => {
            let maker_amount = scale_amount(size_shares * price, decimals);
            let taker_amount = scale_amount(size_shares, decimals);
            (maker_amount, taker_amount)
        }
        Side::Sell => {
            let maker_amount = scale_amount(size_shares, decimals);
            let taker_amount = scale_amount(size_shares * price, decimals);
            (maker_amount, taker_amount)
        }
    }
}

fn generate_salt() -> U256 {
    let mut rng = rand::thread_rng();
    let salt: u128 = rng.gen();
    U256::from(salt)
}
