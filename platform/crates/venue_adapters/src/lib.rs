mod config;
mod error;
mod registry;
mod traits;
mod venues;

pub use config::{
    CoinbaseConfig, ConnectorConfig, ExecutionPolicy, IbkrConfig, KalshiConfig,
};
pub use error::{AdapterError, AdapterResult};
pub use registry::AdapterRegistry;
pub use traits::{HealthReport, VenueAdapter, VenueOrderResult};
pub use venues::{CoinbaseAdapter, IbkrAdapter, KalshiAdapter};
