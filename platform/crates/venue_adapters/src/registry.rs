use crate::error::{AdapterError, AdapterResult};
use crate::traits::VenueAdapter;
use domain::Venue;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct AdapterRegistry {
    adapters: HashMap<Venue, Arc<dyn VenueAdapter>>,
}

impl AdapterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<A>(&mut self, adapter: A)
    where
        A: VenueAdapter + 'static,
    {
        self.adapters.insert(adapter.venue(), Arc::new(adapter));
    }

    pub fn register_arc(&mut self, adapter: Arc<dyn VenueAdapter>) {
        self.adapters.insert(adapter.venue(), adapter);
    }

    pub fn get(&self, venue: &Venue) -> AdapterResult<Arc<dyn VenueAdapter>> {
        self.adapters
            .get(venue)
            .cloned()
            .ok_or_else(|| AdapterError::MissingAdapter(format!("{venue:?}")))
    }

    pub fn all(&self) -> Vec<Arc<dyn VenueAdapter>> {
        self.adapters.values().cloned().collect()
    }
}
