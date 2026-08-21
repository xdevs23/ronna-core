//! Type-keyed lookup of provider modules.
//!
//! This is the whole seam between generic code and vendor code: everything
//! above holds a registry, asks it for a type id, and gets back something that
//! answers the trait. Nothing above ever names a vendor, which is what makes
//! adding one a matter of writing a module and registering it.
//!
//! A consumer's own module registers exactly the same way as one shipped here.
//! There is no privileged set.

use std::collections::HashMap;

use tracing::{info, warn};

use crate::store::Store;

use super::ProviderModule;

/// Every registered provider module, keyed by its type id.
#[derive(Default)]
pub struct ProviderRegistry {
    modules: HashMap<&'static str, Box<dyn ProviderModule>>,
}

impl ProviderRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a module under its own declared type id.
    ///
    /// The module names its key rather than the caller naming it, so the key a
    /// module is registered under and the key it reports are the same string by
    /// construction — two spellings would mean an instance that saves fine and
    /// then cannot be found again.
    pub fn register(&mut self, module: Box<dyn ProviderModule>) {
        let key = module.type_id();
        self.modules.insert(key, module);
    }

    /// The module registered for a type id, if any.
    #[must_use]
    pub fn get(&self, type_id: &str) -> Option<&dyn ProviderModule> {
        self.modules.get(type_id).map(AsRef::as_ref)
    }

    /// Every registered module, in no particular order.
    pub fn all(&self) -> impl Iterator<Item = &dyn ProviderModule> {
        self.modules.values().map(AsRef::as_ref)
    }

    /// Run each registered instance's startup preparation and persist whatever
    /// configuration it returns.
    ///
    /// Every failure here is survivable and none of them stops the others: an
    /// instance whose preparation fails keeps its cached configuration and is
    /// simply not refreshed, because the alternative — refusing to start
    /// because one provider of several is unreachable — makes an outage at one
    /// vendor look like a broken application.
    pub async fn startup_init_all(&self, store: &Store) {
        let instances = match store.list_provider_instances().await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "failed to list provider instances for startup init");
                return;
            }
        };

        for instance in &instances {
            let Some(module) = self.get(&instance.provider_type) else {
                continue;
            };

            let config = match module.get_config(instance.id.clone()).await {
                Ok(Some(c)) => c,
                Ok(None) => continue,
                Err(e) => {
                    warn!(
                        provider = %instance.name,
                        error = %e,
                        "failed to read config for startup init"
                    );
                    continue;
                }
            };

            match module.startup_init(config).await {
                Ok(updated) => {
                    if let Err(e) = module.save_config(instance.id.clone(), updated).await {
                        warn!(
                            provider = %instance.name,
                            error = %e,
                            "failed to save updated config after startup init"
                        );
                    }
                }
                Err(e) => {
                    info!(
                        provider = %instance.name,
                        error = %e,
                        "startup init failed, keeping cached config"
                    );
                }
            }
        }
    }
}
