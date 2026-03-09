use crate::store::db::KvStore;
use std::time::Duration;
use tokio::time;

#[allow(dead_code)]
pub struct TtlManager {
    store: KvStore,
}

#[allow(dead_code)]
impl TtlManager {
    #[must_use]
    pub const fn new(store: KvStore) -> Self {
        Self { store }
    }

    pub async fn start_cleanup_task(self) {
        let mut interval = time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            self.store.cleanup_expired();
        }
    }
}
