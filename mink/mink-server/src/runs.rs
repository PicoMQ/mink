use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use tokio::sync::OnceCell;

pub(crate) struct SharedRuns<K, V> {
    running: Mutex<HashMap<K, Arc<OnceCell<V>>>>,
}

impl<K: Eq + Hash + Clone, V: Clone> SharedRuns<K, V> {
    pub(crate) fn new() -> SharedRuns<K, V> {
        SharedRuns {
            running: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn run<E>(
        &self,
        key: K,
        work: impl Future<Output = Result<V, E>>,
    ) -> Result<V, E> {
        let cell = self
            .running
            .lock()
            .expect("the running map is never poisoned")
            .entry(key.clone())
            .or_default()
            .clone();

        let result = cell.get_or_try_init(|| work).await.cloned();

        let mut running = self
            .running
            .lock()
            .expect("the running map is never poisoned");
        if running
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, &cell))
        {
            running.remove(&key);
        }
        result
    }
}
