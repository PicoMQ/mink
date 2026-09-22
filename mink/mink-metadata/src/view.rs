//! A published read view of the state at an applied index, with waiting for an index to be reached.

use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::watch::{Receiver, Sender};

use crate::state::State;

#[derive(Debug, Clone)]
pub struct View {
    pub applied_index: u64,
    pub state: State,
}

#[derive(Debug)]
pub struct ViewPublisher {
    current: ArcSwap<View>,
    notify: Sender<u64>,
}

impl ViewPublisher {
    pub fn new() -> Self {
        Self::with_view(View {
            applied_index: 0,
            state: State::new(),
        })
    }

    pub fn with_view(view: View) -> Self {
        let (notify, _) = tokio::sync::watch::channel(view.applied_index);

        Self {
            current: ArcSwap::from_pointee(view),
            notify,
        }
    }

    pub fn load(&self) -> Arc<View> {
        self.current.load_full()
    }

    pub fn publish(&self, view: View) {
        let applied_index = view.applied_index;
        debug_assert!(
            applied_index >= self.notify.borrow().to_owned(),
            "applied_index regressed"
        );
        // The view must be stored before waiters are woken.
        self.current.store(Arc::new(view));
        self.notify.send_replace(applied_index);
    }

    pub fn subscribe(&self) -> Receiver<u64> {
        self.notify.subscribe()
    }

    pub async fn wait_applied(&self, index: u64) -> Arc<View> {
        let mut receiver = self.notify.subscribe();
        receiver
            .wait_for(|applied| *applied >= index)
            .await
            .expect("publisher dropped");

        self.load()
    }
}

impl Default for ViewPublisher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn view_at(applied_index: u64) -> View {
        View {
            applied_index,
            state: State::new(),
        }
    }

    #[test]
    fn publish_and_load() {
        let publisher = ViewPublisher::new();
        assert_eq!(publisher.load().applied_index, 0);
        publisher.publish(view_at(3));
        assert_eq!(publisher.load().applied_index, 3);
    }

    #[tokio::test]
    async fn wait_applied_returns_immediately_when_satisfied() {
        let publisher = ViewPublisher::new();
        publisher.publish(view_at(5));
        assert_eq!(publisher.wait_applied(5).await.applied_index, 5);
        assert_eq!(
            publisher.wait_applied(0).await.applied_index,
            5,
            "never older"
        );
    }

    #[tokio::test]
    async fn wait_applied_wakes_on_publish() {
        let publisher = Arc::new(ViewPublisher::new());
        let waiter = {
            let publisher = publisher.clone();
            tokio::spawn(async move { publisher.wait_applied(2).await.applied_index })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        publisher.publish(view_at(1));
        publisher.publish(view_at(2));
        assert!(waiter.await.unwrap() >= 2);
    }

    #[tokio::test]
    async fn restored_publisher_starts_at_snapshot_index() {
        let publisher = ViewPublisher::with_view(view_at(42));
        assert_eq!(publisher.load().applied_index, 42);
        assert_eq!(publisher.wait_applied(42).await.applied_index, 42);
    }
}
