//! A background task that stops on request and is aborted when dropped.

use std::future::Future;
use std::sync::Arc;

use tokio::sync::Notify;
use tokio::task::JoinHandle;

pub(crate) struct Worker {
    stop: Arc<Notify>,
    task: Option<JoinHandle<()>>,
}

impl Worker {
    pub(crate) fn spawn<F, Fut>(run: F) -> Self
    where
        F: FnOnce(Arc<Notify>) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let stop = Arc::new(Notify::new());
        let task = tokio::spawn(run(stop.clone()));

        Worker {
            stop,
            task: Some(task),
        }
    }

    pub(crate) async fn shutdown(mut self) {
        self.stop.notify_one();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
