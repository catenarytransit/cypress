//! Bounded CPU work for the geocoder. Search must never run on a Tokio reactor.
//!
//! A fixed-size Rayon pool is deliberately used instead of Tokio's elastic
//! blocking pool: each OS thread retains a potentially large GuessContext
//! cache. Canceled requests cannot release a running worker permit early.

use std::sync::Arc;
use std::time::Duration;
use rayon::{ThreadPool, ThreadPoolBuilder};
use tokio::sync::{oneshot, Semaphore};

#[derive(Debug, thiserror::Error)]
pub enum SearchCpuError {
    #[error("search capacity exhausted; retry shortly")]
    Saturated,
    #[error("timed out waiting for a search worker; retry shortly")]
    WaitTimedOut,
    #[error("search worker panicked")]
    Panicked,
    #[error("search worker shut down")]
    WorkerClosed,
}

pub struct SearchCpu {
    workers: Arc<Semaphore>,
    waiting: Arc<Semaphore>,
    pool: ThreadPool,
    wait_timeout: Duration,
}

impl SearchCpu {
    pub fn new(
        workers: usize,
        waiting: usize,
        wait_timeout: Duration,
    ) -> Result<Self, rayon::ThreadPoolBuildError> {
        assert!(workers > 0 && waiting > 0);
        let pool = ThreadPoolBuilder::new()
            .num_threads(workers)
            .thread_name(|i| format!("cypress-search-{i}"))
            .build()?;
        Ok(Self {
            workers: Arc::new(Semaphore::new(workers)),
            waiting: Arc::new(Semaphore::new(waiting)),
            pool,
            wait_timeout,
        })
    }

    pub async fn run<F, T>(&self, compute: F) -> Result<T, SearchCpuError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        // Fail fast if the bounded waiting room is full. No task is created
        // and no expensive per-worker cache is allocated on rejection.
        let waiting = self
            .waiting
            .clone()
            .try_acquire_owned()
            .map_err(|_| SearchCpuError::Saturated)?;
        let worker = tokio::time::timeout(
            self.wait_timeout,
            self.workers.clone().acquire_owned(),
        )
        .await
        .map_err(|_| SearchCpuError::WaitTimedOut)?
        .map_err(|_| SearchCpuError::Saturated)?;
        drop(waiting);

        // The permit lives inside the OS-thread job: canceling the HTTP
        // future must not make a still-running search invisible to admission.
        let (tx, rx) = oneshot::channel();
        self.pool.spawn(move || {
            let _worker = worker;
            let value = std::panic::catch_unwind(std::panic::AssertUnwindSafe(compute));
            let _ = tx.send(value);
        });
        match rx.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err(SearchCpuError::Panicked),
            Err(_) => Err(SearchCpuError::WorkerClosed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn overload_returns_promptly_without_blocking_tokio() {
        let pool = Arc::new(SearchCpu::new(1, 1, Duration::from_secs(2)).unwrap());
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();

        let p = Arc::clone(&pool);
        let first = tokio::spawn(async move {
            p.run(move || {
                let _ = started_tx.send(());
                release_rx.blocking_recv().unwrap();
                10
            })
            .await
        });
        started_rx.await.unwrap();

        let p = Arc::clone(&pool);
        let second = tokio::spawn(async move { p.run(|| 20).await });
        tokio::time::timeout(Duration::from_millis(500), async {
            while pool.waiting.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second job never entered waiting room");

        assert!(matches!(
            pool.run(|| 30).await,
            Err(SearchCpuError::Saturated)
        ));
        tokio::time::timeout(Duration::from_millis(200), async {
            tokio::time::sleep(Duration::from_millis(5)).await;
        })
        .await
        .expect("the reactor was blocked by a CPU job");

        release_tx.send(()).unwrap();
        assert_eq!(first.await.unwrap().unwrap(), 10);
        assert_eq!(second.await.unwrap().unwrap(), 20);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_does_not_release_running_cpu_capacity() {
        let pool = Arc::new(SearchCpu::new(1, 1, Duration::from_secs(2)).unwrap());
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let p = Arc::clone(&pool);
        let first = tokio::spawn(async move {
            p.run(move || {
                let _ = started_tx.send(());
                release_rx.blocking_recv().unwrap();
            })
            .await
        });
        started_rx.await.unwrap();
        first.abort(); // The OS thread is still running.

        let p = Arc::clone(&pool);
        let second = tokio::spawn(async move { p.run(|| 7).await });
        tokio::time::timeout(Duration::from_millis(500), async {
            while pool.waiting.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!second.is_finished());
        release_tx.send(()).unwrap();
        assert_eq!(second.await.unwrap().unwrap(), 7);
    }
}
