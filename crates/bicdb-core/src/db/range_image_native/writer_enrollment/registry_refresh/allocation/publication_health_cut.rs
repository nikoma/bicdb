//! Inject the real shared health producer on a separate thread. No failure
//! payload is cloned/formatted by the thread holding the actual version guard.
//! This is a health-fence component, not an actual storage IO fault.
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;

pub(super) struct PublicationHealthCut {
    start: Option<Sender<bool>>,
    done: Receiver<()>,
    fired: Arc<AtomicU64>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl PublicationHealthCut {
    pub(super) fn start(health: Arc<bicdb_page::DurabilityHealth>) -> Result<Self> {
        let (start, receive) = mpsc::channel::<bool>();
        let (finished, done) = mpsc::channel::<()>();
        let fired = Arc::new(AtomicU64::new(0));
        let actual_fired = Arc::clone(&fired);
        let worker = std::thread::Builder::new()
            .name("allocation-publication-health-cut".into())
            .spawn(move || {
                if receive.recv().ok() == Some(true) {
                    // Use the existing one-way SAME-health failure producer;
                    // there is no setter for the flag or retained diagnostic.
                    let _actual_failure = health.record_failure(
                        "allocation-publication.test-health-fence",
                        &"injected actual allocation publication health-fence component",
                    );
                    actual_fired.store(1, Ordering::Release);
                    let _ = finished.send(());
                }
            })
            .map_err(|_| invalid("allocation health-cut worker could not start"))?;
        Ok(Self {
            start: Some(start),
            done,
            fired,
            worker: Some(worker),
        })
    }
    pub(super) fn fire(&mut self, parent: &CancellationToken) -> Result<()> {
        parent.check()?;
        let remaining = parent
            .deadline()
            .and_then(|deadline| deadline.checked_duration_since(std::time::Instant::now()))
            .ok_or(crate::BicDbError::QueryTimedOut)?;
        let start = self
            .start
            .take()
            .ok_or_else(|| invalid("allocation health-cut already consumed"))?;
        start
            .send(true)
            .map_err(|_| invalid("allocation health-cut worker disconnected"))?;
        self.done
            .recv_timeout(remaining)
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => crate::BicDbError::QueryTimedOut,
                mpsc::RecvTimeoutError::Disconnected => {
                    invalid("allocation health-cut completion disconnected")
                }
            })?;
        parent.check()
    }
    pub(super) fn fired(&self) -> u64 {
        self.fired.load(Ordering::Acquire)
    }
}
impl Drop for PublicationHealthCut {
    fn drop(&mut self) {
        // Declared outside with_current: service/version guards have ended
        // before join, including pre-cut refusal and unwind. Original caller
        // DB/journal scopes may remain; this worker borrows neither of them.
        if let Some(start) = self.start.take() {
            let _ = start.send(false);
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
