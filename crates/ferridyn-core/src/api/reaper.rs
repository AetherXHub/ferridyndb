//! Background TTL reaper thread that periodically sweeps expired items.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use super::database::FerridynDB;

/// Handle to a background reaper thread.
///
/// The reaper periodically scans all tables with a `ttl_attribute` and deletes
/// expired items. Dropping the handle stops the reaper thread.
pub struct ReaperHandle {
    stop_flag: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl ReaperHandle {
    /// Stop the reaper thread and wait for it to exit.
    pub fn stop(&mut self) {
        self.stop_flag.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for ReaperHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Start a background reaper thread that sweeps expired TTL items.
///
/// The reaper checks all tables at the given `interval`. For each table with
/// a `ttl_attribute`, it calls `sweep_expired_ttl()` in a loop until no more
/// expired items remain.
///
/// Returns a [`ReaperHandle`] — dropping or calling `.stop()` on it will
/// terminate the background thread.
pub(crate) fn start_reaper(db: FerridynDB, interval: Duration) -> ReaperHandle {
    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop = stop_flag.clone();

    let thread = std::thread::spawn(move || {
        reaper_loop(&db, interval, &stop);
    });

    ReaperHandle {
        stop_flag,
        thread: Some(thread),
    }
}

fn reaper_loop(db: &FerridynDB, interval: Duration, stop: &AtomicBool) {
    loop {
        if stop.load(Ordering::Acquire) {
            break;
        }

        // List all tables and sweep those with TTL configured.
        if let Ok(tables) = db.list_tables() {
            for table in &tables {
                if stop.load(Ordering::Acquire) {
                    return;
                }
                if let Ok(schema) = db.describe_table(table)
                    && schema.ttl_attribute.is_some()
                {
                    // Sweep in a loop until no more expired items.
                    loop {
                        if stop.load(Ordering::Acquire) {
                            return;
                        }
                        match db.sweep_expired_ttl(table) {
                            Ok(0) => break,
                            Ok(_) => continue,
                            Err(_) => break,
                        }
                    }
                }
            }
        }

        // Sleep in small chunks so we can respond to stop quickly.
        let chunks = (interval.as_millis() / 100).max(1) as u64;
        for _ in 0..chunks {
            if stop.load(Ordering::Acquire) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}
