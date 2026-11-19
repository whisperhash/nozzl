//! The persisted `checkpoints.json` must be `0644` *regardless of the process
//! umask* — a read-only account reads it, and the
//! container's umask is not something this crate controls.
//!
//! This lives in its own integration binary on purpose: it mutates the
//! process-global umask, which would race the concurrently-run tests in
//! `atomic_write.rs`. As the sole test in this binary it runs alone, so the
//! umask change is self-contained.
//!
//! It also pins the design choice: the mode is applied via `fchmod`
//! (`set_permissions` on the temp fd), which is umask-independent. Had it been
//! applied via the `open(2)` create-mode (e.g. `tempfile::Builder::permissions`,
//! which threads the mode through `OpenOptions::mode`), the kernel would filter
//! it as `mode & ~umask` and this test would observe `0600` under umask `0o077`.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::time::Duration;

use nozzl_checkpoint::{CheckpointStore, FileCheckpointStore};
use nozzl_types::{EventId, RelayUrl};

const FOREVER: Duration = Duration::from_secs(60 * 60 * 24);

/// Sets a hostile umask for the duration of the test and restores the previous
/// value on drop — so a panicking assertion can't leak the umask to the harness.
struct UmaskGuard(libc::mode_t);

impl UmaskGuard {
    fn set(new: libc::mode_t) -> Self {
        // umask() returns the previous mask and cannot fail.
        let prev = unsafe { libc::umask(new) };
        UmaskGuard(prev)
    }
}

impl Drop for UmaskGuard {
    fn drop(&mut self) {
        unsafe {
            libc::umask(self.0);
        }
    }
}

fn evid(suffix: u8) -> EventId {
    let hex = format!("{:0>64x}", suffix);
    EventId::from_hex(&hex).unwrap()
}

#[tokio::test]
async fn persisted_mode_is_0644_under_hostile_umask() {
    // 0o077 would knock a umask-filtered create-mode down to 0o600 (owner-only),
    // exactly the regression we're guarding against.
    let _umask = UmaskGuard::set(0o077);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.json");

    let store = FileCheckpointStore::open(path.clone(), FOREVER)
        .await
        .unwrap();
    let r = RelayUrl::parse("wss://relay.example").unwrap();
    store.observe(&r, 0, 1, &evid(1)).await;
    store.flush().await.unwrap();

    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(
        mode, 0o644,
        "under umask 0o077 the file was {mode:o}; fchmod must force 0644 \
         regardless of umask (a create-mode would have been filtered to 0600)"
    );

    // The shutdown final flush takes the same write path under the same umask.
    Arc::clone(&store).shutdown().await.unwrap();
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o644, "post-shutdown mode was {mode:o}, want 0644");
}
