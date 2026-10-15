//! Keeping the machine awake while an execution runs.
//!
//! A computer that goes to sleep stops every process on it, so a job that is
//! still running would stall and the controller would soon call it lost. While
//! at least one execution is running, the node asks the operating system not to
//! sleep on its own. This is best effort: it cannot stop a person from closing
//! a laptop lid or choosing Sleep, and a failure is logged, never a failure of
//! the job.
//!
//! Executions share one request to the system; it is made when the first one
//! starts and given back when the last one ends.

use std::{
    io,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

/// Whatever keeps the system awake; dropping it gives the request back.
type Hold = Box<dyn Send>;
type Acquire = dyn Fn() -> io::Result<Hold> + Send + Sync;

/// Hands out [`SleepGuard`]s. Cheap to clone; clones share one request.
#[derive(Clone)]
pub struct SleepInhibitor {
    inner: Option<Arc<Inner>>,
}

struct Inner {
    acquire: Box<Acquire>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    holders: usize,
    hold: Option<Hold>,
}

impl Inner {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// While this is alive, the system is asked not to sleep.
#[must_use = "the system may sleep as soon as the guard is dropped"]
pub struct SleepGuard {
    inner: Option<Arc<Inner>>,
}

impl SleepInhibitor {
    /// Uses the operating system's own mechanism.
    pub fn system() -> Self {
        Self::with_acquire(platform::acquire)
    }

    /// Never asks the system for anything.
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    fn with_acquire(acquire: impl Fn() -> io::Result<Hold> + Send + Sync + 'static) -> Self {
        Self {
            inner: Some(Arc::new(Inner {
                acquire: Box::new(acquire),
                state: Mutex::new(State::default()),
            })),
        }
    }

    pub fn guard(&self) -> SleepGuard {
        let Some(inner) = &self.inner else {
            return SleepGuard { inner: None };
        };
        let mut state = inner.state();
        state.holders += 1;
        // Tried again for each new execution while there is no hold, so a
        // failure at one moment does not disable this for good.
        if state.hold.is_none() {
            match (inner.acquire)() {
                Ok(hold) => {
                    tracing::debug!("asked the system not to sleep while executions run");
                    state.hold = Some(hold);
                }
                Err(error) => {
                    tracing::warn!(%error, "could not ask the system not to sleep; the machine may sleep during the job");
                }
            }
        }
        SleepGuard {
            inner: Some(Arc::clone(inner)),
        }
    }
}

impl Drop for SleepGuard {
    fn drop(&mut self) {
        let Some(inner) = &self.inner else {
            return;
        };
        let released = {
            let mut state = inner.state();
            state.holders = state.holders.saturating_sub(1);
            if state.holders == 0 {
                state.hold.take()
            } else {
                None
            }
        };
        if released.is_some() {
            tracing::debug!("the system may sleep again");
        }
    }
}

#[cfg(windows)]
mod platform {
    use std::{io, sync::mpsc, thread};

    use super::Hold;

    const ES_SYSTEM_REQUIRED: u32 = 0x0000_0001;
    const ES_CONTINUOUS: u32 = 0x8000_0000;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn SetThreadExecutionState(flags: u32) -> u32;
    }

    /// The request belongs to the thread that made it, so it is made on a
    /// thread of its own that waits until the hold is dropped.
    struct ThreadHold {
        release: Option<mpsc::Sender<()>>,
        thread: Option<thread::JoinHandle<()>>,
    }

    impl Drop for ThreadHold {
        fn drop(&mut self) {
            drop(self.release.take());
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    pub fn acquire() -> io::Result<Hold> {
        let (release, released) = mpsc::channel::<()>();
        let (ready, ready_receiver) = mpsc::channel::<io::Result<()>>();
        let thread = thread::Builder::new()
            .name("meld-keep-awake".to_owned())
            .spawn(move || {
                // SAFETY: a plain system call with flag values from its documentation.
                let previous =
                    unsafe { SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED) };
                if previous == 0 {
                    let _ = ready.send(Err(io::Error::last_os_error()));
                    return;
                }
                let _ = ready.send(Ok(()));
                // Returns when the sender is dropped.
                let _ = released.recv();
                // SAFETY: as above.
                unsafe { SetThreadExecutionState(ES_CONTINUOUS) };
            })?;
        let hold = ThreadHold {
            release: Some(release),
            thread: Some(thread),
        };
        ready_receiver
            .recv()
            .map_err(|_| io::Error::other("the keep-awake thread ended before it was ready"))??;
        Ok(Box::new(hold))
    }
}

#[cfg(unix)]
mod platform {
    use std::{
        io,
        os::unix::process::CommandExt,
        process::{Child, Command, Stdio},
    };

    use super::Hold;

    /// A helper process that holds the request for as long as it lives. It
    /// is in a group of its own so that the group can be stopped as a whole.
    struct ProcessHold(Child);

    impl Drop for ProcessHold {
        fn drop(&mut self) {
            let group = self.0.id() as libc::pid_t;
            // SAFETY: signals the group this hold created and nothing else.
            unsafe { libc::killpg(group, libc::SIGKILL) };
            let _ = self.0.wait();
        }
    }

    fn helper() -> Command {
        let node = std::process::id().to_string();
        if cfg!(target_os = "macos") {
            // Ends by itself when the node does.
            let mut command = Command::new("caffeinate");
            command.args(["-i", "-w", &node]);
            command
        } else {
            // Holds the lock for as long as its command runs, and that
            // command ends once the node is gone.
            let mut command = Command::new("systemd-inhibit");
            command.args([
                "--what=sleep",
                "--who=meld-node",
                "--why=an execution is running",
                "--mode=block",
                "sh",
                "-c",
                "while kill -0 \"$0\" 2>/dev/null; do sleep 5; done",
                &node,
            ]);
            command
        }
    }

    pub fn acquire() -> io::Result<Hold> {
        let child = helper()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()?;
        Ok(Box::new(ProcessHold(child)))
    }
}

#[cfg(not(any(windows, unix)))]
mod platform {
    use std::io;

    use super::Hold;

    pub fn acquire() -> io::Result<Hold> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "keeping the system awake is not supported on this system",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Counts requests made and requests still held.
    #[derive(Default)]
    struct Counts {
        acquired: AtomicUsize,
        held: AtomicUsize,
    }

    struct Held(Arc<Counts>);

    impl Drop for Held {
        fn drop(&mut self) {
            self.0.held.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn counting() -> (SleepInhibitor, Arc<Counts>) {
        let counts = Arc::new(Counts::default());
        let shared = Arc::clone(&counts);
        let inhibitor = SleepInhibitor::with_acquire(move || {
            shared.acquired.fetch_add(1, Ordering::SeqCst);
            shared.held.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Held(Arc::clone(&shared))) as Hold)
        });
        (inhibitor, counts)
    }

    #[test]
    fn executions_share_one_request() {
        let (inhibitor, counts) = counting();
        assert_eq!(counts.held.load(Ordering::SeqCst), 0);

        let first = inhibitor.guard();
        let second = inhibitor.guard();
        assert_eq!(counts.acquired.load(Ordering::SeqCst), 1);
        assert_eq!(counts.held.load(Ordering::SeqCst), 1);

        drop(first);
        assert_eq!(counts.held.load(Ordering::SeqCst), 1);
        drop(second);
        assert_eq!(counts.held.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_later_execution_asks_again() {
        let (inhibitor, counts) = counting();
        drop(inhibitor.guard());
        let _again = inhibitor.guard();
        assert_eq!(counts.acquired.load(Ordering::SeqCst), 2);
        assert_eq!(counts.held.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_failed_request_does_not_stop_the_execution() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&attempts);
        let inhibitor = SleepInhibitor::with_acquire(move || {
            counted.fetch_add(1, Ordering::SeqCst);
            Err(io::Error::other("no such helper"))
        });
        let first = inhibitor.guard();
        // Another execution tries again rather than giving up for good.
        let second = inhibitor.guard();
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        drop(first);
        drop(second);
    }

    #[test]
    fn a_disabled_inhibitor_asks_for_nothing() {
        drop(SleepInhibitor::disabled().guard());
    }
}
