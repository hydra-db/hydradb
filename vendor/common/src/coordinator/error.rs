/// Errors that can occur during write coordination.
///
/// The `W` parameter carries the original write value back to the caller on
/// retryable errors ([`Backpressure`](WriteError::Backpressure) and
/// [`TimeoutError`](WriteError::TimeoutError)), allowing retry without
/// cloning. Methods that cannot return the write use the default `W = ()`.
pub enum WriteError<W = ()> {
    /// The write queue is full and backpressure is being applied.
    /// Contains the write that could not be enqueued.
    Backpressure(W),
    /// The write queue timed out while awaiting space.
    /// Contains the write that could not be enqueued.
    TimeoutError(W),
    /// The coordinator has been dropped/shutdown
    Shutdown,
    /// Error applying the write to the delta
    ApplyError(u64, String),
    /// Error flushing the delta to storage
    FlushError(String),
    /// Internal error
    Internal(String),
}

impl<W> WriteError<W> {
    /// Extracts the write value from retryable error variants.
    ///
    /// Returns `Some(W)` for `Backpressure` and `TimeoutError`, `None` for
    /// all other variants.
    pub fn into_inner(self) -> Option<W> {
        match self {
            WriteError::Backpressure(w) | WriteError::TimeoutError(w) => Some(w),
            _ => None,
        }
    }

    /// Discards the write value, converting to a `WriteError<()>`.
    pub fn discard_inner(self) -> WriteError {
        match self {
            WriteError::Backpressure(_) => WriteError::Backpressure(()),
            WriteError::TimeoutError(_) => WriteError::TimeoutError(()),
            WriteError::Shutdown => WriteError::Shutdown,
            WriteError::ApplyError(epoch, msg) => WriteError::ApplyError(epoch, msg),
            WriteError::FlushError(msg) => WriteError::FlushError(msg),
            WriteError::Internal(msg) => WriteError::Internal(msg),
        }
    }
}

impl<W> std::fmt::Debug for WriteError<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::Backpressure(_) => write!(f, "Backpressure(..)"),
            WriteError::TimeoutError(_) => write!(f, "TimeoutError(..)"),
            WriteError::Shutdown => write!(f, "Shutdown"),
            WriteError::ApplyError(epoch, msg) => {
                f.debug_tuple("ApplyError").field(epoch).field(msg).finish()
            }
            WriteError::FlushError(msg) => f.debug_tuple("FlushError").field(msg).finish(),
            WriteError::Internal(msg) => f.debug_tuple("Internal").field(msg).finish(),
        }
    }
}

impl<W> std::fmt::Display for WriteError<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::Backpressure(_) => write!(f, "write queue is full, backpressure applied"),
            WriteError::TimeoutError(_) => {
                write!(f, "timed out waiting for space in write queue")
            }
            WriteError::Shutdown => write!(f, "coordinator has been dropped/shutdown"),
            WriteError::ApplyError(epoch, msg) => {
                write!(f, "error applying write @{}: {}", epoch, msg)
            }
            WriteError::FlushError(msg) => write!(f, "error flushing delta: {}", msg),
            WriteError::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl<W> std::error::Error for WriteError<W> {}

/// Result type for write operations.
pub type WriteResult<T> = Result<T, WriteError>;
