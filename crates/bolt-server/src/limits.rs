use std::fmt;
use std::time::Duration;

/// Most bytes one connection may hold in pipelined requests: the reader's
/// queue plus the session's, each `max_pipelined_messages` deep.
pub const MAX_PIPELINE_BUFFER_BYTES: usize = 64 * 1024 * 1024;

/// Per-connection limits of a Bolt session.
#[derive(Clone, Copy, Debug)]
pub struct SessionLimits {
    /// Largest client message, after dechunking.
    pub max_message_bytes: usize,
    /// Requests a client may pipeline ahead of the one being served.
    pub max_pipelined_messages: usize,
    /// For the version handshake.
    pub handshake_timeout: Duration,
    /// From accept to a successful LOGON, and again after LOGOFF.
    pub authentication_timeout: Duration,
    /// Longest wait for the next request once authenticated.
    pub idle_timeout: Duration,
    /// From the end of the handshakes; the session then closes between
    /// requests.
    pub max_connection_age: Duration,
    /// For each response to reach the client.
    pub write_timeout: Duration,
}

impl Default for SessionLimits {
    fn default() -> Self {
        Self {
            max_message_bytes: 2 * 1024 * 1024,
            max_pipelined_messages: 8,
            handshake_timeout: Duration::from_secs(10),
            authentication_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(15 * 60),
            max_connection_age: Duration::from_secs(60 * 60),
            write_timeout: Duration::from_secs(30),
        }
    }
}

/// Why a [`SessionLimits`] is unusable. The `Display` text is the operator's
/// error message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionLimitsError {
    ZeroLimit,
    PipelineBudgetOverflow,
    PipelineBudgetExceeded,
    ZeroTimeout,
}

impl fmt::Display for SessionLimitsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ZeroLimit => "message and pipeline limits must be greater than zero",
            Self::PipelineBudgetOverflow => "Bolt pipeline byte budget overflows usize",
            Self::PipelineBudgetExceeded => "message size and pipeline depth can buffer more than 64 MiB per connection",
            Self::ZeroTimeout => "Bolt connection timeouts must be greater than zero",
        })
    }
}

impl std::error::Error for SessionLimitsError {}

impl SessionLimits {
    /// Checked in this order: non-zero limits, the pipeline byte budget, then
    /// non-zero timeouts.
    pub fn validate(&self) -> Result<(), SessionLimitsError> {
        if self.max_message_bytes == 0 || self.max_pipelined_messages == 0 {
            return Err(SessionLimitsError::ZeroLimit);
        }
        let buffered_bytes = self
            .max_message_bytes
            .checked_mul(self.max_pipelined_messages)
            .and_then(|bytes| bytes.checked_mul(2))
            .ok_or(SessionLimitsError::PipelineBudgetOverflow)?;
        if buffered_bytes > MAX_PIPELINE_BUFFER_BYTES {
            return Err(SessionLimitsError::PipelineBudgetExceeded);
        }
        if self.handshake_timeout.is_zero()
            || self.authentication_timeout.is_zero()
            || self.idle_timeout.is_zero()
            || self.max_connection_age.is_zero()
            || self.write_timeout.is_zero()
        {
            return Err(SessionLimitsError::ZeroTimeout);
        }
        Ok(())
    }
}
