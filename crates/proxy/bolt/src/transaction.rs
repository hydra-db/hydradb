//! An explicit transaction, served as one auto-commit unit.
//!
//! No transaction is held open on a graph-node. BEGIN is answered at once and
//! its extra (mode, db, bookmarks, tx_metadata, tx_timeout) is kept. The
//! transaction's RUN takes that extra, since inside a transaction BEGIN is
//! what carries it, and runs as an ordinary unit: routed, retried and refused
//! exactly as an auto-commit RUN is. The statement commits when its unit ends,
//! so COMMIT only hands back the unit's bookmark.
//!
//! That serves a client whose transactions hold one statement, such as a
//! driver's `execute_query`. What would need a real transaction is refused
//! rather than served with other semantics: a second statement, and a
//! ROLLBACK after a write, which cannot undo what already committed. Whether
//! the statement wrote is the node's answer, its final summary's `type`, not
//! the mode the client declared.
//!
//! The shapes are the same in every version the handshake offers, Bolt 5.1
//! to 5.4: BEGIN carries one extra dictionary, and COMMIT and ROLLBACK carry
//! no fields.

use hydradb_protocol::bolt::{failure, sig, strict_decode_client_message, success, ClientMessage, Message};
use hydradb_protocol::boltr::error::BoltError;
use hydradb_protocol::boltr::types::{BoltDict, BoltValue};
use hydradb_protocol::codes;
use tokio::io::{AsyncRead, AsyncWrite};
use turbolay_proxy_core::Placement;

use crate::session::{invalid_in_state, Continue, Session};
use crate::unit::UnitEnd;
use crate::BoltProxy;

impl<P: Placement> BoltProxy<P> {
    /// Serve a transaction from its BEGIN to its end.
    pub(crate) async fn transaction<S>(&self, session: &mut Session<S>, begin: &Message) -> Result<Continue, BoltError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let ClientMessage::Begin { extra: begin } = strict_decode_client_message(begin.body())? else {
            unreachable!("tag checked")
        };
        session.writer.send(&success(BoltDict::new())).await?;

        let frame = next(session).await?;
        let completed = match frame.tag {
            sig::RUN => {
                let run = match with_begin_extra(&frame, begin) {
                    Ok(run) => run,
                    Err(error) => {
                        session.writer.send(&failure(codes::REQUEST_INVALID, &error.to_string())).await?;
                        return Ok(Continue::Failed);
                    }
                };
                match self.run_unit(session, run).await? {
                    UnitEnd::Completed(completed) => completed,
                    other => return Ok(other.into()),
                }
            }
            // Nothing ran, so there is nothing to commit or to undo.
            sig::COMMIT | sig::ROLLBACK => {
                session.writer.send(&success(BoltDict::new())).await?;
                return Ok(Continue::Ready);
            }
            _ => return self.outside_transaction(session, &frame).await,
        };

        let frame = next(session).await?;
        match frame.tag {
            sig::COMMIT => {
                let metadata = match completed.bookmark {
                    Some(bookmark) => BoltDict::from([("bookmark".to_string(), BoltValue::String(bookmark))]),
                    None => BoltDict::new(),
                };
                session.writer.send(&success(metadata)).await?;
                Ok(Continue::Ready)
            }
            sig::ROLLBACK if completed.read => {
                session.writer.send(&success(BoltDict::new())).await?;
                Ok(Continue::Ready)
            }
            sig::ROLLBACK => {
                self.router.metrics().rollbacks_after_write.inc();
                let refused = failure(
                    codes::TRANSACTION_ROLLBACK_FAILED,
                    "the transaction's write already committed and cannot be rolled back; \
                     each transaction runs its statement as an auto-commit unit",
                );
                session.writer.send(&refused).await?;
                Ok(Continue::Failed)
            }
            sig::RUN => {
                self.router.metrics().multi_statement_transactions.inc();
                let refused = failure(
                    codes::REQUEST_INVALID,
                    "only single-statement transactions are supported; \
                     commit this transaction and run the next statement in a new one",
                );
                session.writer.send(&refused).await?;
                Ok(Continue::Failed)
            }
            _ => self.outside_transaction(session, &frame).await,
        }
    }

    /// A request that does not continue the transaction: RESET and GOODBYE
    /// end it, anything else is invalid in a transaction.
    async fn outside_transaction<S>(&self, session: &mut Session<S>, frame: &Message) -> Result<Continue, BoltError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        match frame.tag {
            sig::RESET => {
                session.writer.send(&success(BoltDict::new())).await?;
                Ok(Continue::Ready)
            }
            sig::GOODBYE => Ok(Continue::Closed),
            _ => {
                self.router.metrics().protocol_errors.inc();
                session.writer.send(&invalid_in_state()).await?;
                Ok(Continue::Failed)
            }
        }
    }
}

/// Answer what is buffered, then read the client's next request.
async fn next<S>(session: &mut Session<S>) -> Result<Message, BoltError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    session.writer.flush().await?;
    session.reader.next().await
}

/// The transaction's RUN as the auto-commit RUN it becomes: BEGIN's extra
/// added to the RUN's own. BEGIN's keys win, since a transaction's database,
/// mode and bookmarks are fixed when it begins.
fn with_begin_extra(run: &Message, begin: BoltDict) -> Result<Message, BoltError> {
    let ClientMessage::Run {
        query,
        parameters,
        mut extra,
    } = strict_decode_client_message(run.body())?
    else {
        unreachable!("tag checked")
    };
    extra.extend(begin);
    Ok(Message::encode(&ClientMessage::Run { query, parameters, extra }))
}
