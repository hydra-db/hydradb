//! Every query the extension runs, through one cursor-based `scan`.
//!
//! A query runs read-only, so on the calling statement's snapshot, including
//! the transaction's own writes, and either as the adjacency tables' owner with
//! a plan kept for the backend, or as the caller with text built for one use.
//! Rows come back a batch at a time, and the caller says after each batch how
//! many to fetch next, so a scan over a large result can stop at a budget
//! without PostgreSQL buffering the rest.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{c_char, CString};

use hydradb_pg_graph_core::GraphError;
use pgrx::{pg_sys, FromDatum, Spi};

use crate::adjacency::source_error;

/// The most rows an adjacency lookup fetches per batch: one batch covers most
/// layers, and a hub's layer is read a batch at a time.
pub(crate) const LOOKUP_BATCH: usize = 8192;

/// One row of the batch a `scan` hands its caller; valid during that call.
pub(crate) struct Row {
    tuple: pg_sys::HeapTuple,
    descriptor: pg_sys::TupleDesc,
}

impl Row {
    pub(crate) fn get<T: FromDatum>(&self, column: i32) -> Option<T> {
        let mut null = false;
        // SAFETY: the tuple and descriptor come from the batch being handed
        // out, which is freed only after the caller returns.
        unsafe {
            let datum = pg_sys::SPI_getbinval(self.tuple, self.descriptor, column, &mut null);
            T::from_datum(datum, null)
        }
    }

    pub(crate) fn require<T: FromDatum>(&self, column: i32) -> Result<T, GraphError> {
        self.get(column)
            .ok_or(GraphError::InconsistentGraph("adjacency row has a null column"))
    }
}

/// Whose privileges a query runs with.
pub(crate) enum RunAs {
    /// The adjacency tables' owner, as a SECURITY DEFINER function would run.
    /// Callers check the source tables' privileges first.
    Owner(pg_sys::Oid),
    Caller,
}

/// A query's text: a constant, whose plan is kept for the backend, or text
/// built for one use.
pub(crate) enum Query<'a> {
    Kept(&'static str),
    Once(&'a str),
}

thread_local! {
    static PLANS: RefCell<HashMap<&'static str, pg_sys::SPIPlanPtr>> = RefCell::new(HashMap::new());
}

/// The kept plan of `sql`, prepared on first use. Each kept query is an index
/// scan whatever its arguments, so one generic plan serves every call; left
/// to choose, PostgreSQL keeps planning a custom one per call.
fn kept_plan(sql: &'static str, types: &mut [pg_sys::Oid]) -> Result<pg_sys::SPIPlanPtr, GraphError> {
    PLANS.with_borrow_mut(|plans| {
        if let Some(&plan) = plans.get(sql) {
            return Ok(plan);
        }
        let text = CString::new(sql).map_err(source_error)?;
        // SAFETY: SPI is connected; the kept plan lives in a long-lived
        // context, so it stays valid for later calls in this backend.
        let plan = unsafe {
            let plan = pg_sys::SPI_prepare_cursor(
                text.as_ptr(),
                types.len() as i32,
                types.as_mut_ptr(),
                (pg_sys::CURSOR_OPT_GENERIC_PLAN | pg_sys::CURSOR_OPT_NO_SCROLL) as i32,
            );
            if plan.is_null() || pg_sys::SPI_keepplan(plan) != 0 {
                return Err(source_error("could not prepare an adjacency lookup"));
            }
            plan
        };
        plans.insert(sql, plan);
        Ok(plan)
    })
}

/// Runs as the adjacency tables' owner until dropped.
struct Owner {
    user: pg_sys::Oid,
    context: i32,
}

impl Owner {
    fn switch(owner: pg_sys::Oid) -> Self {
        let (mut user, mut context) = (pg_sys::Oid::INVALID, 0);
        // SAFETY: plain reads and writes of the backend's current user.
        unsafe {
            pg_sys::GetUserIdAndSecContext(&mut user, &mut context);
            pg_sys::SetUserIdAndSecContext(owner, context | pg_sys::SECURITY_LOCAL_USERID_CHANGE as i32);
        }
        Self { user, context }
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        // SAFETY: restores the values read in `switch`.
        unsafe { pg_sys::SetUserIdAndSecContext(self.user, self.context) }
    }
}

/// Runs `step` with `who`'s privileges; the caller's are restored on return
/// or unwind, and by PostgreSQL on abort.
fn as_user<T>(who: &RunAs, step: impl FnOnce() -> T) -> T {
    match *who {
        RunAs::Owner(owner) => {
            let _owner = Owner::switch(owner);
            step()
        }
        RunAs::Caller => step(),
    }
}

/// Runs `query` read-only as `who` with `args` as `$1`, `$2`, ..., and hands
/// its rows to `batch` `first` at a time. `batch` returns how many rows to
/// fetch next, or 0 to stop; an error stops the scan and is returned.
pub(crate) fn scan(
    query: Query<'_>,
    args: &[(pg_sys::Oid, pg_sys::Datum)],
    who: RunAs,
    first: usize,
    mut batch: impl FnMut(&[Row]) -> Result<usize, GraphError>,
) -> Result<(), GraphError> {
    Spi::connect(|_| -> Result<(), GraphError> {
        let mut types = args.iter().map(|&(oid, _)| oid).collect::<Vec<_>>();
        let mut values = args.iter().map(|&(_, datum)| datum).collect::<Vec<_>>();
        let nulls = vec![b' ' as c_char; args.len()];
        // SAFETY: SPI is connected and the arguments match their types; a
        // failure to open raises an ERROR rather than returning null.
        let portal = match query {
            Query::Kept(sql) => {
                let plan = kept_plan(sql, &mut types)?;
                as_user(&who, || unsafe {
                    pg_sys::SPI_cursor_open(std::ptr::null(), plan, values.as_mut_ptr(), nulls.as_ptr(), true)
                })
            }
            Query::Once(sql) => {
                let text = CString::new(sql).map_err(source_error)?;
                as_user(&who, || unsafe {
                    pg_sys::SPI_cursor_open_with_args(
                        std::ptr::null(),
                        text.as_ptr(),
                        types.len() as i32,
                        types.as_mut_ptr(),
                        values.as_mut_ptr(),
                        nulls.as_ptr(),
                        true,
                        pg_sys::CURSOR_OPT_NO_SCROLL as i32,
                    )
                })
            }
        };
        let mut size = first;
        let result = loop {
            if size == 0 {
                break Ok(());
            }
            // SAFETY: the portal is open; each batch is read, handed out and
            // freed before the next fetch replaces SPI_tuptable.
            let (rows, table) = unsafe {
                as_user(&who, || pg_sys::SPI_cursor_fetch(portal, true, size as _));
                let table = pg_sys::SPI_tuptable;
                let rows = (0..pg_sys::SPI_processed as usize)
                    .map(|i| Row {
                        tuple: *(*table).vals.add(i),
                        descriptor: (*table).tupdesc,
                    })
                    .collect::<Vec<_>>();
                (rows, table)
            };
            let complete = rows.len() < size;
            let next = if rows.is_empty() { Ok(0) } else { batch(&rows) };
            drop(rows);
            // SAFETY: no row of this batch outlives the call above.
            unsafe { pg_sys::SPI_freetuptable(table) };
            match next {
                Err(error) => break Err(error),
                Ok(_) if complete => break Ok(()),
                Ok(next) => size = next,
            }
        };
        // SAFETY: the portal was opened above and is closed once.
        unsafe { pg_sys::SPI_cursor_close(portal) };
        result
    })
}

/// Every row of a kept query, run as the adjacency tables' owner,
/// `rows_per_fetch` at a time.
pub(crate) fn select(
    sql: &'static str,
    owner: pg_sys::Oid,
    args: &[(pg_sys::Oid, pg_sys::Datum)],
    rows_per_fetch: usize,
    mut row: impl FnMut(&Row) -> Result<(), GraphError>,
) -> Result<(), GraphError> {
    scan(Query::Kept(sql), args, RunAs::Owner(owner), rows_per_fetch, |rows| {
        for each in rows {
            pgrx::check_for_interrupts!();
            row(each)?;
        }
        Ok(rows_per_fetch)
    })
}

/// The first column of the first row of `sql`, run once as the caller.
pub(crate) fn select_one<T: FromDatum>(sql: &str) -> Result<Option<T>, GraphError> {
    let mut value = None;
    scan(Query::Once(sql), &[], RunAs::Caller, 1, |rows| {
        value = rows.first().and_then(|row| row.get(1));
        Ok(0)
    })?;
    Ok(value)
}
