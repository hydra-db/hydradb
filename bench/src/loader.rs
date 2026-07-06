//! Bulk-load the CSVs `dataset::generate` emits into a live turbolay
//! [`Writer`].
//!
//! Two loading paths, selected by `batch_size`:
//!
//! - `batch_size <= 1` — the legacy path: plain per-record `upsert_node` /
//!   `upsert_edge` / `upsert_edge_with_props` calls, one durable commit each.
//!   Fine for small/local datasets; on a real object-store-backed SlateDB
//!   this is one network round trip per element, which does not scale to
//!   LDBC scale-10-ish datasets (millions of elements).
//! - `batch_size > 1` — groups up to `batch_size` logical records into one
//!   [`turbolay::write::Writer::ingest_batch`] call (one physical commit per
//!   chunk), per RFC 0004's batched write path.
//!
//! Column layouts mirror `dataset.rs`'s own header docs exactly:
//!
//! - `persons.csv` — `id|firstName|lastName|age|creationDate`
//! - `posts.csv` / `comments.csv` — `id|content|creationDate|length`
//! - `knows.csv` — `src|dst|since` (Person -> Person)
//! - `has_creator.csv` — `src|dst` (Post/Comment -> Person)
//! - `likes.csv` — `src|dst|creationDate` (Person -> Post/Comment)
//! - `reply_of.csv` — `src|dst` (Comment -> Post|Comment)
//!
//! The 32-hex-char `id`/`src`/`dst` cells are used directly as the xid bytes
//! for `upsert_node`/`upsert_edge*`/`IngestRecord` — no separate internal id
//! translation layer, unlike the NamiDB reference harness this is
//! pattern-matched from (which parses the hex back into a `NodeId`).
//! turbolay's own xid -> uid resolution (`Writer::lookup_uid`) does that job.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{Context, Result};
use turbolay::value::TypedValue;
use turbolay::write::{IngestRecord, Writer};

/// Loads every CSV `dataset::generate` emits from `csv_dir` into `writer`.
/// Nodes are loaded before the edges that reference them (though
/// `upsert_edge*`/a batched `Edge` record would stub-create a missing
/// endpoint anyway, per RFC 0004 §"UpsertEdge").
///
/// `batch_size`: `0` or `1` selects the legacy per-record path (unchanged
/// method calls, one commit per record); anything greater groups records
/// into `ingest_batch` calls of at most that many logical records each.
pub async fn load_into_writer(
    writer: &mut Writer,
    csv_dir: &Path,
    batch_size: usize,
) -> Result<()> {
    if batch_size <= 1 {
        load_persons(writer, &csv_dir.join("persons.csv")).await?;
        load_posts(writer, &csv_dir.join("posts.csv")).await?;
        load_comments(writer, &csv_dir.join("comments.csv")).await?;
        load_knows(writer, &csv_dir.join("knows.csv")).await?;
        load_has_creator(writer, &csv_dir.join("has_creator.csv")).await?;
        load_likes(writer, &csv_dir.join("likes.csv")).await?;
        load_reply_of(writer, &csv_dir.join("reply_of.csv")).await?;
        return Ok(());
    }

    // Batched path: parse each source CSV into `IngestRecord`s (one file at
    // a time, so peak memory is bounded by the largest single CSV rather
    // than the whole dataset) and flush it in chunks of `batch_size` before
    // moving to the next file. Node-bearing files are all flushed before any
    // edge-bearing file, so every edge's endpoints are already durably
    // committed by the time its batch's in-batch xid cache falls through to
    // a storage read (see `Writer::ingest_batch`'s doc on why that matters).
    ingest_in_batches(
        writer,
        parse_persons(&csv_dir.join("persons.csv"))?,
        batch_size,
    )
    .await?;
    ingest_in_batches(writer, parse_posts(&csv_dir.join("posts.csv"))?, batch_size).await?;
    ingest_in_batches(
        writer,
        parse_comments(&csv_dir.join("comments.csv"))?,
        batch_size,
    )
    .await?;
    ingest_in_batches(writer, parse_knows(&csv_dir.join("knows.csv"))?, batch_size).await?;
    ingest_in_batches(
        writer,
        parse_has_creator(&csv_dir.join("has_creator.csv"))?,
        batch_size,
    )
    .await?;
    ingest_in_batches(writer, parse_likes(&csv_dir.join("likes.csv"))?, batch_size).await?;
    ingest_in_batches(
        writer,
        parse_reply_of(&csv_dir.join("reply_of.csv"))?,
        batch_size,
    )
    .await?;
    Ok(())
}

/// Flushes `records` through [`Writer::ingest_batch`] in chunks of at most
/// `batch_size` (one physical commit per chunk).
async fn ingest_in_batches(
    writer: &mut Writer,
    records: Vec<IngestRecord>,
    batch_size: usize,
) -> Result<()> {
    for chunk in records.chunks(batch_size.max(1)) {
        writer.ingest_batch(chunk).await?;
    }
    Ok(())
}

/// Reads `path` line-by-line, buffered (no need to hold the whole file in
/// memory — datasets can run into the hundreds of thousands of rows).
fn read_lines(path: &Path) -> Result<Vec<String>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    BufReader::new(file)
        .lines()
        .collect::<std::io::Result<Vec<String>>>()
        .with_context(|| format!("read {}", path.display()))
}

async fn load_persons(writer: &mut Writer, path: &Path) -> Result<()> {
    for (i, line) in read_lines(path)?.into_iter().enumerate() {
        if i == 0 {
            continue; // header
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() < 5 {
            continue;
        }
        let mut props: BTreeMap<String, TypedValue> = BTreeMap::new();
        props.insert(
            "firstName".to_string(),
            TypedValue::String(parts[1].to_string()),
        );
        props.insert(
            "lastName".to_string(),
            TypedValue::String(parts[2].to_string()),
        );
        props.insert("age".to_string(), TypedValue::Int(parts[3].parse::<i64>()?));
        props.insert(
            "creationDate".to_string(),
            TypedValue::Int(parts[4].parse::<i64>()?),
        );
        writer
            .upsert_node(parts[0].as_bytes(), &["Person".to_string()], props)
            .await?;
    }
    Ok(())
}

async fn load_posts(writer: &mut Writer, path: &Path) -> Result<()> {
    for (i, line) in read_lines(path)?.into_iter().enumerate() {
        if i == 0 {
            continue;
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() < 4 {
            continue;
        }
        let mut props: BTreeMap<String, TypedValue> = BTreeMap::new();
        props.insert(
            "content".to_string(),
            TypedValue::String(parts[1].to_string()),
        );
        props.insert(
            "creationDate".to_string(),
            TypedValue::Int(parts[2].parse::<i64>()?),
        );
        props.insert(
            "length".to_string(),
            TypedValue::Int(parts[3].parse::<i64>()?),
        );
        writer
            .upsert_node(parts[0].as_bytes(), &["Post".to_string()], props)
            .await?;
    }
    Ok(())
}

async fn load_comments(writer: &mut Writer, path: &Path) -> Result<()> {
    for (i, line) in read_lines(path)?.into_iter().enumerate() {
        if i == 0 {
            continue;
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() < 4 {
            continue;
        }
        let mut props: BTreeMap<String, TypedValue> = BTreeMap::new();
        props.insert(
            "content".to_string(),
            TypedValue::String(parts[1].to_string()),
        );
        props.insert(
            "creationDate".to_string(),
            TypedValue::Int(parts[2].parse::<i64>()?),
        );
        props.insert(
            "length".to_string(),
            TypedValue::Int(parts[3].parse::<i64>()?),
        );
        writer
            .upsert_node(parts[0].as_bytes(), &["Comment".to_string()], props)
            .await?;
    }
    Ok(())
}

async fn load_knows(writer: &mut Writer, path: &Path) -> Result<()> {
    for (i, line) in read_lines(path)?.into_iter().enumerate() {
        if i == 0 {
            continue;
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() < 3 {
            continue;
        }
        let mut props: BTreeMap<String, TypedValue> = BTreeMap::new();
        props.insert(
            "since".to_string(),
            TypedValue::Int(parts[2].parse::<i64>()?),
        );
        writer
            .upsert_edge_with_props(parts[0].as_bytes(), "KNOWS", parts[1].as_bytes(), props)
            .await?;
    }
    Ok(())
}

async fn load_has_creator(writer: &mut Writer, path: &Path) -> Result<()> {
    for (i, line) in read_lines(path)?.into_iter().enumerate() {
        if i == 0 {
            continue;
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() < 2 {
            continue;
        }
        writer
            .upsert_edge(parts[0].as_bytes(), "HAS_CREATOR", parts[1].as_bytes())
            .await?;
    }
    Ok(())
}

async fn load_likes(writer: &mut Writer, path: &Path) -> Result<()> {
    for (i, line) in read_lines(path)?.into_iter().enumerate() {
        if i == 0 {
            continue;
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() < 3 {
            continue;
        }
        let mut props: BTreeMap<String, TypedValue> = BTreeMap::new();
        props.insert(
            "creationDate".to_string(),
            TypedValue::Int(parts[2].parse::<i64>()?),
        );
        writer
            .upsert_edge_with_props(parts[0].as_bytes(), "LIKES", parts[1].as_bytes(), props)
            .await?;
    }
    Ok(())
}

async fn load_reply_of(writer: &mut Writer, path: &Path) -> Result<()> {
    for (i, line) in read_lines(path)?.into_iter().enumerate() {
        if i == 0 {
            continue;
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() < 2 {
            continue;
        }
        writer
            .upsert_edge(parts[0].as_bytes(), "REPLY_OF", parts[1].as_bytes())
            .await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Batched path: the same per-line parsing as the `load_*` functions above,
// just building `IngestRecord`s for `Writer::ingest_batch` instead of
// awaiting one `upsert_*` call per line.
// ---------------------------------------------------------------------------

fn parse_persons(path: &Path) -> Result<Vec<IngestRecord>> {
    let mut out = Vec::new();
    for (i, line) in read_lines(path)?.into_iter().enumerate() {
        if i == 0 {
            continue; // header
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() < 5 {
            continue;
        }
        let mut props: BTreeMap<String, TypedValue> = BTreeMap::new();
        props.insert(
            "firstName".to_string(),
            TypedValue::String(parts[1].to_string()),
        );
        props.insert(
            "lastName".to_string(),
            TypedValue::String(parts[2].to_string()),
        );
        props.insert("age".to_string(), TypedValue::Int(parts[3].parse::<i64>()?));
        props.insert(
            "creationDate".to_string(),
            TypedValue::Int(parts[4].parse::<i64>()?),
        );
        out.push(IngestRecord::Node {
            xid: parts[0].as_bytes().to_vec(),
            labels: vec!["Person".to_string()],
            props,
        });
    }
    Ok(out)
}

fn parse_posts(path: &Path) -> Result<Vec<IngestRecord>> {
    let mut out = Vec::new();
    for (i, line) in read_lines(path)?.into_iter().enumerate() {
        if i == 0 {
            continue;
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() < 4 {
            continue;
        }
        let mut props: BTreeMap<String, TypedValue> = BTreeMap::new();
        props.insert(
            "content".to_string(),
            TypedValue::String(parts[1].to_string()),
        );
        props.insert(
            "creationDate".to_string(),
            TypedValue::Int(parts[2].parse::<i64>()?),
        );
        props.insert(
            "length".to_string(),
            TypedValue::Int(parts[3].parse::<i64>()?),
        );
        out.push(IngestRecord::Node {
            xid: parts[0].as_bytes().to_vec(),
            labels: vec!["Post".to_string()],
            props,
        });
    }
    Ok(out)
}

fn parse_comments(path: &Path) -> Result<Vec<IngestRecord>> {
    let mut out = Vec::new();
    for (i, line) in read_lines(path)?.into_iter().enumerate() {
        if i == 0 {
            continue;
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() < 4 {
            continue;
        }
        let mut props: BTreeMap<String, TypedValue> = BTreeMap::new();
        props.insert(
            "content".to_string(),
            TypedValue::String(parts[1].to_string()),
        );
        props.insert(
            "creationDate".to_string(),
            TypedValue::Int(parts[2].parse::<i64>()?),
        );
        props.insert(
            "length".to_string(),
            TypedValue::Int(parts[3].parse::<i64>()?),
        );
        out.push(IngestRecord::Node {
            xid: parts[0].as_bytes().to_vec(),
            labels: vec!["Comment".to_string()],
            props,
        });
    }
    Ok(out)
}

fn parse_knows(path: &Path) -> Result<Vec<IngestRecord>> {
    let mut out = Vec::new();
    for (i, line) in read_lines(path)?.into_iter().enumerate() {
        if i == 0 {
            continue;
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() < 3 {
            continue;
        }
        let mut props: BTreeMap<String, TypedValue> = BTreeMap::new();
        props.insert(
            "since".to_string(),
            TypedValue::Int(parts[2].parse::<i64>()?),
        );
        out.push(IngestRecord::Edge {
            src_xid: parts[0].as_bytes().to_vec(),
            pred: "KNOWS".to_string(),
            dst_xid: parts[1].as_bytes().to_vec(),
            props,
        });
    }
    Ok(out)
}

fn parse_has_creator(path: &Path) -> Result<Vec<IngestRecord>> {
    let mut out = Vec::new();
    for (i, line) in read_lines(path)?.into_iter().enumerate() {
        if i == 0 {
            continue;
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() < 2 {
            continue;
        }
        out.push(IngestRecord::Edge {
            src_xid: parts[0].as_bytes().to_vec(),
            pred: "HAS_CREATOR".to_string(),
            dst_xid: parts[1].as_bytes().to_vec(),
            props: BTreeMap::new(),
        });
    }
    Ok(out)
}

fn parse_likes(path: &Path) -> Result<Vec<IngestRecord>> {
    let mut out = Vec::new();
    for (i, line) in read_lines(path)?.into_iter().enumerate() {
        if i == 0 {
            continue;
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() < 3 {
            continue;
        }
        let mut props: BTreeMap<String, TypedValue> = BTreeMap::new();
        props.insert(
            "creationDate".to_string(),
            TypedValue::Int(parts[2].parse::<i64>()?),
        );
        out.push(IngestRecord::Edge {
            src_xid: parts[0].as_bytes().to_vec(),
            pred: "LIKES".to_string(),
            dst_xid: parts[1].as_bytes().to_vec(),
            props,
        });
    }
    Ok(out)
}

fn parse_reply_of(path: &Path) -> Result<Vec<IngestRecord>> {
    let mut out = Vec::new();
    for (i, line) in read_lines(path)?.into_iter().enumerate() {
        if i == 0 {
            continue;
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() < 2 {
            continue;
        }
        out.push(IngestRecord::Edge {
            src_xid: parts[0].as_bytes().to_vec(),
            pred: "REPLY_OF".to_string(),
            dst_xid: parts[1].as_bytes().to_vec(),
            props: BTreeMap::new(),
        });
    }
    Ok(out)
}
