use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

use hydradb::OfflineImageRow;
use slatedb::bytes::Bytes;
use slatedb::object_store::{path::Path as ObjectPath, ObjectStore};
use slatedb::{OfflineImageBuilder, OfflineImageOptions, OfflineImageResult};
use thiserror::Error;

const MAX_MERGE_FAN_IN: usize = 64;

#[derive(Debug, Error)]
pub enum KvSortError {
    #[error("offline key/value sort I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("offline key/value run contains an invalid length")]
    InvalidLength,
    #[error("offline image contains conflicting values for key {0:?}")]
    ConflictingKey(Bytes),
    #[error("SlateDB offline image failed: {0}")]
    SlateDb(#[from] slatedb::Error),
}

pub struct ExternalKvSorter {
    scratch: PathBuf,
    max_buffer_bytes: usize,
    buffered_bytes: usize,
    rows: Vec<OfflineImageRow>,
    runs: Vec<PathBuf>,
}

impl ExternalKvSorter {
    pub fn new(scratch: impl Into<PathBuf>, max_buffer_bytes: usize) -> Result<Self, KvSortError> {
        if max_buffer_bytes == 0 {
            return Err(KvSortError::InvalidLength);
        }
        let scratch = scratch.into();
        std::fs::create_dir_all(&scratch)?;
        Ok(Self {
            scratch,
            max_buffer_bytes,
            buffered_bytes: 0,
            rows: Vec::new(),
            runs: Vec::new(),
        })
    }

    pub fn push(&mut self, row: OfflineImageRow) -> Result<(), KvSortError> {
        self.buffered_bytes = self
            .buffered_bytes
            .saturating_add(row.key.len())
            .saturating_add(row.value.len())
            .saturating_add(16);
        self.rows.push(row);
        if self.buffered_bytes >= self.max_buffer_bytes {
            self.flush_run()?;
        }
        Ok(())
    }

    pub fn extend(&mut self, rows: impl IntoIterator<Item = OfflineImageRow>) -> Result<(), KvSortError> {
        for row in rows {
            self.push(row)?;
        }
        Ok(())
    }

    /// Take over another sorter's rows as sorted runs, so rows several threads
    /// sorted apart publish as one image.
    pub fn absorb(&mut self, mut other: ExternalKvSorter) -> Result<(), KvSortError> {
        other.flush_run()?;
        self.runs.append(&mut other.runs);
        Ok(())
    }

    pub async fn publish(
        self,
        path: ObjectPath,
        object_store: Arc<dyn ObjectStore>,
        target_sst_size_bytes: usize,
    ) -> Result<OfflineImageResult, KvSortError> {
        let options = OfflineImageOptions {
            target_sst_size_bytes,
            compression_codec: hydradb::GRAPH_SST_COMPRESSION_CODEC,
            filter_policies: hydradb::graph_filter_policies(),
        };
        let mut builder = OfflineImageBuilder::create(path, object_store, options).await?;
        for row in self.into_sorted_iter()? {
            let (key, value) = row?;
            builder.add(key, value).await?;
        }
        Ok(builder.finish().await?)
    }

    /// Every row pushed, in key order, each key once: the rows `publish`
    /// writes, for a caller that merges them with rows of its own. Two values
    /// for one key are refused as the iterator reaches them.
    pub fn into_sorted_iter(mut self) -> Result<SortedRows, KvSortError> {
        self.flush_run()?;
        self.collapse_runs()?;
        Ok(SortedRows {
            merger: RunMerger::open(&self.runs)?,
        })
    }

    fn collapse_runs(&mut self) -> Result<(), KvSortError> {
        let mut pass = 0usize;
        while self.runs.len() > MAX_MERGE_FAN_IN {
            let mut next = Vec::with_capacity(self.runs.len().div_ceil(MAX_MERGE_FAN_IN));
            for (group_index, group) in self.runs.chunks(MAX_MERGE_FAN_IN).enumerate() {
                if group.len() == 1 {
                    next.push(group[0].clone());
                    continue;
                }
                let output = self.scratch.join(format!("kv-merge-{pass:04}-{group_index:06}.bin"));
                let mut writer = BufWriter::new(File::create(&output)?);
                let mut merger = RunMerger::open(group)?;
                while let Some((key, value)) = merger.next_unique()? {
                    write_bytes(&mut writer, &key)?;
                    write_bytes(&mut writer, &value)?;
                }
                writer.flush()?;
                drop(merger);
                for input in group {
                    std::fs::remove_file(input)?;
                }
                next.push(output);
            }
            self.runs = next;
            pass += 1;
        }
        Ok(())
    }

    fn flush_run(&mut self) -> Result<(), KvSortError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        self.rows.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        let path = self.scratch.join(format!("kv-run-{:06}.bin", self.runs.len()));
        let mut writer = BufWriter::new(File::create(&path)?);
        let mut previous: Option<&OfflineImageRow> = None;
        for row in &self.rows {
            if let Some(last) = previous {
                if last.key == row.key {
                    if last.value != row.value {
                        return Err(KvSortError::ConflictingKey(row.key.clone()));
                    }
                    continue;
                }
            }
            write_bytes(&mut writer, &row.key)?;
            write_bytes(&mut writer, &row.value)?;
            previous = Some(row);
        }
        writer.flush()?;
        self.runs.push(path);
        self.rows.clear();
        self.buffered_bytes = 0;
        Ok(())
    }
}

/// The merged rows of an [`ExternalKvSorter`], from
/// [`ExternalKvSorter::into_sorted_iter`], read from its run files in the
/// caller's scratch directory.
pub struct SortedRows {
    merger: RunMerger,
}

impl Iterator for SortedRows {
    type Item = Result<(Bytes, Bytes), KvSortError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.merger.next_unique().transpose()
    }
}

fn write_bytes(writer: &mut impl Write, bytes: &[u8]) -> Result<(), KvSortError> {
    let length = u32::try_from(bytes.len()).map_err(|_| KvSortError::InvalidLength)?;
    writer.write_all(&length.to_be_bytes())?;
    writer.write_all(bytes)?;
    Ok(())
}

struct RunReader {
    reader: BufReader<File>,
}

struct RunMerger {
    readers: Vec<RunReader>,
    heap: BinaryHeap<HeapRow>,
    pending: Option<(Bytes, Bytes)>,
}

impl RunMerger {
    fn open(paths: &[PathBuf]) -> Result<Self, KvSortError> {
        if paths.len() > MAX_MERGE_FAN_IN {
            return Err(KvSortError::InvalidLength);
        }
        let mut readers = paths.iter().map(RunReader::open).collect::<Result<Vec<_>, _>>()?;
        let mut heap = BinaryHeap::new();
        for (run, reader) in readers.iter_mut().enumerate() {
            if let Some((key, value)) = reader.next()? {
                heap.push(HeapRow { run, key, value });
            }
        }
        Ok(Self {
            readers,
            heap,
            pending: None,
        })
    }

    fn next_unique(&mut self) -> Result<Option<(Bytes, Bytes)>, KvSortError> {
        loop {
            let Some(row) = self.heap.pop() else {
                return Ok(self.pending.take());
            };
            if let Some((key, value)) = self.readers[row.run].next()? {
                self.heap.push(HeapRow { run: row.run, key, value });
            }
            let Some((pending_key, pending_value)) = self.pending.take() else {
                self.pending = Some((row.key, row.value));
                continue;
            };
            if pending_key == row.key {
                if pending_value != row.value {
                    return Err(KvSortError::ConflictingKey(row.key));
                }
                self.pending = Some((pending_key, pending_value));
                continue;
            }
            self.pending = Some((row.key, row.value));
            return Ok(Some((pending_key, pending_value)));
        }
    }
}

impl RunReader {
    fn open(path: &PathBuf) -> Result<Self, KvSortError> {
        Ok(Self {
            reader: BufReader::new(File::open(path)?),
        })
    }

    fn next(&mut self) -> Result<Option<(Bytes, Bytes)>, KvSortError> {
        let Some(key) = read_bytes(&mut self.reader)? else {
            return Ok(None);
        };
        let value = read_bytes(&mut self.reader)?.ok_or(KvSortError::InvalidLength)?;
        Ok(Some((key, value)))
    }
}

fn read_bytes(reader: &mut impl Read) -> Result<Option<Bytes>, KvSortError> {
    let mut encoded_length = [0u8; 4];
    match reader.read(&mut encoded_length[..1]) {
        Ok(0) => return Ok(None),
        Ok(1) => reader.read_exact(&mut encoded_length[1..])?,
        Ok(_) => unreachable!("the read buffer has length one"),
        Err(error) => return Err(error.into()),
    }
    let length = u32::from_be_bytes(encoded_length) as usize;
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes)?;
    Ok(Some(Bytes::from(bytes)))
}

#[derive(Eq, PartialEq)]
struct HeapRow {
    run: usize,
    key: Bytes,
    value: Bytes,
}

impl Ord for HeapRow {
    fn cmp(&self, other: &Self) -> Ordering {
        other.key.cmp(&self.key).then_with(|| other.run.cmp(&self.run))
    }
}

impl PartialOrd for HeapRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
