//! Synthetic LDBC-shaped dataset generator.
//!
//! COPIED VERBATIM (with this attribution header) from the sibling
//! graphdb-experiments / NamiDB project:
//!   graphdb-experiments/crates/namidb-bench/src/dataset.rs
//! It has zero NamiDB dependencies, so turbolay reuses it wholesale to
//! guarantee both engines are benchmarked over byte-identical CSVs
//! (seed=42). Keep it in sync with the upstream generator if that changes.
//!
//! The schema mirrors a *subset* of LDBC SNB (Person + Post + Comment +
//! KNOWS / HAS_CREATOR / LIKES / REPLY_OF) that the four queries
//! NamiDB supports end-to-end today (IC2 / IC7 / IC8 / IC9) need.
//! At scale = 1.0 we generate:
//!
//! - 10 000 Person - 100 000 Post - 50 000 Comment
//! - 100 000 KNOWS - 150 000 HAS_CREATOR - 100 000 LIKES - 30 000 REPLY_OF
//!
//! `scale` < 1.0 shrinks every count proportionally for quick smoke tests.
//!
//! All identifiers are stable across runs: a `ChaCha8` RNG seeded with
//! `seed` produces the same node ids and edge picks, so the harness can
//! diff results across backends.
//!
//! Output: CSV files compatible with Kuzu `COPY <Label> FROM '<path>.csv'`
//! and with our own bulk-load path.

use std::fs::File;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// Configuration knobs for the generator.
#[derive(Debug, Clone)]
pub struct DatasetConfig {
    pub scale: f64,
    pub seed: u64,
    /// Number of "hub" persons (indices 0..hub_count) that receive extra
    /// KNOWS edges so their total degree reaches approximately `hub_degree`.
    /// Defaults to 0 (no hubs). Useful for benchmarking high-degree traversal:
    /// the uniform-random generator produces max degree ~77 at scale=1.0
    /// regardless of scale, so hubs are the only way to produce degree > 300.
    pub hub_count: usize,
    /// Target total degree for each hub person. The generator adds
    /// `(hub_degree - avg_regular_degree).max(0)` extra incoming KNOWS edges
    /// to each hub. Default 0 (ignored when hub_count == 0).
    pub hub_degree: usize,
}

impl Default for DatasetConfig {
    fn default() -> Self {
        Self {
            scale: 1.0,
            seed: 42,
            hub_count: 0,
            hub_degree: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DatasetSizes {
    pub persons: usize,
    pub posts: usize,
    pub comments: usize,
    pub knows: usize,
    pub has_creator: usize,
    pub likes: usize,
    pub reply_of: usize,
}

impl DatasetSizes {
    pub fn from_scale(scale: f64) -> Self {
        let scale = scale.max(0.001);
        Self {
            persons: ((10_000.0 * scale) as usize).max(10),
            posts: ((100_000.0 * scale) as usize).max(20),
            comments: ((50_000.0 * scale) as usize).max(10),
            knows: ((100_000.0 * scale) as usize).max(20),
            has_creator: ((150_000.0 * scale) as usize).max(20),
            likes: ((100_000.0 * scale) as usize).max(20),
            reply_of: ((30_000.0 * scale) as usize).max(10),
        }
    }
}

const FIRST_NAMES: &[&str] = &[
    "Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank", "Iris", "Jack", "Karen",
    "Liam", "Mia", "Noah", "Olivia", "Paul",
];
const LAST_NAMES: &[&str] = &[
    "Anderson", "Brown", "Clark", "Davies", "Edwards", "Foley", "Garcia", "Hall", "Iqbal",
    "Johnson", "Khan", "Lopez", "Martinez", "Nguyen", "Olsen", "Park",
];

/// Generate the dataset and write it to `out_dir`. Files emitted:
///
/// - `persons.csv` — `id|firstName|lastName|age|creationDate`
/// - `posts.csv` — `id|content|creationDate|length`
/// - `comments.csv` — `id|content|creationDate|length`
/// - `knows.csv` — `src|dst|since`
/// - `has_creator.csv` — `src|dst` (src is Post/Comment id, dst is Person id)
/// - `likes.csv` — `src|dst|creationDate` (src is Person, dst is Post/Comment)
/// - `reply_of.csv` — `src|dst` (src is Comment, dst is Post or Comment)
pub fn generate(out_dir: &Path, cfg: &DatasetConfig) -> Result<DatasetSizes> {
    std::fs::create_dir_all(out_dir).context("create out_dir")?;
    let sizes = DatasetSizes::from_scale(cfg.scale);
    let mut rng = ChaCha8Rng::seed_from_u64(cfg.seed);

    // ── Persons ────────────────────────────────────────────────────────
    {
        let mut f = File::create(out_dir.join("persons.csv"))?;
        writeln!(f, "id|firstName|lastName|age|creationDate")?;
        for i in 0..sizes.persons {
            let id = person_id(i);
            let fname = FIRST_NAMES[i % FIRST_NAMES.len()];
            let lname = LAST_NAMES[(i / FIRST_NAMES.len()) % LAST_NAMES.len()];
            let age: u32 = 18 + (rng.gen_range(0u32..60u32));
            let creation: i64 = 1_500_000_000 + (i as i64 * 86_400);
            writeln!(f, "{id}|{fname}|{lname}|{age}|{creation}")?;
        }
    }

    // ── Posts ──────────────────────────────────────────────────────────
    {
        let mut f = File::create(out_dir.join("posts.csv"))?;
        writeln!(f, "id|content|creationDate|length")?;
        for i in 0..sizes.posts {
            let id = post_id(i);
            let content = format!("Post body #{i}");
            let creation: i64 = 1_500_000_000 + ((i as i64) * 30);
            let length = content.len() as u32;
            writeln!(f, "{id}|{content}|{creation}|{length}")?;
        }
    }

    // ── Comments ───────────────────────────────────────────────────────
    {
        let mut f = File::create(out_dir.join("comments.csv"))?;
        writeln!(f, "id|content|creationDate|length")?;
        for i in 0..sizes.comments {
            let id = comment_id(i);
            let content = format!("Comment #{i}");
            let creation: i64 = 1_500_001_000 + ((i as i64) * 17);
            let length = content.len() as u32;
            writeln!(f, "{id}|{content}|{creation}|{length}")?;
        }
    }

    // ── KNOWS edges (Person → Person) ──────────────────────────────────
    {
        let mut f = File::create(out_dir.join("knows.csv"))?;
        writeln!(f, "src|dst|since")?;
        for _ in 0..sizes.knows {
            let src = rng.gen_range(0..sizes.persons);
            let mut dst = rng.gen_range(0..sizes.persons);
            if dst == src {
                dst = (src + 1) % sizes.persons;
            }
            let since: i64 = 1_500_010_000 + rng.gen_range(0i64..10_000_000);
            writeln!(f, "{}|{}|{}", person_id(src), person_id(dst), since)?;
        }
        // Hub persons: append extra *outgoing* KNOWS edges so each hub reaches
        // approximately `hub_degree` total degree AND has a high outgoing
        // fan-out. Outgoing edges are what IC02/IC09 actually traverse from the
        // anchor node, so this is the direction that exercises high-degree
        // traversal cost. The avg regular degree is ~45; we split the extra
        // edges evenly between outgoing (hub→random) and incoming
        // (random→hub) so the hub's total degree matches the target.
        if cfg.hub_count > 0 && cfg.hub_degree > 45 {
            let extra_per_hub = cfg.hub_degree - 45;
            let extra_out = extra_per_hub / 2 + extra_per_hub % 2; // ceiling
            let extra_in = extra_per_hub / 2;
            for hub_idx in 0..cfg.hub_count.min(sizes.persons) {
                // outgoing: hub → random person
                for _ in 0..extra_out {
                    let mut dst = rng.gen_range(0..sizes.persons);
                    if dst == hub_idx {
                        dst = (hub_idx + 1) % sizes.persons;
                    }
                    let since: i64 = 1_500_010_000 + rng.gen_range(0i64..10_000_000);
                    writeln!(f, "{}|{}|{}", person_id(hub_idx), person_id(dst), since)?;
                }
                // incoming: random person → hub
                for _ in 0..extra_in {
                    let mut src = rng.gen_range(0..sizes.persons);
                    if src == hub_idx {
                        src = (hub_idx + 1) % sizes.persons;
                    }
                    let since: i64 = 1_500_010_000 + rng.gen_range(0i64..10_000_000);
                    writeln!(f, "{}|{}|{}", person_id(src), person_id(hub_idx), since)?;
                }
            }
        }
    }

    // ── HAS_CREATOR edges (Post/Comment → Person) ──────────────────────
    {
        let mut f = File::create(out_dir.join("has_creator.csv"))?;
        writeln!(f, "src|dst")?;
        // First half: Posts pick a creator; second half: Comments pick.
        for _ in 0..sizes.has_creator / 2 {
            let post_idx = rng.gen_range(0..sizes.posts);
            let person_idx = rng.gen_range(0..sizes.persons);
            writeln!(f, "{}|{}", post_id(post_idx), person_id(person_idx))?;
        }
        for _ in sizes.has_creator / 2..sizes.has_creator {
            let comment_idx = rng.gen_range(0..sizes.comments);
            let person_idx = rng.gen_range(0..sizes.persons);
            writeln!(f, "{}|{}", comment_id(comment_idx), person_id(person_idx))?;
        }
    }

    // ── LIKES edges (Person → Post/Comment) ─────────────────────────────
    {
        let mut f = File::create(out_dir.join("likes.csv"))?;
        writeln!(f, "src|dst|creationDate")?;
        for _ in 0..sizes.likes / 2 {
            let person_idx = rng.gen_range(0..sizes.persons);
            let post_idx = rng.gen_range(0..sizes.posts);
            let date: i64 = 1_500_020_000 + rng.gen_range(0i64..5_000_000);
            writeln!(
                f,
                "{}|{}|{}",
                person_id(person_idx),
                post_id(post_idx),
                date
            )?;
        }
        for _ in sizes.likes / 2..sizes.likes {
            let person_idx = rng.gen_range(0..sizes.persons);
            let comment_idx = rng.gen_range(0..sizes.comments);
            let date: i64 = 1_500_020_000 + rng.gen_range(0i64..5_000_000);
            writeln!(
                f,
                "{}|{}|{}",
                person_id(person_idx),
                comment_id(comment_idx),
                date
            )?;
        }
    }

    // ── REPLY_OF edges (Comment → Post|Comment) ─────────────────────────
    {
        let mut f = File::create(out_dir.join("reply_of.csv"))?;
        writeln!(f, "src|dst")?;
        for _ in 0..sizes.reply_of {
            let comment_idx = rng.gen_range(0..sizes.comments);
            // 70% reply to Post, 30% reply to Comment.
            let dst = if rng.gen_bool(0.7) {
                post_id(rng.gen_range(0..sizes.posts))
            } else {
                let other = rng.gen_range(0..sizes.comments);
                comment_id(if other == comment_idx {
                    (comment_idx + 1) % sizes.comments
                } else {
                    other
                })
            };
            writeln!(f, "{}|{}", comment_id(comment_idx), dst)?;
        }
    }

    Ok(sizes)
}

/// Build a stable UUIDv4-like 16-byte id from `(prefix, index)`. The
/// prefix tags the kind (P=Person, O=Post, C=Comment) so the same
/// numeric index maps to distinct ids across labels.
fn person_id(i: usize) -> String {
    encode_id(b'P', i)
}
fn post_id(i: usize) -> String {
    encode_id(b'O', i)
}
fn comment_id(i: usize) -> String {
    encode_id(b'C', i)
}

fn encode_id(prefix: u8, i: usize) -> String {
    // Lay out a 16-byte id whose first byte is the prefix and the rest is
    // big-endian u128 = i. Render as 32-hex-char string.
    let mut bytes = [0u8; 16];
    bytes[0] = prefix;
    let i_bytes = (i as u128).to_be_bytes();
    bytes[1..].copy_from_slice(&i_bytes[1..]);
    let mut s = String::with_capacity(32);
    for b in bytes {
        let _ = std::fmt::Write::write_fmt(&mut s, format_args!("{:02x}", b));
    }
    s
}

/// Compute the degree (total incident edges, counting both directions) of
/// every Person node in `csv_dir`. Returns a list of `(person_id_hex, degree)`
/// sorted by degree descending.
pub fn person_degrees_from_csvs(csv_dir: &Path) -> Result<Vec<(String, usize)>> {
    use std::collections::HashMap;
    use std::io::BufRead;

    let mut deg: HashMap<String, usize> = HashMap::new();

    for fname in &["knows.csv", "has_creator.csv", "likes.csv", "reply_of.csv"] {
        let path = csv_dir.join(fname);
        let f = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        let mut lines = std::io::BufReader::new(f).lines();
        lines.next(); // skip header
        for line in lines {
            let line = line?;
            let mut parts = line.splitn(3, '|');
            let src = parts.next().unwrap_or("").to_owned();
            let dst = parts.next().unwrap_or("").to_owned();
            *deg.entry(src).or_default() += 1;
            *deg.entry(dst).or_default() += 1;
        }
    }

    // Keep only Person nodes (hex prefix 0x50 = 'P').
    let mut person_deg: Vec<(String, usize)> = deg
        .into_iter()
        .filter(|(id, _)| id.starts_with("50"))
        .collect();
    person_deg.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    Ok(person_deg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn small_dataset_has_expected_shape() {
        let tmp = tempdir();
        let cfg = DatasetConfig {
            scale: 0.01,
            seed: 7,
            hub_count: 0,
            hub_degree: 0,
        };
        let sizes = generate(tmp.path(), &cfg).unwrap();
        assert!(sizes.persons >= 10);
        assert!(sizes.knows >= 20);
        for f in [
            "persons.csv",
            "posts.csv",
            "comments.csv",
            "knows.csv",
            "has_creator.csv",
            "likes.csv",
            "reply_of.csv",
        ] {
            assert!(tmp.path().join(f).is_file(), "missing {f}");
        }
    }

    #[test]
    fn person_ids_are_unique() {
        let mut seen: BTreeSet<String> = BTreeSet::new();
        for i in 0..1000 {
            assert!(seen.insert(person_id(i)));
        }
    }

    /// Tiny stand-in for `tempfile::tempdir` so we don't pull a dep just
    /// for the test. Uses `std::env::temp_dir()` with a unique suffix.
    fn tempdir() -> TmpDir {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "namidb-bench-test-{}",
            uuid::Uuid::now_v7().simple()
        ));
        std::fs::create_dir_all(&path).unwrap();
        TmpDir(path)
    }

    struct TmpDir(std::path::PathBuf);
    impl TmpDir {
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
