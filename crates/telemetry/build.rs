//! Build script for `hydradb-telemetry`.
//!
//! Its only job is to stamp the commit the binary was built from into the
//! binary, as `rustc-env` values that `src/build_info.rs` reads with `env!`.
//!
//! It lives in *this* crate rather than the root one because a build script's
//! `rustc-env` reaches only its own crate's compilation. `service_version`
//! already lives here (`src/config.rs`), and the fmt layer already stamps it on
//! every log line (`src/layers.rs`), so this is the one crate where the value
//! arrives everywhere it needs to be without a second copy.
//!
//! Resolution order per field (first hit wins):
//!   1. The environment — `GIT_SHA`, `GIT_BRANCH`, `SOURCE_DATE_EPOCH`.
//!   2. `git`, for local builds.
//!   3. `"unknown"`.
//!
//! The environment comes first because of `.dockerignore:1`: it excludes
//! `.git`, so inside an image build there is no repository to interrogate and
//! `git` resolves to nothing. CI passes the sha as a `--build-arg` instead
//! (`.github/workflows/container.yml`), and a build script that preferred `git`
//! would quietly stamp `unknown` into precisely the builds that matter. Getting
//! this order wrong is not a compile error; it is a container that ships
//! without provenance and says nothing about it.
//!
//! Note on the dirty flag: it is a snapshot from the last time this script ran,
//! and `cargo:rerun-if-changed` below watches `HEAD` and the checked-out ref,
//! not the working tree. Editing a tracked file therefore does not by itself
//! re-stamp `-dirty`. Watching the whole tree would rebuild this crate — and
//! relink both binaries — on every keystroke, which is a bad trade for a field
//! that only disambiguates local builds. Released builds come from a clean CI
//! checkout, where the flag is correct by construction.

use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Length of the abbreviated sha. Longer than `git`'s default 7 because the
/// value is compared by eye against a 40-character image tag
/// (`container.yml` sets `IMAGE_TAG: ${{ github.sha }}`), and 12 is enough of a
/// prefix to make that match unambiguous.
const SHORT_SHA_LEN: usize = 12;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    for var in ["GIT_SHA", "GIT_BRANCH", "SOURCE_DATE_EPOCH"] {
        println!("cargo:rerun-if-env-changed={var}");
    }
    watch_git_head();

    let sha = env_or_git("GIT_SHA", &["rev-parse", "HEAD"]);
    let short = sha
        .as_deref()
        .filter(|sha| sha.len() >= SHORT_SHA_LEN)
        .map(|sha| sha[..SHORT_SHA_LEN].to_string());
    let branch = env_or_git("GIT_BRANCH", &["rev-parse", "--abbrev-ref", "HEAD"]);

    emit("HYDRADB_GIT_SHA", sha.as_deref());
    emit("HYDRADB_GIT_SHA_SHORT", short.as_deref());
    emit("HYDRADB_GIT_BRANCH", branch.as_deref());
    println!("cargo:rustc-env=HYDRADB_GIT_DIRTY={}", is_dirty());
    println!("cargo:rustc-env=HYDRADB_BUILD_TIMESTAMP={}", build_time());
    emit("HYDRADB_RUSTC_VERSION", rustc_version().as_deref());
}

/// Emit a value, or `"unknown"` when there is none.
///
/// Always emitting keeps `env!` in `build_info.rs` infallible, so a missing
/// value is a string a reader can see rather than a build that fails on a
/// machine without `git`.
fn emit(key: &str, value: Option<&str>) {
    println!("cargo:rustc-env={key}={}", value.unwrap_or("unknown"));
}

/// Re-run when the checked-out commit changes, and only then.
///
/// Silent when there is no repository: emitting `rerun-if-changed` for a path
/// that does not exist makes cargo re-run the script on *every* build, which
/// would re-stamp the timestamp and recompile this crate each time.
fn watch_git_head() {
    let Some(git_dir) = git(&["rev-parse", "--absolute-git-dir"]) else {
        return;
    };
    let git_dir = std::path::Path::new(&git_dir);
    for path in [git_dir.join("HEAD"), git_dir.join("packed-refs")] {
        if path.is_file() {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
    // The symbolic ref itself, so a commit on the current branch is seen even
    // though `HEAD` keeps pointing at the same ref name.
    if let Some(reference) = git(&["symbolic-ref", "--quiet", "HEAD"]) {
        let path = git_dir.join(&reference);
        if path.is_file() {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
}

fn env_or_git(var: &str, args: &[&str]) -> Option<String> {
    match std::env::var(var) {
        Ok(value) if !value.trim().is_empty() => Some(value.trim().to_string()),
        _ => git(args),
    }
}

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?.trim().to_string();
    (!value.is_empty()).then_some(value)
}

/// Whether the working tree had uncommitted changes to tracked files.
///
/// `false` when the answer cannot be determined — inside an image build there
/// is no repository, and the honest reading of "no tree to be dirty" is clean.
fn is_dirty() -> bool {
    git(&["status", "--porcelain", "--untracked-files=no"]).is_some_and(|out| !out.is_empty())
}

/// The build time as RFC 3339 UTC, honouring `SOURCE_DATE_EPOCH`.
///
/// Formatted by hand rather than with `chrono` so this script needs no
/// build-dependencies: pulling `chrono` in here would compile it a second time,
/// for the host, before the crate itself can start.
fn build_time() -> String {
    let seconds = std::env::var("SOURCE_DATE_EPOCH")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .or_else(|| SystemTime::now().duration_since(UNIX_EPOCH).ok().map(|since| since.as_secs()))
        .unwrap_or(0);
    format_rfc3339(Duration::from_secs(seconds).as_secs())
}

/// Civil time from a Unix timestamp, proleptic Gregorian, UTC.
///
/// The algorithm is Howard Hinnant's `civil_from_days`, which is exact for
/// every date this will ever see and is a dozen lines with no leap-second
/// handling to get wrong — the timestamp is provenance, not a clock.
fn format_rfc3339(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let time_of_day = secs % 86_400;

    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era = (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = if month_prime < 10 { month_prime + 3 } else { month_prime - 9 };
    let year = year_of_era + era * 400 + i64::from(month <= 2);

    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        time_of_day / 3_600,
        (time_of_day % 3_600) / 60,
        time_of_day % 60,
    )
}

/// The compiler that built this, e.g. `rustc 1.91.0 (…)` reduced to `1.91.0`.
fn rustc_version() -> Option<String> {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    let output = Command::new(rustc).arg("--version").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    text.split_whitespace().nth(1).map(str::to_string)
}
