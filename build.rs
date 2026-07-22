//! Build script for `slatedb-graph-kernel`.
//!
//! Its only job is to teach the linker where SuiteSparse:GraphBLAS lives when the
//! `graphblas` feature is enabled (which it now is by default). Without this, a
//! Homebrew install on macOS fails with `ld: library 'graphblas' not found`
//! unless every developer exports `RUSTFLAGS="-L /opt/homebrew/lib"` by hand.
//!
//! Resolution order (first hit wins):
//!   1. `GRAPHBLAS_LIB_DIR` — explicit override, also used by the docs in
//!      `docs/plans/optimisation-phases.md`.
//!   2. `pkg-config --libs-only-L GraphBLAS` (or `graphblas`), which Homebrew and
//!      most distro packages ship.
//!   3. `brew --prefix suite-sparse`/lib on macOS.
//!
//! If none resolve we stay silent: on Debian/Ubuntu `libgraphblas-dev` installs
//! into a directory that is already on the default linker path, so no `-L` is
//! needed and emitting a bogus one would only add noise.
//!
//! The library name itself is declared by `#[link(name = "graphblas")]` in
//! `src/sparse_kernel.rs`; this script deliberately only contributes search paths.

use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=GRAPHBLAS_LIB_DIR");

    // Cargo sets CARGO_FEATURE_<NAME> for every enabled feature.
    if std::env::var_os("CARGO_FEATURE_GRAPHBLAS").is_none() {
        return;
    }

    for dir in graphblas_link_search_paths() {
        println!("cargo:rustc-link-search=native={dir}");
    }
}

fn graphblas_link_search_paths() -> Vec<String> {
    if let Some(dir) = std::env::var_os("GRAPHBLAS_LIB_DIR") {
        let dir = dir.to_string_lossy().into_owned();
        if Path::new(&dir).is_dir() {
            return vec![dir];
        }
        println!("cargo:warning=GRAPHBLAS_LIB_DIR={dir} is not a directory; ignoring it");
    }

    for package in ["GraphBLAS", "graphblas"] {
        let dirs = pkg_config_link_dirs(package);
        if !dirs.is_empty() {
            return dirs;
        }
    }

    if cfg!(target_os = "macos") {
        if let Some(prefix) = command_stdout("brew", &["--prefix", "suite-sparse"]) {
            let lib = Path::new(prefix.trim()).join("lib");
            if lib.is_dir() {
                return vec![lib.to_string_lossy().into_owned()];
            }
        }
    }

    Vec::new()
}

fn pkg_config_link_dirs(package: &str) -> Vec<String> {
    let Some(stdout) = command_stdout("pkg-config", &["--libs-only-L", package]) else {
        return Vec::new();
    };
    stdout
        .split_whitespace()
        .filter_map(|token| token.strip_prefix("-L"))
        .filter(|dir| !dir.is_empty() && Path::new(dir).is_dir())
        .map(str::to_owned)
        .collect()
}

fn command_stdout(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}
