//! Build script for `hydradb`.
//!
//! Its only job is to teach the linker where SuiteSparse:GraphBLAS and LAGraph
//! live. Without this, a Homebrew install on macOS fails with
//! `ld: library 'graphblas' not found` unless every developer exports
//! `RUSTFLAGS="-L /opt/homebrew/lib"` by hand.
//!
//! Resolution order for GraphBLAS (first hit wins):
//!   1. `GRAPHBLAS_LIB_DIR` — explicit override.
//!   2. `pkg-config --libs-only-L GraphBLAS` (or `graphblas`), which Homebrew and
//!      most distro packages ship.
//!   3. `brew --prefix suite-sparse`/lib on macOS.
//!
//! LAGraph resolves the same way: `LAGRAPH_LIB_DIR`, then
//! `pkg-config --libs-only-L LAGraph`, then the same Homebrew prefix, which
//! bundles it. No distro packages LAGraph, so Linux sets `LAGRAPH_LIB_DIR` to
//! the prefix `scripts/install_lagraph.sh` installed into.
//!
//! If none resolve we stay silent: on Debian/Ubuntu `libgraphblas-dev` installs
//! into a directory that is already on the default linker path, so no `-L` is
//! needed and emitting a bogus one would only add noise.
//!
//! The library names themselves are declared by `#[link(name = "graphblas")]`
//! in `src/sparse_kernel/graphblas.rs` and `#[link(name = "lagraph")]` in
//! `src/sparse_kernel/lagraph.rs`; this script deliberately only contributes
//! search paths.

use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=GRAPHBLAS_LIB_DIR");
    println!("cargo:rerun-if-env-changed=LAGRAPH_LIB_DIR");

    let mut dirs = link_search_paths("GRAPHBLAS_LIB_DIR", &["GraphBLAS", "graphblas"]);
    for dir in link_search_paths("LAGRAPH_LIB_DIR", &["LAGraph"]) {
        if !dirs.contains(&dir) {
            dirs.push(dir);
        }
    }
    for dir in dirs {
        println!("cargo:rustc-link-search=native={dir}");
    }
}

fn link_search_paths(override_variable: &str, packages: &[&str]) -> Vec<String> {
    if let Some(dir) = std::env::var_os(override_variable) {
        let dir = dir.to_string_lossy().into_owned();
        if Path::new(&dir).is_dir() {
            return vec![dir];
        }
        println!("cargo:warning={override_variable}={dir} is not a directory; ignoring it");
    }

    for package in packages {
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
