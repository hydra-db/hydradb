#!/usr/bin/env bash
# Build LAGraph as a static archive against the system GraphBLAS and install
# only `lib/liblagraph.a` and `include/LAGraph.h` under the given prefix.
#
# Linux only. Ubuntu 24.04 packages no LAGraph and ships GraphBLAS 7.4, while
# LAGraph 1.2 needs GraphBLAS 9, so this pins the newest 1.0.x tag that builds
# against 7.4. macOS uses Homebrew's suite-sparse, which bundles LAGraph 1.2,
# and does not run this. `src/sparse_kernel/lagraph.rs` calls only entry points
# whose signatures are identical in both.
#
# Static on purpose: the archive is linked into graph-node and graph-indexer,
# so the runtime image and release artifacts gain no new shared library.
# Point `LAGRAPH_LIB_DIR` at `<prefix>/lib` for `build.rs` to find it.
set -euo pipefail

prefix="${1:?usage: install_lagraph.sh <prefix>}"

version=1.0.2
sha256=c94fbba4919d466f8d3ce888827f0be6cce57c0eed7366b3710246e2ef68ba9c
url="https://github.com/GraphBLAS/LAGraph/archive/refs/tags/v${version}.tar.gz"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

curl --proto '=https' --tlsv1.2 -fsSL --retry 3 -o "$work/lagraph.tar.gz" "$url"
echo "${sha256}  $work/lagraph.tar.gz" | sha256sum -c -
tar -xzf "$work/lagraph.tar.gz" -C "$work"

# The 1.0.x `lagraph_static` target pins POSITION_INDEPENDENT_CODE OFF on
# itself, which overrides CMAKE_POSITION_INDEPENDENT_CODE, so `-fPIC` also goes
# in through the flags. Rust links PIE executables and needs it either way.
cmake -S "$work/LAGraph-${version}" -B "$work/build" \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SHARED_LIBS=OFF \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DCMAKE_C_FLAGS=-fPIC
cmake --build "$work/build" --target lagraph_static -j "$(nproc)"

install -d "$prefix/lib" "$prefix/include"
install -m 0644 "$work/build/liblagraph.a" "$prefix/lib/liblagraph.a"
install -m 0644 "$work/LAGraph-${version}/include/LAGraph.h" "$prefix/include/LAGraph.h"
