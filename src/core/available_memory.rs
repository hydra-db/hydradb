//! The memory this process can still take, read when a computation sizes
//! itself rather than configured: the smaller of a container's cgroup v2
//! limit less its working set and the host's `MemAvailable`, or whichever of
//! the two is readable. `None` wherever neither is, which is every non-Linux
//! target.
//!
//! The working set is kubelet's: `memory.current` less the inactive file
//! cache. `memory.current` counts page cache, which storage reads keep full,
//! and the kernel reclaims inactive cache before it refuses an allocation,
//! so that cache is counted as available. Without a readable
//! `inactive_file`, all of `memory.current` counts as used, which can only
//! understate what is available.

use std::path::{Component, Path, PathBuf};

/// Bytes this process can allocate before its cgroup or the host runs out,
/// read afresh on every call.
#[cfg_attr(not(feature = "opencypher"), allow(dead_code))]
pub(crate) fn available_memory_bytes() -> Option<u64> {
    let cgroup = cgroup_directory().and_then(|directory| {
        let max = std::fs::read_to_string(directory.join("memory.max")).ok()?;
        let current = std::fs::read_to_string(directory.join("memory.current")).ok()?;
        let stat = std::fs::read_to_string(directory.join("memory.stat")).unwrap_or_default();
        cgroup_available(&max, &current, &stat)
    });
    let host = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|meminfo| meminfo_available(&meminfo));
    tightest(cgroup, host)
}

/// The smaller of the two readings that exist. The leaf cgroup's headroom
/// says nothing of a parent cgroup's or the host's, which can run out first.
#[cfg_attr(not(feature = "opencypher"), allow(dead_code))]
fn tightest(cgroup: Option<u64>, host: Option<u64>) -> Option<u64> {
    cgroup.into_iter().chain(host).min()
}

/// `memory.max` less the working set, `memory.current` less `memory.stat`'s
/// `inactive_file`, or `None` when `memory.max` is `max`, which means the
/// cgroup sets no limit.
#[cfg_attr(not(feature = "opencypher"), allow(dead_code))]
fn cgroup_available(max: &str, current: &str, stat: &str) -> Option<u64> {
    let max = max.trim().parse::<u64>().ok()?;
    let current = current.trim().parse::<u64>().ok()?;
    let inactive_file = stat.lines().find_map(|line| {
        let mut words = line.split_ascii_whitespace();
        (words.next()? == "inactive_file")
            .then(|| words.next()?.parse::<u64>().ok())
            .flatten()
    });
    Some(max.saturating_sub(current.saturating_sub(inactive_file.unwrap_or(0))))
}

/// The `MemAvailable:` row of `/proc/meminfo`, which the kernel reports in
/// KiB.
#[cfg_attr(not(feature = "opencypher"), allow(dead_code))]
fn meminfo_available(meminfo: &str) -> Option<u64> {
    let row = meminfo.lines().find_map(|line| line.strip_prefix("MemAvailable:"))?;
    let kib = row.trim().strip_suffix("kB")?.trim().parse::<u64>().ok()?;
    kib.checked_mul(1024)
}

/// This process's cgroup v2 directory, or `None` without cgroup v2.
pub fn cgroup_directory() -> Option<PathBuf> {
    let membership = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let mounts = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    resolve_cgroup_directory(&membership, &mounts)
}

/// Resolve cgroup v2 in both private container and host cgroup namespaces.
/// Mountinfo paths are kernel-escaped; unsupported escapes fail closed.
fn resolve_cgroup_directory(membership: &str, mounts: &str) -> Option<PathBuf> {
    let path = membership.lines().find_map(|line| line.strip_prefix("0::"))?;
    if !path.starts_with('/') || Path::new(path).components().any(|p| matches!(p, Component::ParentDir)) {
        return None;
    }
    for line in mounts.lines() {
        let (left, right) = line.split_once(" - ")?;
        if right.split_ascii_whitespace().next() != Some("cgroup2") {
            continue;
        }
        let parts: Vec<_> = left.split_ascii_whitespace().collect();
        let root = *parts.get(3)?;
        let mount = *parts.get(4)?;
        if root.contains('\\') || mount.contains('\\') {
            continue;
        }
        if let Ok(relative) = Path::new(path).strip_prefix(root) {
            return Some(Path::new(mount).join(relative));
        }
    }
    None
}
