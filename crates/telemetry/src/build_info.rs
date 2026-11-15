//! The commit this binary was built from, embedded at compile time.
//!
//! Answers one question: *which source produced the process I am looking at?*
//! A pod's image tag answers a near-miss version of it — which image was
//! pulled — and the two diverge for any build that did not come from CI, which
//! is every build a developer runs.
//!
//! Every value here comes from `build.rs` in this crate. It never fails: a
//! field it cannot resolve is the string `"unknown"`, which is a thing a reader
//! can see and act on, unlike a build that refuses to compile on a machine
//! without `git`.
//!
//! The reason this is worth a module rather than one `env!` at a use site is
//! [`BuildInfo::version_string`]. It feeds [`crate::TelemetryConfig`]'s
//! `service_version`, which `layers.rs` stamps on **every** log line as
//! `version` — so setting it once here is what puts the commit on all of a
//! deployment's logs rather than only on a startup banner.

/// Where a binary came from.
///
/// Every field is a `&'static str` baked into the executable, so reading this
/// costs nothing and cannot be reconfigured at runtime — which is the point.
/// An operator-settable build stamp is a build stamp that can lie.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BuildInfo {
    /// Full 40-character commit sha, or `"unknown"`.
    pub commit: &'static str,
    /// First 12 characters of [`Self::commit`], or `"unknown"`.
    pub commit_short: &'static str,
    /// Branch the build was made from, or `"unknown"`. `"HEAD"` for the
    /// detached checkout CI produces when building a tag.
    pub branch: &'static str,
    /// Whether tracked files had uncommitted changes at build time.
    pub dirty: bool,
    /// Build time, RFC 3339 UTC.
    pub built_at: &'static str,
    /// `CARGO_PKG_VERSION` of this crate.
    pub package_version: &'static str,
    /// The compiler that built it, e.g. `1.91.0`.
    pub rustc: &'static str,
}

/// This binary's provenance.
pub const BUILD_INFO: BuildInfo = BuildInfo {
    commit: env!("HYDRADB_GIT_SHA"),
    commit_short: env!("HYDRADB_GIT_SHA_SHORT"),
    branch: env!("HYDRADB_GIT_BRANCH"),
    // `env!` is a string even for a boolean, and a `const` cannot call
    // `parse`; matching on the bytes keeps the whole struct a compile-time
    // constant rather than a `OnceLock`.
    dirty: matches!(env!("HYDRADB_GIT_DIRTY").as_bytes(), b"true"),
    built_at: env!("HYDRADB_BUILD_TIMESTAMP"),
    package_version: env!("CARGO_PKG_VERSION"),
    rustc: env!("HYDRADB_RUSTC_VERSION"),
};

/// The sentinel a `build.rs` field falls back to.
const UNKNOWN: &str = "unknown";

impl BuildInfo {
    /// `0.1.0+a1b2c3d4e5f6`, or `0.1.0+a1b2c3d4e5f6.dirty`.
    ///
    /// Semver build metadata, so the string stays a legal version and anything
    /// that parses versions keeps working. It degrades to the bare package
    /// version when the sha is unknown rather than emitting `0.1.0+unknown`:
    /// the second form reads like a commit named "unknown" and would become a
    /// distinct series in every log and metric query.
    pub fn version_string(&self) -> String {
        if self.commit_short == UNKNOWN {
            return self.package_version.to_string();
        }
        let dirty = if self.dirty { ".dirty" } else { "" };
        format!("{}+{}{dirty}", self.package_version, self.commit_short)
    }

    /// Whether the commit resolved at build time.
    ///
    /// False for a `cargo build` in a tree with no `git`, and for an image
    /// built without the `GIT_SHA` build-arg — the case worth warning about,
    /// since it is invisible otherwise.
    pub fn is_known(&self) -> bool {
        self.commit != UNKNOWN
    }

    /// The multi-line block `--version` prints.
    pub fn long_version(&self) -> String {
        let dirty = if self.dirty { " (dirty)" } else { "" };
        format!(
            "{version}\ncommit:   {commit}{dirty}\nbranch:   {branch}\nbuilt:    {built}\nrustc:    {rustc}",
            version = self.version_string(),
            commit = self.commit,
            branch = self.branch,
            built = self.built_at,
            rustc = self.rustc,
        )
    }
}

/// The `graph_build_info` series, as Prometheus text exposition.
///
/// The standard build-info shape: a gauge fixed at `1` whose *labels* are the
/// payload. Nothing reads the value; a dashboard joins on the labels
/// (`… * on(instance) group_left(commit) graph_build_info`) to colour a panel
/// by commit, and an alert on `changes(...)` catches a rollout.
///
/// Rendered here rather than in each binary so the two endpoints cannot drift
/// in label spelling — a build-info series is only useful if every service
/// reports it identically — and so the escaping below is written once.
///
/// `rustc` and the build timestamp are deliberately absent. Every label
/// multiplies the series, and neither is something anyone alerts on; both are
/// on the startup line and in `--version`, which is where they belong.
pub fn prometheus_gauge() -> String {
    let build = BUILD_INFO;
    format!(
        "# TYPE graph_build_info gauge\ngraph_build_info{{version=\"{}\",commit=\"{}\",branch=\"{}\",dirty=\"{}\"}} 1\n",
        escape_label(&build.version_string()),
        escape_label(build.commit),
        escape_label(build.branch),
        build.dirty,
    )
}

/// Escape a Prometheus label value: backslash, double quote, newline.
///
/// A branch name cannot contain any of the three — `git check-ref-format`
/// rejects them — so this never fires in practice. It is here because the
/// values also come from `GIT_BRANCH` and `GIT_SHA` in the environment, which
/// nothing validates, and an unescaped quote there would not corrupt one label
/// but break the parse of the whole scrape.
fn escape_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

/// Whether the process was invoked to report its build rather than to run.
///
/// Hand-rolled because neither binary takes arguments — every knob is an
/// environment variable — and a full argument parser for one flag would be the
/// first dependency in a composition root that has none.
///
/// Both binaries call this as the first statement in `main`, ahead of the
/// subscriber: `--version` is asked from a shell, usually as
/// `docker run --rm <image> --version` against an image nobody can otherwise
/// identify, and the answer has to be one plain block on stdout rather than a
/// JSON log line wrapped around it.
pub fn version_flag_requested() -> bool {
    std::env::args().skip(1).any(|arg| arg == "--version" || arg == "-V")
}

/// Log the provenance of the running binary. The first line a process emits.
///
/// Called once, immediately after the subscriber exists, so a pod's logs open
/// by naming the source they came from. It is deliberately *not* the only place
/// the commit appears — [`BuildInfo::version_string`] puts it on every
/// subsequent line as `version` — and exists for the reader scrolling to the
/// top of a restart rather than for a query.
///
/// A build with no commit is a warning rather than an info line. It means the
/// image was built without the `GIT_SHA` build-arg, and since `.dockerignore`
/// excludes `.git` there is no second source to fall back to. The failure is
/// otherwise entirely silent: the process runs perfectly while being
/// untraceable to a source tree, which is the one state this module exists to
/// prevent.
pub fn log() {
    let build = BUILD_INFO;
    if !build.is_known() {
        tracing::warn!(
            build_timestamp = build.built_at,
            build_rustc = build.rustc,
            "build provenance is unknown; this binary cannot be traced to a commit"
        );
        return;
    }
    tracing::info!(
        build_commit = build.commit,
        build_branch = build.branch,
        build_dirty = build.dirty,
        build_timestamp = build.built_at,
        build_rustc = build.rustc,
        "build info"
    );
}
