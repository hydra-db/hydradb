//! Deterministic serialization of stage-level observations.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Pass,
    Fail,
    Blocked,
    NotApplicable,
    Unsupported,
    Skipped,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Record {
    pub case_id: String,
    pub engine: String,
    pub step: usize,
    pub layer: String,
    pub verdict: Verdict,
    pub evidence: serde_json::Value,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Report {
    pub schema_version: u32,
    pub corpus_revision: String,
    pub repository_revision: String,
    pub records: Vec<Record>,
}
impl Report {
    pub fn canonicalize(&mut self) {
        self.records
            .sort_by(|a, b| (&a.case_id, &a.engine, a.step, &a.layer).cmp(&(&b.case_id, &b.engine, b.step, &b.layer)));
    }
    pub fn json(&self) -> Result<String, serde_json::Error> {
        let mut r = self.clone();
        r.canonicalize();
        serde_json::to_string(&r)
    }
    pub fn jsonl(&self) -> Result<String, serde_json::Error> {
        let mut r = self.clone();
        r.canonicalize();
        let mut out = String::new();
        for record in r.records {
            out.push_str(&serde_json::to_string(&record)?);
            out.push('\n');
        }
        Ok(out)
    }
}

fn xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
impl Report {
    /// One JUnit testcase per level, retaining failures as failures.
    pub fn junit(&self) -> String {
        let mut r = self.clone();
        r.canonicalize();
        let failures = r.records.iter().filter(|x| x.verdict == Verdict::Fail).count();
        let skipped = r
            .records
            .iter()
            .filter(|x| !matches!(x.verdict, Verdict::Pass | Verdict::Fail))
            .count();
        let mut out = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<testsuite tests=\"{}\" failures=\"{}\" skipped=\"{}\">\n",
            r.records.len(),
            failures,
            skipped
        );
        for record in r.records {
            out.push_str(&format!(
                "  <testcase classname=\"{}\" name=\"{}\">",
                xml(&record.engine),
                xml(&format!("{}:{}:{}", record.case_id, record.step, record.layer))
            ));
            let evidence = xml(&record.evidence.to_string());
            match record.verdict {
                Verdict::Fail => out.push_str(&format!("<failure>{evidence}</failure>")),
                Verdict::Pass => {}
                _ => out.push_str(&format!("<skipped message=\"{:?}\">{evidence}</skipped>", record.verdict)),
            }
            out.push_str("</testcase>\n");
        }
        out.push_str("</testsuite>\n");
        out
    }
    pub fn summary(&self) -> String {
        let pass = self.records.iter().filter(|r| r.verdict == Verdict::Pass).count();
        let fail = self.records.iter().filter(|r| r.verdict == Verdict::Fail).count();
        format!(
            "{} verdicts: {} pass, {} fail, {} other",
            self.records.len(),
            pass,
            fail,
            self.records.len() - pass - fail
        )
    }
}
