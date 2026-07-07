use std::path::PathBuf;

use slatedb_graph_kernel::parse_opencypher_tck_corpus_dir;

fn main() {
    let Some(root) = std::env::args_os().nth(1).map(PathBuf::from) else {
        eprintln!("usage: opencypher_tck_report <openCypher/tck/features>");
        std::process::exit(2);
    };

    match parse_opencypher_tck_corpus_dir(&root) {
        Ok(corpus) => {
            let report = corpus.compatibility_report();
            println!(
                "{{\"total_scenarios\":{},\"runnable_scenarios\":{},\"skipped_scenarios\":{},\"skipped\":[{}]}}",
                report.total_scenarios,
                report.runnable_scenarios,
                report.skipped_scenarios,
                report
                    .skipped
                    .iter()
                    .map(|item| format!("\"{}\"", json_escape(item)))
                    .collect::<Vec<_>>()
                    .join(",")
            );
        }
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}

fn json_escape(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out
}
