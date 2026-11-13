use std::{env, error::Error, fs, io, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("set by Cargo"));
    let source_grammar_dir = manifest_dir.join("../../query-language/cypher/grammar");
    let overlay_dir = manifest_dir.join("../../query-language/gql/overlays");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("set by Cargo"));
    let grammar_dir = out_dir.join("grammar");
    let generated_dir = out_dir.join("generated");

    fs::create_dir_all(&grammar_dir)?;
    fs::create_dir_all(&generated_dir)?;

    let lexer_path = source_grammar_dir.join("Cypher25Lexer.g4");
    let parser_path = source_grammar_dir.join("Cypher25Parser.g4");
    let lexer_overlay_path = overlay_dir.join("lexer.overlay");
    let parser_overlay_path = overlay_dir.join("parser.overlay");
    for path in [&lexer_path, &parser_path, &lexer_overlay_path, &parser_overlay_path] {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    // `METADATA` is both a Neo4j keyword token and the name of ophi's emitted
    // `GrammarMetadata` static. Rename the staged token, not its matched text,
    // to keep those Rust values out of the same namespace.
    let lexer_grammar = fs::read_to_string(&lexer_path)?.replace("METADATA", "METADATA_KW");
    // GQL syntax Neo4j's grammar lacks, added to the staged copy only.
    let lexer_grammar = apply_overlay(&lexer_grammar, &fs::read_to_string(&lexer_overlay_path)?, "lexer.overlay")?;
    fs::write(grammar_dir.join("Cypher25Lexer.g4"), lexer_grammar)?;

    // Neo4j's grammar carries semantic type hints as ANTLR rule arguments,
    // for example `parameter["INTEGER"]`. They do not change recognition and
    // are consumed only by Neo4j's later AST builder. ophi's Rust generator
    // intentionally accepts a smaller, target-independent argument subset, so
    // strip exactly these unused hints in the generated input. The vendored
    // source grammar remains byte-for-byte untouched.
    let mut parser_grammar = fs::read_to_string(&parser_path)?.replace("METADATA", "METADATA_KW");
    for (from, to) in [
        ("parameter[String paramType]", "parameter"),
        ("parameterName[String paramType]", "parameterName"),
        ("parameterName[paramType]", "parameterName"),
        ("parameter[\"ANY\"]", "parameter"),
        ("parameter[\"INTEGER\"]", "parameter"),
        ("parameter[\"STRING\"]", "parameter"),
        ("parameter[\"MAP\"]", "parameter"),
    ] {
        parser_grammar = parser_grammar.replace(from, to);
    }
    if parser_grammar.contains("paramType") || parser_grammar.contains("parameter[\"") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Cypher grammar added an unsupported parameter type hint; update the explicit sanitizer",
        )
        .into());
    }
    let parser_grammar = apply_overlay(&parser_grammar, &fs::read_to_string(&parser_overlay_path)?, "parser.overlay")?;
    fs::write(grammar_dir.join("Cypher25Parser.g4"), parser_grammar)?;

    // The source grammars are watched above. Do not watch their sanitized
    // OUT_DIR copies: rewriting those inputs would invalidate every no-op build.
    antlr_rust_codegen::Builder::new()
        .grammar(grammar_dir.join("Cypher25Lexer.g4"))
        .grammar(grammar_dir.join("Cypher25Parser.g4"))
        .library_directory(&grammar_dir)
        .out_dir(generated_dir)
        .generate()?;

    Ok(())
}

/// Applies a GQL overlay (`query-language/gql/overlays/*.overlay`) to a staged grammar. Each
/// item starts with a directive line and runs to the next one:
///
/// - `//@ replace RULE`: the item is RULE's new definition.
/// - `//@ extend RULE`: the item is more alternatives, added before RULE's `;`.
/// - `//@ insert-after RULE`: the item is new rules, placed after RULE.
/// - `//@ move-after RULE`: the item names one existing rule to move after RULE.
///
/// A directive whose rule the grammar lacks is an error, so an upgrade of the
/// vendored grammar that moves a rule fails the build instead of quietly
/// dropping GQL syntax.
fn apply_overlay(grammar: &str, overlay: &str, name: &str) -> Result<String, Box<dyn Error>> {
    let invalid = |message: String| io::Error::new(io::ErrorKind::InvalidData, format!("{name}: {message}"));
    let mut lines: Vec<String> = grammar.lines().map(str::to_string).collect();
    let mut items: Vec<(String, String, Vec<String>)> = Vec::new();
    for line in overlay.lines() {
        if let Some(directive) = line.strip_prefix("//@ ") {
            let mut words = directive.split_whitespace();
            let (Some(action), Some(rule), None) = (words.next(), words.next(), words.next()) else {
                return Err(invalid(format!("malformed directive `{line}`")).into());
            };
            items.push((action.to_string(), rule.to_string(), Vec::new()));
        } else if let Some((_, _, body)) = items.last_mut() {
            body.push(line.to_string());
        }
    }
    for (action, rule, mut body) in items {
        // An item's trailing comments and blank lines introduce the next one.
        while body
            .last()
            .is_some_and(|line| line.trim().is_empty() || line.trim_start().starts_with("//"))
        {
            body.pop();
        }
        let (start, end) = rule_span(&lines, &rule).ok_or_else(|| invalid(format!("the grammar has no rule `{rule}`")))?;
        match action.as_str() {
            "replace" => {
                if body
                    .iter()
                    .find(|line| !line.trim().is_empty())
                    .map(|line| line.trim().strip_prefix("fragment ").unwrap_or(line.trim()))
                    != Some(rule.as_str())
                {
                    return Err(invalid(format!("`replace {rule}` must define `{rule}`")).into());
                }
                lines.splice(start..=end, body);
            }
            "extend" => {
                lines.splice(end..end, body);
            }
            "insert-after" => {
                let mut inserted = vec![String::new()];
                inserted.extend(body);
                lines.splice(end + 1..end + 1, inserted);
            }
            "move-after" => {
                let moved = body.iter().filter(|line| !line.trim().is_empty()).collect::<Vec<_>>();
                let [moved] = moved.as_slice() else {
                    return Err(invalid("move-after must name one rule".into()).into());
                };
                let (moved_start, moved_end) =
                    rule_span(&lines, moved.trim()).ok_or_else(|| invalid(format!("the grammar has no rule `{moved}`")))?;
                let moved_body = lines.drain(moved_start..=moved_end).collect::<Vec<_>>();
                let (_, end) = rule_span(&lines, &rule).ok_or_else(|| invalid(format!("cannot move `{rule}` after itself")))?;
                lines.splice(end + 1..end + 1, moved_body);
            }
            other => return Err(invalid(format!("unknown directive `{other}`")).into()),
        }
    }
    let mut staged = lines.join("\n");
    staged.push('\n');
    Ok(staged)
}

/// The lines a rule spans: from the line that names it at the left margin
/// (after `fragment`, for a lexer fragment) to the line holding its `;`.
fn rule_span(lines: &[String], rule: &str) -> Option<(usize, usize)> {
    let start = lines.iter().position(|line| {
        let name = line.strip_prefix("fragment ").unwrap_or(line);
        name.strip_prefix(rule)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with([' ', ':', '\t']))
    })?;
    let end = start + lines[start..].iter().position(|line| line.trim() == ";")?;
    Some((start, end))
}
