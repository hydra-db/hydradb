use slatedb_graph_kernel::{
    object_store_from_env, GraphError, GraphShard, QueryContext, QueryResultSet, QueryValue,
    Result, VertexPropertyValue,
};

#[tokio::main]
async fn main() -> Result<()> {
    let config = QueryConfig::from_args(std::env::args().skip(1).collect())?;
    let object_store = object_store_from_env(config.env_file)?;
    let shard = GraphShard::open(config.db_path.clone(), object_store).await?;
    let rows = shard
        .execute_cypher_rows(
            QueryContext::new(&config.cell_id, "cypher-query-example"),
            &config.query,
        )
        .await?;
    shard.close().await?;
    print_result(&rows);
    Ok(())
}

#[derive(Clone, Debug)]
struct QueryConfig {
    env_file: Option<String>,
    db_path: String,
    cell_id: String,
    query: String,
}

impl QueryConfig {
    fn from_args(args: Vec<String>) -> Result<Self> {
        let mut parser = ArgParser::new(args);
        if parser.flag("--help") || parser.flag("-h") {
            print_usage();
            std::process::exit(0);
        }
        let config = Self {
            env_file: parser.optional("--env-file")?,
            db_path: parser.required("--db-path")?,
            cell_id: parser.required("--cell-id")?,
            query: parser.required("--query")?,
        };
        parser.finish()?;
        Ok(config)
    }
}

struct ArgParser {
    args: Vec<String>,
}

impl ArgParser {
    fn new(args: Vec<String>) -> Self {
        Self { args }
    }

    fn required(&mut self, name: &str) -> Result<String> {
        self.optional(name)?
            .ok_or_else(|| GraphError::UnsupportedQuery {
                dialect: "CypherQuery",
                feature: format!("missing required argument {name}"),
            })
    }

    fn optional(&mut self, name: &str) -> Result<Option<String>> {
        let Some(idx) = self.args.iter().position(|arg| arg == name) else {
            return Ok(None);
        };
        self.args.remove(idx);
        if idx >= self.args.len() || self.args[idx].starts_with('-') {
            return Err(GraphError::UnsupportedQuery {
                dialect: "CypherQuery",
                feature: format!("{name} requires a value"),
            });
        }
        let value = self.args.remove(idx);
        if value.trim().is_empty() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "CypherQuery",
                feature: format!("{name} cannot be empty"),
            });
        }
        Ok(Some(value))
    }

    fn flag(&mut self, name: &str) -> bool {
        match self.args.iter().position(|arg| arg == name) {
            Some(idx) => {
                self.args.remove(idx);
                true
            }
            None => false,
        }
    }

    fn finish(self) -> Result<()> {
        if self.args.is_empty() {
            Ok(())
        } else {
            Err(GraphError::UnsupportedQuery {
                dialect: "CypherQuery",
                feature: format!("unknown arguments: {}", self.args.join(" ")),
            })
        }
    }
}

fn print_result(result: &QueryResultSet) {
    println!("rows={}", result.rows.len());
    println!(
        "{}",
        result
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>()
            .join("\t")
    );
    for row in &result.rows {
        println!(
            "{}",
            row.values
                .iter()
                .map(format_query_value)
                .collect::<Vec<_>>()
                .join("\t")
        );
    }
}

fn format_query_value(value: &QueryValue) -> String {
    match value {
        QueryValue::Null => "null".to_string(),
        QueryValue::VertexId(value) | QueryValue::Count(value) => value.to_string(),
        QueryValue::Bool(value) => value.to_string(),
        QueryValue::Float(value) => format_float(value.0),
        QueryValue::Property(value) => format_property_value(value),
        QueryValue::List(values) => format!(
            "[{}]",
            values
                .iter()
                .map(format_query_value)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        QueryValue::Path(path) => format!(
            "path({})",
            path.nodes
                .iter()
                .map(|node| node.id.to_string())
                .collect::<Vec<_>>()
                .join(" -> ")
        ),
    }
}

fn format_property_value(value: &VertexPropertyValue) -> String {
    match value {
        VertexPropertyValue::Integer(value) => value.to_string(),
        VertexPropertyValue::SignedInteger(value) => value.to_string(),
        VertexPropertyValue::Bool(value) => value.to_string(),
        VertexPropertyValue::Float(value) => format_float(value.0),
        VertexPropertyValue::String(value) => value.clone(),
    }
}

fn format_float(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.1}")
    } else {
        value.to_string()
    }
}

fn print_usage() {
    eprintln!(
        "usage: cargo run --features opencypher --example cypher_query -- \\
         --db-path <graph-db-path> --cell-id <cell> --query <cypher> [--env-file .env]"
    );
}
