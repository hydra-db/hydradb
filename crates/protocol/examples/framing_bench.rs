//! The proxy's per-byte and per-message hot paths, in isolation.
//!
//! - record relay: read framed RECORDs with `MessageReader` and forward each
//!   with `MessageWriter::send`, the work the proxy does on every row of a
//!   result. Input and output are in memory, so this is the CPU cost of
//!   framing alone, without the syscalls a socket adds
//! - RUN routing: `run_extra`, the only decode a unit needs to be routed
//!
//! `cargo run --release -p hydradb-protocol --example framing_bench`

use std::time::Instant;

use boltr::message::encode::encode_client_message;
use boltr::types::{BoltDict, BoltValue};
use bytes::BytesMut;
use hydradb_protocol::bolt::{run_extra, ClientMessage, MessageReader, MessageWriter, ServerMessage};

#[derive(Debug)]
#[allow(dead_code)] // read through Debug
struct RecordRelay {
    row_bytes: usize,
    gib_per_sec: f64,
    ns_per_record: f64,
}

#[derive(Debug)]
#[allow(dead_code)] // read through Debug
struct RunExtra {
    run_bytes: usize,
    ns_per_run: f64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    const RECORDS: usize = 1000;
    for row_bytes in [16usize, 128, 1024] {
        let record = ServerMessage::Record {
            data: vec![BoltValue::Integer(1), BoltValue::String("x".repeat(row_bytes))],
        };
        let mut writer = MessageWriter::new(Vec::new());
        for _ in 0..RECORDS {
            writer.send(&record).await.unwrap();
        }
        writer.flush().await.unwrap();
        let block = writer.get_ref().clone();
        let iterations = 2000;
        let mut out = MessageWriter::new(tokio::io::sink());
        let started = Instant::now();
        let mut records = 0;
        for _ in 0..iterations {
            let mut reader = MessageReader::new(&block[..], usize::MAX);
            for _ in 0..RECORDS {
                out.send(&reader.next().await.unwrap()).await.unwrap();
            }
            out.flush().await.unwrap();
            records += RECORDS;
        }
        let elapsed = started.elapsed().as_secs_f64();
        let result = RecordRelay {
            row_bytes,
            gib_per_sec: (block.len() * iterations) as f64 / elapsed / bytesize::GIB as f64,
            ns_per_record: elapsed * 1e9 / records as f64,
        };
        println!("{result:?}");
    }

    let mut run = BytesMut::new();
    encode_client_message(
        &mut run,
        &ClientMessage::Run {
            query: "UNWIND $rows AS row MERGE (n:Entity {id: row.id}) SET n += row.props".into(),
            parameters: BoltDict::from([(
                "rows".to_string(),
                BoltValue::List(
                    (0..50)
                        .map(|id| {
                            BoltValue::Dict(BoltDict::from([
                                ("id".to_string(), BoltValue::Integer(id)),
                                (
                                    "props".to_string(),
                                    BoltValue::Dict(BoltDict::from([("name".to_string(), BoltValue::String("x".repeat(40)))])),
                                ),
                            ]))
                        })
                        .collect(),
                ),
            )]),
            extra: BoltDict::from([
                ("db".to_string(), BoltValue::String("hydradb.scope1.dGVuYW50LWE._".into())),
                ("mode".to_string(), BoltValue::String("r".into())),
                (
                    "bookmarks".to_string(),
                    BoltValue::List(vec![BoltValue::String("sgk:1:abc".into())]),
                ),
            ]),
        },
    );
    let iterations = 200_000;
    let started = Instant::now();
    for _ in 0..iterations {
        std::hint::black_box(run_extra(&run).unwrap());
    }
    let result = RunExtra {
        run_bytes: run.len(),
        ns_per_run: started.elapsed().as_nanos() as f64 / iterations as f64,
    };
    println!("{result:?}");
}
