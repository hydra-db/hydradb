# Common Library

Shared utilities and abstractions used by all OpenData database implementations.

## Modules

### `src/serde/` - Serialization Utilities

Shared encoding/decoding patterns used across all databases:

| Module | Description |
|--------|-------------|
| `key_prefix.rs` | Standard 2-byte key prefix (subsystem + version) — see RFC 0001 |
| `record_tag.rs` | Optional 4+4 bit tag-byte encoding (record type + reserved); subsystems choose where to place it |
| `terminated_bytes.rs` | Variable-length byte encoding with 0x00 terminator |
| `sortable.rs` | Lexicographically sortable numeric encodings (sign-bit flip + big-endian) |
| `encoding.rs` | Common value encodings (Utf8, Array, etc.) |
| `varint.rs` | Variable-length integer encoding |
| `seq_block.rs` | SeqBlock record for sequence allocation |

**Encoding Conventions**:
- Keys: Big-endian for lexicographic ordering
- Values: Little-endian for performance

### `src/storage/` - Storage Abstraction

| Module | Description |
|--------|-------------|
| `mod.rs` | `Storage` trait wrapping SlateDB operations |
| `config.rs` | `StorageConfig` for backend configuration |
| `slate.rs` | SlateDB implementation of Storage trait |
| `in_memory.rs` | In-memory implementation for testing |
| `loader.rs` | Record loading utilities |
| `factory.rs` | Storage factory for creating backends |

### `src/sequence.rs` - Sequence Allocation

Block-based sequence allocator for crash-safe ID generation:
- Pre-allocates blocks of IDs
- Persists allocation state via SeqBlock
- On crash recovery, skips to next block (gaps but monotonic)

Used by: `vector` (internal vector IDs), `log` (sequence numbers)

### Other Modules

- `src/bytes.rs` - BytesRange utilities
- `src/clock.rs` - Clock abstraction for testing

## Usage Pattern

All databases follow this pattern for record types:

```rust
// In database crate's src/serde/
use common::serde::{key_prefix, terminated_bytes, encoding};

// Key: subsystem | version | ... subsystem-defined fields (big-endian)
// Value: ... (little-endian)
```

## When to Modify Common

Add to common when:
- Utility is needed by 2+ database crates
- Pattern is documented in multiple RFCs

Keep in database crate when:
- Logic is specific to that database's data model
- No clear reuse path
