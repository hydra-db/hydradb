# Macros

Macros for OpenData test automation and setup simplification.

## Architecture

Provides attribute macros to reduce boilerplate in storage-backed tests across all database crates.

### Key Concepts

- **Attribute Macro**: `#[storage_test]` transforms test functions to handle storage lifecycle
- **Arc<dyn Storage>**: Automatically creates in-memory SlateDB-backed storage
- **Merge Operators**: Optional parameter for custom merge operator specification
- **Async Support**: Leverages `tokio::test` under the hood

### Key Modules

- `src/test/` - Test-related macros
- `src/test/storage.rs` - `#[storage_test]` attribute macro implementation
  - `TestMacroArgs` - Argument parser supporting flexible merge operator expressions
  - `test_impl` - Core macro expansion logic

## Usage Pattern

### Basic Test

```rust
#[storage_test]
async fn my_test(storage: Arc<dyn Storage>) {
    // storage is automatically created and cleaned up
}
```

### With Merge Operator

```rust
#[storage_test(merge_operator = OpenTsdbMergeOperator)]
async fn my_test(storage: Arc<dyn Storage>) {
    // storage uses custom merge operator
}
```

## Cross-References

- Used by `timeseries`, `vector`, and other database crates for test setup
- Integrates with `common::Storage` trait for SlateDB abstraction
- Reduces test boilerplate across all database implementations
