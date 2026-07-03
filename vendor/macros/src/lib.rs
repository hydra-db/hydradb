//! Procedural macros for OpenData

mod test;

use proc_macro::TokenStream;

/// Attribute macro for test functions that need storage
///
/// Automatically creates SlateDB-backed in-memory storage, passes it to the test,
/// and ensures proper cleanup via `close()`.
///
/// # Basic usage
///
/// ```ignore
/// #[opendata_macros::storage_test]
/// async fn my_test(storage: Arc<dyn Storage>) {
///     // test body
/// }
/// ```
///
/// # With merge operator
///
/// ```ignore
/// #[opendata_macros::storage_test(merge_operator = VectorDbMergeOperator::new(3))]
/// async fn my_test(storage: Arc<dyn Storage>) {
///     // test body
/// }
/// ```
#[proc_macro_attribute]
pub fn storage_test(args: TokenStream, input: TokenStream) -> TokenStream {
    test::storage::test_impl(args.into(), input.into()).into()
}
