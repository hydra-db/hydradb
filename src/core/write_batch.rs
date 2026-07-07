use slatedb::bytes::Bytes;

use crate::GraphWriteOp;

#[derive(Clone, Debug, Default)]
pub(crate) struct GraphWriteBatch {
    pub(crate) ops: Vec<GraphWriteOp>,
}

impl GraphWriteBatch {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn put<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.ops.push(GraphWriteOp::Put(
            Bytes::copy_from_slice(key.as_ref()),
            Bytes::copy_from_slice(value.as_ref()),
        ));
    }

    pub(crate) fn delete<K>(&mut self, key: K)
    where
        K: AsRef<[u8]>,
    {
        self.ops
            .push(GraphWriteOp::Delete(Bytes::copy_from_slice(key.as_ref())));
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.ops.len()
    }
}
