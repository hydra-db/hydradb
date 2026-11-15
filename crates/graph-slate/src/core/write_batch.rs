use slatedb::bytes::Bytes;

use crate::GraphWriteOp;

#[derive(Clone, Debug)]
pub enum GraphWriteGuard {
    Absent(Bytes),
    Equals(Bytes, Bytes),
}

impl GraphWriteGuard {
    pub fn absent<K>(key: K) -> Self
    where
        K: AsRef<[u8]>,
    {
        Self::Absent(Bytes::copy_from_slice(key.as_ref()))
    }

    pub fn equals<K, V>(key: K, value: V) -> Self
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        Self::Equals(Bytes::copy_from_slice(key.as_ref()), Bytes::copy_from_slice(value.as_ref()))
    }
}

#[derive(Clone, Debug, Default)]
pub struct GraphWriteBatch {
    pub ops: Vec<GraphWriteOp>,
}

impl GraphWriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.ops.push(GraphWriteOp::Put(
            Bytes::copy_from_slice(key.as_ref()),
            Bytes::copy_from_slice(value.as_ref()),
        ));
    }

    pub fn delete<K>(&mut self, key: K)
    where
        K: AsRef<[u8]>,
    {
        self.ops.push(GraphWriteOp::Delete(Bytes::copy_from_slice(key.as_ref())));
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }
}
