//! A record's properties, compact enough to hold millions of records at once.
//!
//! A graph names few properties but holds millions of records, each with a
//! handful of them. A `BTreeMap<String, _>` gives every record a node sized
//! for eleven entries and every key its own string: on a production graph of
//! 8.5 million records that was 750 bytes a record before any value. Here a
//! record's properties are one exact-size vector sorted by name, and a name
//! is an interned `Arc<str>` shared by every record that carries it.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::Arc;

use serde::de::{Deserializer, MapAccess, Visitor};
use serde::ser::{SerializeMap, Serializer};
use serde::{Deserialize, Serialize};

use crate::PropertyValue;

/// The shared copy of a property name.
pub fn intern(name: &str) -> Arc<str> {
    thread_local! {
        static NAMES: RefCell<HashSet<Arc<str>>> = RefCell::new(HashSet::new());
    }
    NAMES.with(|names| {
        let mut names = names.borrow_mut();
        if let Some(interned) = names.get(name) {
            return Arc::clone(interned);
        }
        let interned: Arc<str> = Arc::from(name);
        names.insert(Arc::clone(&interned));
        interned
    })
}

/// Properties sorted by name, each name once. Serialized as the map it
/// represents, byte for byte as a `BTreeMap<String, PropertyValue>`.
#[derive(Clone, Default, PartialEq)]
pub struct Properties(Vec<(Arc<str>, PropertyValue)>);

impl Properties {
    pub fn new() -> Self {
        Self::default()
    }

    /// Properties from entries in any order, or the name that repeats.
    pub fn from_entries(mut entries: Vec<(Arc<str>, PropertyValue)>) -> Result<Self, Arc<str>> {
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        if let Some(pair) = entries.windows(2).find(|pair| pair[0].0 == pair[1].0) {
            return Err(Arc::clone(&pair[0].0));
        }
        entries.shrink_to_fit();
        Ok(Self(entries))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get(&self, name: &str) -> Option<&PropertyValue> {
        self.position(name).ok().map(|index| &self.0[index].1)
    }

    pub fn contains_key(&self, name: &str) -> bool {
        self.position(name).is_ok()
    }

    /// Set a property, returning the value it replaces. The vector grows by
    /// exactly the one entry, so a record holds no spare capacity.
    pub fn insert(&mut self, name: &str, value: PropertyValue) -> Option<PropertyValue> {
        match self.position(name) {
            Ok(index) => Some(std::mem::replace(&mut self.0[index].1, value)),
            Err(index) => {
                self.0.reserve_exact(1);
                self.0.insert(index, (intern(name), value));
                None
            }
        }
    }

    pub fn remove(&mut self, name: &str) -> Option<PropertyValue> {
        let index = self.position(name).ok()?;
        let (_, value) = self.0.remove(index);
        self.0.shrink_to_fit();
        Some(value)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &PropertyValue)> {
        self.0.iter().map(|(name, value)| (&**name, value))
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(|(name, _)| &**name)
    }

    fn position(&self, name: &str) -> Result<usize, usize> {
        self.0.binary_search_by(|(entry, _)| (**entry).cmp(name))
    }
}

impl IntoIterator for Properties {
    type Item = (Arc<str>, PropertyValue);
    type IntoIter = std::vec::IntoIter<(Arc<str>, PropertyValue)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Properties {
    type Item = (&'a str, &'a PropertyValue);
    type IntoIter =
        std::iter::Map<std::slice::Iter<'a, (Arc<str>, PropertyValue)>, fn(&'a (Arc<str>, PropertyValue)) -> (&'a str, &'a PropertyValue)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter().map(|(name, value)| (&**name, value))
    }
}

/// Later entries for a name replace earlier ones, as a map's do.
impl FromIterator<(String, PropertyValue)> for Properties {
    fn from_iter<I: IntoIterator<Item = (String, PropertyValue)>>(entries: I) -> Self {
        let mut properties = Self::new();
        for (name, value) in entries {
            properties.insert(&name, value);
        }
        properties
    }
}

impl From<BTreeMap<String, PropertyValue>> for Properties {
    fn from(map: BTreeMap<String, PropertyValue>) -> Self {
        Self(map.into_iter().map(|(name, value)| (intern(&name), value)).collect())
    }
}

impl fmt::Debug for Properties {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_map().entries(self.iter()).finish()
    }
}

impl Serialize for Properties {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, value) in &self.0 {
            map.serialize_entry(&**name, value)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Properties {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct PropertiesVisitor;

        impl<'de> Visitor<'de> for PropertiesVisitor {
            type Value = Properties;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a map of property names to values")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Properties, A::Error> {
                let mut entries = Vec::with_capacity(access.size_hint().unwrap_or(0));
                while let Some((name, value)) = access.next_entry::<std::borrow::Cow<'de, str>, PropertyValue>()? {
                    entries.push((intern(&name), value));
                }
                Properties::from_entries(entries).map_err(|name| serde::de::Error::custom(format!("property {name} repeats")))
            }
        }

        deserializer.deserialize_map(PropertiesVisitor)
    }
}
