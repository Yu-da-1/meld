//! A map that remembers which keys were changed since the last flush.
//!
//! Persistence needs to know what to write. Tracking it in the container,
//! rather than at every call site, means a new mutation cannot forget to mark
//! its record: anything that can change a value goes through `get_mut`,
//! `insert`, `remove` or `entry`, and all of them mark the key.

use std::{
    collections::{BTreeMap, BTreeSet, btree_map},
    ops::Deref,
};

#[derive(Debug)]
pub struct Tracked<K: Ord + Copy, V> {
    map: BTreeMap<K, V>,
    dirty: BTreeSet<K>,
}

impl<K: Ord + Copy, V> Default for Tracked<K, V> {
    fn default() -> Self {
        Self {
            map: BTreeMap::new(),
            dirty: BTreeSet::new(),
        }
    }
}

/// Reads go straight to the map; only mutation is intercepted.
impl<K: Ord + Copy, V> Deref for Tracked<K, V> {
    type Target = BTreeMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<K: Ord + Copy, V> Tracked<K, V> {
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let value = self.map.get_mut(key)?;
        self.dirty.insert(*key);
        Some(value)
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.dirty.insert(key);
        self.map.insert(key, value)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let removed = self.map.remove(key);
        if removed.is_some() {
            self.dirty.insert(*key);
        }
        removed
    }

    /// Marks the key even if the entry ends up unchanged; a redundant write is harmless.
    pub fn entry(&mut self, key: K) -> btree_map::Entry<'_, K, V> {
        self.dirty.insert(key);
        self.map.entry(key)
    }

    /// Adds a value read from storage without marking it for writing.
    pub fn load(&mut self, key: K, value: V) {
        self.map.insert(key, value);
    }

    /// Keys changed since the last [`Self::clear_dirty`], in key order.
    ///
    /// A key with no value was removed.
    pub fn dirty(&self) -> impl Iterator<Item = (K, Option<&V>)> {
        self.dirty.iter().map(|key| (*key, self.map.get(key)))
    }

    pub fn clear_dirty(&mut self) {
        self.dirty.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutations_mark_keys_and_reads_do_not() {
        let mut map = Tracked::<u8, &str>::default();
        map.load(1, "loaded");
        assert_eq!(map.dirty().count(), 0);

        assert_eq!(map.get(&1), Some(&"loaded"));
        assert_eq!(map.dirty().count(), 0);

        map.insert(2, "new");
        *map.get_mut(&1).expect("present") = "changed";
        assert_eq!(
            map.dirty().collect::<Vec<_>>(),
            vec![(1, Some(&"changed")), (2, Some(&"new"))]
        );
    }

    #[test]
    fn removed_keys_are_reported_without_a_value() {
        let mut map = Tracked::<u8, &str>::default();
        map.load(1, "x");

        map.remove(&1);

        assert_eq!(map.dirty().collect::<Vec<_>>(), vec![(1, None)]);
    }

    #[test]
    fn missing_keys_are_not_marked() {
        let mut map = Tracked::<u8, &str>::default();

        assert!(map.get_mut(&9).is_none());
        map.remove(&9);

        assert_eq!(map.dirty().count(), 0);
    }
}
