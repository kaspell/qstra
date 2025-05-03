use std::collections::HashMap;
use std::io;


#[derive(Debug)]
pub struct Registry<T> {
        items: Vec<T>,
        items_index: HashMap<Vec<u8>, usize>,
}


impl<T> Registry<T> {
        #[must_use]
        pub fn new_blank() -> Self {
                Self {
                        items: Vec::new(),
                        items_index: HashMap::new(),
                }
        }

        #[inline(always)]
        fn get_idx(&self, id: &[u8]) -> Option<usize> {
                self.items_index.get(id).copied()
        }

        pub fn get(&self, id: &[u8]) -> Option<&T> {
                if let Some(idx) = self.get_idx(id) {
                        return Some(&self.items[idx])
                }
                None
        }

        pub fn get_mut(&mut self, id: &[u8]) -> Option<&mut T> {
                if let Some(idx) = self.get_idx(id) {
                        return Some(&mut self.items[idx])
                }
                None
        }

        pub fn add(&mut self, item: T, id: &[u8]) -> io::Result<()> {
                if self.items_index.contains_key(id) {
                        return Err(io::Error::new(io::ErrorKind::Other, "Registry: add: key already exists"));
                }
                self.items.push(item);
                let key = id.to_vec();
                let val = self.count() - 1;
                self.items_index.insert(key, val);
                Ok(())
        }

        pub fn count(&self) -> usize {
                self.items.len()
        }

        pub fn list(&self) -> &Vec<T> {
                &self.items
        }

        #[expect(dead_code)]
        pub fn list_mut(&mut self) -> &mut Vec<T> {
                &mut self.items
        }

        pub fn clear_state(&mut self) {
                self.items = Vec::new();
                self.items_index = HashMap::new();
        }
}