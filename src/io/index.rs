use crate::io::{PAGE_SIZE, PageType};
use crate::io::bitmap::{BitmapPage, BITMAP_PAGE_COUNT, BitmapHeader};
use crate::io::store::{MemoryPage, PageStore};
use std::collections::HashMap;
use std::io::Result;
use std::pin::Pin;

const INDEX_HEADER_SIZE: usize = 16;
const INDEX_BITMAP_COUNT: u16 = ((PAGE_SIZE - INDEX_HEADER_SIZE) / 8) as u16;

pub struct IndexPage {
    page_id: u32,
    first_managed_page_id: u32,
    current_bitmap_count: u16,
    current_bitmap_idx: u16,
    first_free_bitmap_idx: u16,
    dirty_bitmaps: HashMap<u16, Pin<Box<BitmapPage>>>,
    buffer: [u8; PAGE_SIZE],
}

impl<'a> IndexPage {
    pub fn grow(bitmap: &dyn BitmapHeader) -> Pin<Box<IndexPage>> {
        let mut second = BitmapPage::new(BITMAP_PAGE_COUNT as u32);

        let page_id = second.allocate(|_| true).unwrap();

        let mut index = Box::pin(IndexPage {
            page_id,
            first_managed_page_id: bitmap.first_managed_page_id(),
            current_bitmap_count: 2,
            current_bitmap_idx: 1,
            first_free_bitmap_idx: if bitmap.free_page_count() > 0 { 0 } else { 1 },
            dirty_bitmaps: HashMap::new(),
            buffer: [0; PAGE_SIZE],
        });
        index.update(bitmap);
        index.update(&second);
        index.dirty_bitmaps.insert(1, second);
        index
    }

    pub fn load(memory: &MemoryPage, page_store: &PageStore, mut f: impl FnMut(u32) -> bool) -> Option<Pin<Box<IndexPage>>> {
        let old_page_id = memory.page_id();
        let first_managed_page_id = memory.get_u32(8);
        let current_bitmap_count = memory.get_u16(12);
        let first_free_bitmap_idx = memory.get_u16(14);

        let mut buffer = [0; PAGE_SIZE];
        buffer.copy_from_slice(memory.content());

        let mut index = Box::pin( IndexPage {
            page_id: 0xFFFFFFFF,
            first_managed_page_id,
            current_bitmap_count,
            current_bitmap_idx: first_free_bitmap_idx,
            first_free_bitmap_idx,
            dirty_bitmaps: HashMap::new(),
            buffer
        });

        if index.activate_next_bitmap(page_store, first_free_bitmap_idx, &mut f) {
            index.page_id = index.allocate(page_store, &mut f)?;
            index.free(old_page_id, page_store, &mut f)?;
            Some(index)
        } else {
            None
        }
    }

    pub fn persist(&mut self, page_store: &mut PageStore) -> Result<()> {
        self.dirty_bitmaps.iter_mut().map(|(_, v)| {
            v.persist(page_store)
        }).filter(|r| r.is_err()).collect::<Result<Vec<_>>>()?;

        self.update_header();
        page_store.write_page(self.page_id as usize, &self.buffer)
    }

    fn update_header(&mut self) {
        put_u32(&mut self.buffer, 0, self.page_id);
        put_u32(&mut self.buffer, 4, PageType::Index as u32);
        put_u32(&mut self.buffer, 8, self.first_managed_page_id);
        put_u16(&mut self.buffer, 12, self.current_bitmap_count);
        put_u16(&mut self.buffer, 14, self.first_free_bitmap_idx);
    }

    fn activate_next_bitmap(&mut self, page_store: &PageStore, bitmap_idx: u16, mut f: impl FnMut(u32) -> bool) -> bool {
        let content = &self.buffer[INDEX_HEADER_SIZE..];
        for idx in bitmap_idx..self.current_bitmap_count {
            let bitmap_page_id = get_u32(content, (bitmap_idx * 8) as usize);
            let bitmap_page = page_store.read_page(bitmap_page_id as usize).unwrap();

            match BitmapPage::load(&bitmap_page, &mut f) {
                Some(bitmap) => {
                    let freed = bitmap.contains(bitmap_page_id);
                    self.update(&bitmap);
                    self.current_bitmap_idx = idx;
                    self.dirty_bitmaps.insert(idx, bitmap);
                    if !freed {
                        match self.free(bitmap_page_id, page_store, &mut f) {
                            None => return false,
                            Some(_) => ()
                        }
                    }
                    return true;
                }
                None => ()
            }
        }

        self.grow_next_bitmap()
    }

    fn grow_next_bitmap(&mut self) -> bool {
        let result = self.current_bitmap_count < INDEX_BITMAP_COUNT;
        if result {
            let bitmap = BitmapPage::new(self.first_managed_page_id + self.current_bitmap_count as u32 * BITMAP_PAGE_COUNT as u32);
            self.update(&bitmap);
            self.dirty_bitmaps.insert(self.current_bitmap_count, bitmap);
            self.current_bitmap_idx = self.current_bitmap_count;
            self.current_bitmap_count = self.current_bitmap_count + 1;
        }
        result
    }

    pub fn allocate(&mut self, page_store: &PageStore, mut f: impl FnMut(u32) -> bool) -> Option<u32> {
        loop {
            let bitmap = self.dirty_bitmaps.get_mut(&self.current_bitmap_idx).unwrap();
            let result = bitmap.allocate(&mut f);
            let page_id = bitmap.page_id;
            let free_page_count = bitmap.free_page_count;

            self.update_bitmap_data(self.current_bitmap_idx, page_id, free_page_count);
            if result.is_some() {
                return result;
            } else {
                if !self.activate_next_bitmap(page_store, self.current_bitmap_idx + 1, &mut f) {
                    return None;
                }
            }
        }
    }

    pub fn free(&mut self, page_id: u32, page_store: &PageStore, f: impl FnMut(u32) -> bool) -> Option<bool> {
        let freed = self.free_dirty(page_id);
        if freed.is_some() {
            return freed;
        }

        self.free_unloaded(page_id, page_store, f)
    }

    fn free_dirty(&mut self, page_id: u32) -> Option<bool> {
        let idx = ((page_id - self.first_managed_page_id) / BITMAP_PAGE_COUNT as u32) as u16;

        let bitmap = self.dirty_bitmaps.get_mut(&idx)?;
        let result = bitmap.free(page_id);
        let page_id = bitmap.page_id;
        let free_page_count = bitmap.free_page_count;
        self.update_bitmap_data(idx, page_id, free_page_count);

        Some(result)
    }

    fn free_unloaded(&mut self, page_id: u32, page_store: &PageStore, mut f: impl FnMut(u32) -> bool) -> Option<bool> {
        let new_bitmap_page_id = self.allocate(page_store, &mut f)?;

        let bitmap_idx = ((page_id - self.first_managed_page_id) / BITMAP_PAGE_COUNT as u32) as u16;

        let old_bitmap_page_id = get_u32(&self.buffer[INDEX_HEADER_SIZE..], bitmap_idx as usize * 8);

        let bitmap_memory = page_store.read_page(old_bitmap_page_id as usize).ok()?;

        let mut bitmap = BitmapPage::load_into(&bitmap_memory, new_bitmap_page_id);

        let result = bitmap.free(page_id);

        self.update(&bitmap);
        self.dirty_bitmaps.insert(bitmap_idx, bitmap);

        Some(result)
    }

    fn update(&mut self, bitmap: &dyn BitmapHeader) {
        let bitmap_idx = ((bitmap.first_managed_page_id() - self.first_managed_page_id) / BITMAP_PAGE_COUNT as u32) as u16;

        self.update_bitmap_data(bitmap_idx, bitmap.page_id(), bitmap.free_page_count())
    }

    fn update_bitmap_data(&mut self, bitmap_idx: u16, page_id: u32, free_page_count: u16) {
        let index = INDEX_HEADER_SIZE + (bitmap_idx * 8) as usize;

        put_u32(&mut self.buffer, index, page_id);
        put_u32(&mut self.buffer, index + 4, free_page_count as u32);

        if bitmap_idx < self.first_free_bitmap_idx && free_page_count > 0 {
            self.first_free_bitmap_idx = bitmap_idx;
        } else if bitmap_idx == self.first_free_bitmap_idx && free_page_count == 0 {
            for idx in bitmap_idx+1..self.current_bitmap_count {
                let index = INDEX_HEADER_SIZE + (idx as usize * 8) + 4;
                let page_count = get_u32(&self.buffer, index);

                if page_count > 0 {
                    self.first_free_bitmap_idx = idx;
                    return ()
                }
            }
            self.first_free_bitmap_idx = self.current_bitmap_count
        }
    }
}

pub fn get_u32(buffer: &[u8], idx: usize) -> u32 {
    let s = &buffer[idx..idx + 4];
    let mut a: [u8; 4] = [0; 4];
    a.copy_from_slice(s);

    u32::from_le_bytes(a)
}

fn put_u16(buffer: &mut [u8], idx: usize, value: u16) {
    let bytes = value.to_le_bytes();
    buffer[idx..idx + 2].clone_from_slice(&bytes);
}

fn put_u32(buffer: &mut [u8], idx: usize, value: u32) {
    let bytes = value.to_le_bytes();
    buffer[idx..idx + 4].clone_from_slice(&bytes);
}

#[cfg(test)]
mod tests {
    use crate::io::bitmap::{BitmapPage, BITMAP_PAGE_COUNT};
    use crate::io::index::IndexPage;

    #[test]
    fn grow_from_first_bitmap() {
        let mut page = BitmapPage::new(2);
        let mut index = IndexPage::grow(&page);

        assert_eq!(BITMAP_PAGE_COUNT as u32 + 1, index.page_id);
        assert_eq!(2, index.first_managed_page_id);
        assert_eq!(2, index.current_bitmap_count);
        assert_eq!(1, index.current_bitmap_idx);
        assert_eq!(0, index.first_free_bitmap_idx);
        assert_eq!(1, index.dirty_bitmaps.len());
    }
}
