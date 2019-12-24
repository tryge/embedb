use std::io::Result;
use crate::io::{PAGE_SIZE, PageType};
use crate::io::store::{MemoryPage, PageStore};
use std::pin::Pin;

const BITMAP_HEADER_SIZE: usize = 16;
pub(crate) const BITMAP_PAGE_COUNT: u16 = ((PAGE_SIZE - BITMAP_HEADER_SIZE) * 8) as u16;

pub struct BitmapPage {
    pub(crate) page_id: u32,
    pub(crate) first_managed_page_id: u32,
    last_managed_page_id: u32,
    current_first_free_page_idx: u16,
    first_free_page_idx: u16,
    pub(crate) free_page_count: u16,
    buffer: [u8; PAGE_SIZE],
}

impl<'a> BitmapPage {
    pub fn new(first_managed_page_id: u32) -> Pin<Box<BitmapPage>> {
        let last_managed_page_id = first_managed_page_id + (BITMAP_PAGE_COUNT as u32) - 1;

        let mut page = Box::pin(BitmapPage {
            page_id: first_managed_page_id,
            first_managed_page_id,
            last_managed_page_id,
            current_first_free_page_idx: 0,
            first_free_page_idx: 0,
            free_page_count: BITMAP_PAGE_COUNT,
            buffer: [0; PAGE_SIZE],
        });
        page.mark_used(first_managed_page_id, |_| true);
        page
    }

    pub fn load(page: &MemoryPage, mut f: impl FnMut(u32) -> bool) -> Option<Pin<Box<BitmapPage>>> {
        let first_managed_page_id = page.get_u32(8);
        let free_page_count = page.get_u16(12);
        let first_free_page_idx = page.get_u16(14);

        let bitmap = &page.content()[BITMAP_HEADER_SIZE..];
        let mut filter = |x: u16| f(first_managed_page_id + x as u32);

        let current_idx = bitmap.find_clear_filtered(first_free_page_idx, &mut filter)?;
        let next_idx = bitmap.find_clear_filtered(current_idx + 1, &mut filter)?;
        let page_id = first_managed_page_id + current_idx as u32;

        let mut buffer = [0; PAGE_SIZE];
        buffer.clone_from_slice(page.content());

        let mut index = Box::pin(BitmapPage {
            page_id,
            first_managed_page_id,
            last_managed_page_id: first_managed_page_id + BITMAP_PAGE_COUNT as u32,
            current_first_free_page_idx: next_idx,
            first_free_page_idx,
            free_page_count,
            buffer,
        });
        index.mark_used(page_id, filter);
        index.free(page.page_id());

        Some(index)
    }

    pub fn load_into(page: &MemoryPage, page_id: u32) -> Pin<Box<BitmapPage>> {
        let first_managed_page_id = page.get_u32(8);
        let last_managed_page_id = first_managed_page_id + (BITMAP_PAGE_COUNT as u32) - 1;
        let free_page_count = page.get_u16(12);
        let first_free_page_idx = page.get_u16(14);
        let current_first_free_page_idx = first_free_page_idx;

        let mut buffer = [0; PAGE_SIZE];
        buffer.clone_from_slice(page.content());

        let mut index = Box::pin(BitmapPage {
            page_id,
            first_managed_page_id,
            last_managed_page_id,
            current_first_free_page_idx,
            first_free_page_idx,
            free_page_count,
            buffer,
        });
        index.free(page.page_id());

        index
    }


    pub fn allocate(&mut self, mut f: impl FnMut(u32) -> bool) -> Option<u32> {
        let start_page = self.first_managed_page_id;
        let mut filter = |x: u16| f(start_page + x as u32);
        let (current_idx, page) = match self.bitmap().find_clear_filtered(self.current_first_free_page_idx, &mut filter) {
            Some(idx) => (idx, Some(self.first_managed_page_id + idx as u32)),
            None => (0xFFFF, None)
        };

        self.current_first_free_page_idx = current_idx;
        page.map(|page_id| {
            self.mark_used(page_id, &mut filter);
            page_id
        })
    }


    fn mark_used(&mut self, page_id: u32, mut f: impl FnMut(u16) -> bool) -> bool {
        let offset = page_id - self.first_managed_page_id;
        let changed = self.bitmap_mut().set(offset as u16);
        if changed {
            self.free_page_count -= 1;
            if page_id == self.page_for(self.current_first_free_page_idx) {
                let next = self.bitmap().find_clear_filtered(self.current_first_free_page_idx + 1, f).unwrap_or(0xFFFF);
                self.current_first_free_page_idx = next;
            }
            if page_id == self.page_for(self.first_free_page_idx) {
                let next = self.bitmap().find_clear_filtered(self.first_free_page_idx + 1, |_| true).unwrap_or(0xFFFF);
                self.first_free_page_idx = next;
            }
        }
        changed
    }

    fn page_for(&self, index: u16) -> u32 {
        self.first_managed_page_id + index as u32
    }


    pub fn free(&mut self, page_id: u32) -> bool {
        let in_range = page_id >= self.first_managed_page_id && page_id <= self.last_managed_page_id;
        if in_range {
            self.mark_free(page_id);
        }
        in_range
    }

    fn mark_free(&mut self, page_id: u32) {
        let offset = page_id - self.first_managed_page_id;
        if self.bitmap_mut().clear(offset as u16) {
            self.free_page_count += 1;
            if page_id < self.page_for(self.first_free_page_idx) {
                self.first_free_page_idx = (page_id - self.first_managed_page_id) as u16
            }
        }
    }


    fn bitmap(&'a self) -> &'a [u8] {
        &self.buffer[BITMAP_HEADER_SIZE..PAGE_SIZE]
    }

    fn bitmap_mut(&'a mut self) -> &'a mut [u8] {
        &mut self.buffer[BITMAP_HEADER_SIZE..PAGE_SIZE]
    }


    pub fn persist(&mut self, store: &mut PageStore) -> Result<()> {
        self.update_header();

        store.write_page(self.page_id as usize, &self.buffer)
    }

    fn update_header(&mut self) {
        put_u32(&mut self.buffer, 0, self.page_id);
        put_u32(&mut self.buffer, 4, PageType::Bitmap as u32);
        put_u32(&mut self.buffer, 8, self.first_managed_page_id);
        put_u16(&mut self.buffer, 12, self.free_page_count);
        put_u16(&mut self.buffer, 14, self.first_free_page_idx);
    }
}

pub trait BitmapHeader {
    fn page_id(&self) -> u32;
    fn page_type(&self) -> u32;
    fn first_managed_page_id(&self) -> u32;
    fn free_page_count(&self) -> u16;
    fn first_free_page_index(&self) -> u16;
}

impl BitmapHeader for MemoryPage {
    fn page_id(&self) -> u32 {
        self.get_u32(0)
    }

    fn page_type(&self) -> u32 {
        PageType::Bitmap as u32
    }

    fn first_managed_page_id(&self) -> u32 {
        self.get_u32(8)
    }

    fn free_page_count(&self) -> u16 {
        self.get_u16(12)
    }

    fn first_free_page_index(&self) -> u16 {
        self.get_u16(14)
    }
}

impl BitmapHeader for Pin<Box<BitmapPage>> {
    fn page_id(&self) -> u32 {
        self.page_id
    }

    fn page_type(&self) -> u32 {
        PageType::Bitmap as u32
    }

    fn first_managed_page_id(&self) -> u32 {
        self.first_managed_page_id
    }

    fn free_page_count(&self) -> u16 {
        self.free_page_count
    }

    fn first_free_page_index(&self) -> u16 {
        self.first_free_page_idx
    }
}

trait Bitmap {
    fn find_clear_filtered(&self, offset: u16, mut f: impl FnMut(u16) -> bool) -> Option<u16>;

    fn set(&mut self, index: u16) -> bool;
    fn clear(&mut self, index: u16) -> bool;

    fn indices(&self, index: u16) -> (usize, u8) {
        let byte_index = index >> 3;
        let bit: u8 = (1 << (index & 0x07)) as u8;

        (byte_index as usize, bit)
    }
}

impl Bitmap for [u8] {
    fn find_clear_filtered(&self, offset: u16, mut f: impl FnMut(u16) -> bool) -> Option<u16> {
        let byte_start_index = (offset >> 3) as usize;
        if byte_start_index >= self.len() {
            return None;
        }

        let byte = self[byte_start_index];
        if byte != 0xFF {
            for bit in (offset & 0x07)..=7 as u16 {
                let mask = (1 << bit) as u8;
                if byte & mask == 0 {
                    let candidate = ((byte_start_index as u16) << 3) + bit;
                    if f(candidate) {
                        return Some(candidate);
                    }
                }
            }
        }

        for byte_index in (byte_start_index + 1)..self.len() {
            let byte = self[byte_index];
            if byte != 0xFF {
                for bit in 0..=7 as u16 {
                    let mask = (1 << bit) as u8;
                    if byte & mask == 0 {
                        let candidate = ((byte_index as u16) << 3) + bit;
                        if f(candidate) {
                            return Some(candidate);
                        }
                    }
                }
            }
        }
        None
    }

    fn set(&mut self, index: u16) -> bool {
        let (byte_index, bit) = self.indices(index);

        let byte: &mut u8 = &mut self[byte_index];
        let is_clear = *byte & bit == 0;
        if is_clear {
            *byte |= bit;
        }
        is_clear
    }

    fn clear(&mut self, index: u16) -> bool {
        let (byte_index, bit) = self.indices(index);

        let byte: &mut u8 = &mut self[byte_index];
        let is_set = *byte & bit == bit;
        if is_set {
            *byte &= !bit;
        }
        is_set
    }
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
    use crate::io::store::PageStore;
    use crate::io::bitmap::{BitmapPage, BITMAP_PAGE_COUNT};
    use crate::io::{PageType, PAGE_SIZE};
    use tempfile::tempfile;

    const TESTDB_MAX_SIZE: usize = 163840;

    fn unfiltered(_: u16) -> bool {
        true
    }

    #[test]
    fn new_allocator_for_new_database() {
        let file = tempfile().unwrap();
        let mut store = PageStore::new(file, TESTDB_MAX_SIZE);

        let mut page = BitmapPage::new(2);

        assert_eq!(2, page.page_id);
        assert_eq!(2, page.first_managed_page_id);
        assert_eq!(1, page.first_free_page_idx);
        assert_eq!(BITMAP_PAGE_COUNT - 1, page.free_page_count);
    }

    #[test]
    fn allocator_allocates_pages_monotonically_increasing() {
        let mut page = BitmapPage::new(2);
        let f = |_: u32| true;

        assert_eq!(Some(3), page.allocate(f));
        assert_eq!(Some(4), page.allocate(f));
        assert_eq!(true, page.free(3));
        assert_eq!(Some(5), page.allocate(f));
    }


    #[test]
    fn allocator_allocates_pages_monotonically_increasing_and_skips_used_pages() {
        let mut page = BitmapPage::new(2);

        let f = |x: u32| x != 4 && x != 5 && x != 7 && x != 16;

        assert_eq!(Some(3), page.allocate(f));
        assert_eq!(Some(6), page.allocate(f));
        assert_eq!(Some(8), page.allocate(f));
        assert_eq!(Some(9), page.allocate(f));
        assert_eq!(Some(10), page.allocate(f));
        assert_eq!(Some(11), page.allocate(f));
        assert_eq!(Some(12), page.allocate(f));
        assert_eq!(Some(13), page.allocate(f));
        assert_eq!(Some(14), page.allocate(f));
        assert_eq!(Some(15), page.allocate(f));
        assert_eq!(Some(17), page.allocate(f));
        assert_eq!(Some(18), page.allocate(f));
    }


    #[test]
    fn persist_writes_correct_index() {
        let file = tempfile().unwrap();
        let mut store = PageStore::new(file, TESTDB_MAX_SIZE).unwrap();
        let mut page = BitmapPage::new(2);

        page.persist(&mut store).unwrap();

        let memory_page = store.read_page(2).unwrap();
        assert_eq!(2, memory_page.page_id());
        assert_eq!(PageType::Bitmap as u32, memory_page.page_type());
        assert_eq!(2, memory_page.get_u32(8)); // first_managed_page_id
        assert_eq!(BITMAP_PAGE_COUNT - 1, memory_page.get_u16(12)); // free page count
        assert_eq!(1, memory_page.get_u16(14)); // free page index
        assert_eq!(0x01, memory_page.content()[16]);
    }

    #[test]
    fn cannot_load_full_page() {
        let file = tempfile().unwrap();
        let mut store = PageStore::new(file, TESTDB_MAX_SIZE).unwrap();

        let mut index = BitmapPage {
            page_id: 2,
            first_managed_page_id: 0,
            last_managed_page_id: PageType::Bitmap as u32,
            current_first_free_page_idx: 0xFFFF,
            first_free_page_idx: 0xFFFF,
            free_page_count: 0,
            buffer: [0xFF; PAGE_SIZE],
        };
        index.persist(&mut store);

        let memory_page = store.read_page(2).unwrap();
        let loaded = BitmapPage::load(&memory_page, |_| true);
        match loaded {
            None => (),
            Some(index) => panic!("shouldn't have loaded the index page!")
        }
    }

    #[test]
    fn cannot_load_almost_full_page() {
        let file = tempfile().unwrap();
        let mut store = PageStore::new(file, TESTDB_MAX_SIZE).unwrap();

        let mut buffer = [0xFF; PAGE_SIZE];
        buffer[PAGE_SIZE - 1] = 0x7F;

        let mut index = BitmapPage {
            page_id: 2,
            first_managed_page_id: 0,
            last_managed_page_id: BITMAP_PAGE_COUNT as u32,
            current_first_free_page_idx: BITMAP_PAGE_COUNT - 1,
            first_free_page_idx: BITMAP_PAGE_COUNT - 1,
            free_page_count: 1,
            buffer,
        };
        index.persist(&mut store);

        let memory_page = store.read_page(2).unwrap();
        let loaded = BitmapPage::load(&memory_page, |_| true);
        match loaded {
            None => (),
            Some(index) => panic!("shouldn't have loaded the index page!")
        }
    }

    #[test]
    fn cannot_load_empty_page_if_still_in_use() {
        let file = tempfile().unwrap();
        let mut store = PageStore::new(file, TESTDB_MAX_SIZE).unwrap();

        let mut index = BitmapPage::new(2);
        index.persist(&mut store);

        let memory_page = store.read_page(2).unwrap();
        let loaded = BitmapPage::load(&memory_page, |_| false);
        match loaded {
            None => (),
            Some(index) => panic!("shouldn't have loaded the index page!")
        }
    }

    #[test]
    fn load_and_persist_viable_index() {
        let file = tempfile().unwrap();
        let mut store = PageStore::new(file, TESTDB_MAX_SIZE).unwrap();
        let mut page = BitmapPage::new(2);
        page.allocate(|_| true);
        page.allocate(|_| true);
        page.free(3);

        page.persist(&mut store).unwrap();

        let memory_page = store.read_page(2).unwrap();

        let mut new_index = BitmapPage::load(&memory_page, |x| x != 3).unwrap();
        new_index.allocate(|x| x != 3);
        new_index.persist(&mut store).unwrap();

        let new_memory_page = store.read_page(5).unwrap();
        assert_eq!(5, new_memory_page.page_id());
        assert_eq!(PageType::Bitmap as u32, new_memory_page.page_type());
        assert_eq!(2, new_memory_page.get_u32(8)); // first_managed_page_id
        assert_eq!(BITMAP_PAGE_COUNT - 3, new_memory_page.get_u16(12)); // free page count
        assert_eq!(0, new_memory_page.get_u16(14)); // free page index
        assert_eq!(0x1C, new_memory_page.content()[16]);
    }
}