use crate::io::PAGE_SIZE;
use std::pin::Pin;
use crate::io::bitmap::{BitmapPage, BITMAP_PAGE_COUNT, BitmapHeader};
use crate::io::store::{MemoryPage, PageStore};
use std::collections::HashMap;
use std::thread::current;

const INDEX_HEADER_SIZE: usize = 16;
const INDEX_BITMAP_COUNT: u16 = ((PAGE_SIZE - INDEX_HEADER_SIZE) * 8) as u16;

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

        // todo do we need to protect against the maximum database size here?
        let page_id = second.allocate(|_| true).unwrap();

        let first_page_id = bitmap.page_id();

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

    fn update(&mut self, bitmap: &dyn BitmapHeader) {
        let index = INDEX_HEADER_SIZE + ((bitmap.first_managed_page_id() - self.first_managed_page_id) / BITMAP_PAGE_COUNT as u32) as usize;

        put_u32(&mut self.buffer, index, bitmap.page_id());
        put_u32(&mut self.buffer, index + 4, bitmap.free_page_count() as u32);
    }
}

pub fn get_u32(buffer: &[u8], idx: usize) -> u32 {
    let s = &buffer[idx..idx + 4];
    let mut a: [u8; 4] = [0; 4];
    a.copy_from_slice(s);

    u32::from_le_bytes(a)
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
