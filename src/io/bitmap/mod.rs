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


    fn mark_used(&mut self, page_id: u32, f: impl FnMut(u16) -> bool) -> bool {
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
        let in_range = self.contains(page_id);
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


    pub fn contains(&self, page_id: u32) -> bool {
        page_id >= self.first_managed_page_id && page_id <= self.last_managed_page_id
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
    fn first_managed_page_id(&self) -> u32;
    fn free_page_count(&self) -> u16;
    fn first_free_page_index(&self) -> u16;
}

impl BitmapHeader for MemoryPage {
    fn page_id(&self) -> u32 {
        self.get_u32(0)
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

impl BitmapHeader for &Pin<Box<BitmapPage>> {
    fn page_id(&self) -> u32 {
        self.page_id
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
    fn find_clear_filtered(&self, offset: u16, f: impl FnMut(u16) -> bool) -> Option<u16>;

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

        for (byte_index, byte) in self[byte_start_index+1..].iter().enumerate() {
            if *byte != 0xFF {
                for bit in 0..=7 as u16 {
                    let mask = (1 << bit) as u8;
                    if *byte & mask == 0 {
                        let candidate = (((byte_start_index + byte_index + 1) as u16) << 3) + bit;
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
mod tests;