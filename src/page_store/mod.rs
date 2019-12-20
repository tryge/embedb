use std::fs::File;
use std::io::{Seek, Write, SeekFrom};
use std::io::{Error, ErrorKind, Result};
use memmap::{Mmap, MmapOptions};
use std::error;
use std::sync::Arc;

#[cfg(test)]
mod tests;

const PAGE_SIZE: usize = 4096;

enum PageType {
    Bitmap = 1
}

pub struct PageStore {
    file: File,
    mmap: Arc<Mmap>,
    pub(crate) max_size: usize,
    pub(crate) current_size: usize,
}

impl PageStore {
    pub fn new(file: File, max_size: usize) -> Result<PageStore> {
        let current_size = file.metadata()?.len() as usize;
        let mem = unsafe {
            MmapOptions::new().len(max_size).map(&file)?
        };
        let mmap = Arc::new(mem);
        Ok(PageStore { file, mmap, max_size, current_size })
    }

    pub fn flush(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.sync_data()
    }

    pub fn read_page(&self, id: usize) -> Result<MemoryPage> {
        let offset = id * PAGE_SIZE;
        let end = offset + PAGE_SIZE;
        if end > self.current_size {
            return invalid_input(
                if end > self.max_size {
                    format!("invalid page, the specified page is beyond maximum file size (max size = {})", self.max_size)
                } else {
                    format!("invalid page, the specified page does not yet exist(current size = {})", self.current_size)
                }
            );
        }
        Ok(MemoryPage { start: offset, end, mmap: self.mmap.clone() })
    }

    pub fn write_page(&mut self, id: usize, buf: &[u8]) -> Result<()> {
        if buf.len() != PAGE_SIZE {
            return invalid_input(
                format!("invalid size, buf needs to hold exactly {} bytes", PAGE_SIZE)
            );
        }
        self.write_buf_at(buf, id * PAGE_SIZE)
    }

    pub fn write_page_range(&mut self, id: usize, offset: usize, buf: &[u8]) -> Result<()> {
        if offset + buf.len() > PAGE_SIZE {
            return invalid_input(
                "invalid (offset,size), write would overrun page"
            );
        }
        self.write_buf_at(buf, id * PAGE_SIZE + offset)
    }

    fn write_buf_at(&mut self, buf: &[u8], pos: usize) -> Result<()> {
        self.ensure_page_exists_at(pos)?;
        self.file.seek(SeekFrom::Start(pos as u64))?;
        self.file.write_all(buf)?;
        Ok(())
    }

    fn ensure_page_exists_at(&mut self, pos: usize) -> Result<()> {
        let new_size = (pos & (!(PAGE_SIZE - 1))) + PAGE_SIZE;
        if new_size > self.max_size {
            return invalid_input(
                format!("invalid page, the specified page is beyond maximum file size ({})", self.max_size)
            );
        }
        if new_size > self.current_size {
            self.file.set_len(new_size as u64)?;
            self.current_size = new_size;
        }
        Ok(())
    }
}

pub struct MemoryPage {
    start: usize,
    end: usize,
    mmap: Arc<Mmap>,
}

impl<'a> MemoryPage {
    pub fn page_id(&self) -> u32 {
        self.extract_u32(0)
    }

    pub fn page_type(&self) -> u32 {
        self.extract_u32(4)
    }

    pub fn extract_u32(&self, idx: usize) -> u32 {
        let s = &self.content()[idx..idx + 4];
        let mut a: [u8; 4] = [0; 4];
        a.copy_from_slice(s);

        u32::from_le_bytes(a)
    }

    pub fn extract_u16(&self, idx: usize) -> u16 {
        let s = &self.content()[idx..idx + 2];
        let mut a: [u8; 2] = [0; 2];
        a.copy_from_slice(s);

        u16::from_le_bytes(a)
    }

    pub fn content(&'a self) -> &'a [u8] {
        &self.mmap[self.start..self.end]
    }
}

const BITMAP_INDEX_PAGE_HEADER_SIZE: usize = 16;
const BITMAP_INDEX_PAGE_COUNT: u16 = ((PAGE_SIZE - BITMAP_INDEX_PAGE_HEADER_SIZE) * 8) as u16;

pub struct BitmapIndexPage {
    page_id: u32,
    first_managed_page_id: u32,
    last_managed_page_id: u32,
    current_first_free_page_idx: u16,
    first_free_page_idx: u16,
    free_page_count: u16,
    buffer: [u8; PAGE_SIZE],
}

impl<'a> BitmapIndexPage {
    pub fn new(page_id: u32, first_managed_page_id: u32) -> BitmapIndexPage {
        let last_managed_page_id = first_managed_page_id + (BITMAP_INDEX_PAGE_COUNT as u32) - 1;

        let mut page = BitmapIndexPage {
            page_id,
            first_managed_page_id,
            last_managed_page_id,
            current_first_free_page_idx: 0,
            first_free_page_idx: 0,
            free_page_count: BITMAP_INDEX_PAGE_COUNT,
            buffer: [0; PAGE_SIZE],
        };
        if page_id >= first_managed_page_id && page_id <= last_managed_page_id {
            page.mark_used(page_id)
        }
        page
    }

    pub fn load_into(page: &MemoryPage, page_id: u32) -> BitmapIndexPage {
        let first_managed_page_id = page.extract_u32(8);
        let last_managed_page_id = first_managed_page_id + (BITMAP_INDEX_PAGE_COUNT as u32) - 1;
        let free_page_count = page.extract_u16(12);
        let first_free_page_idx = page.extract_u16(14);
        let current_first_free_page_idx = first_free_page_idx;

        let mut buffer = [0; PAGE_SIZE];
        buffer.clone_from_slice(page.content());

        let mut index = BitmapIndexPage {
            page_id,
            first_managed_page_id,
            last_managed_page_id,
            current_first_free_page_idx,
            first_free_page_idx,
            free_page_count,
            buffer
        };
        index.free(page.page_id());

        index
    }

    pub fn load(page: &MemoryPage, f: fn(u32) -> bool) -> Option<BitmapIndexPage> {
        let first_managed_page_id = page.extract_u32(8);
        let free_page_count = page.extract_u16(12);
        let first_free_page_idx = page.extract_u16(14);
        let mut current_first_free_page_idx = first_free_page_idx;
        let mut page_id : u32 = 0xFFFFFFFF;

        let bitmap = &page.content()[BITMAP_INDEX_PAGE_HEADER_SIZE..];
        while current_first_free_page_idx != 0xFFFF {
            let candidate = first_managed_page_id + current_first_free_page_idx as u32;
            if f(candidate) {
                page_id = candidate;
                break;
            } else {
                current_first_free_page_idx = BitmapIndexPage::find_next_free_page_index(bitmap, current_first_free_page_idx + 1);
            }
        };
        if current_first_free_page_idx == 0xFFFF {
            return None
        }
        let next_free_page_idx = BitmapIndexPage::find_next_free_page_index(bitmap, current_first_free_page_idx + 1);
        if next_free_page_idx == 0xFFFF {
            return None
        }

        let mut buffer = [0; PAGE_SIZE];
        buffer.clone_from_slice(page.content());

        let mut index = BitmapIndexPage {
            page_id,
            first_managed_page_id,
            last_managed_page_id: first_managed_page_id + BITMAP_INDEX_PAGE_COUNT as u32,
            current_first_free_page_idx,
            first_free_page_idx,
            free_page_count,
            buffer
        };
        index.mark_used(page_id);
        index.free(page.page_id());

        Some(index)
    }


    pub fn allocate(&mut self, f: fn(u32) -> bool) -> Option<u32> {
        while self.current_first_free_page_idx != 0xFFFF {
            let candidate = self.first_managed_page_id + self.current_first_free_page_idx as u32;
            if f(candidate) {
                self.mark_used(candidate);

                return Some(candidate);
            }
            self.current_first_free_page_idx = BitmapIndexPage::find_next_free_page_index(self.bitmap(), self.current_first_free_page_idx + 1)
        }
        None
    }

    fn mark_used(&mut self, page_id: u32) {
        let offset = page_id - self.first_managed_page_id;
        let byte_index = (offset as usize >> 3);
        let bit: u8 = (1 << (offset & 0x07)) as u8;

        let byte = &mut self.bitmap_mut()[byte_index];
        if *byte & bit == 0 {
            *byte |= bit;

            self.free_page_count -= 1;
            if page_id == (self.first_managed_page_id + self.current_first_free_page_idx as u32) {
                let next = BitmapIndexPage::find_next_free_page_index(self.bitmap(), self.current_first_free_page_idx + 1);
                if self.first_free_page_idx == self.current_first_free_page_idx {
                    self.first_free_page_idx = next;
                }
                self.current_first_free_page_idx = next;
            }
        }
    }

    fn find_next_free_page_index(bitmap: &[u8], start: u16) -> u16 {
        let byte_start_index = (start >> 3) as usize;

        for byte_index in byte_start_index..(PAGE_SIZE - BITMAP_INDEX_PAGE_HEADER_SIZE) {
            let byte = bitmap[byte_index];
            if byte != 0xFF {
                for bit in 0..7 as u16 {
                    let mask = (1 << bit) as u8;
                    if byte & mask == 0 {
                        let candidate = ((byte_index as u16) << 3) + bit;
                        if candidate >= start {
                            return candidate;
                        }
                    }
                }
            }
        }

        0xFFFF
    }


    pub fn free(&mut self, page_id: u32) -> bool {
        if page_id >= self.first_managed_page_id && page_id <= self.last_managed_page_id {
            self.mark_free(page_id);
            true
        } else {
            false
        }
    }

    fn mark_free(&mut self, page_id: u32) {
        let offset = page_id - self.first_managed_page_id;
        let byte_index = (offset as usize >> 3);
        let bit: u8 = (1 << (offset & 0x07)) as u8;
        let mask: u8 = !bit;


        let byte = &mut self.bitmap_mut()[byte_index];
        if *byte & bit == bit {
            *byte &= mask;

            self.free_page_count += 1;
            if page_id < (self.first_managed_page_id + self.first_free_page_idx as u32) {
                self.first_free_page_idx = (page_id - self.first_managed_page_id) as u16
            }
        }
    }


    fn bitmap(&'a self) -> &'a [u8] {
        &self.buffer[BITMAP_INDEX_PAGE_HEADER_SIZE..PAGE_SIZE]
    }

    fn bitmap_mut(&'a mut self) -> &'a mut [u8] {
        &mut self.buffer[BITMAP_INDEX_PAGE_HEADER_SIZE..PAGE_SIZE]
    }


    pub fn persist(mut self, store: &mut PageStore) -> Result<()> {
        self.update_header();

        store.write_page(self.page_id as usize, &self.buffer)
    }

    fn update_header(&mut self) {
        let page_id_bytes = self.page_id.to_le_bytes();
        self.buffer[0..4].clone_from_slice(&page_id_bytes);

        let page_type_bytes = (PageType::Bitmap as u32).to_le_bytes();
        self.buffer[4..8].clone_from_slice(&page_type_bytes);

        let first_page_id_bytes = self.first_managed_page_id.to_le_bytes();
        self.buffer[8..12].clone_from_slice(&first_page_id_bytes);

        let free_page_count_bytes = self.free_page_count.to_le_bytes();
        self.buffer[12..14].clone_from_slice(&free_page_count_bytes);

        let free_page_index_bytes = self.first_free_page_idx.to_le_bytes();
        self.buffer[14..16].clone_from_slice(&free_page_index_bytes);
    }
}


fn invalid_input<T, E>(message: E) -> Result<T>
    where E: Into<Box<dyn error::Error + Send + Sync>>
{
    Err(Error::new(
        ErrorKind::InvalidInput,
        message,
    ))
}

