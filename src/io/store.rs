use std::fs::File;
use std::io::{Seek, Write, SeekFrom};
use std::io::{Result};
use memmap::{Mmap, MmapOptions};
use std::sync::Arc;
use crate::io::{PAGE_SIZE, invalid_input};

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
        self.get_u32(0)
    }

    pub fn page_type(&self) -> u32 {
        self.get_u32(4)
    }

    pub fn get_u32(&self, idx: usize) -> u32 {
        let s = &self.content()[idx..idx + 4];
        let mut a: [u8; 4] = [0; 4];
        a.copy_from_slice(s);

        u32::from_le_bytes(a)
    }

    pub fn get_u16(&self, idx: usize) -> u16 {
        let s = &self.content()[idx..idx + 2];
        let mut a: [u8; 2] = [0; 2];
        a.copy_from_slice(s);

        u16::from_le_bytes(a)
    }

    pub fn content(&'a self) -> &'a [u8] {
        &self.mmap[self.start..self.end]
    }
}


#[cfg(test)]
mod tests {
    use crate::io::PAGE_SIZE;
    use crate::io::store::PageStore;
    use tempfile::tempfile;

    const TESTDB_MAX_SIZE: usize = 163840;

    #[test]
    fn buffer_too_small() {
        let vec: Vec<u8> = vec![0; PAGE_SIZE - 1];

        let file = tempfile().unwrap();
        let mut store = PageStore::new(file, TESTDB_MAX_SIZE).unwrap();

        match store.write_page(0, &vec) {
            Err(e) => (),
            Ok(()) => panic!("should not have written the page")
        }
    }

    #[test]
    fn buffer_too_big() {
        let vec: Vec<u8> = vec![0; PAGE_SIZE + 1];

        let file = tempfile().unwrap();
        let mut store = PageStore::new(file, TESTDB_MAX_SIZE).unwrap();

        match store.write_page(0, &vec) {
            Err(e) => (),
            Ok(()) => panic!("should not have written the page")
        }
    }

    #[test]
    fn writes_first_page() {
        let vec: Vec<u8> = vec![0; PAGE_SIZE];

        let file = tempfile().unwrap();
        let mut store = PageStore::new(file, TESTDB_MAX_SIZE).unwrap();

        store.write_page(0, &vec).unwrap();
        store.flush().unwrap();

        assert_eq!(PAGE_SIZE, store.current_size)
    }

    #[test]
    fn writes_existing_page() {
        let vec: Vec<u8> = vec![0; PAGE_SIZE];

        let file = tempfile().unwrap();
        let mut store = PageStore::new(file, TESTDB_MAX_SIZE).unwrap();

        store.write_page(1, &vec).unwrap();
        store.write_page(0, &vec).unwrap();
        store.flush().unwrap();

        assert_eq!(2 * PAGE_SIZE, store.current_size)
    }

    #[test]
    fn range_out_of_bounds() {
        let vec: Vec<u8> = vec![0; 256];

        let file = tempfile().unwrap();
        let mut store = PageStore::new(file, TESTDB_MAX_SIZE).unwrap();

        match store.write_page_range(0, PAGE_SIZE - vec.len() + 1, &vec) {
            Err(e) => (),
            Ok(()) => panic!("should have failed to write page subset")
        }
    }

    #[test]
    fn write_after_last_page() {
        let vec: Vec<u8> = vec![0; 256];

        let file = tempfile().unwrap();
        let mut store = PageStore::new(file, TESTDB_MAX_SIZE).unwrap();

        match store.write_page_range((TESTDB_MAX_SIZE / PAGE_SIZE) + 1, 0, &vec) {
            Err(e) => (),
            Ok(()) => panic!("should have failed to write page subset")
        }
    }

    #[test]
    fn writes_first_page_range_start() {
        let vec: Vec<u8> = vec![0; 256];

        let file = tempfile().unwrap();
        let mut store = PageStore::new(file, TESTDB_MAX_SIZE).unwrap();

        store.write_page_range(0, 0, &vec).unwrap();
        store.flush().unwrap();

        assert_eq!(PAGE_SIZE, store.current_size)
    }

    #[test]
    fn writes_first_page_range_middle() {
        let vec: Vec<u8> = vec![0; 256];

        let file = tempfile().unwrap();
        let mut store = PageStore::new(file, TESTDB_MAX_SIZE).unwrap();

        store.write_page_range(0, 128, &vec).unwrap();
        store.flush().unwrap();

        assert_eq!(PAGE_SIZE, store.current_size);
    }

    #[test]
    fn cannot_read_beyond_current_file_size() {
        let vec: Vec<u8> = vec![1, 2, 3, 4, 5];
        let file = tempfile().unwrap();
        let mut store = PageStore::new(file, TESTDB_MAX_SIZE).unwrap();
        store.write_page_range(0, 0, &vec).unwrap();
        match store.read_page(1) {
            Err(e) => (),
            Ok(v) => panic!("should have failed")
        }
    }

    #[test]
    fn read_back_page() {
        let vec: Vec<u8> = vec![1, 2, 3, 4, 5];

        let file = tempfile().unwrap();
        let mut store = PageStore::new(file, TESTDB_MAX_SIZE).unwrap();

        store.write_page_range(0, 0, &vec).unwrap();
        let page = store.read_page(0).unwrap();

        assert_eq!(&vec[0..5], &page.content()[0..5]);
        assert_eq!(0 as u8, page.content()[PAGE_SIZE - 1])
    }
}