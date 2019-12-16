use std::fs::File;
use std::io::{Seek, Write, SeekFrom};
use std::io::{Error, ErrorKind, Result};

const PAGE_SIZE : usize = 4096;

pub struct PageWriter {
    file: File,
    pub(crate) size: usize,
}

impl PageWriter {
    pub fn flush(&mut self) -> Result<()> {
        self.file.flush()
    }

    pub fn write_page(&mut self, id: usize, buf: &[u8]) -> Result<()> {
        if buf.len() != PAGE_SIZE {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("invalid size, buf needs to hold exactly {} bytes", PAGE_SIZE)
            ))
        }
        self.write_buf_at(buf, id * PAGE_SIZE)
    }

    pub fn write_page_range(&mut self, id: usize, offset: usize, buf: &[u8]) -> Result<()> {
        if offset + buf.len() > PAGE_SIZE {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "invalid (offset,size), write would overrun page"
            ))
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
        let new_size = (pos & (!(PAGE_SIZE-1))) + PAGE_SIZE;
        if new_size > self.size {
            self.file.set_len(new_size as u64)?;
            self.size = new_size
        }
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use crate::page_store::{PageWriter, PAGE_SIZE};

    use std::error::Error;
    use std::fs::File;
    use std::io::Result;
    use tempfile::tempfile;

    #[test]
    fn buffer_too_small() {
        let vec: Vec<u8> = vec![0; PAGE_SIZE-1];

        let file = tempfile().unwrap();
        let mut writer = PageWriter { file, size: 0 };

        match writer.write_page(0, &vec) {
            Err(e) => (),
            Ok(()) => panic!("should not have written the page")
        }
    }

    #[test]
    fn buffer_too_big() {
        let vec: Vec<u8> = vec![0; PAGE_SIZE+1];

        let file = tempfile().unwrap();
        let mut writer = PageWriter { file, size: 0 };

        match writer.write_page(0, &vec) {
            Err(e) => (),
            Ok(()) => panic!("should not have written the page")
        }
    }

    #[test]
    fn writes_first_page() {
        let vec: Vec<u8> = vec![0; PAGE_SIZE];

        let file = tempfile().unwrap();
        let mut writer = PageWriter { file, size: 0 };

        writer.write_page(0, &vec).unwrap();
        writer.flush().unwrap();

        assert_eq!(PAGE_SIZE, writer.size)
    }

    #[test]
    fn writes_existing_page() {
        let vec: Vec<u8> = vec![0; PAGE_SIZE];

        let file = tempfile().unwrap();
        let mut writer = PageWriter { file, size: 0 };

        writer.write_page(1, &vec).unwrap();
        writer.write_page(0, &vec).unwrap();
        writer.flush().unwrap();

        assert_eq!(2 * PAGE_SIZE, writer.size)
    }

    #[test]
    fn range_out_of_bounds() {
        let vec: Vec<u8> = vec![0; 256];

        let file = tempfile().unwrap();
        let mut writer = PageWriter { file, size: 0 };

        match writer.write_page_range(0, PAGE_SIZE - vec.len() + 1, &vec) {
            Err(e) => (),
            Ok(()) => panic!("should have failed to write page subset")
        }
    }

    #[test]
    fn writes_first_page_range_start() {
        let vec: Vec<u8> = vec![0; 256];

        let file = tempfile().unwrap();
        let mut writer = PageWriter { file, size: 0 };

        writer.write_page_range(0, 0, &vec).unwrap();
        writer.flush().unwrap();

        assert_eq!(PAGE_SIZE, writer.size)
    }

    #[test]
    fn writes_first_page_range_middle() {
        let vec: Vec<u8> = vec![0; 256];

        let file = tempfile().unwrap();
        let mut writer = PageWriter { file, size: 0 };

        writer.write_page_range(0, 128, &vec).unwrap();
        writer.flush().unwrap();

        assert_eq!(PAGE_SIZE, writer.size);
    }
}
