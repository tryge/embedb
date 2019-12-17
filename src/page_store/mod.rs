use std::fs::File;
use std::io::{Seek, Write, SeekFrom};
use std::io::{Error, ErrorKind, Result};
use memmap::{Mmap, MmapOptions};
use std::error;

#[cfg(test)]
mod tests;

const PAGE_SIZE: usize = 4096;

pub struct PageStore {
    file: File,
    mmap: Mmap,
    max_size: usize,
    pub(crate) current_size: usize,
}

impl PageStore {
    pub fn new(file: File, max_size: usize) -> Result<PageStore> {
        let current_size = file.metadata()?.len() as usize;
        let mmap = unsafe {
           MmapOptions::new().len(max_size).map(&file)?
        };
        Ok(PageStore { file, mmap, max_size, current_size })
    }

    pub fn flush(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.sync_data()
    }

    pub fn read_page(&self, id: usize) -> Result<&[u8]> {
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
        Ok(&self.mmap[offset..end])
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

fn invalid_input<T, E>(message: E) -> Result<T>
    where E: Into<Box<dyn error::Error + Send + Sync>>
{
    Err(Error::new(
        ErrorKind::InvalidInput,
        message,
    ))
}

