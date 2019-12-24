use std::error;
use std::io::{Error, ErrorKind, Result};

mod bitmap;
mod index;
mod store;

const PAGE_SIZE: usize = 4096;

enum PageType {
    Bitmap = 1,
    Index = 2,
}

fn invalid_input<T, E>(message: E) -> Result<T>
    where E: Into<Box<dyn error::Error + Send + Sync>>
{
    Err(Error::new(
        ErrorKind::InvalidInput,
        message,
    ))
}

