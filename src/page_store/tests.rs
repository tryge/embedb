use crate::page_store::{PageStore, PAGE_SIZE};

use std::error::Error;
use std::fs::File;
use std::io::Result;
use tempfile::tempfile;

const TESTDB_MAX_SIZE: usize = 16384;

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

    assert_eq!(&vec[0..5], &page[0..5]);
    assert_eq!(0 as u8, page[PAGE_SIZE - 1])
}
