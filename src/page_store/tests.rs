use crate::page_store::{PageStore, PAGE_SIZE, BitmapAllocationPage, BITMAP_INDEX_PAGE_COUNT, BITMAP_INDEX_PAGE_TYPE};

use std::error::Error;
use std::fs::File;
use std::io::Result;
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

#[test]
fn new_allocator_for_new_database() {
    let file = tempfile().unwrap();
    let mut store = PageStore::new(file, TESTDB_MAX_SIZE);

    let mut page = BitmapAllocationPage::new(2, 0);
    page.mark_used(0);
    page.mark_used(1);

    assert_eq!(2, page.page_id);
    assert_eq!(0, page.first_managed_page_id);
    assert_eq!(3, page.first_free_page_idx);
    assert_eq!(BITMAP_INDEX_PAGE_COUNT - 3, page.free_page_count);
}

#[test]
fn new_allocator_for_existing_database() {
    let file = tempfile().unwrap();
    let mut store = PageStore::new(file, TESTDB_MAX_SIZE);

    let mut page = BitmapAllocationPage::new(2, 10);

    assert_eq!(2, page.page_id);
    assert_eq!(10, page.first_managed_page_id);
    assert_eq!(0, page.first_free_page_idx);
    assert_eq!(BITMAP_INDEX_PAGE_COUNT, page.free_page_count);
}


#[test]
fn allocator_allocates_pages_monotonically_increasing() {
    let mut page = BitmapAllocationPage::new(2, 10);
    let f = |_: u32| true;

    assert_eq!(Some(10), page.allocate(f));
    assert_eq!(Some(11), page.allocate(f));
    assert_eq!(Some(12), page.allocate(f));
    assert_eq!(true, page.free(11));
    assert_eq!(Some(13), page.allocate(f));
}


#[test]
fn allocator_allocates_pages_monotonically_increasing_and_skips_used_pages() {
    let mut page = BitmapAllocationPage::new(2, 10);

    let f = |x: u32| x != 11 && x != 12 && x != 14;

    assert_eq!(Some(10), page.allocate(f));
    assert_eq!(Some(13), page.allocate(f));
    assert_eq!(Some(15), page.allocate(f));
    assert_eq!(true, page.free(13));
    assert_eq!(Some(16), page.allocate(f));
}


#[test]
fn persist_writes_correct_index() {
    let file = tempfile().unwrap();
    let mut store = PageStore::new(file, TESTDB_MAX_SIZE).unwrap();
    let mut page = BitmapAllocationPage::new(2, 0);
    page.mark_used(0);
    page.mark_used(1);

    page.persist(&mut store).unwrap();

    let memory_page = store.read_page(2).unwrap();
    assert_eq!(2, memory_page.page_id());
    assert_eq!(BITMAP_INDEX_PAGE_TYPE, memory_page.page_type());
    assert_eq!(0, memory_page.extract_u32(8)); // first_managed_page_id
    assert_eq!(BITMAP_INDEX_PAGE_COUNT - 3, memory_page.extract_u16(12)); // free page count
    assert_eq!(3, memory_page.extract_u16(14)); // free page count
}
