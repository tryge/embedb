use crate::io::store::PageStore;
use crate::io::bitmap::{BitmapPage, BITMAP_PAGE_COUNT, BitmapHeader};
use crate::io::{PageType, PAGE_SIZE};
use tempfile::tempfile;
use std::pin::Pin;

const TESTDB_MAX_SIZE: usize = 163840;

#[inline(never)]
fn unfiltered(_: u32) -> bool {
    true
}

#[test]
fn new_allocator_for_new_database() {
    let page = BitmapPage::new(2);

    assert_eq!(2, page.page_id);
    assert_eq!(2, page.first_managed_page_id);
    assert_eq!(1, page.first_free_page_idx);
    assert_eq!(BITMAP_PAGE_COUNT - 1, page.free_page_count);
}

#[test]
fn allocator_allocates_pages_monotonically_increasing() {
    let mut page = BitmapPage::new(2);

    assert_eq!(Some(3), page.allocate(unfiltered));
    assert_eq!(Some(4), page.allocate(unfiltered));
    assert_eq!(true, page.free(3));
    assert_eq!(Some(5), page.allocate(unfiltered));
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
fn searches_through_all_bits_for_next_free_page() {
    let mut index = full_bitmap();
    index.free(2 + BITMAP_PAGE_COUNT as u32 - 1);
    index.current_first_free_page_idx = 0;

    let option = index.allocate(unfiltered);
    assert!(option.is_some())
}

#[test]
fn cannot_allocate_on_full_page() {
    let mut index = full_bitmap();

    assert_eq!(None, index.allocate(&unfiltered));
}


#[test]
fn persist_writes_correct_index() {
    let mut store = temporary_store();
    let mut page = BitmapPage::new(2);

    page.persist(&mut store).unwrap();

    let memory_page = store.read_page(2).unwrap();
    assert_eq!(2, memory_page.page_id());
    assert_eq!(PageType::Bitmap as u32, memory_page.page_type());
    assert_eq!(2, memory_page.first_managed_page_id());
    assert_eq!(BITMAP_PAGE_COUNT - 1, memory_page.free_page_count());
    assert_eq!(1, memory_page.first_free_page_index());
    assert_eq!(0x01, memory_page.content()[16]);
}

#[test]
fn cannot_load_full_page() {
    let mut store = temporary_store();

    full_bitmap().persist(&mut store).unwrap();

    let memory_page = store.read_page(2).unwrap();
    let loaded = BitmapPage::load(&memory_page, |_| true);
    assert!(loaded.is_none());
}

#[test]
fn cannot_load_almost_full_page() {
    let mut store = temporary_store();

    let mut index = full_bitmap();
    index.free(2 + BITMAP_PAGE_COUNT as u32 - 1);
    index.persist(&mut store).unwrap();

    let memory_page = store.read_page(2).unwrap();
    let loaded = BitmapPage::load(&memory_page, |_| true);
    assert!(loaded.is_none());
}

#[test]
fn cannot_load_empty_page_if_still_in_use() {
    let mut store = temporary_store();

    let mut index = BitmapPage::new(2);
    index.persist(&mut store).unwrap();

    let memory_page = store.read_page(2).unwrap();
    let loaded = BitmapPage::load(&memory_page, |_| false);
    assert!(loaded.is_none());
}

#[test]
fn load_viable_index() {
    let mut store = temporary_store();
    let mut page = BitmapPage::new(2);
    page.allocate(unfiltered);
    page.allocate(unfiltered);
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

#[test]
fn load_into_viable_index() {
    let mut store = temporary_store();
    let mut page = BitmapPage::new(2);
    page.persist(&mut store).unwrap();

    let memory_page = store.read_page(2).unwrap();

    let new_index = BitmapPage::load_into(&memory_page, 0);

    assert_eq!(0, new_index.page_id());
    assert_eq!(2, new_index.first_managed_page_id);
    assert_eq!(BITMAP_PAGE_COUNT, new_index.free_page_count);
    assert_eq!(0, new_index.first_free_page_idx);
    assert_eq!(0, new_index.buffer[16]);
}

fn temporary_store() -> PageStore {
    let file = tempfile().unwrap();
    let store = PageStore::new(file, TESTDB_MAX_SIZE).unwrap();
    store
}

fn full_bitmap() -> Pin<Box<BitmapPage>> {
    let mut index = BitmapPage::new(2);
    index.current_first_free_page_idx = 0xFFFF;
    index.first_free_page_idx = 0xFFFF;
    index.free_page_count = 0;
    index.buffer = [0xFF; PAGE_SIZE];
    index
}


// Bitmap Header


#[test]
fn bitmap_page_header() {
    let page = BitmapPage::new(2);

    let header: &dyn BitmapHeader = &page;

    assert_eq!(2, header.page_id());
    assert_eq!(BITMAP_PAGE_COUNT - 1, header.free_page_count());
    assert_eq!(2, header.first_managed_page_id());
    assert_eq!(1, header.first_free_page_index());
}


#[test]
fn bitmap_page_ref_header() {
    let page = &BitmapPage::new(2);

    let header: &dyn BitmapHeader = &page;

    assert_eq!(2, header.page_id());
    assert_eq!(BITMAP_PAGE_COUNT - 1, header.free_page_count());
    assert_eq!(2, header.first_managed_page_id());
    assert_eq!(1, header.first_free_page_index());
}

#[test]
fn memory_page_header() {
    let file = tempfile().unwrap();
    let mut store = PageStore::new(file, TESTDB_MAX_SIZE).unwrap();
    let mut page = BitmapPage::new(2);
    page.persist(&mut store).unwrap();

    let new_memory_page = store.read_page(2).unwrap();
    let header: &dyn BitmapHeader = &new_memory_page;

    assert_eq!(2, header.page_id());
    assert_eq!(BITMAP_PAGE_COUNT - 1, header.free_page_count());
    assert_eq!(2, header.first_managed_page_id());
    assert_eq!(1, header.first_free_page_index());
}
