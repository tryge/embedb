use crate::io::bitmap::{BitmapPage, BITMAP_PAGE_COUNT};
use crate::io::index::IndexPage;
use crate::io::store::PageStore;
use tempfile::tempfile;

#[test]
fn grow_from_first_bitmap() {
    let page = BitmapPage::new(2);
    let index = IndexPage::grow(page);

    assert_eq!(BITMAP_PAGE_COUNT as u32 + 3, index.page_id);
    assert_eq!(2, index.first_managed_page_id);
    assert_eq!(2, index.current_bitmap_count);
    assert_eq!(1, index.current_bitmap_idx);
    assert_eq!(0, index.first_free_bitmap_idx);
    assert_eq!(2, index.dirty_bitmaps.len());
}

#[test]
fn cannot_load_index() {
    let mut store = temporary_store();

    let page = BitmapPage::new(2);
    let mut index = IndexPage::grow(page);

    index.persist(&mut store).unwrap();

    let index_memory = store.read_page(1 + BITMAP_PAGE_COUNT as usize).unwrap();

    let result = IndexPage::load(&index_memory, &store, |_| false);
    assert!(result.is_none());
}

#[test]
fn persist_and_load() {
    let mut store = temporary_store();

    let page = BitmapPage::new(2);
    let mut index = IndexPage::grow(page);

    index.persist(&mut store).unwrap();

    let index_memory = store.read_page(3 + BITMAP_PAGE_COUNT as usize).unwrap();

    let loaded = IndexPage::load(&index_memory, &store, |_| true).unwrap();

    assert_eq!(2, loaded.first_managed_page_id);
    assert_eq!(2, loaded.current_bitmap_count);
    assert_eq!(0, loaded.current_bitmap_idx);
    assert_eq!(0, loaded.first_free_bitmap_idx);
    assert_eq!(2, loaded.dirty_bitmaps.len());
}

#[test]
fn grow_on_load() {
    let mut store = temporary_store();

    let page = BitmapPage::new(2);
    let mut index = IndexPage::grow(page);

    index.persist(&mut store).unwrap();

    let index_memory = store.read_page(3 + BITMAP_PAGE_COUNT as usize).unwrap();

    let loaded = IndexPage::load(&index_memory, &store, |x| x > 2 * BITMAP_PAGE_COUNT as u32 + 2).unwrap();

    assert_eq!(2, loaded.first_managed_page_id);
    assert_eq!(3, loaded.current_bitmap_count);
    assert_eq!(2, loaded.current_bitmap_idx);
    assert_eq!(0, loaded.first_free_bitmap_idx);
    assert_eq!(2, loaded.dirty_bitmaps.len());
}

#[test]
fn allocate_and_free() {
    let store = temporary_store();

    let page = BitmapPage::new(2);
    let mut index = IndexPage::grow(page);

    let page = index.allocate(&store, &mut |_| true).unwrap();

    let freed = index.free(page, &store, &mut |_| true).unwrap();

    assert!(freed);
}

#[test]
fn free_on_full_bitmap() {
    let store = temporary_store();

    let mut page = BitmapPage::new(2);
    for _ in 1..BITMAP_PAGE_COUNT {
        page.allocate(|_| true).unwrap();
    }

    let mut index = IndexPage::grow(page);
    assert_eq!(1, index.first_free_bitmap_idx);

    let freed = index.free(3, &store, &mut |_| true).unwrap();
    assert!(freed);
}

#[test]
fn allocate_full_bitmap() {
    let store = temporary_store();

    let mut page = BitmapPage::new(2);
    for _ in 2..BITMAP_PAGE_COUNT {
        page.allocate(|_| true).unwrap();
    }

    let mut index = IndexPage::grow(page);
    index.current_bitmap_idx = 0;

    index.allocate(&store, &mut |_| true).unwrap();
    assert_eq!(1, index.first_free_bitmap_idx);
    assert_eq!(0, index.current_bitmap_idx);
}

#[test]
fn allocate_two_full_bitmaps() {
    let store = temporary_store();

    let mut page = BitmapPage::new(2);
    for _ in 1..BITMAP_PAGE_COUNT {
        page.allocate(|_| true).unwrap();
    }

    let mut index = IndexPage::grow(page);
    for _ in 2..BITMAP_PAGE_COUNT {
        index.allocate(&store, &mut |_| true).unwrap();
    }
    assert_eq!(2, index.first_free_bitmap_idx);
    assert_eq!(1, index.current_bitmap_idx);

    let freed = index.free(3 + BITMAP_PAGE_COUNT as u32, &store, &mut |_| true).unwrap();
    assert!(freed);

    assert_eq!(1, index.first_free_bitmap_idx);
    assert_eq!(1, index.current_bitmap_idx);

    let page = index.allocate(&store, &mut |_| true).unwrap();
    assert_ne!(3 + BITMAP_PAGE_COUNT as u32, page);
    assert_eq!(2, index.current_bitmap_idx);
}

fn temporary_store() -> PageStore {
    let file = tempfile().unwrap();
    let store = PageStore::new(file, 3 * 4080 * 8 * 4096 + 2).unwrap();
    store
}
