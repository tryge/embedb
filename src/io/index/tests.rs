use crate::io::bitmap::{BitmapPage, BITMAP_PAGE_COUNT};
use crate::io::index::IndexPage;
use crate::io::store::PageStore;
use tempfile::tempfile;

#[test]
fn grow_from_first_bitmap() {
    let page = BitmapPage::new(2);
    let index = IndexPage::grow(page);

    assert_eq!(BITMAP_PAGE_COUNT as u32 + 1, index.page_id);
    assert_eq!(2, index.first_managed_page_id);
    assert_eq!(2, index.current_bitmap_count);
    assert_eq!(1, index.current_bitmap_idx);
    assert_eq!(0, index.first_free_bitmap_idx);
    assert_eq!(2, index.dirty_bitmaps.len());
}

#[test]
fn persist_and_load() {
    let mut store = temporary_store();

    let page = BitmapPage::new(2);
    let mut index = IndexPage::grow(page);

    index.persist(&mut store).unwrap();

    let index_memory = store.read_page(1 + BITMAP_PAGE_COUNT as usize).unwrap();

    let loaded = IndexPage::load(&index_memory, &store, |_| true).unwrap();

    assert_eq!(2, loaded.first_managed_page_id);
    assert_eq!(2, loaded.current_bitmap_count);
    assert_eq!(0, loaded.current_bitmap_idx);
    assert_eq!(0, loaded.first_free_bitmap_idx);
    assert_eq!(1, loaded.dirty_bitmaps.len());
}

fn temporary_store() -> PageStore {
    let file = tempfile().unwrap();
    let store = PageStore::new(file, 3 * 4080 * 8 * 4096 + 2).unwrap();
    store
}
