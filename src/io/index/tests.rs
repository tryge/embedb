use crate::io::bitmap::{BitmapPage, BITMAP_PAGE_COUNT};
use crate::io::index::IndexPage;

#[test]
fn grow_from_first_bitmap() {
    let page = BitmapPage::new(2);
    let index = IndexPage::grow(&page);

    assert_eq!(BITMAP_PAGE_COUNT as u32 + 1, index.page_id);
    assert_eq!(2, index.first_managed_page_id);
    assert_eq!(2, index.current_bitmap_count);
    assert_eq!(1, index.current_bitmap_idx);
    assert_eq!(0, index.first_free_bitmap_idx);
    assert_eq!(1, index.dirty_bitmaps.len());
}
