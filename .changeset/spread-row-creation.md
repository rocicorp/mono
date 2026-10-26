---
'zero-swift': patch
---

`VirtualList` and `VirtualListView` reuse row views, as `UICollectionView` reuses cells: a row that scrolls away is hidden and its view (for `VirtualList`, its hosting controller) is given to the next row that comes, with the row's key as its identity, so state in a row's view stays with its row as in a `List`. Views beyond those kept are released a few per frame. Rows on screen are still made and measured in the pass that shows them; the rest of a newly loaded page is made in the frames after, nearest the screen first, up to a few milliseconds each frame, so loading the next page while scrolling no longer drops frames.
