---
'zero-swift': patch
---

`VirtualList` and `VirtualListView` make and measure only the rows within a viewport's height of the viewport. Rows further away use their last measured height, or the estimated row height, until they come near. A row that turns out taller or shorter than its estimate doesn't move what's on screen, and jumps line up again once their row is measured. Before this, the first layout after a sync measured every loaded row.
