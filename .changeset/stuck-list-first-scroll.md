---
'zero-swift': patch
---

A `VirtualList` or `VirtualListView` stuck to the bottom (`stickToBottom`) no longer pulls the user's first scroll up back to the bottom. When the list tried to scroll past where it could, its next scroll, the user's, was taken for its own, and the list stayed stuck.
