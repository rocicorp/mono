# ``ZeroVirtual``

Long lists that page their rows in from Zero queries, for SwiftUI, UIKit,
and AppKit (`@rocicorp/zero-virtual`).

## Overview

A ``ZeroVirtualizer`` runs at most three queries: a page of rows around
where the list is scrolled, or, opened at a row, that row and a page either
side of it. It drops rows far from the screen. What's on screen stays put as
pages load, rows go, and rows change height.

```swift
import ZeroVirtual

let issues = ZeroVirtualizer(
  zero: zero, listContext: filter, estimatedRowHeight: 56,
  rowKey: \.id, toStartRow: { IssueStart(id: $0.id, modified: $0.modified) },
  pageQuery: { page, filter in IssueQueries.page(filter, page) },
  rowQuery: { row, filter in IssueQueries.byID(filter, row.id) })
```

`listContext` is what the list shows, such as its filter and sort. A new
value starts the list over. `pageQuery` returns `limit` rows after (or
before) a start row in the list's order, and `rowQuery` the row an id names,
if it's in the list.

Render it with ``VirtualList`` in SwiftUI, or ``VirtualListView`` in UIKit
and AppKit:

```swift
VirtualList(issues, selection: $selected) { item, isSelected in
  IssueRow(issue: item.row, highlighted: isSelected)
}

let list = VirtualListView(issues) { item, reusing in
  let row = reusing as? IssueRowView ?? IssueRowView()
  row.show(item.row)
  return row
}
```

## Open at a row, or where the list was

Pass `permalinkID` to open the list at a row, loading it first. Pass
`scrollState` to open it where it was. ``ZeroVirtualizer/onScrollStateChange``
reports where the list is on every scroll and page; save the latest and
pass it back, or call ``ZeroVirtualizer/restore(_:)``. A state is
`Codable` when the start row and list context are, so it can be encoded
into `@SceneStorage`.

A state belongs to one `listContext` and is ignored by any other. To bring
each of several lists back where it was, such as the folders of a mail
app, keep a state and a selection for each:

```swift
var scroll: [Folder.ID: VirtualScrollState<IssueStart, Filter>] = [:]
var selected: [Folder.ID: Issue.ID] = [:]

func show(_ folder: Folder.ID) {
  selection = selected[folder]
  let list = ZeroVirtualizer(
    zero: zero, listContext: Filter(folder: folder), /* … */
    scrollState: scroll[folder])
  list.onScrollStateChange = { state in scroll[folder] = state }
  issues = list
}
```

Update `selected[folder]` whenever the selection changes.

Pass `permalinkID: selected[folder]` instead to open centred on the
selected row.

## Selection in a compact split view

In a compact `NavigationSplitView`, only a `List` selection pushes the next
column. With ``VirtualList``, choose the column yourself:

```swift
@State private var column = NavigationSplitViewColumn.sidebar

NavigationSplitView(preferredCompactColumn: $column) {
  VirtualList(issues, selection: Binding(
    get: { selected },
    set: { selected = $0; if $0 != nil { column = .detail } }
  )) { item, isSelected in IssueRow(issue: item.row, highlighted: isSelected) }
} detail: {
  IssueDetail(id: selected)
}
```

A tap sets the selection even to the selected row, so this opens the detail
again after the user goes back.

## Accessibility and UI tests

Make each ``VirtualListView`` row view an accessibility element. Otherwise
AppKit and UIKit expose only its subviews, and drop the row's identifier and
the selected state the list sets on it:

```swift
// AppKit
row.setAccessibilityElement(true)
row.setAccessibilityRole(.group)
// UIKit
row.isAccessibilityElement = true
```

``VirtualList`` rows are SwiftUI views; use the accessibility modifiers as
usual.

## Topics

### Lists

- ``ZeroVirtualizer``
- ``VirtualList``
- ``VirtualListView``

### Position

- ``VirtualScrollState``
- ``VirtualScrollAlignment``
