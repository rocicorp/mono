# zero-virtual tests in ZeroVirtual

`ZeroVirtual` is a native counterpart of
[`@rocicorp/zero-virtual`](https://github.com/rocicorp/zero-virtual), not a
port of its code. Its behavior tests are ported by hand from the upstream
tests at commit
[`f89c468`](https://github.com/rocicorp/zero-virtual/commit/f89c4685b748fb80fba1077c8c9bcdb29b73326e),
keeping their names, with the DOM replaced by a fake scroll view
(`Harness.swift`) whose geometry the test sets.

When zero-virtual changes, diff its tests from this commit and update this
list.

- **ported**: same steps and expectations.
- **adapted**: same behavior, set up or checked the native way (noted).
- **n/a**: a feature ZeroVirtual doesn't have, or a browser detail with no
  native counterpart (noted).

## `src/core/rows.test.ts` → `RowsTests.swift`

| Test                                                              | Status                                                                  |
| ----------------------------------------------------------------- | ----------------------------------------------------------------------- |
| %s pages exclude the sentinel and retain query order              | ported                                                                  |
| permalink pages reserve the center row and exclude both sentinels | ported                                                                  |
| a lone permalink retains the existing empty-window semantics      | ported                                                                  |
| row lookup stays lazy for mutable query arrays                    | n/a: Swift arrays are values, so there is nothing to see a later change |

## `src/solid/create-rows.test.ts` → `RowsTests.swift`

| Test                                                   | Status                                                                                                                                               |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| forward anchor: main query is built and rows assemble  | adapted: "a page anchor asks for one row more than the page"                                                                                         |
| permalink anchor: stages queries 2/3 on query 1 result | adapted: "a permalink looks its row up, then pages either side of it", and `ZeroVirtualizerTests.opensAtAPermalinkWithAPageEitherSide` over a client |

## `src/core/virtualizer.test.ts` → `VirtualizerTests.swift`

| Test                                                                  | Status                                                               |
| --------------------------------------------------------------------- | -------------------------------------------------------------------- |
| ZeroVirtualizer snapshot — items, space and total (6 cases)           | ported                                                               |
| items carry the correct index and row                                 | ported                                                               |
| explicit count overrides estimated total                              | n/a: no `count` (an estimate is enough)                              |
| explicit zero count reports empty list                                | n/a: no `count`                                                      |
| items survive metadata updates but change with row keys and data      | n/a: snapshots are values; SwiftUI diffs by key                      |
| attachment cleanup runs once per container and restores its style     | n/a: there are no DOM styles or observers to restore                 |
| changing count invalidates the cached snapshot                        | n/a: no `count`                                                      |
| changing estimateSize invalidates the cached snapshot                 | ported                                                               |
| scrolling notifies the settled → false transition                     | ported                                                               |
| snapshot identity is cached until state changes                       | n/a: snapshots are values                                            |
| setRows/setOptions are silent; afterDOMUpdate notifies on transitions | ported (`afterUpdate`)                                               |
| warns once when listContextParams churns identity without content     | n/a: list contexts compare by value, so there is no churn to warn of |
| identity-churn warning is best-effort for non-serializable params     | n/a: as above                                                        |
| query inputs fall back to the new context after a context change      | ported                                                               |
| lanes (grid): all 5 tests                                             | n/a: no grids yet                                                    |
| masonry: both tests                                                   | n/a: no masonry yet                                                  |
| minPageSize: all 3 tests                                              | ported                                                               |

## `src/core/virtualizer.dom.test.ts` → `ScrollTests.swift`

| Test                                                                               | Status                                                                                                                                |
| ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| **paging against a real (fake-geometry) scroll container**                         |                                                                                                                                       |
| initial load anchors at the top and fills one page                                 | ported                                                                                                                                |
| scrolling near the window end advances the forward anchor                          | ported                                                                                                                                |
| scrolling back near the window start pages backward                                | ported                                                                                                                                |
| a far jump into unloaded space below recovers by cascading pages                   | adapted: the space below comes from having seen the rows, not `count`                                                                 |
| a far jump back to the very top re-anchors at the start                            | adapted: as above                                                                                                                     |
| a page too small for what is on screen grows instead of paging back and forth      | ported                                                                                                                                |
| **manual scroll anchoring**                                                        |                                                                                                                                       |
| growth above the anchor row is compensated into scrollTop                          | ported                                                                                                                                |
| shrinkage above the anchor row is compensated too                                  | ported                                                                                                                                |
| growth at scroll offset 0 is revealed, not compensated away                        | ported                                                                                                                                |
| growth below the anchor row does not move the viewport                             | ported                                                                                                                                |
| **a grid (lanes)**: all 4 tests                                                    | n/a: no grids yet                                                                                                                     |
| **scroll-state restore when the container mounts lazily**                          |                                                                                                                                       |
| restores the saved scrollTop when the element attaches on a later commit           | ported                                                                                                                                |
| does not persist a spurious scrollTop:0 while detached                             | ported                                                                                                                                |
| **reporting the scroll state**                                                     |                                                                                                                                       |
| every scroll is reported as it happens, not once the scroll ends                   | ported                                                                                                                                |
| **permalink scroll**                                                               |                                                                                                                                       |
| scrolls the permalink row into view when its row key differs from the permalink id | ported                                                                                                                                |
| %s anchoring: the row stays on top when the page grows under it                    | ported                                                                                                                                |
| the jump holds on while the rows before its target are still on their way          | adapted: `viewportInsets` for the CSS `scroll-padding-top`                                                                            |
| **scroll-state restore of a position we wrote**                                    |                                                                                                                                       |
| a later navigation back to it restores it                                          | adapted: `restore(_:)`, since a value can't be told apart by identity; also restores the same state twice                             |
| from a host that hands every write straight back: scrolling                        | ported                                                                                                                                |
| from a host that hands every write straight back: a jump                           | ported                                                                                                                                |
| **a restore that moves the loaded window**                                         |                                                                                                                                       |
| manual anchoring lands it where it was saved (first answer missing %i rows)        | ported                                                                                                                                |
| %s anchoring: a position the outgoing window was too short to reach still lands    | ported                                                                                                                                |
| %s anchoring: paging leaves the restored window alone until its rows are in        | ported                                                                                                                                |
| **compareStartRows**                                                               |                                                                                                                                       |
| a start row JSON cannot serialize survives the round trip                          | n/a: start rows are `Equatable`, compared with `==`                                                                                   |
| is what makes that work: without it the compare throws                             | n/a: as above                                                                                                                         |
| or narrow the start row in toStartRow: the anchor carries what it returns          | adapted: a narrower `Codable` start row survives saving                                                                               |
| **firstVisibleItem / lastVisibleItem**                                             |                                                                                                                                       |
| report the edges of the viewport, not of the loaded window                         | ported                                                                                                                                |
| follow the scroll position                                                         | ported                                                                                                                                |
| count a row that overlaps the scrollport at all                                    | ported                                                                                                                                |
| carry the row and its key, which scrollToItem takes                                | adapted: a key goes to `scrollToRow(key:)`                                                                                            |
| are undefined with no scroll element attached                                      | ported                                                                                                                                |
| **no-op re-anchor does not spin the render loop**                                  | ported                                                                                                                                |
| **below-viewport window at the start of the list**                                 | ported                                                                                                                                |
| **scrollToItem**                                                                   |                                                                                                                                       |
| a loaded row scrolls immediately, without re-anchoring                             | ported                                                                                                                                |
| an unloaded row re-anchors and the scroll lands once its page renders              | ported                                                                                                                                |
| is level-triggered: the same id twice scrolls twice                                | ported                                                                                                                                |
| align: defaults to auto / auto leaves a visible row / start / center / end         | ported                                                                                                                                |
| aligns below the container's scroll-padding (a sticky header)                      | adapted: `viewportInsets`                                                                                                             |
| keeps the space the target row's scroll-margin asks for                            | n/a: rows have no per-row margin; insets cover sticky headers                                                                         |
| a clamped jump still releases the request, so paging keeps working                 | ported                                                                                                                                |
| a loaded row keyed differently from its id is scrolled to, not re-fetched          | ported                                                                                                                                |
| addressing a rendered row by its key: scrolls, and issues no lookup at all         | ported                                                                                                                                |
| addressing a rendered row by its key: takes a numeric key                          | n/a: keys are typed                                                                                                                   |
| addressing a rendered row by its key: reaches rendered rows only                   | ported, plus `scrollToRow(key:)`                                                                                                      |
| a repeat jump to a row keyed differently from its id still lands                   | ported                                                                                                                                |
| an empty id is a no-op, not a lookup that never answers                            | ported                                                                                                                                |
| a second jump to a rendered row supersedes a lookup still in flight                | ported                                                                                                                                |
| a second jump supersedes one that is still loading                                 | ported                                                                                                                                |
| an id that does not exist: all 5 tests                                             | ported                                                                                                                                |
| a permalink that does not exist leaves the list alone even mid-lookup              | ported                                                                                                                                |
| a permalinkID that does not exist leaves a loaded list alone                       | ported                                                                                                                                |
| a deep link to a permalinkID that does not exist falls back to the top             | ported                                                                                                                                |
| **attachment geometry**                                                            |                                                                                                                                       |
| alignment and visibility with window scrolling=%s                                  | adapted: insets at both ends ("alignment keeps clear of the strips covering both ends of the viewport"); there is no window scrolling |
| moving a container updates its geometry without a resize notification              | n/a: frames are in content coordinates                                                                                                |
| touch growth stays held until scrollend with window scrolling=%s                   | n/a: a browser workaround; native scroll views take offset changes mid-fling. Checked on a device instead                             |
| reattachment falls back to the new viewport until its observer reports             | adapted: "follow the viewport's size"                                                                                                 |

## Stick to bottom → `StickToBottomTests.swift`

| Upstream                                                              | Status                                                                             |
| --------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `src/react/use-zero-virtualizer.test.ts`: the `useStickToBottom` case | adapted: "parked at the bottom, it follows new rows and a last row growing taller" |
| `demo/react/e2e/tests/stick.spec.ts`                                  | adapted: "scrolled away, growth does not follow; back at the bottom, it re-arms"   |

## Not ported

- `src/core/virtualizer.masonry.dom.test.ts`: no masonry yet.
- `src/react/use-history-scroll-state.test.ts`,
  `src/solid/create-history-scroll-state.test.ts`: the Navigation API
  helpers. Apps save `onScrollStateChange`'s state themselves (zbugs-swift
  in `SceneStorage`).
- The rest of the React and Solid binding tests: framework wiring. The
  SwiftUI, UIKit, and AppKit adapters have their own tests.
- `demo/react/e2e`: the browser demo, other than `stick.spec.ts`.

## Swift's own

- `ZeroVirtualizerTests.swift`: the virtualizer over a client and a server
  the test plays: paging, a permalink, a jump, and a new list context.
- "rows of different heights page down and back without moving what is on
  screen", "paging works the same when the scroll view reports scrolls as
  they happen".
