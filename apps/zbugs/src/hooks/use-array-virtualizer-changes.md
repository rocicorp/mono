# Scroll Capture/Restore Changes in `use-array-virtualizer.ts`

## Scroll capture always uses permalink state

`captureAnchorState()` was rewritten to always produce a permalink-based
snapshot. It finds the **first fully visible row** in the viewport (the first
virtual item whose `start >= scrollOffset`, skipping overscan items that are
partially above the viewport) and returns:

- `permalinkID` — the row's `id`
- `index` — the logical data index of that row
- `scrollOffset` — the signed pixel offset from that row's top to the current
  scroll position (always ≤ 0, meaning "the viewport is this many pixels above
  the row's top edge")

Returns `undefined` if no visible row with an `id` exists.

## Simplified `ScrollRestorationState` type

Replaced the old generic `{ anchor: AnchorState<TSort>, scrollOffset }` with a
flat, non-generic struct:

```typescript
type ScrollRestorationState = {
  permalinkID: string;
  index: number;
  scrollOffset: number;
};
```

This removed the `TSort` generic from `UseArrayVirtualizerReturn<T>` as well.

## Restore handles `undefined` → scroll to top

`restoreAnchorState(state)` now accepts `ScrollRestorationState | undefined`.
When `undefined`, it resets to a forward anchor at index 0 and scrolls to
offset 0 (top of list). When defined, it sets a permalink anchor and uses
`pendingScrollIsRelative = true` so the stored offset is applied relative to the
target row's computed position.

## Positioning stability fix for cache-miss scenarios

After a page reload the measurement cache is empty, so `getOffsetForIndex()`
returns estimate-based positions. The old code declared positioning complete as
soon as `scrollToOffset(target)` matched `scrollOffset`, but subsequent
`ResizeObserver` measurements would shift items, leaving the viewport in the
wrong place.

Fix: added `lastTargetOffset` tracking to `scrollStateRef`. Positioning now
requires the computed `targetOffset` to be **stable across two consecutive
effect runs** (within 1px) before finalizing. This ensures item measurements
have settled before the positioning loop exits, preventing drift after reload.
