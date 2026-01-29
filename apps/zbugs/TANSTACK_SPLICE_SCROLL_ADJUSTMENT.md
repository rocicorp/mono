# Tanstack Virtual Splice Scroll Adjustment

## Problem

When performing splice operations (delete + insert) on a Tanstack Virtual list with dynamic item heights, the scroll position shifts unexpectedly. This breaks the user experience as the item they were viewing jumps to a different position on the screen.

## Root Cause

Tanstack Virtual's `shouldAdjustScrollPositionOnItemSizeChange` doesn't handle splice operations natively. The core issues are:

1. **Stale ScrollOffset Period**: After manually adjusting `scrollTop`, there's a period where `virtualizer.scrollOffset` remains stale until the scroll event fires
2. **Additional Unwanted Adjustments**: During this stale period, items that come into view trigger `resizeItem` and cause additional scroll adjustments that compound with the manual adjustment
3. **Replacement Size Differences**: When replacing items (delete N, insert N), the new items start with default heights but the deleted items had actual measured heights - this size difference needs to be accounted for

## Solution Approach

Implement manual scroll adjustment by:

1. **Calculate the height delta** from deleted/inserted items
2. **Manually adjust scrollTop** to compensate
3. **Block all scroll adjustments during the stale period** to prevent compounding
4. **For replacements, calculate size difference** and add to manual adjustment
5. **Use correct check item** to determine if adjustment should apply

## Implementation Details

### 1. State Tracking with Refs

```typescript
const pendingSpliceAdjustmentRef = useRef(0);
const oldScrollOffsetRef = useRef(0);
const spliceReplacedIndicesRef = useRef<Set<number>>(new Set());
```

- `pendingSpliceAdjustmentRef`: Tracks the manual adjustment being applied
- `oldScrollOffsetRef`: Stores the scrollOffset before manual adjustment to detect when it becomes stale
- `spliceReplacedIndicesRef`: Tracks which items were replaced (not just deleted or inserted)

### 2. Override shouldAdjustScrollPositionOnItemSizeChange

```typescript
virtualizer.shouldAdjustScrollPositionOnItemSizeChange = (
  item,
  delta,
  instance,
) => {
  const isScrollOffsetStale =
    instance.scrollOffset === oldScrollOffsetRef.current;

  // Clear pending adjustment once scrollOffset updates
  if (!isScrollOffsetStale && pendingSpliceAdjustmentRef.current !== 0) {
    console.log('Scroll offset updated, clearing pending splice adjustment');
    pendingSpliceAdjustmentRef.current = 0;
  }

  // CRITICAL: During splice (while scrollOffset is stale), return FALSE for ALL items
  // This prevents additional scroll adjustments from items that come into view
  if (isScrollOffsetStale && pendingSpliceAdjustmentRef.current !== 0) {
    return false;
  }

  // Normal logic continues...
  return item.start < instance.scrollOffset;
};
```

### 3. Splice Handler Logic

```typescript
const handleSplice = () => {
  const idx = spliceIndex;
  const delCnt = deleteCount;
  const insCnt = insertCount;

  // Store old scroll offset BEFORE any changes
  oldScrollOffsetRef.current = virtualizer.scrollOffset;

  // Clear replaced indices from previous operation
  spliceReplacedIndicesRef.current.clear();

  // Track which items are being replaced
  const replacedCount = Math.min(delCnt, insCnt);
  for (let i = 0; i < replacedCount; i++) {
    spliceReplacedIndicesRef.current.add(idx + i);
  }

  // Get heights of items being deleted
  const deletedItemHeights: number[] = [];
  for (let i = 0; i < delCnt; i++) {
    const item = virtualItems.find(vi => vi.index === idx + i);
    deletedItemHeights.push(item?.size ?? DEFAULT_HEIGHT);
  }

  // Calculate size delta for replaced items
  let manualHeightDelta = 0;
  if (replacedCount > 0) {
    const oldReplacedSize = deletedItemHeights
      .slice(0, replacedCount)
      .reduce((a, b) => a + b, 0);
    const newReplacedSize = replacedCount * DEFAULT_HEIGHT;
    manualHeightDelta = newReplacedSize - oldReplacedSize;
  }

  // Perform the splice
  const deleted = rows.splice(idx, delCnt, ...newRows);

  // Calculate scroll adjustment for pure deletions/insertions
  const pureDeleteCount = delCnt - replacedCount;
  const pureInsertCount = insCnt - replacedCount;
  const deletedHeight = deletedItemHeights
    .slice(replacedCount)
    .reduce((a, b) => a + b, 0);
  const insertedHeight = pureInsertCount * DEFAULT_HEIGHT;

  // Determine if we need to adjust scroll
  const checkIndex =
    replacedCount > 0
      ? replacedCount // First item AFTER replaced items
      : delCnt > 0
        ? idx + delCnt - 1
        : idx;

  const checkItem = virtualItems.find(vi => vi.index === checkIndex);
  const shouldAdjust = checkItem && checkItem.start < virtualizer.scrollOffset;

  if (shouldAdjust) {
    const totalManualDelta =
      -(deletedHeight - insertedHeight) + manualHeightDelta;
    const newScrollOffset = virtualizer.scrollOffset + totalManualDelta;

    // Store pending adjustment
    pendingSpliceAdjustmentRef.current = totalManualDelta;

    // Apply manual scroll adjustment
    if (parentRef.current) {
      parentRef.current.scrollTop = newScrollOffset;
    }
  }
};
```

### 4. Key Implementation Points

#### Check Item Selection

- **For pure insertions**: Use the first item being pushed down (at `idx`)
- **For pure deletions**: Use the last deleted item (at `idx + delCnt - 1`)
- **For replacements**: Use the first item AFTER replaced items (at `idx + replacedCount`)

This ensures the check item is NOT a replaced item, which would return false and block the manual adjustment.

#### Size Calculation

- Deleted items: Use actual measured sizes from `virtualItems[i].size`
- Inserted items: Use `DEFAULT_HEIGHT` (they haven't been measured yet)
- Replaced items: Calculate difference between old and new sizes

#### Manual Adjustment Formula

```typescript
const totalManualDelta = -(deletedHeight - insertedHeight) + manualHeightDelta;
```

- Negative because we're compensating for height changes
- `deletedHeight - insertedHeight`: Net height change from pure deletions/insertions
- `manualHeightDelta`: Additional adjustment for size difference in replaced items

## Testing Checklist

Test all five splice scenarios with items at **positive positions** (below scrollOffset):

1. ✅ **Pure Deletion** (`del > 0, ins = 0`): e.g., splice(0, 10, 0)
2. ✅ **Pure Insertion** (`del = 0, ins > 0`): e.g., splice(0, 0, 10)
3. ✅ **Pure Replacement** (`del = ins`): e.g., splice(0, 5, 5)
4. ✅ **More Deletions** (`del > ins > 0`): e.g., splice(0, 10, 5)
5. ✅ **More Insertions** (`ins > del > 0`): e.g., splice(0, 5, 10)

Also test in different viewport positions:

- ✅ **Above viewport** (index 0): Should adjust scrollOffset
- ✅ **Within viewport** (index 90-95): Should handle replacements
- ✅ **After viewport** (index 150+): Should not affect scroll (no adjustment needed)

## Success Criteria

- Selected row maintains its **exact** visual position on screen (0px shift, or 1-2px due to rounding)
- scrollOffset adjusts appropriately to compensate for height changes
- No compounding scroll adjustments during the operation
- Works for all splice combinations and viewport positions

## Known Limitations

The fix is specifically needed for items with **positive positions** (item.start > scrollOffset, visible within or below viewport). Items with **negative positions** (above viewport) work correctly with Tanstack's default behavior and don't need this adjustment.

Note: During the brief period after manual scroll adjustment (before the scroll event fires), all scroll adjustments are intentionally blocked to prevent compounding. This is part of the solution, not a limitation.

## Integration Notes

This approach can be adapted for any Tanstack Virtual implementation with dynamic heights. The key requirements are:

1. Access to `virtualizer.scrollOffset` and `virtualItems`
2. Ability to override `shouldAdjustScrollPositionOnItemSizeChange`
3. Refs to track state across React renders
4. Manual control over `scrollTop` via the scroll container ref
