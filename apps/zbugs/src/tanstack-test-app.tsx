import {useVirtualizer} from '@tanstack/react-virtual';
import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from 'react';

interface RowData {
  id: string;
  content: string;
  multiplier: number;
}

const DEFAULT_HEIGHT = 100;
const MAX_INSERT_COUNT = 10000;
const MAX_MULTIPLIER = 50;
const MIN_MULTIPLIER = 1;

// Mulberry32 PRNG - simple and fast seeded random number generator
function mulberry32(seed: number) {
  return function () {
    let t = (seed += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function seededId(rng: () => number): string {
  // Generate a deterministic ID using the RNG
  const chars =
    'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let id = '';
  for (let i = 0; i < 12; i++) {
    id += chars.charAt(Math.floor(rng() * chars.length));
  }
  return id;
}

function generateRows(
  count: number,
  startIndex: number = 0,
  seed: number = 12345,
): RowData[] {
  const rng = mulberry32(seed + startIndex);
  return Array.from({length: count}, (_, i) => {
    const index = startIndex + i;
    // Random multiplier (1-30)
    const multiplier = Math.floor(rng() * 10) + 1;
    const baseSentence =
      'Lorem ipsum dolor sit amet, consectetur adipiscing elit. ';
    return {
      id: seededId(rng),
      content: `Row ${index} - ${baseSentence.repeat(multiplier)}`,
      multiplier,
    };
  });
}

const seed = 12345;

/**
 * Snapshot of anchor state before a mutation.
 */
interface AnchorSnapshot {
  key: string;
  index: number; // index in original rows array
  pixelOffset: number; // item.start - scrollOffset (distance from viewport top)
  absoluteStart: number; // item.start (for fallback if anchor is deleted)
  spliceIndex: number; // where the splice happened
  deleteCount: number; // how many items were deleted
  insertCount: number; // how many items were inserted
  correctionCount: number; // how many correction passes we've done
}

export function TanstackTestApp() {
  const [rows, setRows] = useState<RowData[]>(() => generateRows(100, 0, seed));
  const [selectedRowId, setSelectedRowId] = useState<string>('');
  const [textMultiplier, setTextMultiplier] = useState<string>('1');
  const [forceAdjust, setForceAdjust] = useState<boolean | null>(null);
  const [debug, setDebug] = useState<boolean>(false);
  const [index, setIndex] = useState<string>('0');
  const [deleteCount, setDeleteCount] = useState<string>('0');
  const [insertCount, setInsertCount] = useState<string>('10');

  const parentRef = useRef<HTMLDivElement>(null);
  const checkboxRef = useRef<HTMLInputElement>(null);

  // Size cache: key -> measured height in pixels
  const sizeCacheRef = useRef(new Map<string, number>());

  // Anchor snapshot captured before mutations
  const anchorRef = useRef<AnchorSnapshot | null>(null);

  // Track the previous rows to detect mutations
  const prevRowsRef = useRef<RowData[]>(rows);

  // Update checkbox indeterminate state
  useEffect(() => {
    if (checkboxRef.current) {
      checkboxRef.current.indeterminate = forceAdjust === null;
    }
  }, [forceAdjust]);

  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: useCallback(() => DEFAULT_HEIGHT, []),
    getItemKey: useCallback(
      (index: number) => rows[index]?.id ?? index,
      [rows],
    ),
    overscan: 5,
    debug,
  });

  // Disable Tanstack's automatic scroll adjustments - we handle everything ourselves
  virtualizer.shouldAdjustScrollPositionOnItemSizeChange = () => false;

  // Expose virtualizer and helpers for testing
  useEffect(() => {
    // oxlint-disable-next-line no-explicit-any
    const w = window as any;
    w.virtualizer = virtualizer;
    w.sizeCacheRef = sizeCacheRef;
    w.anchorRef = anchorRef;
  }, [virtualizer]);

  // Wrapped measureElement that updates our size cache and handles resize adjustments
  const measureElement = useCallback(
    (node: HTMLElement | null) => {
      if (node) {
        const key = node.getAttribute('data-key');
        if (key) {
          const rect = node.getBoundingClientRect();
          const newSize = rect.height;
          const oldSize = sizeCacheRef.current.get(key);

          // Update our cache
          sizeCacheRef.current.set(key, newSize);

          // Always handle resize-above-viewport for items we've seen before
          // For new items (no oldSize), we track them but don't adjust yet
          if (oldSize !== undefined && oldSize !== newSize) {
            // Find this item's position in CURRENT rows
            const rowIndex = rows.findIndex(r => r.id === key);
            if (rowIndex !== -1) {
              // Use DOM scrollTop directly - virtualizer.scrollOffset may be stale
              // when multiple ResizeObserver callbacks fire in the same frame
              const scrollElement = parentRef.current;
              const scrollOffset = scrollElement?.scrollTop ?? 0;
              // Use virtualizer's measurementsCache to check if above viewport
              const measurement = virtualizer.measurementsCache[rowIndex];
              if (measurement && measurement.start < scrollOffset) {
                const delta = newSize - oldSize;
                console.log(
                  `[MEASURE] Resize above viewport: key=${key}, oldSize=${oldSize}, newSize=${newSize}, delta=${delta}`,
                );
                // Use DOM scrollTop directly for the same reason
                if (scrollElement) {
                  scrollElement.scrollTop = scrollOffset + delta;
                }
              }
            }
          }
        }
      }
      // Always call through to Tanstack's measureElement
      virtualizer.measureElement(node);
    },
    [virtualizer, rows],
  );

  // // Configure scroll position adjustment behavior
  // // We disable Tanstack's automatic adjustments and handle everything ourselves
  // virtualizer.shouldAdjustScrollPositionOnItemSizeChange = (
  //   item: {index: number; start: number; size: number; end: number},
  //   delta: number,
  //   instance: typeof virtualizer,
  // ) => {
  //   // If this is the selected row being tested, check the tristate value
  //   if (selectedRowId) {
  //     const rowIndex = rows.findIndex(r => r.id === selectedRowId);
  //     if (rowIndex === item.index) {
  //       if (forceAdjust !== null) {
  //         return forceAdjust;
  //       }
  //     }
  //   }

  //   // Disabled for manual testing
  //   return false;
  // };

  const handleChangeText = (showAlerts = false): boolean => {
    const multiplier = parseInt(textMultiplier, 10);
    if (
      isNaN(multiplier) ||
      multiplier < MIN_MULTIPLIER ||
      multiplier > MAX_MULTIPLIER
    ) {
      if (showAlerts) {
        alert(
          `Text multiplier must be between ${MIN_MULTIPLIER} and ${MAX_MULTIPLIER}`,
        );
      }
      return false;
    }

    const rowIndex = rows.findIndex(r => r.id === selectedRowId);
    if (rowIndex === -1) {
      if (showAlerts) {
        alert('Row not found');
      }
      return false;
    }

    setRows(prev => {
      const updated = [...prev];
      const baseSentence =
        'Lorem ipsum dolor sit amet, consectetur adipiscing elit. ';
      const newContent = `Row ${rowIndex} - ${baseSentence.repeat(multiplier)}`;
      updated[rowIndex] = {
        ...updated[rowIndex],
        content: newContent,
        multiplier,
      };
      return updated;
    });

    return true;
  };

  const handleTextInputChange = (value: string) => {
    setTextMultiplier(value);
    const multiplier = parseInt(value, 10);
    if (
      !isNaN(multiplier) &&
      multiplier >= MIN_MULTIPLIER &&
      multiplier <= MAX_MULTIPLIER &&
      selectedRowId
    ) {
      const rowIndex = rows.findIndex(r => r.id === selectedRowId);
      if (rowIndex !== -1) {
        setRows(prev => {
          const updated = [...prev];
          const baseSentence =
            'Lorem ipsum dolor sit amet, consectetur adipiscing elit. ';
          const newContent = `Row ${rowIndex} - ${baseSentence.repeat(multiplier)}`;
          updated[rowIndex] = {
            ...updated[rowIndex],
            content: newContent,
            multiplier,
          };
          return updated;
        });
      }
    }
  };

  const handleSplice = () => {
    const idx = parseInt(index, 10);
    const delCnt = parseInt(deleteCount, 10);
    const insCnt = parseInt(insertCount, 10);

    if (isNaN(idx)) {
      alert('Invalid index');
      return;
    }

    if (isNaN(delCnt) || delCnt < 0) {
      alert('Delete count must be 0 or greater');
      return;
    }

    if (isNaN(insCnt) || insCnt < 0 || insCnt > MAX_INSERT_COUNT) {
      alert(`Insert count must be between 0 and ${MAX_INSERT_COUNT}`);
      return;
    }

    if (idx < 0 || idx > rows.length) {
      alert(`Index must be between 0 and ${rows.length}`);
      return;
    }

    if (idx + delCnt > rows.length) {
      alert(
        `Cannot delete ${delCnt} rows from index ${idx}. Only ${rows.length - idx} rows available.`,
      );
      return;
    }

    // Capture anchor before mutation - find the first item that starts at or after scrollOffset
    const scrollOffset = virtualizer.scrollOffset ?? 0;
    const virtualItems = virtualizer.getVirtualItems();

    // Find the first item whose start is >= scrollOffset (first fully visible or partially visible at top)
    let anchorItem = virtualItems.find(item => item.start >= scrollOffset);
    // If none found, use the first item (it's partially visible at top)
    if (!anchorItem && virtualItems.length > 0) {
      anchorItem = virtualItems[0];
    }

    if (anchorItem) {
      const row = rows[anchorItem.index];
      if (row) {
        // Use actual DOM position to avoid sub-pixel rounding errors in Tanstack's computed positions
        // This prevents drift when doing repeated splices
        const scrollElement = parentRef.current;
        // Use Tanstack's elementsCache - it's keyed by item key and already tracks connected elements
        const anchorElement = virtualizer.elementsCache.get(row.id) as
          | HTMLElement
          | undefined;

        let pixelOffset: number;
        if (anchorElement && scrollElement) {
          // Use actual DOM position
          const anchorRect = anchorElement.getBoundingClientRect();
          const scrollRect = scrollElement.getBoundingClientRect();
          pixelOffset = anchorRect.top - scrollRect.top;
        } else {
          // Fallback to Tanstack's computed position
          pixelOffset = anchorItem.start - scrollOffset;
        }

        // Store splice info along with anchor
        anchorRef.current = {
          key: row.id,
          index: anchorItem.index,
          pixelOffset,
          absoluteStart: anchorItem.start,
          spliceIndex: idx,
          deleteCount: delCnt,
          insertCount: insCnt,
          correctionCount: 0,
        };
        console.log(
          `[ANCHOR] Captured: key=${row.id}, index=${anchorItem.index}, pixelOffset=${anchorRef.current.pixelOffset}, absoluteStart=${anchorItem.start}, scrollOffset=${scrollOffset}, splice(${idx},${delCnt},${insCnt})`,
        );
      }
    }

    // Calculate the new rows array
    const maxIndex =
      rows.length > 0
        ? Math.max(
            ...rows.map(r => {
              const match = r.content.match(/Row (\d+)/);
              return match ? parseInt(match[1], 10) : 0;
            }),
          )
        : -1;
    const newRowsToInsert =
      insCnt > 0 ? generateRows(insCnt, maxIndex + 1, seed) : [];
    const updatedRows = [...rows];
    updatedRows.splice(idx, delCnt, ...newRowsToInsert);

    setRows(updatedRows);

    // No scroll adjustments - for manual testing
  };

  const handleRemoveAll = () => {
    if (confirm('Remove all rows?')) {
      setRows([]);
    }
  };

  const handleReset = () => {
    setRows(generateRows(1000, 0, seed));
    setSelectedRowId('');
  };

  // Function to correct scroll position based on anchor
  const correctScrollPosition = useCallback(() => {
    const anchor = anchorRef.current;
    if (!anchor) return false;

    const scrollElement = parentRef.current;
    if (!scrollElement) return false;

    // Use Tanstack's elementsCache - it's keyed by item key and tracks connected elements
    const anchorElement = virtualizer.elementsCache.get(anchor.key) as
      | HTMLElement
      | undefined;

    if (anchorElement?.isConnected) {
      const anchorRect = anchorElement.getBoundingClientRect();
      const scrollRect = scrollElement.getBoundingClientRect();
      const actualPixelOffset = anchorRect.top - scrollRect.top;
      const currentScroll = scrollElement.scrollTop;

      // How far off are we from where we want the anchor to be?
      const error = actualPixelOffset - anchor.pixelOffset;

      if (Math.abs(error) > 1) {
        // Adjust scroll to put anchor at correct position
        const targetScroll = currentScroll + error;
        console.log(
          `[RESTORE] DOM-based correction #${anchor.correctionCount + 1}: actualPixelOffset=${actualPixelOffset.toFixed(1)}, wantedPixelOffset=${anchor.pixelOffset.toFixed(1)}, error=${error.toFixed(1)}, currentScroll=${currentScroll.toFixed(1)}, targetScroll=${targetScroll.toFixed(1)}`,
        );
        scrollElement.scrollTop = targetScroll;
        anchor.correctionCount++;
        return true; // Made a correction
      } else {
        console.log(
          `[RESTORE] Stable after ${anchor.correctionCount} corrections: actualPixelOffset=${actualPixelOffset.toFixed(1)}, wantedPixelOffset=${anchor.pixelOffset.toFixed(1)}`,
        );
        return false; // No correction needed, position is stable
      }
    } else {
      // Anchor not in DOM, use measurementsCache
      const measurements = virtualizer.measurementsCache;
      const anchorMeasurement = measurements.find(m => m.key === anchor.key);

      if (anchorMeasurement) {
        const newStart = anchorMeasurement.start;
        const targetScroll = newStart - anchor.pixelOffset;
        const currentScroll = virtualizer.scrollOffset ?? 0;
        const error = targetScroll - currentScroll;

        if (Math.abs(error) > 1) {
          console.log(
            `[RESTORE] Cache-based correction #${anchor.correctionCount + 1}: newStart=${newStart}, targetScroll=${targetScroll.toFixed(1)}, currentScroll=${currentScroll.toFixed(1)}, error=${error.toFixed(1)}`,
          );
          virtualizer.scrollToOffset(targetScroll);
          anchor.correctionCount++;
          return true;
        }
      }
      return false;
    }
  }, [virtualizer]);

  // Restore scroll position after mutations
  // This runs AFTER React renders but BEFORE paint
  useLayoutEffect(() => {
    const anchor = anchorRef.current;
    if (anchor === null) {
      // No mutation in progress, just update prevRowsRef
      prevRowsRef.current = rows;
      return;
    }

    // If splice happened AFTER anchor, no adjustment needed
    // Note: when spliceIndex === anchor.index, items are inserted BEFORE
    // the anchor position, so we still need to adjust
    if (anchor.spliceIndex > anchor.index) {
      console.log(
        `[RESTORE] Splice after anchor (spliceIndex=${anchor.spliceIndex} > anchorIndex=${anchor.index}), no adjustment needed`,
      );
      anchorRef.current = null;
      prevRowsRef.current = rows;
      return;
    }

    // Do initial correction
    correctScrollPosition();

    // Update prevRowsRef but DON'T clear anchor yet - useEffect will do follow-up corrections
    prevRowsRef.current = rows;
  }, [rows, correctScrollPosition]);

  // Follow-up correction after paint and ResizeObserver has fired
  // This handles the case where newly inserted items get measured after the initial correction
  useEffect(() => {
    const anchor = anchorRef.current;
    if (!anchor) return;

    // Skip if splice was AFTER anchor (no adjustment needed)
    // Note: when spliceIndex === anchor.index, items are inserted BEFORE
    // the anchor position, so we still need to adjust
    if (anchor.spliceIndex > anchor.index) {
      console.log(
        `[RESTORE] Splice after anchor (spliceIndex=${anchor.spliceIndex} > anchorIndex=${anchor.index}), no adjustment needed`,
      );
      anchorRef.current = null;
      return;
    }

    let rafId: number;
    let correctionAttempts = 0;
    const maxAttempts = 10;

    const attemptCorrection = () => {
      correctionAttempts++;
      const madeCorrection = correctScrollPosition();

      if (madeCorrection && correctionAttempts < maxAttempts) {
        // If we made a correction, wait for the next frame to see if more are needed
        // (ResizeObserver may fire again after scroll changes what's visible)
        rafId = requestAnimationFrame(attemptCorrection);
      } else {
        // Done - either position is stable or we've hit max attempts
        console.log(
          `[RESTORE] Complete after ${anchor.correctionCount} total corrections`,
        );
        anchorRef.current = null;
      }
    };

    // Start correction loop on next frame to let ResizeObserver fire
    rafId = requestAnimationFrame(attemptCorrection);

    return () => {
      cancelAnimationFrame(rafId);
    };
  }, [rows, correctScrollPosition]);

  const virtualItems = virtualizer.getVirtualItems();
  const totalSize = virtualizer.getTotalSize();

  const stats = useMemo(() => {
    if (rows.length === 0) return null;
    const contentLengths = rows.map(r => r.content.length);
    return {
      min: Math.min(...contentLengths),
      max: Math.max(...contentLengths),
      avg: Math.round(
        contentLengths.reduce((sum, l) => sum + l, 0) / contentLengths.length,
      ),
    };
  }, [rows]);

  return (
    <div
      style={{
        display: 'flex',
        height: '100vh',
        fontFamily: 'system-ui, sans-serif',
      }}
    >
      {/* Control Panel */}
      <div
        style={{
          width: '320px',
          padding: '20px',
          borderRight: '1px solid #ccc',
          backgroundColor: '#f5f5f5',
          overflowY: 'auto',
          flexShrink: 0,
        }}
      >
        <h1 style={{margin: '0 0 20px 0', fontSize: '18px'}}>
          Tanstack Virtualizer Test
        </h1>

        {/* Stats */}
        <div
          style={{
            marginBottom: '20px',
            padding: '12px',
            backgroundColor: '#fff',
            borderRadius: '4px',
          }}
        >
          <h3 style={{margin: '0 0 8px 0', fontSize: '14px'}}>Stats</h3>
          <div style={{marginBottom: '8px'}}>
            <label
              style={{
                display: 'flex',
                alignItems: 'center',
                fontSize: '12px',
                cursor: 'pointer',
              }}
            >
              <input
                type="checkbox"
                checked={debug}
                onChange={e => setDebug(e.target.checked)}
                style={{marginRight: '6px'}}
              />
              Debug Mode
            </label>
          </div>
          <div style={{fontSize: '13px'}}>
            <div>
              Total Rows: <strong>{rows.length}</strong>
            </div>
            <div>
              Virtual Items: <strong>{virtualItems.length}</strong>
            </div>
            <div>
              Total Height: <strong>{Math.round(totalSize)}px</strong>
            </div>
            {stats && (
              <>
                <div>
                  Content Length Range:{' '}
                  <strong>
                    {stats.min}-{stats.max} chars
                  </strong>
                </div>
                <div>
                  Avg Content Length: <strong>{stats.avg} chars</strong>
                </div>
              </>
            )}
            <div>
              Scroll Offset:{' '}
              <strong>{Math.round(virtualizer.scrollOffset ?? 0)}px</strong>
            </div>
          </div>
        </div>

        {/* Change Text Length */}
        <div
          style={{
            marginBottom: '20px',
            padding: '12px',
            backgroundColor: '#fff',
            borderRadius: '4px',
          }}
        >
          <h3 style={{margin: '0 0 8px 0', fontSize: '14px'}}>
            Change Text Length
          </h3>
          <div style={{marginBottom: '8px'}}>
            <label
              style={{display: 'block', fontSize: '12px', marginBottom: '4px'}}
            >
              Row ID:
            </label>
            <input
              type="text"
              value={selectedRowId}
              onChange={e => setSelectedRowId(e.target.value)}
              placeholder="Click a row to select"
              style={{
                width: '100%',
                padding: '6px',
                fontSize: '13px',
                border: '1px solid #ccc',
                borderRadius: '3px',
              }}
            />
          </div>
          <div style={{marginBottom: '8px'}}>
            <label
              style={{display: 'block', fontSize: '12px', marginBottom: '4px'}}
            >
              Text Multiplier (1-50x):
            </label>
            <input
              type="number"
              value={textMultiplier}
              step="1"
              onChange={e => handleTextInputChange(e.target.value)}
              min={MIN_MULTIPLIER}
              max={MAX_MULTIPLIER}
              style={{
                width: '100%',
                padding: '6px',
                fontSize: '13px',
                border: '1px solid #ccc',
                borderRadius: '3px',
              }}
            />
          </div>
          <div style={{marginBottom: '8px'}}>
            <label
              style={{
                display: 'flex',
                alignItems: 'center',
                fontSize: '12px',
                cursor: 'pointer',
              }}
            >
              <input
                ref={checkboxRef}
                type="checkbox"
                checked={forceAdjust === true}
                onChange={() => {
                  setForceAdjust(prev => {
                    if (prev === null) return true;
                    if (prev === true) return false;
                    return null;
                  });
                }}
                style={{marginRight: '6px'}}
              />
              Scroll adjustment for this row
              {forceAdjust === null
                ? ' (Default)'
                : forceAdjust
                  ? ' (Force Adjust)'
                  : ' (Force No Adjust)'}
            </label>
          </div>
          <button
            onClick={() => handleChangeText(true)}
            style={{
              width: '100%',
              padding: '8px',
              fontSize: '13px',
              backgroundColor: '#0066cc',
              color: '#fff',
              border: 'none',
              borderRadius: '3px',
              cursor: 'pointer',
            }}
          >
            Change Text Length
          </button>
        </div>

        {/* Splice Operation */}
        <div
          style={{
            marginBottom: '20px',
            padding: '12px',
            backgroundColor: '#fff',
            borderRadius: '4px',
          }}
        >
          <h3 style={{margin: '0 0 8px 0', fontSize: '14px'}}>
            Splice Operation
          </h3>
          <div style={{marginBottom: '8px'}}>
            <label
              style={{display: 'block', fontSize: '12px', marginBottom: '4px'}}
            >
              Index:
            </label>
            <input
              type="number"
              value={index}
              onChange={e => setIndex(e.target.value)}
              min="0"
              style={{
                width: '100%',
                padding: '6px',
                fontSize: '13px',
                border: '1px solid #ccc',
                borderRadius: '3px',
              }}
            />
          </div>
          <div style={{marginBottom: '8px'}}>
            <label
              style={{display: 'block', fontSize: '12px', marginBottom: '4px'}}
            >
              Delete Count:
            </label>
            <input
              type="number"
              value={deleteCount}
              onChange={e => setDeleteCount(e.target.value)}
              min="0"
              style={{
                width: '100%',
                padding: '6px',
                fontSize: '13px',
                border: '1px solid #ccc',
                borderRadius: '3px',
              }}
            />
          </div>
          <div style={{marginBottom: '8px'}}>
            <label
              style={{display: 'block', fontSize: '12px', marginBottom: '4px'}}
            >
              Insert Count:
            </label>
            <input
              type="number"
              value={insertCount}
              onChange={e => setInsertCount(e.target.value)}
              min="0"
              style={{
                width: '100%',
                padding: '6px',
                fontSize: '13px',
                border: '1px solid #ccc',
                borderRadius: '3px',
              }}
            />
          </div>
          <button
            onClick={handleSplice}
            style={{
              width: '100%',
              padding: '8px',
              fontSize: '13px',
              backgroundColor: '#0066cc',
              color: '#fff',
              border: 'none',
              borderRadius: '3px',
              cursor: 'pointer',
              marginBottom: '8px',
            }}
          >
            Splice
          </button>
          <button
            onClick={handleRemoveAll}
            disabled={rows.length === 0}
            style={{
              width: '100%',
              padding: '8px',
              fontSize: '13px',
              backgroundColor: rows.length === 0 ? '#ccc' : '#dc3545',
              color: '#fff',
              border: 'none',
              borderRadius: '3px',
              cursor: rows.length === 0 ? 'not-allowed' : 'pointer',
            }}
          >
            Remove All
          </button>
        </div>

        {/* Reset */}
        <div
          style={{
            padding: '12px',
            backgroundColor: '#fff',
            borderRadius: '4px',
          }}
        >
          <button
            onClick={handleReset}
            style={{
              width: '100%',
              padding: '8px',
              fontSize: '13px',
              backgroundColor: '#6c757d',
              color: '#fff',
              border: 'none',
              borderRadius: '3px',
              cursor: 'pointer',
            }}
          >
            Reset (1000 rows)
          </button>
        </div>
      </div>

      {/* Virtual List */}
      <div style={{flex: 1, display: 'flex', flexDirection: 'column'}}>
        <div
          style={{
            padding: '16px',
            borderBottom: '1px solid #ccc',
            backgroundColor: '#fff',
            flexShrink: 0,
          }}
        >
          <h2 style={{margin: 0, fontSize: '16px'}}>
            Virtual List ({rows.length} rows)
          </h2>
        </div>
        <div
          ref={parentRef}
          style={{
            flex: 1,
            overflow: 'auto',
            position: 'relative',
          }}
        >
          {rows.length === 0 ? (
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                height: '100%',
                color: '#999',
              }}
            >
              No rows. Add some rows to get started.
            </div>
          ) : (
            <div
              style={{
                height: `${totalSize}px`,
                width: '100%',
                position: 'relative',
              }}
            >
              <div
                style={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  width: '100%',
                  transform: `translateY(${virtualItems[0]?.start ?? 0}px)`,
                }}
              >
                {virtualItems.map(virtualItem => {
                  const row = rows[virtualItem.index];
                  if (!row) return null;

                  return (
                    <div
                      key={row.id}
                      data-index={virtualItem.index}
                      data-key={row.id}
                      ref={measureElement}
                      style={{
                        padding: '16px',
                        borderBottom: '1px solid #eee',
                        backgroundColor:
                          selectedRowId === row.id ? '#fff3cd' : '#fff',
                        boxSizing: 'border-box',
                        display: 'flex',
                        flexDirection: 'column',
                      }}
                    >
                      <div
                        onClick={() => {
                          setSelectedRowId(row.id);
                          setTextMultiplier(row.multiplier.toString());
                        }}
                        style={{
                          fontWeight: 'bold',
                          fontSize: '14px',
                          marginBottom: '4px',
                          cursor: 'pointer',
                          display: 'inline-block',
                        }}
                        title="Click to select for text change"
                      >
                        {row.id} (Index: {virtualItem.index})
                      </div>
                      <div
                        style={{
                          fontSize: '13px',
                          color: '#666',
                          marginBottom: '8px',
                        }}
                      >
                        Content: {row.content.length} chars | Position:{' '}
                        {virtualItem.start - (virtualizer.scrollOffset ?? 0)} |
                        Size: {virtualItem.size}px | Start: {virtualItem.start}
                        px
                      </div>
                      <div
                        style={{
                          fontSize: '13px',
                          lineHeight: '1.5',
                          wordBreak: 'break-word',
                        }}
                      >
                        {row.content}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
