import {useVirtualizer} from '@rocicorp/react-virtual';
import {useCallback, useEffect, useRef, useState} from 'react';

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

// Helper to create row content with multiplier
function createRowContent(rowIndex: number, multiplier: number): string {
  const baseSentence =
    'Lorem ipsum dolor sit amet, consectetur adipiscing elit. ';
  return `Row ${rowIndex} - ${baseSentence.repeat(multiplier)}`;
}

// Helper to calculate size based on multiplier and mode
function calculateSize(multiplier: number, useFixedHeight: boolean): number {
  if (useFixedHeight) {
    return DEFAULT_HEIGHT + multiplier * 10;
  }
  return multiplier * 20 + 50;
}

export function TanstackTestApp() {
  const [rows, setRows] = useState<RowData[]>(() => generateRows(100, 0, seed));
  const [selectedRowId, setSelectedRowId] = useState<string>('');
  const [textMultiplier, setTextMultiplier] = useState<string>('1');
  // const [forceAdjust, setForceAdjust] = useState<boolean | null>(null);
  const [debug, setDebug] = useState<boolean>(false);
  const [index, setIndex] = useState<string>('0');
  const [deleteCount, setDeleteCount] = useState<string>('0');
  const [insertCount, setInsertCount] = useState<string>('10');
  const [autoMode, setAutoMode] = useState<boolean>(false);
  const [autoModeLog, setAutoModeLog] = useState<string[]>([]);
  const [useFixedHeight, setUseFixedHeight] = useState<boolean>(false);

  const parentRef = useRef<HTMLDivElement>(null);
  const virtualItemsRef = useRef<
    ReturnType<typeof virtualizer.getVirtualItems>
  >([]);
  const rowsRef = useRef(rows);

  const estimateSize = useCallback(
    (index: number) => {
      const row = rows[index];
      if (!row) {
        return DEFAULT_HEIGHT;
      }
      return calculateSize(row.multiplier, useFixedHeight);
    },
    [useFixedHeight, rows],
  );

  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize,
    getItemKey: useCallback(
      (index: number) => rows[index]?.id ?? index,
      [rows],
    ),
    overscan: 5,
    debug,
  });

  // Render a single row's content (shared between fixed and dynamic modes)
  const renderRowContent = useCallback(
    (
      row: RowData,
      virtualItem: ReturnType<typeof virtualizer.getVirtualItems>[0],
    ) => (
      <>
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
          {virtualItem.start - (virtualizer.scrollOffset ?? 0)} | Size:{' '}
          {virtualItem.size}px | Start: {virtualItem.start}px
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
      </>
    ),
    [virtualizer.scrollOffset],
  );

  // Expose virtualizer and resizeRow for testing
  useEffect(() => {
    // oxlint-disable-next-line no-explicit-any
    const w = window as any;
    w.virtualizer = virtualizer;

    // Expose resizeRow function for testing
    w.resizeRow = (rowIndex: number, multiplier: number) => {
      if (rowIndex < 0 || rowIndex >= rows.length) {
        // oxlint-disable-next-line no-console -- test helper
        console.error(`Invalid row index: ${rowIndex}`);
        return false;
      }
      if (multiplier < MIN_MULTIPLIER || multiplier > MAX_MULTIPLIER) {
        // oxlint-disable-next-line no-console -- test helper
        console.error(
          `Multiplier must be between ${MIN_MULTIPLIER} and ${MAX_MULTIPLIER}`,
        );
        return false;
      }
      const newSize = calculateSize(multiplier, useFixedHeight);
      setRows(prev => {
        const updated = [...prev];
        updated[rowIndex] = {
          ...updated[rowIndex],
          content: createRowContent(rowIndex, multiplier),
          multiplier,
        };
        return updated;
      });
      virtualizer.resizeItem(rowIndex, newSize);
      return true;
    };

    return () => {
      delete w.resizeRow;
    };
  }, [virtualizer, rows.length, setRows]);

  // Reset measurements when fixed height mode changes
  useEffect(() => {
    virtualizer.measure();
  }, [useFixedHeight, virtualizer]);

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
      updated[rowIndex] = {
        ...updated[rowIndex],
        content: createRowContent(rowIndex, multiplier),
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
        const newSize = calculateSize(multiplier, useFixedHeight);
        setRows(prev => {
          const updated = [...prev];
          updated[rowIndex] = {
            ...updated[rowIndex],
            content: createRowContent(rowIndex, multiplier),
            multiplier,
          };
          return updated;
        });
        virtualizer.resizeItem(rowIndex, newSize);
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

  const virtualItems = virtualizer.getVirtualItems();
  const totalSize = virtualizer.getTotalSize();

  // Keep refs up to date for use in auto mode timer
  virtualItemsRef.current = virtualItems;
  rowsRef.current = rows;

  // Auto mode: perform random splices every 0.5-1.5 seconds
  useEffect(() => {
    if (!autoMode) return;

    const performAutoSplice = () => {
      const currentRows = rowsRef.current;
      const currentVirtualItems = virtualItemsRef.current;
      if (currentRows.length === 0 || currentVirtualItems.length === 0) return;

      const scrollElement = parentRef.current;
      if (!scrollElement) return;

      const scrollTop = scrollElement.scrollTop;
      const viewportHeight = scrollElement.clientHeight;
      const scrollBottom = scrollTop + viewportHeight;

      // Find visible range (indices that are on screen)
      const visibleStartIndex = currentVirtualItems.findIndex(
        item => item.start + item.size > scrollTop,
      );
      const visibleEndIndex = currentVirtualItems.findIndex(
        item => item.start >= scrollBottom,
      );

      const firstVisibleIdx =
        visibleStartIndex >= 0
          ? currentVirtualItems[visibleStartIndex].index
          : 0;
      const lastVisibleIdx =
        visibleEndIndex >= 0
          ? (currentVirtualItems[visibleEndIndex - 1]?.index ??
            currentRows.length - 1)
          : (currentVirtualItems[currentVirtualItems.length - 1]?.index ??
            currentRows.length - 1);

      // Decide operation type: 0 = splice (delete+insert), 1 = resize
      // In fixed height mode, resizing changes estimates but still tests stability
      const opType = Math.floor(Math.random() * 2);
      let logEntry = '';

      if (opType === 0 && currentRows.length > 10) {
        // Splice operation - delete and insert at same position, avoid visible range
        let targetIdx: number;
        const canSpliceBefore = firstVisibleIdx > 0;
        const canSpliceAfter = lastVisibleIdx < currentRows.length - 1;

        if (canSpliceBefore && canSpliceAfter) {
          // Pick randomly before or after
          if (Math.random() < 0.5) {
            targetIdx = Math.floor(Math.random() * firstVisibleIdx);
          } else {
            targetIdx =
              lastVisibleIdx +
              1 +
              Math.floor(
                Math.random() * (currentRows.length - lastVisibleIdx - 1),
              );
          }
        } else if (canSpliceBefore) {
          targetIdx = Math.floor(Math.random() * firstVisibleIdx);
        } else if (canSpliceAfter) {
          targetIdx =
            lastVisibleIdx +
            1 +
            Math.floor(
              Math.random() * (currentRows.length - lastVisibleIdx - 1),
            );
        } else {
          // Can't splice without affecting viewport
          logEntry = `Skip splice: no safe range (visible: ${firstVisibleIdx}-${lastVisibleIdx})`;
          setAutoModeLog(prev => [logEntry, ...prev.slice(0, 9)]);
          return;
        }

        // Determine delete count (0-3)
        const maxDelete =
          targetIdx < firstVisibleIdx
            ? firstVisibleIdx - targetIdx
            : currentRows.length - targetIdx;
        const delCount = Math.min(Math.floor(Math.random() * 4), maxDelete);

        // Determine insert count (0-3)
        const insCount = Math.floor(Math.random() * 4);

        if (delCount === 0 && insCount === 0) {
          // Do at least something
          logEntry = `Skip splice: would be no-op`;
          setAutoModeLog(prev => [logEntry, ...prev.slice(0, 9)]);
          return;
        }

        const maxIndex =
          currentRows.length > 0
            ? Math.max(
                ...currentRows.map(r => {
                  const match = r.content.match(/Row (\d+)/);
                  return match ? parseInt(match[1], 10) : 0;
                }),
              )
            : -1;
        const newRows =
          insCount > 0 ? generateRows(insCount, maxIndex + 1, Date.now()) : [];
        logEntry = `Splice at ${targetIdx}: -${delCount} +${insCount} (visible: ${firstVisibleIdx}-${lastVisibleIdx})`;

        setRows(prev => {
          const updated = [...prev];
          updated.splice(targetIdx, delCount, ...newRows);
          return updated;
        });
      } else {
        // Resize operation - target items in virtual range but not visible
        // Virtual items that are rendered but off-screen
        const offScreenVirtual = currentVirtualItems.filter(item => {
          const itemTop = item.start;
          const itemBottom = item.start + item.size;
          // Item is completely above or below viewport
          return itemBottom <= scrollTop || itemTop >= scrollBottom;
        });

        if (offScreenVirtual.length === 0) {
          logEntry = `Skip resize: no off-screen virtual items (visible: ${firstVisibleIdx}-${lastVisibleIdx})`;
          setAutoModeLog(prev => [logEntry, ...prev.slice(0, 9)]);
          return;
        }

        const targetItem =
          offScreenVirtual[Math.floor(Math.random() * offScreenVirtual.length)];
        const targetIdx = targetItem.index;
        const newMultiplier = Math.floor(Math.random() * 10) + 1;
        const newSize = calculateSize(newMultiplier, useFixedHeight);
        logEntry = `Resize index ${targetIdx} to ${newMultiplier}x (visible: ${firstVisibleIdx}-${lastVisibleIdx})`;

        setRows(prev => {
          const updated = [...prev];
          if (updated[targetIdx]) {
            updated[targetIdx] = {
              ...updated[targetIdx],
              content: createRowContent(targetIdx, newMultiplier),
              multiplier: newMultiplier,
            };
          }
          return updated;
        });
        virtualizer.resizeItem(targetIdx, newSize);
      }

      setAutoModeLog(prev => [logEntry, ...prev.slice(0, 9)]);
    };

    // Random interval between 500ms-1.5s
    const scheduleNext = () => {
      const delay = 500 + Math.random() * 1000;
      return setTimeout(() => {
        performAutoSplice();
        timerId = scheduleNext();
      }, delay);
    };

    let timerId = scheduleNext();

    return () => {
      clearTimeout(timerId);
    };
  }, [autoMode]);

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

        {/* Performance Options */}
        <div
          style={{
            marginBottom: '20px',
            padding: '12px',
            backgroundColor: '#f8f9fa',
            borderRadius: '4px',
          }}
        >
          <h3 style={{margin: '0 0 8px 0', fontSize: '14px'}}>
            Performance Options
          </h3>
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
                checked={useFixedHeight}
                onChange={e => {
                  setUseFixedHeight(e.target.checked);
                }}
                style={{marginRight: '6px'}}
              />
              Use Fixed Row Heights
            </label>
          </div>
          <div style={{fontSize: '11px', color: '#666'}}>
            When enabled, all rows use fixed 100px height with absolute
            positioning and transform translateY.
          </div>
        </div>

        {/* Auto Mode */}
        <div
          style={{
            marginBottom: '20px',
            padding: '12px',
            backgroundColor: autoMode ? '#d4edda' : '#fff',
            borderRadius: '4px',
            border: autoMode ? '2px solid #28a745' : '1px solid transparent',
          }}
        >
          <h3 style={{margin: '0 0 8px 0', fontSize: '14px'}}>Auto Mode</h3>
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
                checked={autoMode}
                onChange={e => {
                  setAutoMode(e.target.checked);
                  if (e.target.checked) {
                    setAutoModeLog([]);
                  }
                }}
                style={{marginRight: '6px'}}
              />
              Enable Auto Splicing (0.5-1.5s interval)
            </label>
          </div>
          <div style={{fontSize: '11px', color: '#666', marginBottom: '8px'}}>
            Performs random splices (delete+insert) and resizes outside the
            visible viewport. Resizes target off-screen virtual items.
          </div>
          {autoModeLog.length > 0 && (
            <div
              style={{
                fontSize: '11px',
                fontFamily: 'monospace',
                backgroundColor: '#f8f9fa',
                padding: '8px',
                borderRadius: '3px',
                maxHeight: '120px',
                overflowY: 'auto',
              }}
            >
              {autoModeLog.map((log, i) => (
                <div key={i} style={{marginBottom: '2px'}}>
                  {log}
                </div>
              ))}
            </div>
          )}
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
            // Disable browser scroll anchoring - we handle scroll stability ourselves
            overflowAnchor: 'none',
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
              {useFixedHeight ? (
                // Fixed height mode: use transform translateY positioning
                virtualItems.map(virtualItem => {
                  const row = rows[virtualItem.index];
                  if (!row) return null;

                  return (
                    <div
                      key={row.id}
                      data-index={virtualItem.index}
                      style={{
                        position: 'absolute',
                        top: 0,
                        left: 0,
                        width: '100%',
                        height: `${virtualItem.size}px`,
                        transform: `translateY(${virtualItem.start}px)`,
                        padding: '20px',
                        borderBottom: '1px solid #ddd',
                        backgroundColor:
                          selectedRowId === row.id ? '#fff3cd' : '#fff',
                        boxSizing: 'border-box',
                        overflow: 'hidden',
                        display: 'flex',
                        flexDirection: 'column',
                      }}
                    >
                      {renderRowContent(row, virtualItem)}
                    </div>
                  );
                })
              ) : (
                // Dynamic height mode: use wrapper with translateY + measureElement
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
                        ref={virtualizer.measureElement}
                        style={{
                          padding: '20px',
                          borderBottom: '1px solid #ddd',
                          backgroundColor:
                            selectedRowId === row.id ? '#fff3cd' : '#fff',
                          boxSizing: 'border-box',
                          display: 'flex',
                          flexDirection: 'column',
                        }}
                      >
                        {renderRowContent(row, virtualItem)}
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
