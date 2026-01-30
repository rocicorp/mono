import {useVirtualizer} from '@rocicorp/react-virtual';
import {useCallback, useEffect, useMemo, useRef, useState} from 'react';

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

    return () => {
      delete w.resizeRow;
    };
  }, [virtualizer, rows.length, setRows]);

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
                      ref={virtualizer.measureElement}
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
