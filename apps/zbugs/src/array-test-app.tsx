import {useVirtualizer} from '@rocicorp/react-virtual';
import {useZero} from '@rocicorp/zero/react';
import {useCallback, useEffect, useMemo, useRef, useState} from 'react';
import {useRows} from '../../../packages/zero-react/src/use-rows.js';
import {queries, type IssueRowSort, type Issues} from '../shared/queries.js';
import {ZERO_PROJECT_NAME} from '../shared/schema.js';
import {LoginProvider} from './components/login-provider.js';
import {useLogin} from './hooks/use-login.js';
import {ZeroInit} from './zero-init.js';

type RowData = Issues[number];

const DEFAULT_HEIGHT = 275;
const PLACEHOLDER_HEIGHT = 50; //DEFAULT_HEIGHT / 2;
const PAGE_SIZE = 50;

const toStartRow = (row: {id: string; modified: number; created: number}) => ({
  id: row.id,
  modified: row.modified,
  created: row.created,
});

function ArrayTestAppContent() {
  const z = useZero();

  const listContextParams = useMemo(
    () => ({
      projectName: ZERO_PROJECT_NAME.toLocaleLowerCase(),
      sortDirection: 'desc' as const,
      sortField: 'created' as const,
      assignee: null,
      creator: null,
      labels: [],
      open: null,
      textFilter: null,
      permalinkID: null,
    }),
    [],
  );

  const [anchorIndex, setAnchorIndex] = useState(0);
  const [anchorKind, setAnchorKind] = useState<
    'forward' | 'backward' | 'permalink'
  >('permalink');
  const [startRow, setStartRow] = useState<IssueRowSort | undefined>(undefined);
  const [permalinkID, setPermalinkID] = useState<string | undefined>('3130');
  const [selectedRowIndex, setSelectedRowIndex] = useState<number | null>(null);
  const [debug, setDebug] = useState<boolean>(true);
  const [autoPagingEnabled, setAutoPagingEnabled] = useState<boolean>(false);

  // Track last anchor shift to prevent rapid consecutive shifts
  const lastAnchorShiftRef = useRef<number>(0);
  const ANCHOR_SHIFT_COOLDOWN = 500; // ms

  // Track if we've positioned the permalink
  const hasPositionedPermalinkRef = useRef(false);

  const {
    rowAt,
    rowsLength,
    complete,
    rowsEmpty,
    atStart,
    atEnd,
    firstRowIndex,
    permalinkNotFound,
  } = useRows<RowData, IssueRowSort>({
    pageSize: PAGE_SIZE,
    anchor:
      anchorKind === 'permalink' && permalinkID
        ? {
            kind: 'permalink',
            index: anchorIndex,
            id: permalinkID,
          }
        : anchorKind === 'forward'
          ? {
              kind: 'forward',
              index: anchorIndex,
              startRow,
            }
          : anchorKind === 'backward' && startRow
            ? {
                kind: 'backward',
                index: anchorIndex,
                startRow,
              }
            : {
                kind: 'forward',
                index: anchorIndex,
                startRow,
              },
    getPageQuery: useCallback(
      (
        limit: number,
        start: IssueRowSort | null,
        dir: 'forward' | 'backward',
      ) =>
        queries.issueListV2({
          listContext: listContextParams,
          userID: z.userID,
          limit,
          start,
          dir,
          inclusive: start === null,
        }),
      [listContextParams, z.userID],
    ),
    getSingleQuery: useCallback(
      (id: string) => {
        const isNumeric = /^\d+$/.test(id);
        return queries.listIssueByID({
          idField: isNumeric ? 'shortID' : 'id',
          idValue: isNumeric ? parseInt(id, 10) : id,
          listContext: listContextParams,
        });
      },
      [listContextParams],
    ),
    toStartRow,
  });

  // Log useRows state changes
  useEffect(() => {
    if (debug) {
      console.log('[useRows state]', {
        firstRowIndex,
        rowsLength,
        atStart,
        atEnd,
        complete,
        anchorKind,
        anchorIndex,
      });
    }
  }, [
    firstRowIndex,
    rowsLength,
    atStart,
    atEnd,
    complete,
    anchorKind,
    anchorIndex,
    debug,
  ]);

  // The virtualizer uses indices 0, 1, 2, ... mapping directly to logical data indices
  // rowAt will return undefined for indices outside the current data window

  const endPlaceholder = atEnd ? 0 : 1;
  const startPlaceholder = atStart ? 0 : 1;

  // Convert virtualizer index to logical data index
  // Virtualizer indices: [start placeholder] + [data] + [end placeholder]
  // Need to account for start placeholder offset
  const toLogicalIndex = useCallback(
    (virtualizerIndex: number) => {
      return firstRowIndex + (virtualizerIndex - startPlaceholder);
    },
    [firstRowIndex, startPlaceholder],
  );

  const rows = useMemo(() => {
    // Total length: [start placeholder if !atStart] + data window + [end placeholder if !atEnd]
    const totalLength = startPlaceholder + rowsLength + endPlaceholder;

    const handler: ProxyHandler<RowData[]> = {
      get(target, prop) {
        if (prop === 'length') {
          return totalLength;
        }
        if (typeof prop === 'string') {
          const index = parseInt(prop, 10);
          if (!isNaN(index) && index >= 0) {
            // Start placeholder
            if (!atStart && index === 0) {
              return undefined;
            }
            // End placeholder
            if (!atEnd && index === totalLength - 1) {
              return undefined;
            }
            // Map virtualizer index to logical index, accounting for start placeholder
            const logicalIndex = firstRowIndex + (index - startPlaceholder);
            return rowAt(logicalIndex);
          }
        }
        return Reflect.get(target, prop);
      },
    };
    return new Proxy<RowData[]>([], handler);
  }, [
    rowAt,
    firstRowIndex,
    rowsLength,
    endPlaceholder,
    atEnd,
    atStart,
    startPlaceholder,
  ]);

  const parentRef = useRef<HTMLDivElement>(null);

  const estimateSize = useCallback(
    (index: number) => {
      // Return estimate - virtualizer will measure actual heights
      const row = rows[index];
      return row ? DEFAULT_HEIGHT : PLACEHOLDER_HEIGHT;
    },
    [rows],
  );

  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize,
    getItemKey: useCallback(
      (index: number) => {
        const row = rows[index];
        if (row) {
          return row.id;
        }
        // For placeholders, use a unique key based on position
        if (!atStart && index === 0) {
          return `placeholder-start`;
        }
        return `placeholder-end-${index}`;
      },
      [rows, atStart],
    ),
    overscan: 5,
    debug,
  });

  // Expose virtualizer for testing
  useEffect(() => {
    // oxlint-disable-next-line no-explicit-any
    const w = window as any;
    w.virtualizer = virtualizer;
    w.rowAt = rowAt;
    w.rows = rows;

    return () => {
      delete w.virtualizer;
      delete w.rowAt;
      delete w.rows;
    };
  }, [virtualizer, rowAt, rows]);

  const virtualItems = virtualizer.getVirtualItems();
  const totalSize = virtualizer.getTotalSize();

  // Handle permalink positioning and enable auto-paging when ready
  useEffect(() => {
    // Reset positioning flag when switching modes
    if (anchorKind !== 'permalink') {
      hasPositionedPermalinkRef.current = false;
      if (!autoPagingEnabled) {
        setAutoPagingEnabled(true);
      }
      return;
    }

    // For permalink mode:
    // 1. Once the permalink row is loaded (rowsLength > 0), scroll to position it
    // 2. Once complete (before/after pages loaded), enable auto-paging

    if (rowsLength === 0) {
      // No data yet, keep auto-paging disabled
      return;
    }

    // Permalink row is loaded - ensure it's at the correct scroll position
    // Keep repositioning until data is complete to handle changing firstRowIndex
    if (
      (!hasPositionedPermalinkRef.current || !complete) &&
      anchorIndex !== null
    ) {
      // The permalink is at logical index 0
      // Convert to virtualizer index: virtualizerIndex = logicalIndex - firstRowIndex + startPlaceholder
      const permalinkLogicalIndex = anchorIndex;
      const targetVirtualIndex =
        permalinkLogicalIndex - firstRowIndex + startPlaceholder;

      if (debug) {
        console.log('[Permalink] Scrolling to position:', {
          permalinkLogicalIndex,
          targetVirtualIndex,
          anchorIndex,
          rowsLength,
          atStart,
          atEnd,
          firstRowIndex,
          startPlaceholder,
          complete,
        });
      }

      // Scroll to the permalink row
      virtualizer.scrollToIndex(targetVirtualIndex, {
        align: 'start',
      });

      if (debug) {
        console.log('[Permalink] Scrolled to index', {
          targetVirtualIndex,
          complete,
        });
      }

      // Only mark as positioned once data is complete
      if (complete) {
        hasPositionedPermalinkRef.current = true;
      }
    }

    // Once complete, enable auto-paging
    if (complete && !autoPagingEnabled) {
      if (debug) {
        console.log('[Permalink] Data complete, enabling auto-paging');
      }
      setAutoPagingEnabled(true);
    }
  }, [
    anchorKind,
    rowsLength,
    complete,
    autoPagingEnabled,
    anchorIndex,
    startPlaceholder,
    virtualizer,
    debug,
    atStart,
    atEnd,
  ]);

  // Auto-shift anchor forward when scrolling near the end of the data window
  useEffect(() => {
    if (!autoPagingEnabled || virtualItems.length === 0 || !complete || atEnd) {
      return;
    }

    // Cooldown to prevent rapid consecutive shifts
    const now = Date.now();
    if (now - lastAnchorShiftRef.current < ANCHOR_SHIFT_COOLDOWN) {
      return;
    }

    // Find last non-placeholder item
    let lastItem = virtualItems[virtualItems.length - 1];
    let lastRow = rows[lastItem.index];

    if (!lastRow) {
      // Try to find last non-placeholder item
      for (let i = virtualItems.length - 2; i >= 0; i--) {
        const item = virtualItems[i];
        const row = rows[item.index];
        if (row) {
          lastItem = item;
          lastRow = row;
          break;
        }
      }

      // Skip if no non-placeholder items found
      if (!lastRow) {
        if (debug) {
          console.log(
            '[AutoAnchor forward] Skipping: no non-placeholder items',
          );
        }
        return;
      }
    }

    // Convert virtualizer index to logical index
    const lastLogicalIndex = toLogicalIndex(lastItem.index);
    const lastDataIndex = firstRowIndex + rowsLength - 1;

    // How far is the last visible item from the end of our data window?
    const distanceFromEnd = lastDataIndex - lastLogicalIndex;

    // Threshold: shift anchor when we're within 10% of page size from the end
    // (matches useZeroVirtualizer's getNearPageEdgeThreshold)
    const nearPageEdgeThreshold = Math.ceil(PAGE_SIZE * 0.1);

    if (debug) {
      console.log('[AutoAnchor forward] Check:', {
        distanceFromEnd,
        nearPageEdgeThreshold,
        wouldTrigger: distanceFromEnd <= nearPageEdgeThreshold,
      });
    }

    // Trigger when near end of data window
    // (backward auto-anchor is disabled, so no ping-pong concern)
    if (distanceFromEnd <= nearPageEdgeThreshold) {
      // Shift anchor forward: position anchor so LAST visible item
      // will be at ~60% into the new window (giving 40% buffer ahead)
      const newAnchorIndex = lastLogicalIndex - Math.ceil(PAGE_SIZE * 0.6);
      const newAnchorRow = rowAt(newAnchorIndex);

      if (newAnchorRow && newAnchorIndex !== anchorIndex) {
        if (debug) {
          console.log(
            '[AutoAnchor] Shifting forward:',
            'distanceFromEnd=',
            distanceFromEnd,
            'lastLogicalIndex=',
            lastLogicalIndex,
            'newAnchorIndex=',
            newAnchorIndex,
          );
        }
        lastAnchorShiftRef.current = now;
        setAnchorKind('forward');
        setAnchorIndex(newAnchorIndex);
        setStartRow(toStartRow(newAnchorRow));
        setPermalinkID(undefined);
      }
    }
  }, [
    virtualItems,
    complete,
    atEnd,
    firstRowIndex,
    rowsLength,
    toLogicalIndex,
    rowAt,
    anchorIndex,
    debug,
  ]);

  // Auto-shift anchor backward when scrolling near the start of the data window
  // TODO: Re-enable once we understand the ping-pong issue better
  // For now, disabled like useZeroVirtualizer
  useEffect(() => {
    if (
      !autoPagingEnabled ||
      virtualItems.length === 0 ||
      !complete ||
      atStart ||
      anchorKind === 'permalink' // Permalink anchors naturally have firstRowIndex < 0
    ) {
      if (debug && virtualItems.length > 0) {
        console.log('[AutoAnchor backward] Skipping:', {
          complete,
          atStart,
          anchorKind,
        });
      }
      return;
    }

    // Cooldown to prevent rapid consecutive shifts
    const now = Date.now();
    if (now - lastAnchorShiftRef.current < ANCHOR_SHIFT_COOLDOWN) {
      return;
    }

    // Find first non-placeholder item
    let firstItem = virtualItems[0];
    let firstRow = rows[firstItem.index];

    if (!firstRow) {
      // Try to find first non-placeholder item
      for (let i = 1; i < virtualItems.length; i++) {
        const item = virtualItems[i];
        const row = rows[item.index];
        if (row) {
          firstItem = item;
          firstRow = row;
          break;
        }
      }

      // Skip if no non-placeholder items found
      if (!firstRow) {
        if (debug) {
          console.log(
            '[AutoAnchor backward] Skipping: no non-placeholder items',
          );
        }
        return;
      }
    }

    // Convert virtualizer index to logical index
    const firstLogicalIndex = toLogicalIndex(firstItem.index);

    // How far is the first visible item from the start of our data window?
    const distanceFromStart = firstLogicalIndex - firstRowIndex;

    // Threshold: shift anchor when we're within 10% of page size from the edge
    const nearPageEdgeThreshold = Math.ceil(PAGE_SIZE * 0.1);

    if (debug) {
      console.log('[AutoAnchor backward] Check:', {
        distanceFromStart,
        nearPageEdgeThreshold,
        wouldTrigger: distanceFromStart <= nearPageEdgeThreshold,
        firstLogicalIndex,
        firstRowIndex,
        anchorKind,
        anchorIndex,
      });
    }

    // Trigger when near start of data window
    if (distanceFromStart <= nearPageEdgeThreshold) {
      // Shift anchor backward: position anchor so FIRST visible item
      // will be at ~40% into the new window (giving 40% buffer behind)
      // For backward anchor, data goes from (anchorIndex - rowsLength) to anchorIndex
      // So newAnchorIndex = firstLogicalIndex + 60% of page size
      const newAnchorIndex = firstLogicalIndex + Math.ceil(PAGE_SIZE * 0.6);
      const newAnchorRow = rowAt(newAnchorIndex);

      if (debug) {
        console.log('[AutoAnchor backward] Attempting shift:', {
          firstLogicalIndex,
          newAnchorIndex,
          hasRow: !!newAnchorRow,
          currentAnchorIndex: anchorIndex,
        });
      }

      if (newAnchorRow && newAnchorIndex !== anchorIndex) {
        if (debug) {
          console.log(
            '[AutoAnchor] Shifting backward:',
            'distanceFromStart=',
            distanceFromStart,
            'firstLogicalIndex=',
            firstLogicalIndex,
            'newAnchorIndex=',
            newAnchorIndex,
          );
        }
        lastAnchorShiftRef.current = now;
        setAnchorKind('backward');
        setAnchorIndex(newAnchorIndex);
        setStartRow(toStartRow(newAnchorRow));
        setPermalinkID(undefined);
      }
    }
  }, [
    virtualItems,
    complete,
    atStart,
    firstRowIndex,
    rowsLength,
    toLogicalIndex,
    rowAt,
    anchorIndex,
    debug,
  ]);

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
          Array Virtualizer Test - {ZERO_PROJECT_NAME}
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
                marginBottom: '6px',
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
                checked={autoPagingEnabled}
                onChange={e => setAutoPagingEnabled(e.target.checked)}
                style={{marginRight: '6px'}}
              />
              Auto-Paging on Scroll
            </label>
          </div>
          <div style={{fontSize: '13px'}}>
            <div>
              rows.length: <strong>{rows.length}</strong>
            </div>
            <div>
              Window Size (rowsLength): <strong>{rowsLength}</strong>
            </div>
            <div>
              Virtual Items: <strong>{virtualItems.length}</strong>
            </div>
            <div>
              Total Height: <strong>{Math.round(totalSize)}px</strong>
            </div>
            <div>
              Scroll Offset: <strong>{virtualizer.scrollOffset ?? 0}px</strong>
            </div>
            <div>
              Complete: <strong>{complete ? 'Yes' : 'No'}</strong>
            </div>
            <div>
              Empty: <strong>{rowsEmpty ? 'Yes' : 'No'}</strong>
            </div>
            <div>
              At Start: <strong>{atStart ? 'Yes' : 'No'}</strong>
            </div>
            <div>
              At End: <strong>{atEnd ? 'Yes' : 'No'}</strong>
            </div>
            <div>
              First Row Index: <strong>{firstRowIndex}</strong>
            </div>
            <div>
              Anchor Index: <strong>{anchorIndex}</strong>
            </div>
            <div>
              Anchor Kind: <strong>{anchorKind}</strong>
            </div>
            <div>
              Selected Row: <strong>{selectedRowIndex ?? 'None'}</strong>
            </div>
            {permalinkNotFound && (
              <div style={{color: '#d32f2f', fontWeight: 'bold'}}>
                Permalink Not Found!
              </div>
            )}
          </div>
        </div>

        {/* Window Navigation */}
        <div
          style={{
            marginBottom: '20px',
            padding: '12px',
            backgroundColor: '#fff',
            borderRadius: '4px',
          }}
        >
          <h3 style={{margin: '0 0 8px 0', fontSize: '14px'}}>
            Window Navigation
          </h3>
          <div style={{fontSize: '11px', color: '#666', marginBottom: '8px'}}>
            Click a row to select it as anchor, then choose direction.
          </div>
          <div style={{marginBottom: '8px'}}>
            <button
              onClick={() => {
                hasPositionedPermalinkRef.current = false;
                setAutoPagingEnabled(false);
                setAnchorKind('permalink');
                setAnchorIndex(0);
                setPermalinkID('3130');
                setStartRow(undefined);
                setSelectedRowIndex(null);
              }}
              style={{
                width: '100%',
                padding: '8px',
                fontSize: '13px',
                backgroundColor: '#9c27b0',
                color: '#fff',
                border: 'none',
                borderRadius: '3px',
                cursor: 'pointer',
                marginBottom: '8px',
              }}
            >
              Go to Permalink #3130
            </button>
            <button
              onClick={() => {
                setAnchorKind('forward');
                setAnchorIndex(0);
                setStartRow(undefined);
                setPermalinkID(undefined);
                setSelectedRowIndex(null);
              }}
              style={{
                width: '100%',
                padding: '8px',
                fontSize: '13px',
                backgroundColor: '#28a745',
                color: '#fff',
                border: 'none',
                borderRadius: '3px',
                cursor: 'pointer',
                marginBottom: '8px',
              }}
            >
              Anchor at Top
            </button>
            <button
              onClick={() => {
                if (selectedRowIndex === null) return;
                // Use the previous row as anchor since anchor is exclusive
                const prevIndex = selectedRowIndex - 1;
                const prevRow = rowAt(prevIndex);
                setAnchorKind('forward');
                setPermalinkID(undefined);
                if (prevRow) {
                  setAnchorIndex(prevIndex);
                  setStartRow(toStartRow(prevRow));
                } else {
                  // No previous row means we're at the start
                  setAnchorIndex(0);
                  setStartRow(undefined);
                }
              }}
              disabled={selectedRowIndex === null}
              style={{
                width: '100%',
                padding: '8px',
                fontSize: '13px',
                backgroundColor: selectedRowIndex === null ? '#ccc' : '#0066cc',
                color: '#fff',
                border: 'none',
                borderRadius: '3px',
                cursor: selectedRowIndex === null ? 'not-allowed' : 'pointer',
                marginBottom: '8px',
              }}
            >
              Anchor Forward from Selected
            </button>
            <button
              onClick={() => {
                if (selectedRowIndex === null) return;
                // Use the next row as anchor since anchor is exclusive
                const nextIndex = selectedRowIndex + 1;
                const nextRow = rowAt(nextIndex);
                if (nextRow) {
                  setAnchorKind('backward');
                  setPermalinkID(undefined);
                  setAnchorIndex(nextIndex);
                  setStartRow(toStartRow(nextRow));
                }
              }}
              disabled={selectedRowIndex === null}
              style={{
                width: '100%',
                padding: '8px',
                fontSize: '13px',
                backgroundColor: selectedRowIndex === null ? '#ccc' : '#0066cc',
                color: '#fff',
                border: 'none',
                borderRadius: '3px',
                cursor: selectedRowIndex === null ? 'not-allowed' : 'pointer',
                marginBottom: '8px',
              }}
            >
              Anchor Backward from Selected
            </button>
            <button
              onClick={() => {
                setAnchorKind('forward');
                setAnchorIndex(0);
                setStartRow(undefined);
                setPermalinkID(undefined);
                setSelectedRowIndex(null);
              }}
              disabled={anchorIndex === 0 && selectedRowIndex === null}
              style={{
                width: '100%',
                padding: '8px',
                fontSize: '13px',
                backgroundColor:
                  anchorIndex === 0 && selectedRowIndex === null
                    ? '#ccc'
                    : '#6c757d',
                color: '#fff',
                border: 'none',
                borderRadius: '3px',
                cursor:
                  anchorIndex === 0 && selectedRowIndex === null
                    ? 'not-allowed'
                    : 'pointer',
              }}
            >
              Reset to Start
            </button>
          </div>
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
            Virtual List ({rowsLength} rows)
          </h2>
        </div>
        <div
          ref={parentRef}
          style={{
            flex: 1,
            overflow: 'auto',
            position: 'relative',
            // Disable browser scroll anchoring - virtualizer handles scroll stability
            overflowAnchor: 'none',
          }}
        >
          {rowsEmpty ? (
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                height: '100%',
                color: '#999',
              }}
            >
              No rows.
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
                  const issue = rows[virtualItem.index];
                  const logicalIndex = toLogicalIndex(virtualItem.index);
                  const isSelected = selectedRowIndex === logicalIndex;

                  return (
                    <div
                      key={virtualItem.key}
                      data-index={virtualItem.index}
                      ref={virtualizer.measureElement}
                      onClick={() => {
                        if (issue) {
                          setSelectedRowIndex(logicalIndex);
                        }
                      }}
                      style={{
                        padding: '8px 16px',
                        borderBottom: '1px solid #eee',
                        backgroundColor: isSelected ? '#e3f2fd' : '#fff',
                        cursor: issue ? 'pointer' : 'default',
                        ...(!issue
                          ? {
                              height: PLACEHOLDER_HEIGHT,
                              boxSizing: 'border-box',
                            }
                          : {}),
                      }}
                    >
                      {issue ? (
                        <div>
                          <div style={{fontWeight: 'bold', fontSize: '14px'}}>
                            #{issue.shortID} - {issue.title}
                          </div>
                          <div
                            style={{
                              fontSize: '12px',
                              color: '#666',
                              marginTop: '2px',
                            }}
                          >
                            id: {issue.id}
                          </div>
                          <div
                            style={{
                              fontSize: '12px',
                              color: '#666',
                              marginTop: '2px',
                            }}
                          >
                            Status: {issue.open ? 'Open' : 'Closed'} | Created:{' '}
                            {new Date(issue.created).toLocaleDateString()}
                          </div>
                          {issue.description && (
                            <div
                              style={{
                                fontSize: '12px',
                                color: '#888',
                                marginTop: '4px',
                              }}
                            >
                              {issue.description}
                            </div>
                          )}
                        </div>
                      ) : (
                        <div style={{color: '#999'}}>
                          Loading row {virtualItem.index}...
                        </div>
                      )}
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

function AppContent() {
  const login = useLogin();

  if (!login.loginState) {
    return (
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          height: '100vh',
        }}
      >
        <div>
          <h2>Loading...</h2>
        </div>
      </div>
    );
  }

  return <ArrayTestAppContent />;
}

export function ArrayTestApp() {
  return (
    <LoginProvider>
      <ZeroInit>
        <AppContent />
      </ZeroInit>
    </LoginProvider>
  );
}
