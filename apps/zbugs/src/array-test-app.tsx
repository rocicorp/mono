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
const PLACEHOLDER_HEIGHT = 50;
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
  >('forward');
  const [startRow, setStartRow] = useState<IssueRowSort | undefined>(undefined);
  const [permalinkID, setPermalinkID] = useState<string | undefined>(undefined);
  const [selectedRowIndex, setSelectedRowIndex] = useState<number | null>(null);
  const [debug, setDebug] = useState<boolean>(false);

  const {
    rowAt,
    rowsLength,
    complete,
    rowsEmpty,
    atStart,
    atEnd,
    firstRowIndex,
  } = useRows<RowData, IssueRowSort>({
    pageSize: PAGE_SIZE,
    anchor:
      anchorKind === 'permalink'
        ? {
            kind: 'permalink',
            index: anchorIndex,
            id: permalinkID!,
          }
        : anchorKind === 'forward'
          ? {
              kind: 'forward',
              index: anchorIndex,
              startRow,
            }
          : {
              kind: 'backward',
              index: anchorIndex,
              startRow: startRow!,
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

  // Track estimated total - only grows, except when we reach start or end
  const [estimatedTotal, setEstimatedTotal] = useState(0);

  // Update estimated total based on what we know
  useEffect(() => {
    // The furthest index we've seen is firstRowIndex + rowsLength
    const currentEnd = firstRowIndex + rowsLength;

    if (debug) {
      console.log('[estimatedTotal update check]', {
        currentEnd,
        estimatedTotal,
        atStart,
        atEnd,
        firstRowIndex,
        rowsLength,
      });
    }

    if (atStart && atEnd) {
      // We know the exact total
      if (debug)
        console.log('[estimatedTotal] Setting exact total:', rowsLength);
      setEstimatedTotal(rowsLength);
    } else if (atEnd) {
      // We know the exact end now
      if (debug)
        console.log('[estimatedTotal] Setting from atEnd:', currentEnd);
      setEstimatedTotal(currentEnd);
    } else if (atStart) {
      // We know we start at 0, so current end is accurate
      if (debug)
        console.log(
          '[estimatedTotal] atStart, max with currentEnd:',
          currentEnd,
        );
      setEstimatedTotal(prev => Math.max(prev, currentEnd));
    } else if (currentEnd > estimatedTotal) {
      // We've seen further than before, update estimate
      if (debug) console.log('[estimatedTotal] Growing to:', currentEnd);
      setEstimatedTotal(currentEnd);
    }
    // Don't decrease estimate unless we've reached start or end
  }, [firstRowIndex, rowsLength, atStart, atEnd, estimatedTotal, debug]);

  // Create a Proxy to make rows behave like an array
  // The virtualizer uses indices 0, 1, 2, ...
  // With estimatedTotal, we expose indices 0 to estimatedTotal-1 (plus placeholders)
  // rowAt will return undefined for indices outside the current data window

  const startPlaceholder = atStart ? 0 : 1;
  const endPlaceholder = atEnd ? 0 : 1;

  // Convert virtualizer index to logical data index
  // Virtualizer index 0 is placeholder (if !atStart), then logical indices 0, 1, 2...
  const toLogicalIndex = useCallback(
    (virtualizerIndex: number) => virtualizerIndex - startPlaceholder,
    [startPlaceholder],
  );

  const rows = useMemo(() => {
    // Total length: placeholder + estimatedTotal + placeholder
    const totalLength = startPlaceholder + estimatedTotal + endPlaceholder;

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
            // Map to logical index (0-based)
            const logicalIndex = index - startPlaceholder;
            return rowAt(logicalIndex);
          }
        }
        return Reflect.get(target, prop);
      },
    };
    return new Proxy<RowData[]>([], handler);
  }, [rowAt, estimatedTotal, startPlaceholder, endPlaceholder, atStart, atEnd]);

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

  // Auto-shift anchor forward when scrolling near the end of the data window
  useEffect(() => {
    if (virtualItems.length === 0 || !complete || atEnd) {
      return;
    }

    const lastItem = virtualItems[virtualItems.length - 1];
    // Convert virtualizer index to logical index
    const lastLogicalIndex = toLogicalIndex(lastItem.index);
    const lastDataIndex = firstRowIndex + rowsLength - 1;

    // How far is the last visible item from the end of our data window?
    const distanceFromEnd = lastDataIndex - lastLogicalIndex;

    // Threshold: shift anchor when we're within 20% of page size from the end (like zbugs)
    const nearPageEdgeThreshold = Math.ceil(PAGE_SIZE * 0.2);

    if (distanceFromEnd <= nearPageEdgeThreshold) {
      // Shift anchor forward: go back 40% from first visible
      const firstItem = virtualItems[0];
      const firstLogicalIndex = toLogicalIndex(firstItem.index);
      const newAnchorIndex = Math.max(
        firstRowIndex,
        firstLogicalIndex - Math.ceil(PAGE_SIZE * 0.4),
      );
      const newAnchorRow = rowAt(newAnchorIndex);

      if (newAnchorRow && newAnchorIndex !== anchorIndex) {
        if (debug) {
          console.log(
            '[AutoAnchor] Shifting forward:',
            'distanceFromEnd=',
            distanceFromEnd,
            'newAnchorIndex=',
            newAnchorIndex,
          );
        }
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
  useEffect(() => {
    if (virtualItems.length === 0 || !complete || atStart) {
      if (debug && virtualItems.length > 0) {
        console.log('[AutoAnchor backward] Skipping:', {complete, atStart});
      }
      return;
    }

    const firstItem = virtualItems[0];
    // Convert virtualizer index to logical index
    const firstLogicalIndex = toLogicalIndex(firstItem.index);

    // How far is the first visible item from the start of our data window?
    const distanceFromStart = firstLogicalIndex - firstRowIndex;

    if (debug) {
      console.log('[AutoAnchor backward] Check:', {
        firstLogicalIndex,
        firstRowIndex,
        distanceFromStart,
        anchorKind,
        anchorIndex,
      });
    }

    // Threshold: shift anchor when we're within 20% of page size from the start (like zbugs)
    const nearPageEdgeThreshold = Math.ceil(PAGE_SIZE * 0.2);

    if (distanceFromStart <= nearPageEdgeThreshold) {
      // Shift anchor backward: go forward 40% from last visible
      const lastItem = virtualItems[virtualItems.length - 1];
      const lastLogicalIndex = toLogicalIndex(lastItem.index);
      const lastDataIndex = firstRowIndex + rowsLength - 1;
      const newAnchorIndex = Math.min(
        lastDataIndex,
        lastLogicalIndex + Math.ceil(PAGE_SIZE * 0.4),
      );
      const newAnchorRow = rowAt(newAnchorIndex);

      if (debug) {
        console.log('[AutoAnchor backward] Attempting shift:', {
          lastLogicalIndex,
          lastDataIndex,
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
            'newAnchorIndex=',
            newAnchorIndex,
          );
        }
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

  // When we reach the start with a backward anchor, reset to forward from top
  useEffect(() => {
    if (atStart && anchorKind === 'backward' && firstRowIndex < 0) {
      if (debug) {
        console.log(
          '[AutoAnchor] Reached start with backward anchor, resetting to top',
        );
      }
      setAnchorKind('forward');
      setAnchorIndex(0);
      setStartRow(undefined);
      setPermalinkID(undefined);
      virtualizer.scrollToOffset(0);
    }
  }, [atStart, anchorKind, firstRowIndex, debug, virtualizer]);

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
              Rows Length: <strong>{rows.length}</strong>
            </div>
            <div>
              Window Size: <strong>{rowsLength}</strong>
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
