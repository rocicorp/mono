import {useZero} from '@rocicorp/zero/react';
import {useCallback, useEffect, useMemo, useRef, useState} from 'react';
import {queries, type IssueRowSort, type Issues} from '../shared/queries.js';
import {ZERO_PROJECT_NAME} from '../shared/schema.js';
import {LoginProvider} from './components/login-provider.js';
import {
  useArrayVirtualizer,
  type ScrollRestorationState,
} from './hooks/use-array-virtualizer.js';
import {useHash} from './hooks/use-hash.js';
import {ZeroInit} from './zero-init.js';

type RowData = Issues[number];

const DEFAULT_HEIGHT = 275;
const PLACEHOLDER_HEIGHT = 50;
const PAGE_SIZE = 100;
const UNIFORM_ROW_HEIGHT = 63;

const HASH_PREFIX = 'issue-';

function parsePermalinkFromHash(hash: string): string | undefined {
  if (hash.startsWith(HASH_PREFIX)) {
    const id = hash.slice(HASH_PREFIX.length);
    return id || undefined;
  }
  return undefined;
}

function isScrollRestorationState(
  state: unknown,
): state is ScrollRestorationState {
  return (
    state !== null &&
    typeof state === 'object' &&
    'scrollAnchorID' in state &&
    typeof (state as Record<string, unknown>).scrollAnchorID === 'string' &&
    'index' in state &&
    typeof (state as Record<string, unknown>).index === 'number' &&
    'scrollOffset' in state &&
    typeof (state as Record<string, unknown>).scrollOffset === 'number'
  );
}

function readHistoryScrollState(): ScrollRestorationState | undefined {
  const state = window.history.state;
  if (isScrollRestorationState(state)) {
    return state;
  }
  return undefined;
}

const toStartRow = (row: {id: string; modified: number; created: number}) => ({
  id: row.id,
  modified: row.modified,
  created: row.created,
});

function ArrayTestAppContent() {
  const z = useZero();

  const hash = useHash();
  const permalinkID = useMemo(() => parsePermalinkFromHash(hash), [hash]);
  const [permalinkInput, setPermalinkInput] = useState(
    () => parsePermalinkFromHash(window.location.hash.slice(1)) ?? '3130',
  );

  // Keep input in sync when hash changes externally.
  useEffect(() => {
    if (permalinkID) {
      setPermalinkInput(permalinkID);
    }
  }, [permalinkID]);

  const [notFoundPermalink, setNotFoundPermalink] = useState<
    string | undefined
  >(undefined);
  const [restoreInput, setRestoreInput] = useState<string>('');

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
    }),
    [],
  );

  const [heightMode, setHeightMode] = useState<
    'dynamic' | 'uniform' | 'non-uniform'
  >('dynamic');

  const parentRef = useRef<HTMLDivElement>(null);

  const getPageQuery = useCallback(
    (limit: number, start: IssueRowSort | null, dir: 'forward' | 'backward') =>
      queries.issueListV2({
        listContext: listContextParams,
        userID: z.userID,
        limit,
        start,
        dir,
        inclusive: start === null,
      }),
    [listContextParams, z.userID],
  );

  const getSingleQuery = useCallback(
    (id: string) => {
      const isNumeric = /^\d+$/.test(id);
      return queries.listIssueByID({
        idField: isNumeric ? 'shortID' : 'id',
        idValue: isNumeric ? parseInt(id, 10) : id,
        listContext: listContextParams,
      });
    },
    [listContextParams],
  );

  // ---- Controlled scroll state ----
  // Initialized from history.state on mount (for reload / back-forward).
  const [scrollState, setScrollState] = useState<
    ScrollRestorationState | undefined
  >(readHistoryScrollState);

  // Timer ref for throttled history.state saving.
  const saveStateTimerRef = useRef<ReturnType<typeof setTimeout>>();

  const onScrollStateChange = useCallback((state: ScrollRestorationState) => {
    setScrollState(state);

    // Throttled save to history.state.
    clearTimeout(saveStateTimerRef.current);
    saveStateTimerRef.current = setTimeout(() => {
      window.history.replaceState(state, '');
    }, 50);
  }, []);

  // Clean up timer on unmount.
  useEffect(() => () => clearTimeout(saveStateTimerRef.current), []);

  // Listen for back/forward navigations via the Navigation API.
  // Using `currententrychange` with `navigationType === 'traverse'` instead
  // of the raw `popstate` event avoids a subtle bug: Chrome fires `popstate`
  // as a side-effect of hash-only navigations (e.g. `location.hash = ...` or
  // fragment-only page.goto), which would clobber scrollState to `undefined`
  // mid-permalink-positioning.  The Navigation API only fires `traverse` for
  // genuine back/forward navigations.
  useEffect(() => {
    const nav = window.navigation;
    if (!nav) {
      // Fallback for environments without Navigation API.
      const onPopState = () => {
        setScrollState(readHistoryScrollState());
      };
      window.addEventListener('popstate', onPopState);
      return () => window.removeEventListener('popstate', onPopState);
    }
    const onEntryChange = (event: NavigationCurrentEntryChangeEvent) => {
      if (event.navigationType === 'traverse') {
        setScrollState(readHistoryScrollState());
      }
    };
    nav.addEventListener('currententrychange', onEntryChange);
    return () => nav.removeEventListener('currententrychange', onEntryChange);
  }, []);

  const {
    virtualizer,
    rowAt,
    rowsEmpty,
    permalinkNotFound,
    estimatedTotal,
    total,
  } = useArrayVirtualizer<RowData, IssueRowSort>({
    pageSize: PAGE_SIZE,
    getPageQuery,
    getSingleQuery,
    toStartRow,
    initialPermalinkID: permalinkID,
    scrollState,
    onScrollStateChange,

    estimateSize: useCallback(
      (row: RowData | undefined) => {
        if (!row) {
          return PLACEHOLDER_HEIGHT;
        }

        if (heightMode === 'uniform') {
          return UNIFORM_ROW_HEIGHT;
        }

        if (heightMode === 'non-uniform') {
          const baseHeight = 120;
          if (!row.description) {
            return baseHeight;
          }
          const descriptionLines = Math.ceil(row.description.length / 150);
          const descriptionHeight = descriptionLines * 20;
          return baseHeight + descriptionHeight;
        }

        return DEFAULT_HEIGHT;
      },
      [heightMode],
    ),
    getScrollElement: useCallback(() => parentRef.current, []),
  });

  // Use a ref so setPermalinkHash is stable (doesn't recreate on every
  // scroll-state change).
  const scrollStateRef = useRef(scrollState);
  scrollStateRef.current = scrollState;

  // Navigate to a permalink (or clear it). Saves the current scroll state
  // to the current history entry before pushing a new entry.
  const setPermalinkHash = useCallback((id: string | undefined) => {
    // Flush any pending throttled save and save immediately.
    clearTimeout(saveStateTimerRef.current);
    const currentState = scrollStateRef.current;
    if (currentState) {
      window.history.replaceState(currentState, '');
    }

    const url = new URL(location.href);
    url.hash = id ? `${HASH_PREFIX}${id}` : '';
    window.history.pushState(null, '', url);
  }, []);

  // Reset permalink if not found (but keep the input value)
  useEffect(() => {
    if (permalinkNotFound && permalinkID) {
      setNotFoundPermalink(permalinkID);
      setPermalinkHash(undefined);
    }
  }, [permalinkNotFound, permalinkID, setPermalinkHash]);

  const virtualItems = virtualizer.getVirtualItems();

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
        <div
          style={{
            marginBottom: '20px',
            padding: '12px',
            backgroundColor: '#fff',
            borderRadius: '4px',
            fontSize: '14px',
            fontWeight: 'bold',
            textAlign: 'center',
          }}
        >
          {total ?? `${estimatedTotal}+`} rows
        </div>

        {/* Height Mode Selector */}
        <div
          style={{
            marginBottom: '20px',
            padding: '12px',
            backgroundColor: '#fff',
            borderRadius: '4px',
          }}
        >
          <label
            style={{
              fontSize: '12px',
              fontWeight: 'bold',
              marginBottom: '4px',
              display: 'block',
            }}
          >
            Row Height Mode:
          </label>
          <select
            value={heightMode}
            onChange={e =>
              setHeightMode(
                e.target.value as 'dynamic' | 'uniform' | 'non-uniform',
              )
            }
            style={{
              width: '100%',
              padding: '4px',
              fontSize: '12px',
              borderRadius: '3px',
              border: '1px solid #ccc',
            }}
          >
            <option value="dynamic">Dynamic (Measured)</option>
            <option value="uniform">Fixed Uniform</option>
            <option value="non-uniform">Fixed Non-Uniform</option>
          </select>
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
          <form
            onSubmit={e => {
              e.preventDefault();
              if (permalinkInput.trim()) {
                setPermalinkHash(permalinkInput.trim());
                setNotFoundPermalink(undefined);
              }
            }}
            style={{marginBottom: '8px'}}
          >
            <label
              style={{
                fontSize: '12px',
                fontWeight: 'bold',
                marginBottom: '4px',
                display: 'block',
              }}
            >
              Permalink ID:
            </label>
            <input
              type="text"
              value={permalinkInput}
              onChange={e => setPermalinkInput(e.target.value)}
              placeholder="Enter ID (e.g., 3130)"
              data-testid="permalink-input"
              style={{
                width: '100%',
                padding: '8px',
                fontSize: '13px',
                border: '1px solid #ccc',
                borderRadius: '3px',
                marginBottom: '8px',
                boxSizing: 'border-box',
              }}
            />
            <div style={{display: 'flex', gap: '8px', marginBottom: '8px'}}>
              <button
                type="submit"
                disabled={!permalinkInput.trim()}
                data-testid="permalink-go-btn"
                style={{
                  flex: 1,
                  padding: '8px',
                  fontSize: '13px',
                  backgroundColor: permalinkInput.trim() ? '#9c27b0' : '#ccc',
                  color: '#fff',
                  border: 'none',
                  borderRadius: '3px',
                  cursor: permalinkInput.trim() ? 'pointer' : 'not-allowed',
                }}
              >
                Go
              </button>
              <button
                type="button"
                onClick={() => {
                  setPermalinkHash(undefined);
                  setPermalinkInput('');
                  setNotFoundPermalink(undefined);
                }}
                data-testid="permalink-clear-btn"
                style={{
                  padding: '8px 16px',
                  fontSize: '13px',
                  backgroundColor: '#666',
                  color: '#fff',
                  border: 'none',
                  borderRadius: '3px',
                  cursor: 'pointer',
                }}
              >
                Clear
              </button>
            </div>
          </form>
          {notFoundPermalink && (
            <div
              style={{
                marginTop: '8px',
                padding: '8px',
                backgroundColor: '#fff3cd',
                color: '#856404',
                fontSize: '12px',
                borderRadius: '3px',
                border: '1px solid #ffeaa7',
              }}
            >
              Permalink not found: {notFoundPermalink}
            </div>
          )}
        </div>

        {/* Scroll Anchor State */}
        <div
          style={{
            marginBottom: '20px',
            padding: '12px',
            backgroundColor: '#fff',
            borderRadius: '4px',
          }}
        >
          <h3 style={{margin: '0 0 8px 0', fontSize: '14px'}}>
            Scroll Anchor State
          </h3>
          <div style={{fontSize: '12px', fontFamily: 'monospace'}}>
            {scrollState ? (
              <>
                <div style={{marginBottom: '4px'}}>
                  <strong>scrollAnchorID:</strong> {scrollState.scrollAnchorID}
                </div>
                <div style={{marginBottom: '4px'}}>
                  <strong>index:</strong> {scrollState.index}
                </div>
                <div style={{marginBottom: '4px'}}>
                  <strong>scrollOffset:</strong> {scrollState.scrollOffset}px
                </div>
              </>
            ) : (
              <div style={{color: '#999'}}>No visible rows</div>
            )}
          </div>
          <button
            data-testid="capture-btn"
            onClick={() => {
              const stateStr = JSON.stringify(scrollState, null, 2);
              setRestoreInput(stateStr);
            }}
            disabled={!scrollState}
            style={{
              width: '100%',
              padding: '8px',
              fontSize: '13px',
              backgroundColor: scrollState ? '#2196f3' : '#ccc',
              color: '#fff',
              border: 'none',
              borderRadius: '3px',
              cursor: scrollState ? 'pointer' : 'not-allowed',
              marginTop: '8px',
            }}
          >
            Capture State
          </button>

          {/* Restore State */}
          <div
            style={{
              marginTop: '16px',
              paddingTop: '16px',
              borderTop: '1px solid #e0e0e0',
            }}
          >
            <label
              style={{
                fontSize: '12px',
                fontWeight: 'bold',
                marginBottom: '4px',
                display: 'block',
              }}
            >
              Restore State:
            </label>
            <textarea
              data-testid="restore-input"
              value={restoreInput}
              onChange={e => setRestoreInput(e.target.value)}
              placeholder="Paste captured state JSON here..."
              style={{
                width: '100%',
                padding: '8px',
                fontSize: '11px',
                fontFamily: 'monospace',
                border: '1px solid #ccc',
                borderRadius: '3px',
                marginBottom: '8px',
                boxSizing: 'border-box',
                minHeight: '80px',
                resize: 'vertical',
              }}
            />
            <button
              data-testid="restore-btn"
              onClick={() => {
                try {
                  const parsed = JSON.parse(restoreInput);
                  // null/falsy → undefined (scroll to top)
                  setScrollState(parsed || undefined);
                } catch (err) {
                  alert('Invalid JSON: ' + (err as Error).message);
                }
              }}
              disabled={!restoreInput.trim()}
              style={{
                width: '100%',
                padding: '8px',
                fontSize: '13px',
                backgroundColor: restoreInput.trim() ? '#ff9800' : '#ccc',
                color: '#fff',
                border: 'none',
                borderRadius: '3px',
                cursor: restoreInput.trim() ? 'pointer' : 'not-allowed',
              }}
            >
              Restore State
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
          <h2 style={{margin: 0, fontSize: '16px'}}>Virtual List</h2>
        </div>
        {notFoundPermalink && (
          <div
            style={{
              padding: '12px 16px',
              backgroundColor: '#fff3cd',
              color: '#856404',
              borderBottom: '2px solid #ffc107',
              fontSize: '14px',
              fontWeight: 500,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
            }}
          >
            <span>
              ⚠️ Permalink not found: <strong>{notFoundPermalink}</strong>
            </span>
            <button
              onClick={() => {
                setPermalinkInput('');
                setNotFoundPermalink(undefined);
              }}
              style={{
                background: 'none',
                border: 'none',
                color: '#856404',
                cursor: 'pointer',
                fontSize: '18px',
                padding: '0 4px',
                lineHeight: 1,
              }}
              title="Dismiss"
            >
              ×
            </button>
          </div>
        )}
        <div
          ref={parentRef}
          style={{
            flex: 1,
            overflow: 'auto',
            position: 'relative',
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
                height: `${virtualizer.getTotalSize()}px`,
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
                  const issue = rowAt(virtualItem.index);
                  const isPermalinkRow =
                    permalinkID &&
                    issue &&
                    (issue.id === permalinkID ||
                      (issue.shortID !== null &&
                        issue.shortID.toString() === permalinkID));
                  return (
                    <div
                      key={virtualItem.key}
                      data-index={virtualItem.index}
                      data-row-id={issue ? issue.id : undefined}
                      ref={
                        heightMode === 'dynamic'
                          ? virtualizer.measureElement
                          : undefined
                      }
                      style={{
                        padding: '8px 16px',
                        borderBottom: '1px solid #eee',
                        backgroundColor: isPermalinkRow ? '#e1bee7' : '#fff',
                        cursor: issue ? 'pointer' : 'default',
                        boxSizing: 'border-box',
                        ...(!issue
                          ? {
                              height: PLACEHOLDER_HEIGHT,
                            }
                          : heightMode !== 'dynamic'
                            ? {
                                height: virtualItem.size,
                                overflow: 'hidden',
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
