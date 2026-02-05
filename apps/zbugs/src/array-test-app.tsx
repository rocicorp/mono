import {useZero} from '@rocicorp/zero/react';
import {useCallback, useEffect, useMemo, useRef, useState} from 'react';
import {queries, type IssueRowSort, type Issues} from '../shared/queries.js';
import {ZERO_PROJECT_NAME} from '../shared/schema.js';
import {LoginProvider} from './components/login-provider.js';
import {useArrayVirtualizer} from './hooks/use-array-virtualizer.js';
import {useLogin} from './hooks/use-login.js';
import {ZeroInit} from './zero-init.js';

type RowData = Issues[number];

const DEFAULT_HEIGHT = 275;
const PLACEHOLDER_HEIGHT = 50;
const PAGE_SIZE = 50;
const UNIFORM_ROW_HEIGHT = 63;

const toStartRow = (row: {id: string; modified: number; created: number}) => ({
  id: row.id,
  modified: row.modified,
  created: row.created,
});

function ArrayTestAppContent() {
  const z = useZero();

  const [permalinkID, setPermalinkID] = useState<string | undefined>(undefined);
  const [permalinkInput, setPermalinkInput] = useState('3130');
  const [notFoundPermalink, setNotFoundPermalink] = useState<
    string | undefined
  >(undefined);

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
      permalinkID: permalinkID ?? null,
    }),
    [permalinkID],
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

  const {virtualizer, rowAt, rowsEmpty, permalinkNotFound} =
    useArrayVirtualizer<RowData, IssueRowSort>({
      pageSize: PAGE_SIZE,
      placeholderHeight: PLACEHOLDER_HEIGHT,
      getPageQuery,
      getSingleQuery,
      toStartRow,
      initialPermalinkID: permalinkID,

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

  // Reset permalink if not found (but keep the input value)
  useEffect(() => {
    if (permalinkNotFound && permalinkID) {
      setNotFoundPermalink(permalinkID);
      setPermalinkID(undefined);
    }
  }, [permalinkNotFound, permalinkID]);

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
                setPermalinkID(permalinkInput);
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
                  setPermalinkID(undefined);
                  setPermalinkInput('');
                  setNotFoundPermalink(undefined);
                }}
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
                  return (
                    <div
                      key={virtualItem.key}
                      data-index={virtualItem.index}
                      ref={
                        heightMode === 'dynamic'
                          ? virtualizer.measureElement
                          : undefined
                      }
                      style={{
                        padding: '8px 16px',
                        borderBottom: '1px solid #eee',
                        backgroundColor: '#fff',
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
