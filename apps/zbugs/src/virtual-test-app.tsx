import type {VirtualItem} from '@rocicorp/react-virtual';
import {useZero, useZeroVirtualizer} from '@rocicorp/zero/react';
import {
  memo,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import {queries, type IssueRowSort} from '../shared/queries.js';
import {ZERO_PROJECT_NAME} from '../shared/schema.js';
import {LoginProvider} from './components/login-provider.js';
import {Markdown} from './components/markdown.js';
import {useLogin} from './hooks/use-login.js';
import './virtual-test.css';
import {ZeroInit} from './zero-init.js';

const ITEM_SIZE = 275;
const MAX_DESCRIPTION_HEIGHT = 200;

const estimateSize = () => ITEM_SIZE;

const toStartRow = (row: {id: string; modified: number; created: number}) => ({
  id: row.id,
  modified: row.modified,
  created: row.created,
});

const getRowKey = (row: {id: string}) => row.id;

// Memoized markdown description component to avoid re-parsing on every render
const IssueDescription = memo(({text}: {text: string}) => {
  const contentRef = useRef<HTMLDivElement>(null);
  const [isOverflowing, setIsOverflowing] = useState(false);

  useLayoutEffect(() => {
    if (contentRef.current) {
      setIsOverflowing(
        contentRef.current.scrollHeight > MAX_DESCRIPTION_HEIGHT,
      );
    }
  }, [text]);

  return (
    <div
      ref={contentRef}
      className="markdown-container virtual-list-preview"
      style={{
        fontSize: '12px',
        color: '#888',
        marginTop: '4px',
        maxHeight: `${MAX_DESCRIPTION_HEIGHT}px`,
        overflow: 'hidden',
        position: 'relative',
      }}
    >
      <Markdown>{text}</Markdown>
      {isOverflowing && (
        <div
          style={{
            position: 'absolute',
            bottom: 0,
            left: 0,
            right: 0,
            height: '30px',
            background: 'linear-gradient(to bottom, transparent, white)',
            pointerEvents: 'none',
          }}
        />
      )}
    </div>
  );
});

function useHash() {
  const [hash, setHash] = useState(() => window.location.hash.slice(1));

  useEffect(() => {
    const handleHashChange = () => {
      setHash(window.location.hash.slice(1));
    };
    window.addEventListener('hashchange', handleHashChange);
    return () => window.removeEventListener('hashchange', handleHashChange);
  }, []);

  return hash;
}

function VirtualList() {
  const z = useZero();
  const hash = useHash();
  const permalinkID = useMemo(
    () => (hash.startsWith('issue-') ? hash.slice(6) : null),
    [hash],
  );

  const listRef = useRef<HTMLDivElement>(null);

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
      permalinkID: null, // Don't include actual permalinkID to avoid context changes
    }),
    [], // No dependencies - list context is stable
  );

  // // Simple state management for permalink state (instead of using wouter history)
  // const [permalinkState, setPermalinkState] =
  //   useState<PermalinkHistoryState<IssueRowSort> | null>(null);

  const {virtualizer, rowAt, complete, rowsEmpty, permalinkNotFound, total} =
    useZeroVirtualizer({
      estimateSize,
      getScrollElement: useCallback(() => listRef.current, [listRef.current]),

      getRowKey,

      listContextParams,
      permalinkID,

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
          // Check if id is numeric (shortID) or a UUID
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

      // permalinkState,
      // onPermalinkStateChange: setPermalinkState,

      debug: true, // Enable to debug page load drift issue
    });

  const virtualItems = virtualizer.getVirtualItems();

  const handleIssueClick = (issue: {id: string; shortID: number | null}) => {
    window.location.hash = `issue-${issue.shortID ?? issue.id}`;
  };

  const handleClearPermalink = () => {
    window.location.hash = '';
  };

  return (
    <div style={{display: 'flex', flexDirection: 'column', height: '100vh'}}>
      <div
        style={{
          padding: '16px',
          borderBottom: '1px solid #ccc',
          backgroundColor: '#f5f5f5',
          flexShrink: 0,
        }}
      >
        <h1 style={{margin: '0 0 8px 0', fontSize: '20px'}}>
          Virtual List Test - {ZERO_PROJECT_NAME}
        </h1>
        <div style={{fontSize: '14px', color: '#666'}}>
          <div style={{minHeight: '22px', marginBottom: '4px'}}>
            {permalinkID && (
              <>
                Permalink: <strong>#{permalinkID}</strong>{' '}
                <button onClick={handleClearPermalink}>Clear</button>
                {permalinkNotFound && (
                  <span style={{color: 'red', marginLeft: '8px'}}>
                    (Not Found)
                  </span>
                )}
              </>
            )}
          </div>
          <div style={{minHeight: '20px', marginBottom: '4px'}}>
            {complete && (
              <>
                Total Issues: <strong>{total ?? 'Loading...'}</strong>
              </>
            )}
            {rowsEmpty && <>No issues found</>}
          </div>
          <div style={{fontSize: '12px', color: '#999'}}>
            Scroll offset: {Math.round(virtualizer.scrollOffset ?? 0)}px | Est
            total: {virtualizer.options.count}
          </div>
        </div>
      </div>

      <div
        ref={listRef}
        style={{
          flex: 1,
          overflow: 'auto',
          position: 'relative',
          // Disable browser scroll anchoring - virtualizer handles scroll stability
          overflowAnchor: 'none',
        }}
      >
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
            {virtualItems.map((virtualItem: VirtualItem) => {
              const issue = rowAt(virtualItem.index);
              const isPermalink =
                issue &&
                permalinkID &&
                (permalinkID === issue.id ||
                  permalinkID === String(issue.shortID));

              return (
                <div
                  key={virtualItem.key}
                  data-index={virtualItem.index}
                  ref={virtualizer.measureElement}
                  style={{
                    padding: '8px 16px',
                    borderBottom: '1px solid #eee',
                    backgroundColor: isPermalink ? '#fff3cd' : '#fff',
                    cursor: issue ? 'pointer' : 'default',
                    transition: isPermalink ? 'background-color 0.3s' : 'none',
                    ...(!issue
                      ? {height: ITEM_SIZE, boxSizing: 'border-box'}
                      : {}),
                  }}
                  onClick={() => issue && handleIssueClick(issue)}
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
                        <IssueDescription text={issue.description} />
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

  return <VirtualList />;
}

export function VirtualTestApp() {
  return (
    <LoginProvider>
      <ZeroInit>
        <AppContent />
      </ZeroInit>
    </LoginProvider>
  );
}
