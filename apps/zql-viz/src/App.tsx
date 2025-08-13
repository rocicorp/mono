/* eslint-disable @typescript-eslint/no-explicit-any */
import {useState, useCallback, useEffect} from 'react';
import {Panel, PanelGroup, PanelResizeHandle} from 'react-resizable-panels';
import {QueryEditor} from './components/QueryEditor';
import {ResultsViewer} from './components/ResultsViewer';
import {QueryHistory} from './components/QueryHistory';
import {type QueryHistoryItem} from './types';
import './App.css';

const DEFAULT_QUERY = `const {
  createBuilder,
  createSchema,
  table,
  number,
  string,
  relationships,
} = zero;

// === Schema Declaration ===
const user = table('user')
  .columns({
    id: string(),
    name: string(),
  });

const session = table('session')
  .columns({
    id: string(),
    userId: string(),
    createdAt: number(),
  });

const userToSession = relationships(user, ({many}) => ({
  sessions: many({
    sourceField: ['id'],
    destField: ['userId'],
    destSchema: session,
  }),
}));

const builder = createBuilder(createSchema({
  tables: [user, session],
  relationships: [userToSession]
}));

//: Get user with recent sessions
run(
  builder.user.where('id', '=', 'some-user-id')
    .related('sessions', q => q.orderBy('createdAt', 'desc').one())
)`;

function App() {
  const [queryCode, setQueryCode] = useState(() => {
    const savedQuery = localStorage.getItem('zql-query');
    if (savedQuery) {
      return savedQuery;
    }
    return DEFAULT_QUERY;
  });
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [history, setHistory] = useState<QueryHistoryItem[]>(() => {
    const savedHistory = localStorage.getItem('zql-history');
    if (savedHistory) {
      const parsed = JSON.parse(savedHistory);
      return parsed.map((item: any) => ({
        ...item,
        timestamp: new Date(item.timestamp),
      }));
    }

    return [];
  });

  useEffect(() => {
    localStorage.setItem('zql-query', queryCode);
  }, [queryCode]);

  useEffect(() => {
    localStorage.setItem('zql-history', JSON.stringify(history));
  }, [history]);

  const executeQuery = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    setResult(null);

    // Extract text after //:  comment for history preview
    const extractHistoryPreview = (code: string): string => {
      const lines = code.split('\n');

      // Find the line with //: comment
      let previewStartIndex = -1;
      for (let i = 0; i < lines.length; i++) {
        const trimmedLine = lines[i].trim();
        if (trimmedLine.startsWith('//:')) {
          previewStartIndex = i;
          break;
        }
      }

      if (previewStartIndex === -1) {
        return ''; // No //: comment found, fallback to full code
      }

      const previewLines = lines.slice(previewStartIndex).join('\n');

      // Combine title and preview
      return previewLines;
    };

    const zero = await import('@rocicorp/zero');
    let capturedQuery: any = null;
    const historyPreviewText = extractHistoryPreview(queryCode);

    const customGlobals = {
      zero,
      run: (query: any) => {
        capturedQuery = query;
        console.log('RUNNING QUERY!', query);
        return query; // Return the query for potential chaining
      },
    };

    function executeCode(code: string) {
      const func = new Function(...Object.keys(customGlobals), code);
      return func(...Object.values(customGlobals));
    }

    try {
      executeCode(queryCode);

      setResult(capturedQuery);

      const historyItem: QueryHistoryItem = {
        id: Date.now().toString(),
        query: historyPreviewText || queryCode, // Use the preview text or fallback to full code
        fullCode: queryCode, // Store the full code for re-execution
        timestamp: new Date(),
        result: capturedQuery,
      };

      setHistory(prev => [historyItem, ...prev].slice(0, 150));
    } catch (err) {
      const errorMessage =
        err instanceof Error ? err.message : 'Unknown error occurred';
      setError(errorMessage);

      const historyItem: QueryHistoryItem = {
        id: Date.now().toString(),
        query: historyPreviewText || queryCode, // Use the preview text or fallback to full code
        fullCode: queryCode, // Store the full code for re-execution
        timestamp: new Date(),
        error: errorMessage,
      };

      setHistory(prev => [historyItem, ...prev].slice(0, 150));
    } finally {
      setIsLoading(false);
    }
  }, [queryCode]);

  const handleSelectHistoryQuery = useCallback(
    (historyItem: QueryHistoryItem) => {
      // Use fullCode if available (the complete executable code), otherwise use the query preview
      setQueryCode(historyItem.fullCode || historyItem.query);
    },
    [],
  );

  const handleClearHistory = useCallback(() => {
    setHistory([]);
  }, []);

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        executeQuery();
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [executeQuery]);

  return (
    <div className="app">
      <header className="app-header">
        <h1>ZQL Viz</h1>
        <span className="subtitle">Interactive ZQL Query Explorer</span>
      </header>

      <div className="app-body">
        <PanelGroup direction="horizontal">
          <Panel defaultSize={20} minSize={15}>
            <PanelGroup direction="vertical">
              <Panel defaultSize={100} minSize={30}>
                <QueryHistory
                  history={history}
                  onSelectQuery={handleSelectHistoryQuery}
                  onClearHistory={handleClearHistory}
                />
              </Panel>
            </PanelGroup>
          </Panel>

          <PanelResizeHandle className="resize-handle-vertical" />

          <Panel defaultSize={40} minSize={30}>
            <QueryEditor
              value={queryCode}
              onChange={setQueryCode}
              onExecute={executeQuery}
            />
          </Panel>

          <PanelResizeHandle className="resize-handle-vertical" />

          <Panel defaultSize={40} minSize={30}>
            <ResultsViewer
              result={result}
              error={error}
              isLoading={isLoading}
            />
          </Panel>
        </PanelGroup>
      </div>
    </div>
  );
}

export default App;
