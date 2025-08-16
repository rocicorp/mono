/* eslint-disable @typescript-eslint/no-explicit-any */
import {useState, useCallback, useEffect} from 'react';
import {Panel, PanelGroup, PanelResizeHandle} from 'react-resizable-panels';
import {QueryEditor} from './components/query-editor.tsx';
import {ResultsViewer} from './components/results-viewer.tsx';
import {QueryHistory} from './components/query-history.tsx';
import {CredentialsModal} from './components/credentials-modal.tsx';
import {
  type QueryHistoryItem,
  type RemoteRunResult,
  type Result,
} from './types.ts';
import './App.css';
import * as zero from '@rocicorp/zero';
import {VizDelegate} from './query-delegate.ts';
import * as ts from 'typescript';
import {clientToServer} from '../../../packages/zero-schema/src/name-mapper.ts';
import {mapAST} from '../../../packages/zero-protocol/src/ast.ts';

type AnyQuery = zero.Query<any, any, any>;
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
  })
    .primaryKey('id');

const session = table('session')
  .columns({
    id: string(),
    userId: string(),
    createdAt: number(),
  })
    .primaryKey('id');

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
  const [result, setResult] = useState<Result | undefined>(undefined);
  const [error, setError] = useState<string | undefined>(undefined);
  const [isLoading, setIsLoading] = useState(false);
  const [auth, setAuth] = useState<
    {username: string; password: string} | undefined
  >(() => {
    const savedAuth = localStorage.getItem('zql-auth');
    return savedAuth ? JSON.parse(savedAuth) : undefined;
  });
  const [isCredentialsModalOpen, setIsCredentialsModalOpen] = useState(false);
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

  useEffect(() => {
    if (auth) {
      localStorage.setItem('zql-auth', JSON.stringify(auth));
    } else {
      localStorage.removeItem('zql-auth');
    }
  }, [auth]);

  const executeQuery = useCallback(async () => {
    setIsLoading(true);
    setError(undefined);
    setResult(undefined);

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

    let capturedQuery: AnyQuery | undefined;
    let capturedSchema: zero.Schema | undefined;
    const historyPreviewText = extractHistoryPreview(queryCode);

    const customGlobals = {
      zero: {
        ...zero,
        createSchema(args: Parameters<typeof zero.createSchema>[0]) {
          capturedSchema = zero.createSchema(args);
          return capturedSchema;
        },
      },
      run: (query: AnyQuery) => {
        capturedQuery = query;
        return query; // Return the query for potential chaining
      },
    };

    function executeCode(code: string) {
      // Transpile TypeScript to JavaScript
      const result = ts.transpileModule(code, {
        compilerOptions: {
          target: ts.ScriptTarget.ESNext,
          module: ts.ModuleKind.ESNext,
          strict: false,
          esModuleInterop: true,
          skipLibCheck: true,
          allowSyntheticDefaultImports: true,
        },
      });

      const func = new Function(
        ...Object.keys(customGlobals),
        result.outputText,
      );
      return func(...Object.values(customGlobals));
    }

    try {
      executeCode(queryCode);
      if (capturedSchema === undefined) {
        throw new Error('Failed to capture the schema definition');
      }
      if (capturedQuery === undefined) {
        throw new Error('Failed to capture the query definition');
      }
      const vizDelegate = new VizDelegate(capturedSchema);
      capturedQuery = capturedQuery.delegate(vizDelegate);
      (await capturedQuery.run()) as any;
      const graph = vizDelegate.getGraph();
      const mapper = clientToServer(capturedSchema.tables);

      let remoteRunResult: RemoteRunResult | undefined;
      try {
        if (auth) {
          const credentials = btoa(`${auth?.username}:${auth?.password}`);
          const response = await fetch(
            `${import.meta.env.VITE_PUBLIC_SERVER}/analyze-queryz`,
            {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
                'Authorization': `Basic ${credentials}`,
              },
              body: JSON.stringify({
                ast: mapAST(capturedQuery.ast, mapper),
              }),
            },
          );

          remoteRunResult = await response.json();
          console.log('REMOTE RESULT', remoteRunResult);
        } else {
          console.warn(
            'No auth credentials set, will not run the query server side or analyze it server side',
          );
        }
      } catch (e) {
        console.error(e);
      }

      setResult({
        ast: capturedQuery?.ast,
        graph,
        remoteRunResult,
      });

      // Check if the current query code is the same as the previous entry
      setHistory(prev => {
        if (prev.length > 0 && prev[0].fullCode === queryCode) {
          // Update the timestamp of the most recent entry
          const updatedHistory = [...prev];
          updatedHistory[0] = {
            ...updatedHistory[0],
            timestamp: new Date(),
            result: capturedQuery, // Update result too
          };
          return updatedHistory;
        } else {
          // Add new history entry
          const historyItem: QueryHistoryItem = {
            id: Date.now().toString(),
            query: historyPreviewText || queryCode,
            fullCode: queryCode,
            timestamp: new Date(),
            result: capturedQuery,
          };
          return [historyItem, ...prev].slice(0, 150);
        }
      });
    } catch (err) {
      const errorMessage =
        err instanceof Error ? err.message : 'Unknown error occurred';
      setError(errorMessage);

      // Check if the current query code is the same as the previous entry
      setHistory(prev => {
        if (prev.length > 0 && prev[0].fullCode === queryCode) {
          // Update the timestamp and error of the most recent entry
          const updatedHistory = [...prev];
          updatedHistory[0] = {
            ...updatedHistory[0],
            timestamp: new Date(),
            error: errorMessage,
            result: undefined, // Clear result since there's an error
          };
          return updatedHistory;
        } else {
          // Add new history entry
          const historyItem: QueryHistoryItem = {
            id: Date.now().toString(),
            query: historyPreviewText || queryCode,
            fullCode: queryCode,
            timestamp: new Date(),
            error: errorMessage,
          };
          return [historyItem, ...prev].slice(0, 150);
        }
      });
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

  const handleOpenCredentials = useCallback(() => {
    setIsCredentialsModalOpen(true);
  }, []);

  const handleSaveCredentials = useCallback(
    (username: string, password: string) => {
      setAuth({username, password});
    },
    [],
  );

  const handleCloseCredentials = useCallback(() => {
    setIsCredentialsModalOpen(false);
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
              onOpenCredentials={handleOpenCredentials}
              hasCredentials={!!auth}
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
      <CredentialsModal
        isOpen={isCredentialsModalOpen}
        onClose={handleCloseCredentials}
        onSave={handleSaveCredentials}
        initialUsername={auth?.username || ''}
        initialPassword={auth?.password || ''}
      />
    </div>
  );
}

export default App;
