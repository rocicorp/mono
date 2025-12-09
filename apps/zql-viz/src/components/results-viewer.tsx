import {
  AlertCircle,
  BarChart3,
  Code,
  Database,
  GitBranch,
  List,
  TreePine,
} from 'lucide-react';
import type {FC} from 'react';
import {useState} from 'react';
import type {Result} from '../types.ts';
import {DataFlowGraph} from './data-flow-graph.tsx';

interface ResultsViewerProps {
  result: Result | undefined;
  error: string | undefined;
  isLoading: boolean;
}

type TabType =
  | 'results'
  | 'ast'
  | 'dataflow'
  | 'queryplan'
  | 'querystats'
  | 'indices';

export const ResultsViewer: FC<ResultsViewerProps> = ({
  result,
  error,
  isLoading,
}) => {
  const [activeTab, setActiveTab] = useState<TabType>('dataflow');

  const renderTabContent = () => {
    if (isLoading) {
      return (
        <div className="loading">
          <div className="spinner"></div>
          <span>Executing query...</span>
        </div>
      );
    }

    if (error) {
      return (
        <div className="error">
          <AlertCircle size={20} />
          <pre>{error}</pre>
        </div>
      );
    }

    switch (activeTab) {
      case 'results':
        return result?.remoteRunResult?.syncedRows ? (
          <div className="results-content">
            <div className="tables-container">
              {Object.entries(result.remoteRunResult.syncedRows).map(
                ([tableName, rows]) => (
                  <div key={tableName} className="table-section">
                    <h3 className="table-title">{tableName}</h3>
                    <div className="table-info">
                      <span className="row-count">{rows.length} rows</span>
                    </div>
                    {rows.length > 0 ? (
                      <div className="table-wrapper full-scroll">
                        <table className="data-table">
                          <thead>
                            <tr>
                              {Object.keys(rows[0]).map(column => (
                                <th key={column}>{column}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {rows.map((row, index) => (
                              <tr key={index}>
                                {Object.values(row).map((value, colIndex) => {
                                  const displayValue =
                                    value === null || value === undefined
                                      ? 'null'
                                      : typeof value === 'object'
                                        ? JSON.stringify(value)
                                        : String(value);
                                  return (
                                    <td
                                      key={colIndex}
                                      className="truncate"
                                      title={displayValue}
                                    >
                                      {value === null || value === undefined ? (
                                        <span className="null-value">null</span>
                                      ) : (
                                        displayValue
                                      )}
                                    </td>
                                  );
                                })}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    ) : (
                      <div className="empty-table">No rows found</div>
                    )}
                  </div>
                ),
              )}
            </div>
          </div>
        ) : (
          <div className="tab-content">
            <div className="placeholder-content">
              <BarChart3 size={48} />
              <h4>Query Rows</h4>
              <p>Rows returned by your query.</p>
            </div>
          </div>
        );

      case 'ast':
        return result?.ast ? (
          <div className="success">
            <pre>{JSON.stringify(result?.ast, null, 2)}</pre>
          </div>
        ) : (
          <div className="tab-content">
            <div className="placeholder-content">
              <Code size={48} />
              <h4>AST</h4>
              <p>Abstract syntax tree of your query.</p>
            </div>
          </div>
        );

      case 'dataflow':
        return result?.graph ? (
          <div style={{height: '100%', width: '100%'}}>
            <DataFlowGraph graph={result.graph} />
          </div>
        ) : (
          <div className="tab-content">
            <div className="placeholder-content">
              <GitBranch size={48} />
              <h4>Data Flow Graph</h4>
              <p>Visual representation of how data flows through your query.</p>
            </div>
          </div>
        );

      case 'queryplan':
        return result?.remoteRunResult?.sqlitePlans ? (
          <div className="results-content">
            <div className="tables-container">
              {Object.entries(result.remoteRunResult.sqlitePlans).map(
                ([queryName, planSteps]) => (
                  <div key={queryName} className="table-section">
                    <h3 className="table-title">Query Plan</h3>
                    <div className="table-info">
                      <span className="row-count scrollable" title={queryName}>
                        {queryName}
                      </span>
                    </div>
                    <div className="query-plan-content">
                      {planSteps.map((step, index) => {
                        const renderStep = (text: string) =>
                          // Split by words and apply color coding
                          text
                            .split(/(\b(?:SCAN|SEARCH)\b)/g)
                            .map((part, partIndex) => {
                              if (part === 'SCAN') {
                                return (
                                  <span key={partIndex} className="plan-scan">
                                    {part}
                                  </span>
                                );
                              } else if (part === 'SEARCH') {
                                return (
                                  <span key={partIndex} className="plan-search">
                                    {part}
                                  </span>
                                );
                              }
                              return part;
                            });
                        return (
                          <div key={index} className="plan-step">
                            <div className="plan-step-number">{index + 1}</div>
                            <div className="plan-step-text">
                              {renderStep(step)}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                ),
              )}
            </div>
          </div>
        ) : (
          <div className="tab-content">
            <div className="placeholder-content">
              <List size={48} />
              <h4>Query Plan</h4>
              <p>Execution plan and optimization details for your query.</p>
            </div>
          </div>
        );

      case 'querystats':
        return result?.remoteRunResult?.vendedRowCounts ? (
          <div className="results-content">
            <div className="tables-container">
              {Object.entries(result.remoteRunResult.vendedRowCounts).map(
                ([tableName, queries]) => (
                  <div key={tableName} className="table-section">
                    <h3 className="table-title">Table: {tableName}</h3>
                    <div className="table-info">
                      <span className="row-count">
                        {Object.keys(queries).length} queries executed
                      </span>
                    </div>
                    <div className="table-wrapper">
                      <table className="data-table">
                        <thead>
                          <tr>
                            <th>SQL</th>
                            <th>Rows Vended</th>
                          </tr>
                        </thead>
                        <tbody>
                          {Object.entries(queries).map(
                            ([queryName, rowCount]) => (
                              <tr key={queryName}>
                                <td className="scrollable" title={queryName}>
                                  {queryName}
                                </td>
                                <td>{rowCount.toLocaleString()}</td>
                              </tr>
                            ),
                          )}
                        </tbody>
                      </table>
                    </div>
                  </div>
                ),
              )}
              {result.remoteRunResult?.syncedRowCount !== undefined && (
                <div className="stats-summary">
                  <h3>Summary</h3>
                  <div className="stats-grid">
                    <div className="stat-item">
                      <span className="stat-label">Total Synced Rows:</span>
                      <span className="stat-value">
                        {result.remoteRunResult.syncedRowCount.toLocaleString()}
                      </span>
                    </div>
                    {result.remoteRunResult?.start !== undefined &&
                      result.remoteRunResult?.end !== undefined && (
                        <div className="stat-item">
                          <span className="stat-label">Query Time:</span>
                          <span className="stat-value">
                            {result.remoteRunResult.end -
                              result.remoteRunResult.start}
                            ms
                          </span>
                        </div>
                      )}
                  </div>
                </div>
              )}
            </div>
          </div>
        ) : (
          <div className="tab-content">
            <div className="placeholder-content">
              <TreePine size={48} />
              <h4>Query Stats</h4>
              <p>Stats about underlying SQLite queries run.</p>
            </div>
          </div>
        );

      case 'indices':
        return (
          <div className="tab-content">
            <div className="placeholder-content">
              <Database size={48} />
              <h4>Suggested Indices</h4>
              <p>
                Recommended database indices to optimize your query performance.
              </p>
            </div>
          </div>
        );

      default:
        return null;
    }
  };

  return (
    <div className="results-viewer">
      <div className="tabs-container">
        <div className="tabs-header">
          <button
            className={`tab ${activeTab === 'dataflow' ? 'active' : ''}`}
            onClick={() => setActiveTab('dataflow')}
          >
            <GitBranch size={16} />
            Data Flow
          </button>
          <button
            className={`tab ${activeTab === 'ast' ? 'active' : ''}`}
            onClick={() => setActiveTab('ast')}
          >
            <Code size={16} />
            AST
          </button>
          <button
            className={`tab ${activeTab === 'results' ? 'active' : ''}`}
            onClick={() => setActiveTab('results')}
          >
            <BarChart3 size={16} />
            Results
          </button>
          <button
            className={`tab ${activeTab === 'queryplan' ? 'active' : ''}`}
            onClick={() => setActiveTab('queryplan')}
          >
            <List size={16} />
            Query Plan
          </button>
          <button
            className={`tab ${activeTab === 'querystats' ? 'active' : ''}`}
            onClick={() => setActiveTab('querystats')}
          >
            <TreePine size={16} />
            Query Stats
          </button>
          <button
            className={`tab ${activeTab === 'indices' ? 'active' : ''}`}
            onClick={() => setActiveTab('indices')}
          >
            <Database size={16} />
            Suggested Indices
          </button>
        </div>

        <div className="tab-content-wrapper">{renderTabContent()}</div>
      </div>
    </div>
  );
};
