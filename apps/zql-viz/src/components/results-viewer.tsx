import type {FC} from 'react';
import {useState} from 'react';
import {
  AlertCircle,
  BarChart3,
  GitBranch,
  List,
  Code,
  TreePine,
  Database,
} from 'lucide-react';
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
  | 'rootqueries'
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
              {Object.entries(result.remoteRunResult.syncedRows).map(([tableName, rows]) => (
                <div key={tableName} className="table-section">
                  <h3 className="table-title">{tableName}</h3>
                  <div className="table-info">
                    <span className="row-count">{rows.length} rows</span>
                  </div>
                  {rows.length > 0 ? (
                    <div className="table-wrapper">
                      <table className="data-table">
                        <thead>
                          <tr>
                            {Object.keys(rows[0]).map((column) => (
                              <th key={column}>{column}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {rows.map((row, index) => (
                            <tr key={index}>
                              {Object.values(row).map((value, colIndex) => {
                                const displayValue = value === null || value === undefined 
                                  ? 'null'
                                  : typeof value === 'object' 
                                  ? JSON.stringify(value)
                                  : String(value);
                                return (
                                  <td key={colIndex} title={displayValue}>
                                    {value === null || value === undefined 
                                      ? <span className="null-value">null</span>
                                      : displayValue
                                    }
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
              ))}
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
        return (
          <div className="tab-content">
            <div className="placeholder-content">
              <List size={48} />
              <h4>Query Plan</h4>
              <p>Execution plan and optimization details for your query.</p>
            </div>
          </div>
        );

      case 'rootqueries':
        return (
          <div className="tab-content">
            <div className="placeholder-content">
              <TreePine size={48} />
              <h4>Root Queries</h4>
              <p>Base queries that your current query depends on.</p>
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
            className={`tab ${activeTab === 'rootqueries' ? 'active' : ''}`}
            onClick={() => setActiveTab('rootqueries')}
          >
            <TreePine size={16} />
            Root Queries
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
