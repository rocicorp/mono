import type {FC} from 'react';
import {useState} from 'react';
import {
  AlertCircle,
  CheckCircle,
  BarChart3,
  GitBranch,
  List,
  Code,
} from 'lucide-react';

interface ResultsViewerProps {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  result: any;
  error: string | null;
  isLoading: boolean;
}

type TabType = 'results' | 'ast' | 'dataflow' | 'queryplan';

export const ResultsViewer: FC<ResultsViewerProps> = ({
  result,
  error,
  isLoading,
}) => {
  const [activeTab, setActiveTab] = useState<TabType>('results');

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

    if (!result) {
      return (
        <div className="empty">
          <p>Execute a query to see results here</p>
        </div>
      );
    }

    switch (activeTab) {
      case 'results':
        return (
          <div className="success">
            <div className="success-header">
              <CheckCircle size={20} />
              <span>Query executed successfully</span>
            </div>
            <pre>{JSON.stringify(result, null, 2)}</pre>
          </div>
        );

      case 'ast':
        return (
          <div className="success">
            <div className="success-header">
              <Code size={20} />
              <span>Abstract Syntax Tree</span>
            </div>
            <pre>{JSON.stringify(result, null, 2)}</pre>
          </div>
        );

      case 'dataflow':
        return (
          <div className="tab-content">
            <div className="placeholder-content">
              <GitBranch size={48} />
              <h4>Data Flow Graph</h4>
              <p>Visual representation of how data flows through your query.</p>
              <div className="placeholder-box">
                <p>Data flow visualization would be rendered here</p>
                <small>
                  This would show the relationships between tables and
                  operations
                </small>
              </div>
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
              <div className="placeholder-box">
                <p>Query execution plan would be displayed here</p>
                <small>
                  This would show the steps taken to execute the query
                </small>
              </div>
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
            className={`tab ${activeTab === 'results' ? 'active' : ''}`}
            onClick={() => setActiveTab('results')}
          >
            <BarChart3 size={16} />
            Results
          </button>
          <button
            className={`tab ${activeTab === 'ast' ? 'active' : ''}`}
            onClick={() => setActiveTab('ast')}
          >
            <Code size={16} />
            AST
          </button>
          <button
            className={`tab ${activeTab === 'dataflow' ? 'active' : ''}`}
            onClick={() => setActiveTab('dataflow')}
          >
            <GitBranch size={16} />
            Data Flow
          </button>
          <button
            className={`tab ${activeTab === 'queryplan' ? 'active' : ''}`}
            onClick={() => setActiveTab('queryplan')}
          >
            <List size={16} />
            Query Plan
          </button>
        </div>

        <div className="tab-content-wrapper">{renderTabContent()}</div>
      </div>
    </div>
  );
};
