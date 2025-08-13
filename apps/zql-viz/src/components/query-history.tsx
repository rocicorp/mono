import type {FC} from 'react';
import {Clock, Play, Trash2} from 'lucide-react';
import {type QueryHistoryItem} from '../types.ts';

interface QueryHistoryProps {
  history: QueryHistoryItem[];
  onSelectQuery: (historyItem: QueryHistoryItem) => void;
  onClearHistory: () => void;
}

export const QueryHistory: FC<QueryHistoryProps> = ({
  history,
  onSelectQuery,
  onClearHistory,
}) => {
  const formatTime = (date: Date) => {
    return new Date(date).toLocaleTimeString('en-US', {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
  };

  return (
    <div className="query-history">
      <div className="editor-header">
        <h3>
          <Clock size={16} />
          Query History
        </h3>
        {history.length > 0 && (
          <button
            onClick={onClearHistory}
            className="clear-button"
            title="Clear History"
          >
            <Trash2 size={14} />
          </button>
        )}
      </div>
      <div className="history-content">
        {history.length === 0 ? (
          <div className="empty">
            <p>No queries executed yet</p>
          </div>
        ) : (
          <div className="history-list">
            {history.map(item => (
              <div
                key={item.id}
                className={`history-item ${item.error ? 'error' : 'success'}`}
                onClick={() => onSelectQuery(item)}
              >
                <div className="history-item-header">
                  <span className="time">{formatTime(item.timestamp)}</span>
                  <Play size={12} />
                </div>
                <div className="history-item-query">
                  <pre>
                    {item.query.length > 253
                      ? item.query.substring(0, 250) + '...'
                      : item.query}
                  </pre>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};
