import type {FC} from 'react';
import {X, Activity, Check, AlertCircle} from 'lucide-react';

interface ServerStatusModalProps {
  isOpen: boolean;
  onClose: () => void;
  hasCredentials: boolean;
  serverUrl?: string | undefined;
}

export const ServerStatusModal: FC<ServerStatusModalProps> = ({
  isOpen,
  onClose,
  hasCredentials,
  serverUrl = 'Not configured',
}) => {
  if (!isOpen) return null;

  return (
    <div className="modal-overlay">
      <div className="modal-content">
        <div className="modal-header">
          <h3>Server Status</h3>
          <button onClick={onClose} className="modal-close-button">
            <X size={16} />
          </button>
        </div>
        <div className="server-status-content">
          <div className="status-section">
            <div className="status-item">
              <Activity size={16} />
              <span className="status-label">Server URL:</span>
              <span className="status-value">{serverUrl}</span>
            </div>

            <div className="status-item">
              {hasCredentials ? (
                <>
                  <Check size={16} className="status-icon-success" />
                  <span className="status-label">Authentication:</span>
                  <span className="status-value success">Configured</span>
                </>
              ) : (
                <>
                  <AlertCircle size={16} className="status-icon-warning" />
                  <span className="status-label">Authentication:</span>
                  <span className="status-value warning">Not configured</span>
                </>
              )}
            </div>

            <div className="status-item">
              <Activity size={16} />
              <span className="status-label">Environment:</span>
              <span className="status-value">
                {import.meta.env.MODE || 'development'}
              </span>
            </div>
          </div>

          <div className="status-info">
            <p>
              {hasCredentials
                ? 'Server queries and analysis are enabled.'
                : 'Set credentials to enable server-side query execution and analysis.'}
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};
