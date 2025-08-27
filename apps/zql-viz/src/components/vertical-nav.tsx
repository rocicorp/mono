import type {FC} from 'react';
import {History, Key, Activity} from 'lucide-react';

interface VerticalNavProps {
  onShowHistory: () => void;
  onOpenCredentials: () => void;
  onShowServerStatus: () => void;
  hasCredentials: boolean;
  isHistoryVisible: boolean;
}

export const VerticalNav: FC<VerticalNavProps> = ({
  onShowHistory,
  onOpenCredentials,
  onShowServerStatus,
  hasCredentials,
  isHistoryVisible,
}) => {
  return (
    <div className="vertical-nav">
      <div className="nav-section">
        <button
          className={`nav-button ${isHistoryVisible ? 'active' : ''}`}
          onClick={onShowHistory}
          title="Toggle Query History"
        >
          <History size={20} />
        </button>
        
        <button
          className={`nav-button ${hasCredentials ? 'has-credentials' : ''}`}
          onClick={onOpenCredentials}
          title="Set Server Credentials"
        >
          <Key size={20} />
        </button>
        
        <button
          className="nav-button"
          onClick={onShowServerStatus}
          title="View Server Status"
        >
          <Activity size={20} />
        </button>
      </div>
    </div>
  );
};