import type {FC} from 'react';
import {useState} from 'react';
import {X, User, Lock, Globe} from 'lucide-react';

interface CredentialsModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (serverUrl: string, username: string, password: string) => void;
  initialServerUrl?: string;
  initialUsername?: string;
  initialPassword?: string;
}

export const CredentialsModal: FC<CredentialsModalProps> = ({
  isOpen,
  onClose,
  onSave,
  initialServerUrl = '',
  initialUsername = '',
  initialPassword = '',
}) => {
  const [serverUrl, setServerUrl] = useState(initialServerUrl);
  const [username, setUsername] = useState(initialUsername);
  const [password, setPassword] = useState(initialPassword);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSave(serverUrl, username, password);
    onClose();
  };

  const handleClose = () => {
    setServerUrl(initialServerUrl);
    setUsername(initialUsername);
    setPassword(initialPassword);
    onClose();
  };

  if (!isOpen) return null;

  return (
    <div className="modal-overlay">
      <div className="modal-content">
        <div className="modal-header">
          <h3>Server Configuration</h3>
          <button onClick={handleClose} className="modal-close-button">
            <X size={16} />
          </button>
        </div>
        <form onSubmit={handleSubmit} className="credentials-form">
          <div className="form-group">
            <label htmlFor="serverUrl">
              <Globe size={16} />
              Server URL
            </label>
            <input
              id="serverUrl"
              type="url"
              value={serverUrl}
              onChange={e => setServerUrl(e.target.value)}
              placeholder="https://server.example.com"
              required
              autoFocus
            />
          </div>
          <div className="form-group">
            <label htmlFor="username">
              <User size={16} />
              Username
            </label>
            <input
              id="username"
              type="text"
              value={username}
              onChange={e => setUsername(e.target.value)}
              placeholder="Enter username"
              required
            />
          </div>
          <div className="form-group">
            <label htmlFor="password">
              <Lock size={16} />
              Password
            </label>
            <input
              id="password"
              type="password"
              value={password}
              onChange={e => setPassword(e.target.value)}
              placeholder="Enter password"
              required
            />
          </div>
          <div className="modal-actions">
            <button
              type="button"
              onClick={handleClose}
              className="button-secondary"
            >
              Cancel
            </button>
            <button type="submit" className="button-primary">
              Save
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};
