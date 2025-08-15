import type {FC} from 'react';
import {useState} from 'react';
import {X, User, Lock} from 'lucide-react';

interface CredentialsModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (username: string, password: string) => void;
  initialUsername?: string;
  initialPassword?: string;
}

export const CredentialsModal: FC<CredentialsModalProps> = ({
  isOpen,
  onClose,
  onSave,
  initialUsername = '',
  initialPassword = '',
}) => {
  const [username, setUsername] = useState(initialUsername);
  const [password, setPassword] = useState(initialPassword);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSave(username, password);
    onClose();
  };

  const handleClose = () => {
    setUsername(initialUsername);
    setPassword(initialPassword);
    onClose();
  };

  if (!isOpen) return null;

  return (
    <div className="modal-overlay">
      <div className="modal-content">
        <div className="modal-header">
          <h3>Enter Credentials</h3>
          <button onClick={handleClose} className="modal-close-button">
            <X size={16} />
          </button>
        </div>
        <form onSubmit={handleSubmit} className="credentials-form">
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
              autoFocus
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
