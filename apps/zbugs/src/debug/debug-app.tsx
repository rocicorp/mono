import {useState} from 'react';
import {QueryUsers} from './query-users.tsx';

export function DebugApp() {
  const [queryUserComponents, setQueryUserComponents] = useState<number[]>([]);
  const [nextId, setNextId] = useState(0);

  const addQueryUserComponent = () => {
    setQueryUserComponents(prev => [...prev, nextId]);
    setNextId(prev => prev + 1);
  };

  const removeQueryUserComponent = (id: number) => {
    setQueryUserComponents(prev => prev.filter(compId => compId !== id));
  };

  return (
    <div style={{padding: '20px'}}>
      <h1>Debug Mode</h1>
      <p>This is the debug entrypoint for exploring and testing.</p>

      <div style={{marginTop: '20px'}}>
        <button
          onClick={addQueryUserComponent}
          style={{
            padding: '10px 20px',
            fontSize: '16px',
            cursor: 'pointer',
            marginBottom: '20px',
          }}
        >
          Add Query Users Component
        </button>

        <div style={{display: 'flex', flexDirection: 'column', gap: '15px'}}>
          {queryUserComponents.map(id => (
            <div
              key={id}
              style={{
                border: '1px solid #ccc',
                padding: '15px',
                borderRadius: '5px',
                position: 'relative',
              }}
            >
              <button
                onClick={() => removeQueryUserComponent(id)}
                style={{
                  position: 'absolute',
                  top: '10px',
                  right: '10px',
                  padding: '5px 10px',
                  cursor: 'pointer',
                  backgroundColor: '#ff4444',
                  color: 'white',
                  border: 'none',
                  borderRadius: '3px',
                }}
              >
                Remove
              </button>
              <QueryUsers />
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
