import React, {useEffect} from 'react';
import ReactDOM from 'react-dom/client';
import {mutators} from './reflect/mutators';
import {ExperimentalMemKVStore, Reflect} from '@rocicorp/reflect/client';
import {nanoid} from 'nanoid';
import {randUserInfo} from './reflect/client-state';
import styles from './index.module.css';
import CursorField from './cursor-field';
import {useCount} from './subscriptions';

const userID = nanoid();
const roomID = 'my-room';
const incrementKey = 'count';

const socketOrigin: string | undefined = import.meta.env.VITE_WORKER_URL;
if (socketOrigin === undefined || socketOrigin === '') {
  throw new Error('VITE_WORKER_URL required');
}

const r = new Reflect({
  socketOrigin,
  userID,
  roomID,
  auth: userID,
  mutators,

  // Turns off local persistence. This will go away soon.
  createKVStore: (name: string) => {
    return new ExperimentalMemKVStore(name);
  },
});

const App = () => {
  useEffect(() => {
    void (async () => {
      const userInfo = randUserInfo();
      await r.mutate.initClientState(userInfo);
    })();
  }, []);

  const handleButtonClick = () => {
    void r.mutate.increment({key: incrementKey, delta: 1});
  };

  const count = useCount(r, incrementKey);

  // Render app.
  return (
    <div className={styles.container}>
      <img className={styles.logo} src="/reflect.svg" />
      <div className={styles.content}>
        <div className={styles.count}>{count}</div>
        <button onClick={handleButtonClick}>Bonk</button>
        <CursorField r={r} />
      </div>
    </div>
  );
};

const rootElement = document.getElementById('root');
if (rootElement === null) {
  throw new Error('root element is null');
}
const root = ReactDOM.createRoot(rootElement);
root.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);

if (import.meta.hot) {
  import.meta.hot.dispose(async () => {
    // this makes sure that there is only one instance of the reflect client during hmr reloads
    await r.close();
    root.unmount();
  });
}
