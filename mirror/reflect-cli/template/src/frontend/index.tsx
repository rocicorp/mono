import React, {useRef, useEffect} from 'react';
import ReactDOM from 'react-dom/client';
import {mutators} from '../shared/mutators';
import {Reflect} from '@rocicorp/reflect/client';
import {nanoid} from 'nanoid';
import {ClientState, randUserInfo} from '../shared/client-state';
import {useClientStates} from '../shared/subscription';
import styles from './styles.module.css';
const userID = nanoid();

const socketOrigin: string | undefined = import.meta.env.VITE_WORKER_URL;
if (socketOrigin === undefined || socketOrigin === '') {
  throw new Error('VITE_WORKER_URL required');
}

type M = typeof mutators;

const r = new Reflect({
  socketOrigin,
  userID,
  roomID: 'current-room',
  auth: userID,
  mutators,
});

const App = ({reflect}: {reflect: Reflect<M>}) => {
  const ref = useRef<HTMLDivElement | null>(null);

  const onMouseMove = ({
    pageX,
    pageY,
  }: {
    pageX: number;
    pageY: number;
  }) => {
    if (ref && ref.current) {
      reflect.mutate.setCursor({
        x: pageX,
        y: pageY - ref.current.offsetTop,
      });
    }
  };

  const clientStates = useClientStates(r);

  useEffect(() => {
    void (async () => {
      const userInfo = randUserInfo();
      await reflect.mutate.initClientState(userInfo);
    })();
  }, []);

  // Render app.
  return (
    <div
      style={{
        position: 'absolute',
        display: 'flex',
        flexDirection: 'column',
        left: 0,
        top: 0,
        width: '100%',
        height: '100%',
        background: 'rgb(229,229,229)',
      }}
    >
      <div
        {...{
          ref,
          style: {
            position: 'relative',
            display: 'flex',
            flex: 1,
            overflow: 'hidden',
          },
          onMouseMove,
        }}
      >
        {Object.entries(clientStates).map(
          ([_id, {userInfo, cursor}]: [string, ClientState]) =>
            cursor && (
              <div className={styles.collaborator}>
                <div
                  className={styles.cursor}
                  style={{
                    left: cursor.x,
                    top: cursor.y,
                    overflow: 'auto',
                  }}
                >
                  <div
                    className={styles.pointer}
                    style={{color: userInfo.color}}
                  >
                    ➤
                  </div>
                  <div
                    className={styles.userinfo}
                    style={{
                      backgroundColor: userInfo.color,
                      color: 'white',
                    }}
                  >
                    {userInfo.avatar}&nbsp;{userInfo.name}
                  </div>
                </div>
              </div>
            ),
        )}
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
    <App reflect={r} />
  </React.StrictMode>,
);

if (import.meta.hot) {
  import.meta.hot.dispose(async () => {
    // this makes sure that there is only one instance of the reflect client during hmr reloads
    await r.close();
    root.unmount();
  });
}
