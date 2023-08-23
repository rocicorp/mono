import {Reflect} from '@rocicorp/reflect/client';
import {M} from '../shared/mutators';
import {useClientStates} from '../shared/subscription';
import {ClientState} from '../shared/client-state';
import styles from './cursor-field.module.css';
import {useEffect} from 'react';

export default function CursorField({r}: {r: Reflect<M>}) {
  useEffect(() => {
    const handler = ({pageX, pageY}: {pageX: number; pageY: number}) => {
      void r.mutate.setCursor({
        x: pageX,
        y: pageY,
      });
    };
    window.addEventListener('mousemove', handler);
    return () => window.removeEventListener('mousemove', handler);
  }, []);

  const clientStates = useClientStates(r);

  return clientStates.map(
    ([id, {userInfo, cursor}]: [string, ClientState]) =>
      cursor && (
        <div key={id} className={styles.collaborator}>
          <div
            className={styles.cursor}
            style={{
              left: cursor.x,
              top: cursor.y,
              overflow: 'auto',
            }}
          >
            <div className={styles.pointer} style={{color: userInfo.color}}>
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
  );
}
