import type {M} from '@/demo/shared/mutators';
import type {Reflect} from '@rocicorp/reflect/client';
import classNames from 'classnames';
import {useContext, useEffect, useState} from 'react';
import {ClientIDContext} from './ClientIDContext';
import styles from './ServerConsole.module.css';
import {useServerLogs} from './howtoUtils';

export function ServerConsole({reflect}: {reflect: Reflect<M> | undefined}) {
  const logs = useServerLogs(reflect);
  const {client1ID, client2ID} = useContext(ClientIDContext);
  const [bright, setBright] = useState(false);

  useEffect(() => {
    if (logs?.length === 0) return;
    setBright(true);
    const timer = setTimeout(() => {
      setBright(false);
    }, 300);

    return () => clearTimeout(timer);
  }, [logs]);

  return (
    <div
      className={classNames(styles.serverConsole, {[styles.bright]: bright})}
    >
      <h4 className={styles.panelLabel}>Server Console</h4>
      <div className={styles.consoleOutput}>
        {logs &&
          logs.slice(-10).map((log, i) => (
            <p className={styles.consoleItem} key={i}>
              {log.replace(client1ID, 'client1').replace(client2ID, 'client2')}
            </p>
          ))}
      </div>
    </div>
  );
}
