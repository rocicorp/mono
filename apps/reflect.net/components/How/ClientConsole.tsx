import classNames from 'classnames';
import {useContext, useEffect, useState} from 'react';
import styles from './ClientConsole.module.css';
import {ClientIDContext} from './ClientIDContext';

export default function ClientConsole({logs}: {logs: string[] | undefined}) {
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
      className={classNames(styles.clientConsole, {[styles.bright]: bright})}
    >
      <h4 className={styles.panelLabel}>Console</h4>
      <div className={styles.consoleOutput}>
        {logs &&
          logs.map((log, i) => (
            <p className={styles.consoleItem} key={i}>
              {log.replace(client1ID, 'client1').replace(client2ID, 'client2')}
            </p>
          ))}
      </div>
    </div>
  );
}
