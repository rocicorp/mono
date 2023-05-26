import {M, registerClientConsole} from '@/demo/shared/mutators';
import type {Latency} from '@/demo/shared/types';
import type {Reflect} from '@rocicorp/reflect';
import {event} from 'nextjs-google-analytics';
import {useCallback, useEffect, useState} from 'react';
import {ClientConsole} from './ClientConsole';
import demoButtonStyles from './DemoButton.module.css';
import styles from './How.module.css';
import {Slider} from './Slider';
import {useClientConsoleReducer, useCount} from './howtoUtils';

export function IncrementClient({
  title,
  reflect,
  latency,
  setLatency,
}: {
  title: string;
  reflect: Reflect<M> | undefined;
  latency: Latency | undefined;
  setLatency: (latency: Latency) => void;
}) {
  const [clientConsoleState, clientConsoleDispatch] = useClientConsoleReducer();

  const increment = useCallback(async (reflect: Reflect<M> | undefined) => {
    await reflect?.mutate.increment({key: 'count', delta: 1});
    event('demo_increment', {
      category: 'How it Works',
      action: 'Press increment button',
      label: 'Demo 2',
    });
  }, []);

  const [currentClientID, setCurrentClientID] = useState('');

  const count =
    useCount(reflect, 'count', (key: string, val: number) => {
      clientConsoleDispatch({
        type: 'APPEND',
        payload: `Key "${key}" changed to: ${val}`,
      });
    }) ?? 0;

  useEffect(() => {
    void reflect?.clientID.then(id => {
      registerClientConsole(id, (log: string) =>
        clientConsoleDispatch({type: 'APPEND', payload: log}),
      );
      setCurrentClientID(id);
    });
  }, [clientConsoleDispatch, reflect?.clientID]);

  return (
    <div className={styles.client}>
      <h4 className={styles.panelLabel}>{title}</h4>
      <Slider
        clientID={currentClientID}
        clientLatency={latency}
        setClientLatency={setLatency}
      />
      <div className={demoButtonStyles.demoContainer}>
        <span className={styles.incrementCounter}>{count}</span>
        <button
          onClick={() => increment(reflect)}
          className={demoButtonStyles.demoButton}
        >
          Increment
        </button>
      </div>
      <ClientConsole logs={clientConsoleState} />
    </div>
  );
}
