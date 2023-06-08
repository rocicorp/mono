import {M, registerClientConsole} from '@/demo/shared/mutators';
import type {Latency} from '@/demo/shared/types';
import type {Reflect} from '@rocicorp/reflect/client';
import {useCallback, useEffect, useState} from 'react';
import {ClientConsole} from './ClientConsole';
import styles from './How.module.css';
import {Roci} from './Roci';
import buttonStyles from './RotateButton.module.css';
import {RotateSlider} from './RotateSlider';
import {Slider} from './Slider';
import {useClientConsoleReducer, useCount} from './howtoUtils';

export function RotateClient({
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

  const incrementCount = useCallback(
    (deg: number) => {
      void reflect?.mutate.degree({key: 'degree', deg});
    },
    [reflect],
  );

  const [currentClientID, setCurrentClientID] = useState('');

  const count = useCount(reflect, 'degree', (key: string, val: number) => {
    clientConsoleDispatch({
      type: 'APPEND',
      payload: `Key  "${key}" changed to: ${val}`,
    });
  });

  useEffect(() => {
    void reflect?.clientID.then(id => {
      setCurrentClientID(id);
      registerClientConsole(id, (log: string) =>
        clientConsoleDispatch({type: 'APPEND', payload: log}),
      );
    });
  }, [reflect, clientConsoleDispatch]);

  return (
    <div className={`${styles.client} ${styles.clientExpanded}`}>
      <h4 className={styles.panelLabel}>{title}</h4>
      <Slider
        clientID={currentClientID}
        clientLatency={latency}
        setClientLatency={setLatency}
      />
      <div className={styles.demo2layout}>
        <div className={buttonStyles.rotateButtonContainer}>
          <RotateSlider increment={incrementCount} degree={count} />
        </div>
        <Roci rotation={count === undefined ? 0 : count} />
      </div>
      <ClientConsole logs={clientConsoleState} />
    </div>
  );
}
