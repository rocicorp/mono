import React, {useEffect, useState, useCallback} from 'react';
import Slider from './Slider';
import ClientConsole from './ClientConsole';
import styles from './How.module.css';
import buttonStyles from './RotateButton.module.css';
import type {Reflect} from '@rocicorp/reflect';
import {M, registerClientConsole} from '@/demo/shared/mutators';
import Roci from './Roci';
import {useClientConsoleReducer, useCount} from './howtoUtils';
import RotateSlider from './RotateSlider';
import type {Latency} from '@/demo/shared/types';

function RotateClient({
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
      reflect?.mutate.degree({key: 'degree', deg});
    },
    [reflect],
  );

  const [currentClientID, setCurrentClientID] = useState('');

  const count = useCount(reflect, 'degree', (key: string, val: number) => {
    clientConsoleDispatch({
      type: 'APPEND',
      payload: `Key  \"${key}\" changed to: ${val}`,
    });
  });

  useEffect(() => {
    reflect?.clientID.then(id => {
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

export default RotateClient;
