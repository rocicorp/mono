import type {M} from '@/demo/shared/mutators';
import type {Latency} from '@/demo/shared/types';
import type {Reflect} from '@rocicorp/reflect/client';
import {useState} from 'react';
import {Demo1a} from './Demos/Demo1a';
import {Demo1b} from './Demos/Demo1b';
import styles from './How.module.css';
import {IncrementClient} from './IncrementClient';
import {Reset} from './Reset';
import {ServerConsole} from './ServerConsole';

export function Demo1({
  reflect1,
  reflect2,
  reflectServer,
  reset,
  latency1,
  latency2,
  setLatency1,
  setLatency2,
}: {
  reflect1: Reflect<M> | undefined;
  reflect2: Reflect<M> | undefined;
  reflectServer: Reflect<M> | undefined;
  reset: () => void;
  key?: string | undefined;
  latency1?: Latency | undefined;
  latency2?: Latency | undefined;
  setLatency1: (latency: Latency) => void;
  setLatency2: (latency: Latency) => void;
}) {
  const [toggleDemo, setToggleDemo] = useState(true);

  const toggleSwitchDemo = () => setToggleDemo(!toggleDemo);

  return (
    <div className={styles.howStep}>
      <h3 className={styles.howHeader}>
        <strong>Step 2:</strong> Define Mutators
      </h3>
      <p className={styles.howDescription}>
        Mutators are how you make changes to rooms. They are JavaScript
        functions you define, that run on{' '}
        <strong>both the client and the server</strong>.
      </p>
      <p className={styles.howDescription}>
        By replaying mutators on the server, Reflect naturally resolves many
        types of conflicts, while allowing for custom, authoritative server
        logic.
      </p>
      <p className={styles.howTryIt}>
        <strong>Try it:</strong> Use the demo below to increment a multiplayer
        counter. Increase the latency and quickly increment on both clients.
        Notice how normal arithmetic logic naturally sums concurrent operations,
        without the need for CRDTs.
      </p>
      <div className={styles.howGridLayout2}>
        <div className={styles.codeBlock}>
          {toggleDemo ? (
            <>
              <div className={styles.codeBlockToggle}>
                <button onClick={toggleSwitchDemo}>client.tsx</button>
                <button className={styles.codeToggleActive}>mutators.ts</button>
              </div>
              <Demo1a />
            </>
          ) : (
            <>
              <div className={styles.codeBlockToggle}>
                <button className={styles.codeToggleActive}>client.tsx</button>
                <button onClick={toggleSwitchDemo}>mutators.ts</button>
              </div>
              <Demo1b />
            </>
          )}
        </div>
        <IncrementClient
          title="Client 1"
          reflect={reflect1}
          latency={latency1}
          setLatency={setLatency1}
        />
        <ServerConsole reflect={reflectServer} />
        <IncrementClient
          title="Client 2"
          reflect={reflect2}
          latency={latency2}
          setLatency={setLatency2}
        />
      </div>
      <Reset reset={reset} />
    </div>
  );
}
