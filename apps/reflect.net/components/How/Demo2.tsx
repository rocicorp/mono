import type {M} from '@/demo/shared/mutators';
import type {Latency} from '@/demo/shared/types';
import type {Reflect} from '@rocicorp/reflect';
import {useState} from 'react';
import {Demo2a} from './Demos/Demo2a';
import {Demo2b} from './Demos/Demo2b';
import styles from './How.module.css';
import {Reset} from './Reset';
import {RotateClient} from './RotateClient';
import {ServerConsole} from './ServerConsole';

export function Demo2({
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
    <>
      {/* Step 2: Render Reactively */}
      <div className={styles.howStep}>
        <h3 className={styles.howHeader}>
          <strong>Step 3:</strong> Render Reactively
        </h3>
        <p className={styles.howDescription}>
          Subscribe to changes in Reflect and render your UI reactively.
          There&apos;s no need to interpolate. You receive updates at up to 120
          fps, just as if the collaborator was local.
        </p>
        <p className={styles.howTryIt}>
          <strong>Try it:</strong> Notice how even when the latency is high,
          changes playback on the receiver exactly as they happened on the
          source.
        </p>
        <div className={styles.howGridLayout2}>
          <div className={styles.codeBlock}>
            {!toggleDemo ? (
              <>
                <div className={styles.codeBlockToggle}>
                  <button onClick={toggleSwitchDemo}>client.tsx</button>
                  <button className={styles.codeToggleActive}>
                    mutators.ts
                  </button>
                </div>
                <Demo2a />
              </>
            ) : (
              <>
                <div className={styles.codeBlockToggle}>
                  <button className={styles.codeToggleActive}>
                    client.tsx
                  </button>
                  <button onClick={toggleSwitchDemo}>mutators.ts</button>
                </div>
                <Demo2b />
              </>
            )}
          </div>
          <RotateClient
            reflect={reflect1}
            title="Client 1"
            latency={latency1}
            setLatency={setLatency1}
          />
          <ServerConsole reflect={reflectServer} />
          <RotateClient
            title="Client 2"
            reflect={reflect2}
            latency={latency2}
            setLatency={setLatency2}
          />
        </div>
        <Reset reset={reset} />
      </div>
    </>
  );
}
