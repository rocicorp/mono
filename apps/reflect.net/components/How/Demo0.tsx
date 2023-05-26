import {Demo0a} from './Demos/Demo0a';
import styles from './How.module.css';

export function Demo0() {
  return (
    <div className={styles.howStep}>
      <h3 className={styles.howHeader}>
        <strong>Step 1:</strong> Create Room
      </h3>
      <p className={styles.howDescription}>
        Users connected to the same room see each others&apos; changes in
        realtime.
      </p>
      <div className={styles.howGridLayout2}>
        <div className={styles.codeBlock}>
          <>
            <div className={styles.codeBlockToggle}>
              <button className={styles.codeToggleActive}>client.tsx</button>
            </div>
            <Demo0a />
          </>
        </div>
      </div>
    </div>
  );
}
