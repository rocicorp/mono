import resetIcon from '@/public/how-it-works/reset.svg';
import Image from 'next/image';
import styles from './Reset.module.css';

export function Reset({reset}: {reset: () => void}) {
  return (
    <button className={styles.resetButton} onClick={reset}>
      <Image src={resetIcon} className={styles.resetIcon} alt="Reset" />
      <span className={styles.resetLabel}>Reset demo</span>
    </button>
  );
}
