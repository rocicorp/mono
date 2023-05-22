import styles from './Reset.module.css';
import Image from 'next/image';
import resetIcon from '@/public/how-it-works/reset.svg';

export default function Reset({reset}: {reset: () => void}) {
  return (
    <button className={styles.resetButton} onClick={reset}>
      <Image src={resetIcon} className={styles.resetIcon} alt="Reset" />
      <span className={styles.resetLabel}>Reset demo</span>
    </button>
  );
}
