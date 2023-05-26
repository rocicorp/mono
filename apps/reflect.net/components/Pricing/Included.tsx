import included from '@/public/pricing/pricing-check.svg';
import Image from 'next/image';
import styles from './Included.module.css';

export function Included() {
  return (
    <Image
      src={included}
      className={styles.pricingGridCheck}
      alt="Included"
      title="Included"
    />
  );
}
