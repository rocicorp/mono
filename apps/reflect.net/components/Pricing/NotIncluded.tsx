import notincluded from '@/public/pricing/pricing-x.svg';
import Image from 'next/image';
import styles from './Included.module.css';

export function NotIncluded() {
  return (
    <Image
      src={notincluded}
      className={styles.pricingGridX}
      alt="Not included"
      title="Not included"
    />
  );
}
