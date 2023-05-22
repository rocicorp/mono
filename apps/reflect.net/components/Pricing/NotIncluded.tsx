import styles from './Included.module.css';
import Image from 'next/image';
import notincluded from '@/public/pricing/pricing-x.svg';

const NotIncluded = () => (
  <Image
    src={notincluded}
    className={styles.pricingGridX}
    alt="Not included"
    title="Not included"
  />
);

export default NotIncluded;
