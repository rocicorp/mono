import styles from './Included.module.css';
import Image from 'next/image';
import included from '@/public/pricing/pricing-check.svg';

const Included = () => (
  <Image
    src={included}
    className={styles.pricingGridCheck}
    alt="Included"
    title="Included"
  />
);

export default Included;
