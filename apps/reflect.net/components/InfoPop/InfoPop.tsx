import infoIcon from '@/public/pricing/info.svg';
import Image from 'next/image';
import styles from './InfoPop.module.css';

export function InfoPop(props: any) {
  return (
    <div className={styles.infoPopContainer}>
      <Image src={infoIcon} className={styles.infoIcon} alt="More info" />
      <div className={styles.messageContainer}>{props.message}</div>
    </div>
  );
}
