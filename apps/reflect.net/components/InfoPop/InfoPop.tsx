import infoIcon from '@/public/pricing/info.svg';
import Image from 'next/image';
import type {ReactNode} from 'react';
import styles from './InfoPop.module.css';

export function InfoPop(props: {message: ReactNode}) {
  return (
    <div className={styles.infoPopContainer}>
      <Image src={infoIcon} className={styles.infoIcon} alt="More info" />
      <div className={styles.messageContainer}>{props.message}</div>
    </div>
  );
}
