import styles from './InfoPop.module.css';
import Image from 'next/image';
import infoIcon from '@/public/pricing/info.svg';

const InfoPop = (props: any) => (
  <div className={styles.infoPopContainer}>
    <Image src={infoIcon} className={styles.infoIcon} alt="More info" />
    <div className={styles.messageContainer}>{props.message}</div>
  </div>
);

export default InfoPop;
