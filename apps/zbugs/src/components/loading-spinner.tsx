import {useEffect, useState} from 'react';
import spinnerUrl from '../assets/images/spinner.webp';

export function LoadingSpinner() {
  const [show, setShow] = useState(false);

  useEffect(() => {
    const timer = setTimeout(() => setShow(true), 500);
    return () => clearTimeout(timer);
  }, []);

  if (!show) {
    return null;
  }

  return (
    <div className="loading-spinner">
      <img src={spinnerUrl} alt="" width={20} height={20} />
      <span>Just a moment…</span>
    </div>
  );
}
