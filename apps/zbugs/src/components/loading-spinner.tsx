import {useEffect, useState} from 'react';
import spinnerUrl from '../assets/images/spinner.webp';

// Track demo loading start time globally
let demoStartTime: number | null = null;

export function getDemoLoadTime(): number | null {
  if (demoStartTime === null) {
    return null;
  }
  return Date.now() - demoStartTime;
}

export function LoadingSpinner({forceShow}: {forceShow?: boolean | undefined}) {
  const [show, setShow] = useState(false);
  const qs = new URLSearchParams(window.location.search);
  const isDemoMode = qs.has('demo');
  const isDemoVideo = qs.has('demovideo');

  // Set the start time immediately when mounted in demo mode
  if ((isDemoMode || isDemoVideo) && demoStartTime === null) {
    demoStartTime = Date.now();
  }

  useEffect(() => {
    const timer = setTimeout(() => setShow(true), 500);
    return () => clearTimeout(timer);
  }, []);

  if (!show && !forceShow) {
    return null;
  }

  if (isDemoMode) {
    return <DemoLoadingSpinner />;
  }

  return (
    <div className="loading-spinner">
      <img src={spinnerUrl} alt="" width={20} height={20} />
      <span>Just a moment…</span>
    </div>
  );
}

function DemoLoadingSpinner() {
  const [dots, setDots] = useState('.');
  const [elapsed, setElapsed] = useState(0);

  useEffect(() => {
    const dotsInterval = setInterval(() => {
      setDots(d => (d.length >= 3 ? '.' : d + '.'));
    }, 400);

    const timerInterval = setInterval(() => {
      setElapsed(Date.now() - (demoStartTime ?? Date.now()));
    }, 100);

    return () => {
      clearInterval(dotsInterval);
      clearInterval(timerInterval);
    };
  }, []);

  const seconds = (elapsed / 1000).toFixed(1);

  return (
    <div className="loading-spinner loading-spinner-demo">
      <div className="loading-spinner-demo-container">
        <img src={spinnerUrl} alt="" width={96} height={96} />
        <span>Loading 1.2 million bugs</span>
        <span>
          in {seconds} seconds{dots}
        </span>
      </div>
    </div>
  );
}
