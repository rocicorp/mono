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
    const timer = setTimeout(setShow, 500, true);
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
  const [elapsed, setElapsed] = useState(0);

  useEffect(() => {
    const timerInterval = setInterval(() => {
      setElapsed(Date.now() - (demoStartTime ?? Date.now()));
    }, 100);

    return () => {
      clearInterval(timerInterval);
    };
  }, []);

  const seconds = (elapsed / 1000).toFixed(1);

  return (
    <div className="loading-spinner loading-spinner-demo">
      <div className="loading-spinner-demo-container">
        <div className="loading-spinner-demo-icon">
          <img src={spinnerUrl} alt="" width={96} height={96} />
        </div>
        <span className="loading-spinner-demo-title">
          Loading 1.2 million bugs
        </span>
        <span className="loading-spinner-demo-time">
          in <span className="loading-spinner-demo-seconds">{seconds}</span>{' '}
          seconds...
        </span>
      </div>
    </div>
  );
}
