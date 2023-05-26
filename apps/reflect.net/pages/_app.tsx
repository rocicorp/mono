import '@/styles/demo.css';
import '@/styles/globals.css';
import type {AppProps, NextWebVitalsMetric} from 'next/app';
import {GoogleAnalytics, event} from 'nextjs-google-analytics';

export function reportWebVitals({id, name, label, value}: NextWebVitalsMetric) {
  event(name, {
    category: label === 'web-vital' ? 'Web Vitals' : 'Next.js custom metric',
    value: Math.round(name === 'CLS' ? value * 1000 : value), // values must be integers
    label: id, // id unique to current page load
    nonInteraction: true, // avoids affecting bounce rate.
  });
}

export default function App({
  // eslint-disable-next-line @typescript-eslint/naming-convention
  Component,
  pageProps,
}: AppProps) {
  return (
    <>
      <GoogleAnalytics trackPageViews />
      <Component {...pageProps} />
    </>
  );
}
