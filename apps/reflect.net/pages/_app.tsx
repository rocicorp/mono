import '@/styles/globals.css';
import '@/styles/demo.css';
import type {AppProps} from 'next/app';
import { GoogleAnalytics, event } from "nextjs-google-analytics";
import type { NextWebVitalsMetric } from "next/app";

export function reportWebVitals({ id, name, label, value }: NextWebVitalsMetric) {
  event(name, {
    category: label === "web-vital" ? "Web Vitals" : "Next.js custom metric",
    value: Math.round(name === "CLS" ? value * 1000 : value), // values must be integers
    label: id, // id unique to current page load
    nonInteraction: true, // avoids affecting bounce rate.
  });
}


export default function App({Component, pageProps}: AppProps) {
  return (
    <>
      <GoogleAnalytics trackPageViews />
      <Component {...pageProps} />
    </>
  );
}
