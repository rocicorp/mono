import type {AppProps} from 'next/app';

export default function App({
  // eslint-disable-next-line @typescript-eslint/naming-convention
  Component,
  pageProps,
}: AppProps) {
  return <Component {...pageProps} />;
}
