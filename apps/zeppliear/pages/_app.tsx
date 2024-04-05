import React from 'react';
import type {AppProps} from 'next/app';

import Head from 'next/head';

import '../styles/index.css';

function MyApp({
  // eslint-disable-next-line @typescript-eslint/naming-convention
  Component,
  pageProps,
}: AppProps) {
  return (
    <>
      <Head>
        <title>Repliear</title>
        <meta name="viewport" content="width=device-width,initial-scale=1" />
        <link
          rel="icon"
          type="image/png"
          href="/static/replicache-logo-96.png"
        />
      </Head>
      <Component {...pageProps} />
    </>
  );
}

export default MyApp;
