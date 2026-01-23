import {ZeroProvider} from '@rocicorp/zero/solid';
import type {JSX} from 'solid-js';
import {mutators, schema} from '../shared/schema.ts';

export function ZeroInit(props: {children: JSX.Element}) {
  const cacheURL = import.meta.env.VITE_PUBLIC_SERVER;
  console.log('[ZeroInit] cacheURL:', cacheURL);
  console.log('[ZeroInit] schema tables:', Object.keys(schema.tables));

  return (
    <ZeroProvider
      schema={schema}
      mutators={mutators}
      cacheURL={cacheURL}
      userID="anon"
      logLevel="debug"
      mutateURL="http://localhost:5173/api/mutate"
      queryURL="http://localhost:5173/api/query"
    >
      {props.children}
    </ZeroProvider>
  );
}
