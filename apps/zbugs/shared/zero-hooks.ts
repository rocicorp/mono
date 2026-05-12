import {initZeroReact} from '@rocicorp/zero/react';
import type {AuthData} from './auth.ts';
import type {schema} from './schema.ts';

export const {
  ZeroProvider,
  useZero,
  useQuery,
  useSuspenseQuery,
  useConnectionState,
} = initZeroReact<typeof schema, AuthData | undefined>();
