import type {Zero} from '@rocicorp/zero';
import {initZero} from '@rocicorp/zero/react';
import type {AuthData} from './auth.ts';
import type {schema} from './schema.ts';

export type ZbugsZero = Zero<typeof schema, undefined, AuthData | undefined>;

export const {
  ZeroProvider,
  useZero,
  useQuery,
  useSuspenseQuery,
  useConnectionState,
} = initZero<typeof schema, AuthData | undefined>();
