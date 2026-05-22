import {wrapZeroReact} from '@rocicorp/zero/react';
import {zero} from './zero.ts';

export const {
  ZeroProvider,
  useZero,
  useQuery,
  useSuspenseQuery,
  useConnectionState,
} = wrapZeroReact(zero);
