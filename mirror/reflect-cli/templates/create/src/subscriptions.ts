// This file defines our "subscriptions". Subscriptions are how Reflect
// notifies you when data has changed. Subscriptions fire whenever the watched
// data changes, whether because the local client changed it, or because some
// other collaborating client changed it. The model is that you render your app
// reactively based on these subscriptions. This guarantees the UI always
// consistently shows the latest data.
//
// Subscriptions can be arbitrary computations of the data in Reflect. The
// subscription "query" is re-run whenever any of the data it depends on
// changes. The subscription "fires" when the result of the query changes.

import type {Reflect} from '@rocicorp/reflect/client';
import {useSubscribe} from '@rocicorp/reflect/react';
import {getClientState} from './client-state.js';
import type {M} from './mutators.js';

export function useCount(reflect: Reflect<M>, key: string) {
  return useSubscribe(reflect, tx => tx.get<number>(key), 0);
}

export function useClientState(r: Reflect<M>, id: string) {
  return useSubscribe(r, tx => getClientState(tx, id), null);
}
