import type {ReflectServerOptions} from '@rocicorp/reflect/server';
import {M, mutators} from './mutators.js';

function makeOptions(): ReflectServerOptions<M> {
  return {
    mutators,
  };
}

export {makeOptions as default};
