// This file is a place-holder for the actual app module provided by the
// developer, referenced by the various *-script.ts templates.
import type {
  BuildableOptionsEnv,
  ReflectServerBaseEnv,
  ReflectServerOptions,
} from '../mod.js';

function makeOptions(
  _: BuildableOptionsEnv & ReflectServerBaseEnv,
): ReflectServerOptions<{}> {
  throw new Error('This module should never be referenced');
}

export {makeOptions as default};
