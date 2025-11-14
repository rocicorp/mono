import {builtinModules} from 'node:module';
import {makeDefine} from '../../shared/src/build.ts';
import {getExternalFromPackageJSON} from '../../shared/src/tool/get-external-from-package-json.ts';

async function getExternal(): Promise<string[]> {
  return [
    ...(await getExternalFromPackageJSON(import.meta.url, true)),
    'node:*',
    'expo*',
    '@op-engineering/*',
    ...builtinModules,
  ].sort();
}

export const external = await getExternal();

export const define = {
  ...makeDefine('unknown'),
  'process.env.DISABLE_MUTATION_RECOVERY': 'true',
};
