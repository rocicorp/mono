import {builtinModules} from 'node:module';
import {type UserConfig} from 'vite';
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

const external = await getExternal();

const define = {
  ...makeDefine('unknown'),
  'process.env.DISABLE_MUTATION_RECOVERY': 'true',
};

export const baseConfig: UserConfig = {
  logLevel: 'warn',
  build: {
    outDir: 'out',
    emptyOutDir: false,
    sourcemap: true,
    target: 'es2022',
    ssr: true,
    reportCompressedSize: false,
    rollupOptions: {
      external,
      output: {
        format: 'es',
        entryFileNames: '[name].js',
      },
    },
  },
  define,
  resolve: {
    conditions: ['import', 'module', 'default'],
  },
};
