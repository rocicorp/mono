import {createRequire} from 'node:module';
import {compile} from './compile.js';

export async function buildReflectServerContent(): Promise<string> {
  const require = createRequire(import.meta.url);
  const serverPath = require.resolve('@rocicorp/reflect/server');
  const {code} = await compile(serverPath, false);
  return code.text;
}
