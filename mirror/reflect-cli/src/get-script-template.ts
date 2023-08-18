import {createRequire} from 'node:module';
import {dirname, resolve} from 'node:path';
import {readFile} from 'node:fs/promises';

export async function getScriptTemplate(
  scriptType: 'dev' | 'prod',
  appModuleName?: string,
  serverModuleName?: string,
  appScriptName?: string,
): Promise<string> {
  const require = createRequire(import.meta.url);
  const reflectPath = require.resolve('@rocicorp/reflect');
  const scriptPath = resolve(
    dirname(reflectPath),
    'script-templates/',
    `${scriptType}-script.js`,
  );
  let template = await readFile(scriptPath, 'utf-8');
  if (appModuleName) {
    template = template.replaceAll('app-module-name.js', appModuleName);
  }
  if (serverModuleName) {
    template = template.replaceAll('server-module-name.js', serverModuleName);
  }
  if (appScriptName) {
    template = template.replaceAll('app-script-name', appScriptName);
  }
  return template;
}
