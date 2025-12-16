import data from '../../../tsconfig.json' with {type: 'json'};

export function getESLibVersion(): number {
  const libs = data.compilerOptions.lib as string[];
  const esVersion = libs.find(lib => lib.toLowerCase().startsWith('es'));
  if (!esVersion) {
    throw new Error('Could not find ES lib version');
  }
  return parseInt(esVersion.slice(2), 10);
}
