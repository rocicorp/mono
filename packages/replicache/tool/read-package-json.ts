import {readFile} from 'node:fs/promises';

type PackageJSON = {
  [key: string]: any;
  name: string;
  version: string;
};

export async function readPackageJSON(): Promise<PackageJSON> {
  const url = new URL('../package.json', import.meta.url);
  const s = await readFile(url, 'utf-8');
  return JSON.parse(s);
}
