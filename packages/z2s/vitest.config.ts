import {defineConfig} from 'vitest/config';
import {
  configForCustomPg,
  configForNoPg,
  configForVersion,
} from '../zero-cache/vitest.config.ts';

const {url} = import.meta;

export const workspace = [
  configForNoPg(url),
  configForVersion(15, url),
  configForVersion(16, url),
  configForVersion(17, url),
  ...configForCustomPg(url),
];

export default defineConfig({test: {workspace}});
