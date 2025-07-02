import {defineConfig, mergeConfig} from 'vitest/config';
import {
  configForCustomPg,
  configForNoPg,
  configForVersion,
} from '../zero-cache/vitest.config.ts';

const {url} = import.meta;

export const projects = [
  configForNoPg(url),
  configForVersion(15, url),
  configForVersion(16, url),
  configForVersion(17, url),
  ...configForCustomPg(url),
].map(c =>
  mergeConfig(c, {
    test: {
      testTimeout: 20_000,
    },
  }),
);

export default defineConfig({test: {projects, testTimeout: 20_000}});
