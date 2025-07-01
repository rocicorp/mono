import solid from 'vite-plugin-solid';
import {defineConfig, mergeConfig} from 'vitest/config';
import config from '../shared/src/tool/vitest-config.ts';

export default mergeConfig(config, defineConfig({plugins: [solid()]}));
