import {defineConfig, configDefaults} from 'vitest/config';

export default defineConfig({
  test: {
    fakeTimers: {
      toFake: [...(configDefaults.fakeTimers.toFake ?? []), 'performance'],
    },
  },
});
