import {readFile} from 'node:fs/promises';
import type {PluginOption} from 'vite';
import {defineConfig} from 'vitest/config';

/**
 * This plugin creates a default export for `.wasm` files that exports a
 * `WebAssembly.Module`. This matches the Cloudflare Workers environment.
 * However, this cannot be used in workers because `WebAssembly.instantiate` is
 * not allowed to take an ArrayBuffer in workers.
 */
function inlineWASM(): PluginOption {
  return {
    name: 'inline-wasm',
    async load(id) {
      if (id.endsWith('.wasm')) {
        return `export default new WebAssembly.Module(new Uint8Array(${JSON.stringify(
          Array.from(await readFile(id)),
        )}));`;
      }
    },
  };
}

export default defineConfig({
  test: {
    name: 'pg',
    include: ['src/**/*.pg-test.?(c|m)[jt]s?(x)'],
    retry: 2,
    globalSetup: ['./test/pg-container-setup.ts'],
    onConsoleLog(log: string) {
      if (
        log.includes(
          'insert or update on table "fk_ref" violates foreign key constraint "fk_ref_ref_fkey"',
        )
      ) {
        return false;
      }
      return undefined;
    },
  },
  plugins: [inlineWASM()],
});
