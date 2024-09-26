/**
 * @param {boolean=} minify
 * @param {boolean=} metafile
 */
export function sharedOptions(
  minify?: boolean | undefined,
  metafile?: boolean | undefined,
): {
  readonly bundle: true;
  readonly target: 'es2022';
  readonly format: 'esm';
  readonly external: string[];
  readonly minify: boolean;
  readonly sourcemap: true;
  readonly metafile: boolean;
};
/**
 * @param {'debug'|'release'|'unknown'} mode
 * @return {Record<string, string>}
 */
export function makeDefine(
  mode?: 'debug' | 'release' | 'unknown',
): Record<string, string>;
