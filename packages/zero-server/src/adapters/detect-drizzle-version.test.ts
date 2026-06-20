import {describe, expect, test} from 'vitest';

import {detectUsesModeArg} from './detect-drizzle-version.ts';

describe('detectUsesModeArg', () => {
  test('detects the new (>= rc.1) signature by its `mode` parameter', () => {
    // Mirrors `drizzle-orm@>=1.0.0-rc.1` `PgSession.prepareQuery`. Parameter
    // names must match drizzle's exactly — detection reads them off the source.
    function prepareQuery(
      query: unknown,
      mode: 'arrays' | 'objects' | 'raw',
      name: unknown,
      mapper?: unknown,
      queryMetadata?: unknown,
      cacheConfig?: unknown,
    ) {
      void [query, mode, name, mapper, queryMetadata, cacheConfig];
    }
    expect(detectUsesModeArg(prepareQuery)).toBe(true);
  });

  test('detects the old (<= beta / 0.45) signature by its `fields` parameter', () => {
    // Mirrors `drizzle-orm@^0.45` `PgSession.prepareQuery`. Parameter names
    // must match drizzle's exactly — detection reads them off the source.
    function prepareQuery(
      query: unknown,
      fields: unknown,
      name: unknown,
      isResponseInArrayMode: boolean,
      customResultMapper?: unknown,
      queryMetadata?: unknown,
      cacheConfig?: unknown,
    ) {
      void [
        query,
        fields,
        name,
        isResponseInArrayMode,
        customResultMapper,
        queryMetadata,
        cacheConfig,
      ];
    }
    expect(detectUsesModeArg(prepareQuery)).toBe(false);
  });

  test('falls back to arity when parameter names are unavailable (minified new)', () => {
    // 4 leading params, no recognizable `mode`/`fields` name → new signature.
    const prepareQuery = (a: unknown, b: unknown, c: unknown, d?: unknown) => {
      void [a, b, c, d];
    };
    expect(detectUsesModeArg(prepareQuery)).toBe(true);
  });

  test('falls back to arity when parameter names are unavailable (minified old)', () => {
    // 7 leading params → old signature.
    const prepareQuery = (
      a: unknown,
      b: unknown,
      c: unknown,
      d: unknown,
      e: unknown,
      f: unknown,
      g: unknown,
    ) => {
      void [a, b, c, d, e, f, g];
    };
    expect(detectUsesModeArg(prepareQuery)).toBe(false);
  });
});
