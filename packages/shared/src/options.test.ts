import {SilentLogger} from '@rocicorp/logger';
import {stripVTControlCharacters as stripAnsi} from 'node:util';
import type {PartialDeep} from 'type-fest';
import {expect, test, vi} from 'vitest';
import {
  envSchema,
  parseOptions,
  parseOptionsAdvanced,
  type Config,
  type Options,
} from './options.ts';
import * as v from './valita.ts';

const options = {
  port: {
    type: v.number().default(4848),
    desc: ['blah blah blah'],
    alias: 'p',
  },
  replicaDBFile: v.string(),
  litestream: v.boolean().optional(),
  log: {
    level: v.string(), // required grouped option tests deepPartial
    format: v.union(v.literal('text'), v.literal('json')).default('text'),
  },
  shard: {
    id: {
      type: v.string().default('0'),
      desc: ['blah blah blah'],
    },
    publications: {type: v.array(v.string()).optional(() => [])},
  },
  tuple: v
    .tuple([
      v.union(
        v.literal('a'),
        v.literal('c'),
        v.literal('e'),
        v.literal('g'),
        v.literal('i'),
        v.literal('k'),
      ),
      v.union(
        v.literal('b'),
        v.literal('d'),
        v.literal('f'),
        v.literal('h'),
        v.literal('j'),
        v.literal('l'),
      ),
    ])
    .optional(() => ['a', 'b']),
  deprecatedFlag: {
    type: v.string().optional(),
    deprecated: [`don't set me anymore`],
    hidden: true,
  },
  hideMe: {
    type: v.string().optional(),
    hidden: true,
  },
};

type TestConfig = Config<typeof options>;

test.each([
  [
    'defaults',
    ['--replica-db-file', '/tmp/replica.db', '--log-level', 'info'],
    {},
    false,
    {
      port: 4848,
      replicaDBFile: '/tmp/replica.db',
      log: {level: 'info', format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
    {
      Z_LOG_LEVEL: 'info',
      Z_LOG_FORMAT: 'text',
      Z_PORT: '4848',
      Z_REPLICA_DB_FILE: '/tmp/replica.db',
      Z_SHARD_ID: '0',
      Z_SHARD_PUBLICATIONS: '',
      Z_TUPLE: 'a,b',
    },
    undefined,
  ],
  [
    'env values',
    [],
    {
      ['Z_PORT']: '6000',
      ['Z_REPLICA_DB_FILE']: '/tmp/env-replica.db',
      ['Z_LITESTREAM']: 'true',
      ['Z_LOG_LEVEL']: 'info',
      ['Z_LOG_FORMAT']: 'json',
      ['Z_SHARD_ID']: 'xyz',
      ['Z_SHARD_PUBLICATIONS']: 'zero_foo',
      ['Z_TUPLE']: 'c,d',
      ['Z_HIDE_ME']: 'hello!',
    },
    false,
    {
      port: 6000,
      replicaDBFile: '/tmp/env-replica.db',
      litestream: true,
      log: {level: 'info', format: 'json'},
      shard: {id: 'xyz', publications: ['zero_foo']},
      tuple: ['c', 'd'],
      hideMe: 'hello!',
    },
    {
      Z_HIDE_ME: 'hello!',
      Z_LITESTREAM: 'true',
      Z_LOG_LEVEL: 'info',
      Z_LOG_FORMAT: 'json',
      Z_PORT: '6000',
      Z_REPLICA_DB_FILE: '/tmp/env-replica.db',
      Z_SHARD_ID: 'xyz',
      Z_SHARD_PUBLICATIONS: 'zero_foo',
      Z_TUPLE: 'c,d',
    },
    undefined,
  ],
  [
    'env value for array flag separated by commas',
    [],
    {
      ['Z_PORT']: '6000',
      ['Z_REPLICA_DB_FILE']: '/tmp/env-replica.db',
      ['Z_LITESTREAM']: 'true',
      ['Z_LOG_LEVEL']: 'info',
      ['Z_LOG_FORMAT']: 'json',
      ['Z_SHARD_ID']: 'xyz',
      ['Z_SHARD_PUBLICATIONS']: 'zero_foo,zero_bar',
      ['Z_TUPLE']: 'e,f',
    },
    false,
    {
      port: 6000,
      replicaDBFile: '/tmp/env-replica.db',
      litestream: true,
      log: {level: 'info', format: 'json'},
      shard: {id: 'xyz', publications: ['zero_foo', 'zero_bar']},
      tuple: ['e', 'f'],
    },
    {
      Z_LITESTREAM: 'true',
      Z_LOG_LEVEL: 'info',
      Z_LOG_FORMAT: 'json',
      Z_PORT: '6000',
      Z_REPLICA_DB_FILE: '/tmp/env-replica.db',
      Z_SHARD_ID: 'xyz',
      Z_SHARD_PUBLICATIONS: 'zero_foo,zero_bar',
      Z_TUPLE: 'e,f',
    },
    undefined,
  ],
  [
    'argv values, short alias',
    ['-p', '6000', '--replica-db-file=/tmp/replica.db', '--log-level=info'],
    {},
    false,
    {
      port: 6000,
      replicaDBFile: '/tmp/replica.db',
      log: {level: 'info', format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
    {
      Z_LOG_LEVEL: 'info',
      Z_LOG_FORMAT: 'text',
      Z_PORT: '6000',
      Z_REPLICA_DB_FILE: '/tmp/replica.db',
      Z_SHARD_ID: '0',
      Z_SHARD_PUBLICATIONS: '',
      Z_TUPLE: 'a,b',
    },
    undefined,
  ],
  [
    'argv values, hex numbers',
    ['-p', '0x1234', '--replica-db-file=/tmp/replica.db', '--log-level=info'],
    {},
    false,
    {
      port: 4660,
      replicaDBFile: '/tmp/replica.db',
      log: {level: 'info', format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
    {
      Z_LOG_LEVEL: 'info',
      Z_LOG_FORMAT: 'text',
      Z_PORT: '4660',
      Z_REPLICA_DB_FILE: '/tmp/replica.db',
      Z_SHARD_ID: '0',
      Z_SHARD_PUBLICATIONS: '',
      Z_TUPLE: 'a,b',
    },
    undefined,
  ],
  [
    'argv values, scientific notation',
    ['-p', '1.234E3', '--replica-db-file=/tmp/replica.db', '--log-level=info'],
    {},
    false,
    {
      port: 1234,
      replicaDBFile: '/tmp/replica.db',
      log: {level: 'info', format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
    {
      Z_LOG_LEVEL: 'info',
      Z_LOG_FORMAT: 'text',
      Z_PORT: '1234',
      Z_REPLICA_DB_FILE: '/tmp/replica.db',
      Z_SHARD_ID: '0',
      Z_SHARD_PUBLICATIONS: '',
      Z_TUPLE: 'a,b',
    },
    undefined,
  ],
  [
    'argv values, eager multiples',
    [
      '--port',
      '6000',
      '--replica-db-file=/tmp/replica.db',
      '--log-level=info',
      '--litestream',
      'true',
      '--log-format=json',
      '--shard-id',
      'abc',
      '--shard-publications',
      'zero_foo',
      'zero_bar',
      '--tuple',
      'g',
      'h',
    ],
    {},
    false,
    {
      port: 6000,
      replicaDBFile: '/tmp/replica.db',
      litestream: true,
      log: {level: 'info', format: 'json'},
      shard: {id: 'abc', publications: ['zero_foo', 'zero_bar']},
      tuple: ['g', 'h'],
    },
    {
      Z_LITESTREAM: 'true',
      Z_LOG_LEVEL: 'info',
      Z_LOG_FORMAT: 'json',
      Z_PORT: '6000',
      Z_REPLICA_DB_FILE: '/tmp/replica.db',
      Z_SHARD_ID: 'abc',
      Z_SHARD_PUBLICATIONS: 'zero_foo,zero_bar',
      Z_TUPLE: 'g,h',
    },
    undefined,
  ],
  [
    'argv values, separate multiples',
    [
      '--port',
      '6000',
      '--replica-db-file',
      '/tmp/replica.db',
      '--log-level',
      'info',
      '--litestream',
      'true',
      '--log-format=json',
      '--shard-id',
      'abc',
      '--shard-publications',
      'zero_foo',
      '--shard-publications',
      'zero_bar',
      '--tuple',
      'i',
      '--tuple',
      'j',
    ],
    {},
    false,
    {
      port: 6000,
      replicaDBFile: '/tmp/replica.db',
      litestream: true,
      log: {level: 'info', format: 'json'},
      shard: {id: 'abc', publications: ['zero_foo', 'zero_bar']},
      tuple: ['i', 'j'],
    },
    {
      Z_LITESTREAM: 'true',
      Z_LOG_LEVEL: 'info',
      Z_LOG_FORMAT: 'json',
      Z_PORT: '6000',
      Z_REPLICA_DB_FILE: '/tmp/replica.db',
      Z_SHARD_ID: 'abc',
      Z_SHARD_PUBLICATIONS: 'zero_foo,zero_bar',
      Z_TUPLE: 'i,j',
    },
    undefined,
  ],
  [
    'argv value override env values',
    [
      '--port',
      '8888',
      '--log-level=info',
      '--log-format=json',
      '--shard-id',
      'abc',
      '--shard-publications',
      'zero_foo',
      'zero_bar',
      '--tuple',
      'k',
      'l',
      '--hide-me=foo',
    ],
    {
      ['Z_PORT']: '6000',
      ['Z_REPLICA_DB_FILE']: '/tmp/env-replica.db',
      ['Z_LITESTREAM']: 'true',
      ['Z_LOG_FORMAT']: 'text',
      ['Z_SHARD_ID']: 'xyz',
      ['Z_SHARD_PUBLICATIONS']: 'zero_blue',
      ['Z_TUPLE']: 'e,f',
      ['Z_HIDE_ME']: 'bar',
    },
    false,
    {
      port: 8888,
      replicaDBFile: '/tmp/env-replica.db',
      litestream: true,
      log: {level: 'info', format: 'json'},
      shard: {id: 'abc', publications: ['zero_foo', 'zero_bar']},
      tuple: ['k', 'l'],
      hideMe: 'foo',
    },
    {
      Z_HIDE_ME: 'foo',
      Z_LITESTREAM: 'true',
      Z_LOG_LEVEL: 'info',
      Z_LOG_FORMAT: 'json',
      Z_PORT: '8888',
      Z_REPLICA_DB_FILE: '/tmp/env-replica.db',
      Z_SHARD_ID: 'abc',
      Z_SHARD_PUBLICATIONS: 'zero_foo,zero_bar',
      Z_TUPLE: 'k,l',
    },
    undefined,
  ],
  [
    '--bool flag',
    ['--litestream', '--replica-db-file=/tmp/replica.db', '--log-level=info'],
    {},
    false,
    {
      port: 4848,
      replicaDBFile: '/tmp/replica.db',
      litestream: true,
      log: {level: 'info', format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
    {
      Z_LITESTREAM: 'true',
      Z_LOG_LEVEL: 'info',
      Z_LOG_FORMAT: 'text',
      Z_PORT: '4848',
      Z_REPLICA_DB_FILE: '/tmp/replica.db',
      Z_SHARD_ID: '0',
      Z_SHARD_PUBLICATIONS: '',
      Z_TUPLE: 'a,b',
    },
    undefined,
  ],
  [
    '--bool=true flag',
    [
      '--litestream=true',
      '--replica-db-file=/tmp/replica.db',
      '--log-level=info',
    ],
    {},
    false,
    {
      port: 4848,
      replicaDBFile: '/tmp/replica.db',
      litestream: true,
      log: {level: 'info', format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
    {
      Z_LITESTREAM: 'true',
      Z_LOG_LEVEL: 'info',
      Z_LOG_FORMAT: 'text',
      Z_PORT: '4848',
      Z_REPLICA_DB_FILE: '/tmp/replica.db',
      Z_SHARD_ID: '0',
      Z_SHARD_PUBLICATIONS: '',
      Z_TUPLE: 'a,b',
    },
    undefined,
  ],
  [
    '--bool 1 flag',
    [
      '--litestream',
      '1',
      '--replica-db-file=/tmp/replica.db',
      '--log-level=info',
    ],
    {},
    false,
    {
      port: 4848,
      replicaDBFile: '/tmp/replica.db',
      litestream: true,
      log: {level: 'info', format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
    {
      Z_LITESTREAM: 'true',
      Z_LOG_LEVEL: 'info',
      Z_LOG_FORMAT: 'text',
      Z_PORT: '4848',
      Z_REPLICA_DB_FILE: '/tmp/replica.db',
      Z_SHARD_ID: '0',
      Z_SHARD_PUBLICATIONS: '',
      Z_TUPLE: 'a,b',
    },
    undefined,
  ],
  [
    '--bool=0 flag',
    ['--litestream=0', '--replica-db-file=/tmp/replica.db', '--log-level=info'],
    {},
    false,
    {
      port: 4848,
      replicaDBFile: '/tmp/replica.db',
      litestream: false,
      log: {level: 'info', format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
    {
      Z_LITESTREAM: 'false',
      Z_LOG_LEVEL: 'info',
      Z_LOG_FORMAT: 'text',
      Z_PORT: '4848',
      Z_REPLICA_DB_FILE: '/tmp/replica.db',
      Z_SHARD_ID: '0',
      Z_SHARD_PUBLICATIONS: '',
      Z_TUPLE: 'a,b',
    },
    undefined,
  ],
  [
    '--bool False flag',
    [
      '--litestream',
      'False',
      '--replica-db-file=/tmp/replica.db',
      '--log-level=info',
    ],
    {},
    false,
    {
      port: 4848,
      replicaDBFile: '/tmp/replica.db',
      litestream: false,
      log: {level: 'info', format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
    {
      Z_LITESTREAM: 'false',
      Z_LOG_LEVEL: 'info',
      Z_LOG_FORMAT: 'text',
      Z_PORT: '4848',
      Z_REPLICA_DB_FILE: '/tmp/replica.db',
      Z_SHARD_ID: '0',
      Z_SHARD_PUBLICATIONS: '',
      Z_TUPLE: 'a,b',
    },
    undefined,
  ],
  [
    'unknown flags',
    [
      '--foo',
      '--replica-db-file',
      '/tmp/replica.db',
      'bar',
      '--log-level=info',
      '--baz=3',
    ],
    {},
    false,
    {
      port: 4848,
      replicaDBFile: '/tmp/replica.db',
      log: {level: 'info', format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
    {
      Z_LOG_LEVEL: 'info',
      Z_LOG_FORMAT: 'text',
      Z_PORT: '4848',
      Z_REPLICA_DB_FILE: '/tmp/replica.db',
      Z_SHARD_ID: '0',
      Z_SHARD_PUBLICATIONS: '',
      Z_TUPLE: 'a,b',
    },
    ['--foo', 'bar', '--baz=3'],
  ],
  [
    'partial',
    ['--port', '4888'],
    {},
    true,
    {
      port: 4888,
      log: {format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
    {
      Z_LOG_FORMAT: 'text',
      Z_PORT: '4888',
      Z_SHARD_ID: '0',
      Z_SHARD_PUBLICATIONS: '',
      Z_TUPLE: 'a,b',
    },
    undefined,
  ],
] satisfies [
  string,
  string[],
  Record<string, string>,
  boolean,
  PartialDeep<TestConfig>,
  Record<string, string>,
  string[] | undefined,
][])('%s', (_name, argv, env, allowPartial, result, envObj, unknown) => {
  const parsed = parseOptionsAdvanced(options, {
    argv,
    envNamePrefix: 'Z_',
    allowUnknown: true,
    allowPartial,
    env,
  });
  expect(parsed.config).toEqual(result);
  expect(parsed.env).toEqual(envObj);
  expect(parsed.unknown).toEqual(unknown);

  // Sanity check: Ensure that parsing the parsed.env computes the same result.
  const reparsed = parseOptionsAdvanced(options, {
    argv: [],
    envNamePrefix: 'Z_',
    allowUnknown: true,
    allowPartial,
    env: parsed.env,
  });
  expect(reparsed.config).toEqual(result);
  expect(reparsed.env).toEqual(envObj);
});

test('envSchema', () => {
  const schema = envSchema(options, 'ZERO_');
  expect(JSON.stringify(schema, null, 2)).toMatchInlineSnapshot(`
    "{
      "shape": {
        "ZERO_PORT": {
          "type": {
            "name": "string",
            "issue": {
              "ok": false,
              "code": "invalid_type",
              "expected": [
                "string"
              ]
            }
          },
          "name": "optional"
        },
        "ZERO_REPLICA_DB_FILE": {
          "name": "string",
          "issue": {
            "ok": false,
            "code": "invalid_type",
            "expected": [
              "string"
            ]
          }
        },
        "ZERO_LITESTREAM": {
          "type": {
            "name": "string",
            "issue": {
              "ok": false,
              "code": "invalid_type",
              "expected": [
                "string"
              ]
            }
          },
          "name": "optional"
        },
        "ZERO_LOG_LEVEL": {
          "name": "string",
          "issue": {
            "ok": false,
            "code": "invalid_type",
            "expected": [
              "string"
            ]
          }
        },
        "ZERO_LOG_FORMAT": {
          "type": {
            "name": "string",
            "issue": {
              "ok": false,
              "code": "invalid_type",
              "expected": [
                "string"
              ]
            }
          },
          "name": "optional"
        },
        "ZERO_SHARD_ID": {
          "type": {
            "name": "string",
            "issue": {
              "ok": false,
              "code": "invalid_type",
              "expected": [
                "string"
              ]
            }
          },
          "name": "optional"
        },
        "ZERO_SHARD_PUBLICATIONS": {
          "type": {
            "name": "string",
            "issue": {
              "ok": false,
              "code": "invalid_type",
              "expected": [
                "string"
              ]
            }
          },
          "name": "optional"
        },
        "ZERO_TUPLE": {
          "type": {
            "name": "string",
            "issue": {
              "ok": false,
              "code": "invalid_type",
              "expected": [
                "string"
              ]
            }
          },
          "name": "optional"
        },
        "ZERO_DEPRECATED_FLAG": {
          "type": {
            "name": "string",
            "issue": {
              "ok": false,
              "code": "invalid_type",
              "expected": [
                "string"
              ]
            }
          },
          "name": "optional"
        },
        "ZERO_HIDE_ME": {
          "type": {
            "name": "string",
            "issue": {
              "ok": false,
              "code": "invalid_type",
              "expected": [
                "string"
              ]
            }
          },
          "name": "optional"
        }
      },
      "name": "object",
      "_invalidType": {
        "ok": false,
        "code": "invalid_type",
        "expected": [
          "object"
        ]
      }
    }"
  `);

  expect(
    v.test(
      {
        ['ZERO_PORT']: '1234',
        ['ZERO_REPLICA_DB_FILE']: '/foo/bar.db',
        ['ZERO_LOG_LEVEL']: 'info',
      },
      schema,
    ).ok,
  ).toBe(true);

  expect(v.test({['ZERO_PORT']: '/foo/bar.db'}, schema)).toMatchInlineSnapshot(`
    {
      "error": "Missing property ZERO_REPLICA_DB_FILE",
      "ok": false,
    }
  `);
});

test('duplicate flag detection', () => {
  expect(() =>
    parseOptions(
      {
        fooBar: v.string().optional(),
        foo: {bar: v.number().optional()},
      },
      {argv: []},
    ),
  ).toThrowError('Two or more option definitions have the same name');
});

test('duplicate short flag', () => {
  expect(() =>
    parseOptions(
      {
        foo: {
          type: v.string().optional(),
          alias: 'b',
        },
        bar: {
          type: v.number().optional(),
          alias: 'b',
        },
      },
      {argv: []},
    ),
  ).toThrowError('Two or more option definitions have the same alias');
});

test.each([
  [
    'missing required flag',
    {requiredFlag: v.string()},
    [],
    'Missing required option ZORRO_REQUIRED_FLAG at requiredFlag. Got undefined',
  ],
  [
    'missing required multiple flag',
    {requiredFlag: v.array(v.string())},
    [],
    'Missing required option ZORRO_REQUIRED_FLAG at requiredFlag. Got undefined',
  ],
  [
    'missing grouped flag',
    {
      group: {
        requiredFlag: v.string(),
      },
    },
    [],
    'Missing required option ZORRO_GROUP_REQUIRED_FLAG at group.requiredFlag. Got undefined',
  ],
  [
    'mixed type union',
    // Options type forbids this, but cast to verify runtime check.
    {bad: v.union(v.literal('123'), v.literal(456))} as Options,
    [],
    'ZORRO_BAD has mixed types string,number',
  ],
  [
    'mixed type tuple',
    // Options type forbids this, but cast to verify runtime check.
    {bad: v.tuple([v.number(), v.string()])} as Options,
    [],
    'ZORRO_BAD has mixed types number,string',
  ],
  [
    'mixed type tuple of unions',
    // Options type forbids this, but cast to verify runtime check.
    {
      bad: v.tuple([
        v.union(v.literal('a'), v.literal('b')),
        v.union(v.literal(1), v.literal(2)),
      ]),
    } as Options,
    [],
    'ZORRO_BAD has mixed types string,number',
  ],
  [
    'bad number',
    {num: v.number()},
    ['--num=foobar'],
    'Invalid input for ZORRO_NUM: "foobar"',
  ],
  [
    'bad bool',
    {bool: v.boolean()},
    ['--bool=yo'],
    'Invalid input for ZORRO_BOOL: "yo"',
  ],
] satisfies [string, Options, string[], string][])(
  'invalid config: %s',
  (_name, opts, argv, errorMsg) => {
    let message;
    try {
      parseOptions(opts, {
        argv,
        envNamePrefix: 'ZORRO_',
        env: {},
        logger: new SilentLogger(),
      });
    } catch (e) {
      expect(e).toBeInstanceOf(TypeError);
      message = (e as TypeError).message;
    }
    expect(message).toEqual(errorMsg);
  },
);

class ExitAfterUsage extends Error {}
const exit = () => {
  throw new ExitAfterUsage();
};

test('deprecation warning (flag)', () => {
  const logger = {warn: vi.fn()};
  parseOptions(options, {
    argv: [
      '--replica-db-file',
      'bar',
      '--log-level',
      'info',
      '--deprecated-flag',
      'foo',
    ],
    envNamePrefix: 'Z_',
    env: {},
    logger,
    exit,
  });
  expect(logger.warn).toHaveBeenCalled();
  expect(stripAnsi(logger.warn.mock.calls[0][0])).toMatchInlineSnapshot(`
    "
    Z_DEPRECATED_FLAG is deprecated:
    don't set me anymore
    "
  `);
});

test('deprecation warning (env)', () => {
  const logger = {warn: vi.fn()};
  parseOptions(options, {
    argv: ['--replica-db-file', 'bar', '--log-level', 'info'],
    envNamePrefix: 'Z_',
    env: {['Z_DEPRECATED_FLAG']: 'boo'},
    logger,
    exit,
  });
  expect(logger.warn).toHaveBeenCalled();
  expect(stripAnsi(logger.warn.mock.calls[0][0])).toMatchInlineSnapshot(`
    "
    Z_DEPRECATED_FLAG is deprecated:
    don't set me anymore
    "
  `);
});

test('includeDefaults: false excludes defaults from env and config', () => {
  const parsed = parseOptionsAdvanced(options, {
    argv: ['--replica-db-file', '/tmp/replica.db', '--log-level', 'info'],
    envNamePrefix: 'Z_',
    env: {},
    includeDefaults: false,
  });

  // Config should not include defaults
  expect(parsed.config.port).toBeUndefined();
  expect(parsed.config.log.format).toBeUndefined();
  expect(parsed.config.shard).toEqual({});
  expect(parsed.config.tuple).toBeUndefined();

  // Config should include explicitly set values
  expect(parsed.config.replicaDBFile).toBe('/tmp/replica.db');
  expect(parsed.config.log.level).toBe('info');

  // Env should not include defaults
  expect(parsed.env.Z_PORT).toBeUndefined();
  expect(parsed.env.Z_LOG_FORMAT).toBeUndefined();
  expect(parsed.env.Z_SHARD_ID).toBeUndefined();

  // Env should include explicitly set values
  expect(parsed.env.Z_REPLICA_DB_FILE).toBe('/tmp/replica.db');
  expect(parsed.env.Z_LOG_LEVEL).toBe('info');
});

test('--help', () => {
  const logger = {info: vi.fn()};
  expect(() =>
    parseOptions(options, {
      argv: ['--help'],
      envNamePrefix: 'Z_',
      env: {},
      logger,
      exit,
    }),
  ).toThrow(ExitAfterUsage);
  expect(logger.info).toHaveBeenCalled();
  expect(stripAnsi(logger.info.mock.calls[0][0])).toMatchInlineSnapshot(`
    "
     --port, -p number                  default: 4848                                                        
       Z_PORT env                                                                                            
                                        blah blah blah                                                       
                                                                                                             
     --replica-db-file string           required                                                             
       Z_REPLICA_DB_FILE env                                                                                 
                                                                                                             
     --litestream boolean               optional                                                             
       Z_LITESTREAM env                                                                                      
                                                                                                             
     --log-level string                 required                                                             
       Z_LOG_LEVEL env                                                                                       
                                                                                                             
     --log-format text,json             default: "text"                                                      
       Z_LOG_FORMAT env                                                                                      
                                                                                                             
     --shard-id string                  default: "0"                                                         
       Z_SHARD_ID env                                                                                        
                                        blah blah blah                                                       
                                                                                                             
     --shard-publications string[]      default: []                                                          
       Z_SHARD_PUBLICATIONS env                                                                              
                                                                                                             
     --tuple a,c,e,g,i,k,b,d,f,h,j,l    default: ["a","b"]                                                   
       Z_TUPLE env                                                                                           
                                                                                                             
    "
  `);
});

test('-h', () => {
  const logger = {info: vi.fn()};
  expect(() =>
    parseOptions(options, {
      argv: ['-h'],
      envNamePrefix: 'ZERO_',
      env: {},
      logger,
      exit,
    }),
  ).toThrow(ExitAfterUsage);
  expect(logger.info).toHaveBeenCalled();
  expect(stripAnsi(logger.info.mock.calls[0][0])).toMatchInlineSnapshot(`
    "
     --port, -p number                  default: 4848                                                        
       ZERO_PORT env                                                                                         
                                        blah blah blah                                                       
                                                                                                             
     --replica-db-file string           required                                                             
       ZERO_REPLICA_DB_FILE env                                                                              
                                                                                                             
     --litestream boolean               optional                                                             
       ZERO_LITESTREAM env                                                                                   
                                                                                                             
     --log-level string                 required                                                             
       ZERO_LOG_LEVEL env                                                                                    
                                                                                                             
     --log-format text,json             default: "text"                                                      
       ZERO_LOG_FORMAT env                                                                                   
                                                                                                             
     --shard-id string                  default: "0"                                                         
       ZERO_SHARD_ID env                                                                                     
                                        blah blah blah                                                       
                                                                                                             
     --shard-publications string[]      default: []                                                          
       ZERO_SHARD_PUBLICATIONS env                                                                           
                                                                                                             
     --tuple a,c,e,g,i,k,b,d,f,h,j,l    default: ["a","b"]                                                   
       ZERO_TUPLE env                                                                                        
                                                                                                             
    "
  `);
});

test('unknown arguments', () => {
  const logger = {info: vi.fn(), error: vi.fn()};
  expect(() =>
    parseOptions(options, {argv: ['--shardID', 'foo'], env: {}, logger, exit}),
  ).toThrow(ExitAfterUsage);
  expect(logger.error).toHaveBeenCalled();
  expect(logger.error.mock.calls[0]).toMatchInlineSnapshot(`
    [
      "Invalid arguments:",
      [
        "--shardID",
        "foo",
      ],
    ]
  `);
  expect(logger.info).toHaveBeenCalled();
  expect(stripAnsi(logger.info.mock.calls[0][0])).toMatchInlineSnapshot(`
    "
     --port, -p number                  default: 4848                                                        
       PORT env                                                                                              
                                        blah blah blah                                                       
                                                                                                             
     --replica-db-file string           required                                                             
       REPLICA_DB_FILE env                                                                                   
                                                                                                             
     --litestream boolean               optional                                                             
       LITESTREAM env                                                                                        
                                                                                                             
     --log-level string                 required                                                             
       LOG_LEVEL env                                                                                         
                                                                                                             
     --log-format text,json             default: "text"                                                      
       LOG_FORMAT env                                                                                        
                                                                                                             
     --shard-id string                  default: "0"                                                         
       SHARD_ID env                                                                                          
                                        blah blah blah                                                       
                                                                                                             
     --shard-publications string[]      default: []                                                          
       SHARD_PUBLICATIONS env                                                                                
                                                                                                             
     --tuple a,c,e,g,i,k,b,d,f,h,j,l    default: ["a","b"]                                                   
       TUPLE env                                                                                             
                                                                                                             
    "
  `);
});

test('ungrouped config', () => {
  const ungroupedOptions = {
    port: {
      type: v.number().default(4848),
      desc: ['port description'],
      alias: 'p',
    },
    format: v.union(v.literal('text'), v.literal('json')).default('text'),
    enabled: v.boolean().optional(),
    name: v.string(),
    pids: v.array(v.number()).default([]),
    topLevelCamel: v.string().optional(),
  };

  const result = parseOptions(ungroupedOptions, {
    argv: [
      '--top-level-camel',
      'case',
      '--name',
      'test',
      '--format',
      'json',
      '--enabled',
      'true',
      '--pids',
      '123',
      '456',
    ],
    envNamePrefix: 'Z_',
    env: {},
  });

  expect(result).toEqual({
    port: 4848,
    format: 'json',
    enabled: true,
    name: 'test',
    pids: [123, 456],
    topLevelCamel: 'case',
  });

  const envResult = parseOptions(ungroupedOptions, {
    argv: ['--name', 'test2'],
    envNamePrefix: 'x',
    env: {xFORMAT: 'text'},
  });

  expect(envResult).toEqual({
    port: 4848,
    format: 'text',
    name: 'test2',
    pids: [],
  });
});

test('deprecated options are hidden in help by default', () => {
  const optionsWithDeprecated = {
    normalFlag: v.string(),
    deprecatedButNotExplicitlyHidden: {
      type: v.string().optional(),
      deprecated: ['This flag is deprecated'],
    },
    explicitlyHiddenDeprecated: {
      type: v.string().optional(),
      deprecated: ['This flag is also deprecated'],
      hidden: true,
    },
  };

  const logger = {info: vi.fn()};
  expect(() =>
    parseOptions(optionsWithDeprecated, {
      argv: ['--help'],
      envNamePrefix: 'TEST_',
      env: {},
      logger,
      exit,
    }),
  ).toThrow(ExitAfterUsage);

  expect(logger.info).toHaveBeenCalled();
  const helpOutput = stripAnsi(logger.info.mock.calls[0][0]);

  // Normal flag should be shown
  expect(helpOutput).toContain('--normal-flag');

  // Deprecated flags should be hidden (regardless of explicit hidden property)
  expect(helpOutput).not.toContain('--deprecated-but-not-explicitly-hidden');
  expect(helpOutput).not.toContain('--explicitly-hidden-deprecated');
});
