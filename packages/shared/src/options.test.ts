/* eslint-disable @typescript-eslint/naming-convention */
import {SilentLogger} from '@rocicorp/logger';
import stripAnsi from 'strip-ansi';
import type {PartialDeep} from 'type-fest';
import {expect, test, vi} from 'vitest';
import {
  envSchema,
  parseOptions,
  parseOptionsAdvanced,
  type Config,
  type Options,
} from './options.js';
import * as v from './valita.js';

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
  const parsed = parseOptionsAdvanced(
    options,
    argv,
    'Z_',
    true,
    allowPartial,
    env,
  );
  expect(parsed.config).toEqual(result);
  expect(parsed.env).toEqual(envObj);
  expect(parsed.unknown).toEqual(unknown);

  // Sanity check: Ensure that parsing the parsed.env computes the same result.
  const reparsed = parseOptionsAdvanced(
    options,
    [],
    'Z_',
    true,
    allowPartial,
    parsed.env,
  );
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
      [],
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
      [],
    ),
  ).toThrowError('Two or more option definitions have the same alias');
});

test.each([
  [
    'missing required flag',
    {requiredFlag: v.string()},
    [],
    'Missing property requiredFlag',
  ],
  [
    'missing required multiple flag',
    {requiredFlag: v.array(v.string())},
    [],
    'Missing property requiredFlag',
  ],
  [
    'mixed type union',
    // Options type forbids this, but cast to verify runtime check.
    {bad: v.union(v.literal('123'), v.literal(456))} as Options,
    [],
    '--bad has mixed types string,number',
  ],
  [
    'mixed type tuple',
    // Options type forbids this, but cast to verify runtime check.
    {bad: v.tuple([v.number(), v.string()])} as Options,
    [],
    '--bad has mixed types number,string',
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
    '--bad has mixed types string,number',
  ],
  [
    'bad number',
    {num: v.number()},
    ['--num=foobar'],
    'Invalid input for --num: "foobar"',
  ],
  [
    'bad bool',
    {bool: v.boolean()},
    ['--bool=yo'],
    'Invalid input for --bool: "yo"',
  ],
] satisfies [string, Options, string[], string][])(
  'invalid config: %s',
  (_name, opts, argv, errorMsg) => {
    let message;
    try {
      parseOptions(opts, argv, '', {}, new SilentLogger());
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

test('--help', () => {
  const logger = {info: vi.fn()};
  expect(() =>
    parseOptions(options, ['--help'], 'Z_', {}, logger, exit),
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
    parseOptions(options, ['-h'], 'ZERO_', {}, logger, exit),
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
    parseOptions(options, ['--shardID', 'foo'], '', {}, logger, exit),
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
  };

  const result = parseOptions(
    ungroupedOptions,
    ['--name', 'test', '--format', 'json', '--enabled', 'true'],
    'Z_',
    {},
  );

  expect(result).toEqual({
    port: 4848,
    format: 'json',
    enabled: true,
    name: 'test',
  });

  const envResult = parseOptions(ungroupedOptions, ['--name', 'test2'], 'x', {
    xFORMAT: 'text',
  });

  expect(envResult).toEqual({
    port: 4848,
    format: 'text',
    name: 'test2',
  });
});
