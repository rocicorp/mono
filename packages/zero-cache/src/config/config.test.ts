import {SilentLogger} from '@rocicorp/logger';
import {Ansis} from 'ansis';
import {expect, test, vi} from 'vitest';
import * as v from '../../../shared/src/valita.js';
import {
  ExitAfterUsage,
  parseOptions,
  type Config,
  type Options,
} from './config.js';

// For some reason, Typescript doesn't figure out `import ansis from 'ansis'`
const ansis = new Ansis();

const options = {
  port: {
    type: v.number().default(4848),
    desc: ['blah blah blah'],
    allCaps: true, // verify that ungrouped flags are not capitalized
    alias: 'p',
  },
  replicaDBFile: v.string(),
  litestream: v.boolean().optional(),
  log: {
    format: v.union(v.literal('text'), v.literal('json')).default('text'),
  },
  shard: {
    id: {
      type: v.string().default('0'),
      desc: ['blah blah blah'],
      allCaps: true, // grouped flags are capitalized
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
};

type TestConfig = Config<typeof options>;

test.each([
  [
    'defaults',
    ['--replicaDBFile', '/tmp/replica.db'],
    {},
    {
      port: 4848,
      replicaDBFile: '/tmp/replica.db',
      log: {format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
  ],
  [
    'env values',
    [],
    {
      ['Z_PORT']: '6000',
      ['Z_REPLICA_DB_FILE']: '/tmp/env-replica.db',
      ['Z_LITESTREAM']: 'true',
      ['Z_LOG_FORMAT']: 'json',
      ['Z_SHARD_ID']: 'xyz',
      ['Z_SHARD_PUBLICATIONS']: 'zero_foo',
      ['Z_TUPLE']: 'c,d',
    },
    {
      port: 6000,
      replicaDBFile: '/tmp/env-replica.db',
      litestream: true,
      log: {format: 'json'},
      shard: {id: 'xyz', publications: ['zero_foo']},
      tuple: ['c', 'd'],
    },
  ],
  [
    'env value for array flag separated by commas',
    [],
    {
      ['Z_PORT']: '6000',
      ['Z_REPLICA_DB_FILE']: '/tmp/env-replica.db',
      ['Z_LITESTREAM']: 'true',
      ['Z_LOG_FORMAT']: 'json',
      ['Z_SHARD_ID']: 'xyz',
      ['Z_SHARD_PUBLICATIONS']: 'zero_foo,zero_bar',
      ['Z_TUPLE']: 'e,f',
    },
    {
      port: 6000,
      replicaDBFile: '/tmp/env-replica.db',
      litestream: true,
      log: {format: 'json'},
      shard: {id: 'xyz', publications: ['zero_foo', 'zero_bar']},
      tuple: ['e', 'f'],
    },
  ],
  [
    'argv values, short alias',
    ['-p', '6000', '--replicaDBFile=/tmp/replica.db'],
    {},
    {
      port: 6000,
      replicaDBFile: '/tmp/replica.db',
      log: {format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
  ],
  [
    'argv values, hex numbers',
    ['-p', '0x1234', '--replicaDBFile=/tmp/replica.db'],
    {},
    {
      port: 4660,
      replicaDBFile: '/tmp/replica.db',
      log: {format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
  ],
  [
    'argv values, scientific notation',
    ['-p', '1.234E3', '--replicaDBFile=/tmp/replica.db'],
    {},
    {
      port: 1234,
      replicaDBFile: '/tmp/replica.db',
      log: {format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
  ],
  [
    'argv values, eager multiples',
    [
      '--port',
      '6000',
      '--replicaDBFile=/tmp/replica.db',
      '--litestream',
      'true',
      '--logFormat=json',
      '--shardID',
      'abc',
      '--shardPublications',
      'zero_foo',
      'zero_bar',
      '--tuple',
      'g',
      'h',
    ],
    {},
    {
      port: 6000,
      replicaDBFile: '/tmp/replica.db',
      litestream: true,
      log: {format: 'json'},
      shard: {id: 'abc', publications: ['zero_foo', 'zero_bar']},
      tuple: ['g', 'h'],
    },
  ],
  [
    'argv values, separate multiples',
    [
      '--port',
      '6000',
      '--replicaDBFile',
      '/tmp/replica.db',
      '--litestream',
      'true',
      '--logFormat=json',
      '--shardID',
      'abc',
      '--shardPublications',
      'zero_foo',
      '--shardPublications',
      'zero_bar',
      '--tuple',
      'i',
      '--tuple',
      'j',
    ],
    {},
    {
      port: 6000,
      replicaDBFile: '/tmp/replica.db',
      litestream: true,
      log: {format: 'json'},
      shard: {id: 'abc', publications: ['zero_foo', 'zero_bar']},
      tuple: ['i', 'j'],
    },
  ],
  [
    'argv value override env values',
    [
      '--port',
      '8888',
      '--logFormat=json',
      '--shardID',
      'abc',
      '--shardPublications',
      'zero_foo',
      'zero_bar',
      '--tuple',
      'k',
      'l',
    ],
    {
      ['Z_PORT']: '6000',
      ['Z_REPLICA_DB_FILE']: '/tmp/env-replica.db',
      ['Z_LITESTREAM']: 'true',
      ['Z_LOG_FORMAT']: 'text',
      ['Z_SHARD_ID']: 'xyz',
      ['Z_SHARD_PUBLICATIONS']: 'zero_blue',
      ['Z_TUPLE']: 'e,f',
    },
    {
      port: 8888,
      replicaDBFile: '/tmp/env-replica.db',
      litestream: true,
      log: {format: 'json'},
      shard: {id: 'abc', publications: ['zero_foo', 'zero_bar']},
      tuple: ['k', 'l'],
    },
  ],
  [
    '--bool flag',
    ['--litestream', '--replicaDBFile=/tmp/replica.db'],
    {},
    {
      port: 4848,
      replicaDBFile: '/tmp/replica.db',
      litestream: true,
      log: {format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
  ],
  [
    '--bool=true flag',
    ['--litestream=true', '--replicaDBFile=/tmp/replica.db'],
    {},
    {
      port: 4848,
      replicaDBFile: '/tmp/replica.db',
      litestream: true,
      log: {format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
  ],
  [
    '--bool 1 flag',
    ['--litestream', '1', '--replicaDBFile=/tmp/replica.db'],
    {},
    {
      port: 4848,
      replicaDBFile: '/tmp/replica.db',
      litestream: true,
      log: {format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
  ],
  [
    '--bool=0 flag',
    ['--litestream=0', '--replicaDBFile=/tmp/replica.db'],
    {},
    {
      port: 4848,
      replicaDBFile: '/tmp/replica.db',
      litestream: false,
      log: {format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
  ],
  [
    '--bool False flag',
    ['--litestream', 'False', '--replicaDBFile=/tmp/replica.db'],
    {},
    {
      port: 4848,
      replicaDBFile: '/tmp/replica.db',
      litestream: false,
      log: {format: 'text'},
      shard: {id: '0', publications: []},
      tuple: ['a', 'b'],
    },
  ],
] satisfies [string, string[], Record<string, string>, TestConfig][])(
  '%s',
  (_name, argv, env, result) => {
    expect(parseOptions(options, argv, 'Z_', env)).toEqual(result);
  },
);

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

test('--help', () => {
  const logger = {info: vi.fn()};
  expect(() => parseOptions(options, ['--help'], 'Z_', {}, logger)).toThrow(
    ExitAfterUsage,
  );
  expect(logger.info).toHaveBeenCalledOnce();
  expect(ansis.strip(logger.info.mock.calls[0][0])).toMatchInlineSnapshot(`
    "
     --port, -p number                  default: 4848                                                        
       Z_PORT env                                                                                            
                                        blah blah blah                                                       
                                                                                                             
     --replicaDBFile string             required                                                             
       Z_REPLICA_DB_FILE env                                                                                 
                                                                                                             
     --litestream boolean               optional                                                             
       Z_LITESTREAM env                                                                                      
                                                                                                             
     --logFormat text,json              default: "text"                                                      
       Z_LOG_FORMAT env                                                                                      
                                                                                                             
     --shardID string                   default: "0"                                                         
       Z_SHARD_ID env                                                                                        
                                        blah blah blah                                                       
                                                                                                             
     --shardPublications string[]       default: []                                                          
       Z_SHARD_PUBLICATIONS env                                                                              
                                                                                                             
     --tuple a,c,e,g,i,k,b,d,f,h,j,l    default: ["a","b"]                                                   
       Z_TUPLE env                                                                                           
                                                                                                             
    "
  `);
});

test('-h', () => {
  const logger = {info: vi.fn()};
  expect(() => parseOptions(options, ['-h'], 'ZERO_', {}, logger)).toThrow(
    ExitAfterUsage,
  );
  expect(logger.info).toHaveBeenCalledOnce();
  expect(ansis.strip(logger.info.mock.calls[0][0])).toMatchInlineSnapshot(`
    "
     --port, -p number                  default: 4848                                                        
       ZERO_PORT env                                                                                         
                                        blah blah blah                                                       
                                                                                                             
     --replicaDBFile string             required                                                             
       ZERO_REPLICA_DB_FILE env                                                                              
                                                                                                             
     --litestream boolean               optional                                                             
       ZERO_LITESTREAM env                                                                                   
                                                                                                             
     --logFormat text,json              default: "text"                                                      
       ZERO_LOG_FORMAT env                                                                                   
                                                                                                             
     --shardID string                   default: "0"                                                         
       ZERO_SHARD_ID env                                                                                     
                                        blah blah blah                                                       
                                                                                                             
     --shardPublications string[]       default: []                                                          
       ZERO_SHARD_PUBLICATIONS env                                                                           
                                                                                                             
     --tuple a,c,e,g,i,k,b,d,f,h,j,l    default: ["a","b"]                                                   
       ZERO_TUPLE env                                                                                        
                                                                                                             
    "
  `);
});
