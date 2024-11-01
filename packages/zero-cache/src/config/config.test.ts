import {SilentLogger} from '@rocicorp/logger';
import stripAnsi from 'strip-ansi';
import {expect, test, vi} from 'vitest';
import * as v from '../../../shared/src/valita.js';
import {ExitAfterUsage, parseOptions, type Options} from './config.js';

const options = {
  port: {type: v.number().default(4848), desc: ['blah blah blah']},
  replicaDBFile: v.string(),
  litestream: v.boolean().optional(),
  log: {
    format: v.union(v.literal('text'), v.literal('json')).default('text'),
  },
  shard: {
    id: {type: v.string().default('0'), desc: ['blah blah blah']},
    publications: {type: v.array(v.string()).optional(() => [])},
  },
  tuple: v.tuple([v.string(), v.string()]).optional(() => ['a', 'b']),
};

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
      ['PORT']: '6000',
      ['REPLICA_DB_FILE']: '/tmp/env-replica.db',
      ['LITESTREAM']: 'true',
      ['LOG_FORMAT']: 'json',
      ['SHARD_ID']: 'xyz',
      ['SHARD_PUBLICATIONS']: 'zero_foo',
      ['TUPLE']: 'c,d',
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
      ['PORT']: '6000',
      ['REPLICA_DB_FILE']: '/tmp/env-replica.db',
      ['LITESTREAM']: 'true',
      ['LOG_FORMAT']: 'json',
      ['SHARD_ID']: 'xyz',
      ['SHARD_PUBLICATIONS']: 'zero_foo,zero_bar',
      ['TUPLE']: 'e,f',
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
    'argv values, eager multiples',
    [
      '--port',
      '6000',
      '--replicaDBFile=/tmp/replica.db',
      '--litestream',
      'true',
      '--logFormat=json',
      '--shardId',
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
      '--shardId',
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
      '--shardId',
      'abc',
      '--shardPublications',
      'zero_foo',
      'zero_bar',
      '--tuple',
      'k',
      'l',
    ],
    {
      ['PORT']: '6000',
      ['REPLICA_DB_FILE']: '/tmp/env-replica.db',
      ['LITESTREAM']: 'true',
      ['LOG_FORMAT']: 'text',
      ['SHARD_ID']: 'xyz',
      ['SHARD_PUBLICATIONS']: 'zero_blue',
      ['TUPLE']: 'e,f',
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
])('%s', (_name, argv, env, result) => {
  expect(parseOptions(options, argv, env)).toEqual(result);
});

test.each([
  [
    'missing required flag',
    {required: v.string()},
    [],
    'Missing property required',
  ],
  [
    'missing required multiple flag',
    {required: v.array(v.string())},
    [],
    'Missing property required',
  ],
  [
    'mixed type union',
    {bad: v.union(v.literal('123'), v.literal(456))},
    [],
    '--bad flag has mixed types number and string',
  ],
  [
    'mixed type tuple',
    {bad: v.tuple([v.number(), v.string()])},
    [],
    '--bad has mixed types string and number',
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
      parseOptions(opts, argv, {}, new SilentLogger());
    } catch (e) {
      expect(e).toBeInstanceOf(TypeError);
      message = (e as TypeError).message;
    }
    expect(message).toEqual(errorMsg);
  },
);

test('--help', () => {
  const logger = {error: vi.fn()};
  expect(() => parseOptions(options, ['--help'], {}, logger)).toThrow(
    ExitAfterUsage,
  );
  expect(logger.error).toHaveBeenCalledOnce();
  expect(stripAnsi(logger.error.mock.calls[0][0])).toMatchInlineSnapshot(`
    "
     --port number                      blah blah blah                                                       
                                        default: 4848                                                        
                                        env: PORT                                                            
                                                                                                             
     --replicaDBFile string             env: REPLICA_DB_FILE                                                 
                                                                                                             
     --litestream boolean               env: LITESTREAM                                                      
                                                                                                             
     --logFormat text,json              default: "text"                                                      
                                        env: LOG_FORMAT                                                      
                                                                                                             
     --shardId string                   blah blah blah                                                       
                                        default: "0"                                                         
                                        env: SHARD_ID                                                        
                                                                                                             
     --shardPublications string[]       default: []                                                          
                                        env: SHARD_PUBLICATIONS                                              
                                                                                                             
     --tuple string[]                   default: ["a","b"]                                                   
                                        env: TUPLE                                                           
                                                                                                             
    "
  `);
});

test('-h', () => {
  const logger = {error: vi.fn()};
  expect(() => parseOptions(options, ['-h'], {}, logger)).toThrow(
    ExitAfterUsage,
  );
  expect(logger.error).toHaveBeenCalledOnce();
  expect(stripAnsi(logger.error.mock.calls[0][0])).toMatchInlineSnapshot(`
    "
     --port number                      blah blah blah                                                       
                                        default: 4848                                                        
                                        env: PORT                                                            
                                                                                                             
     --replicaDBFile string             env: REPLICA_DB_FILE                                                 
                                                                                                             
     --litestream boolean               env: LITESTREAM                                                      
                                                                                                             
     --logFormat text,json              default: "text"                                                      
                                        env: LOG_FORMAT                                                      
                                                                                                             
     --shardId string                   blah blah blah                                                       
                                        default: "0"                                                         
                                        env: SHARD_ID                                                        
                                                                                                             
     --shardPublications string[]       default: []                                                          
                                        env: SHARD_PUBLICATIONS                                              
                                                                                                             
     --tuple string[]                   default: ["a","b"]                                                   
                                        env: TUPLE                                                           
                                                                                                             
    "
  `);
});
