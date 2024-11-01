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
    },
    {
      port: 6000,
      replicaDBFile: '/tmp/env-replica.db',
      litestream: true,
      log: {format: 'json'},
      shard: {id: 'xyz', publications: ['zero_foo']},
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
    },
    {
      port: 6000,
      replicaDBFile: '/tmp/env-replica.db',
      litestream: true,
      log: {format: 'json'},
      shard: {id: 'xyz', publications: ['zero_foo', 'zero_bar']},
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
    ],
    {},
    {
      port: 6000,
      replicaDBFile: '/tmp/replica.db',
      litestream: true,
      log: {format: 'json'},
      shard: {id: 'abc', publications: ['zero_foo', 'zero_bar']},
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
    ],
    {},
    {
      port: 6000,
      replicaDBFile: '/tmp/replica.db',
      litestream: true,
      log: {format: 'json'},
      shard: {id: 'abc', publications: ['zero_foo', 'zero_bar']},
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
    ],
    {
      ['PORT']: '6000',
      ['REPLICA_DB_FILE']: '/tmp/env-replica.db',
      ['LITESTREAM']: 'true',
      ['LOG_FORMAT']: 'text',
      ['SHARD_ID']: 'xyz',
      ['SHARD_PUBLICATIONS']: 'zero_blue',
    },
    {
      port: 8888,
      replicaDBFile: '/tmp/env-replica.db',
      litestream: true,
      log: {format: 'json'},
      shard: {id: 'abc', publications: ['zero_foo', 'zero_bar']},
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
    },
  ],
])('%s', (_name, argv, env, result) => {
  expect(parseOptions(options, argv, env)).toEqual(result);
});

test.each([
  ['missing required flag', {required: v.string()}, []],
  ['missing required multiple flag', {required: v.array(v.string())}, []],
  ['mixed type tuple', {bad: v.union(v.literal('123'), v.literal(456))}, []],
  ['bad number', {num: v.number()}, ['--num=foobar']],
  ['bad bool', {bool: v.boolean()}, ['--bool=yo']],
] satisfies [string, Options, string[]][])(
  'invalid config: %s',
  (_name, opts, argv) => {
    expect(() => parseOptions(opts, argv, {}, new SilentLogger())).toThrow(
      TypeError,
    );
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
                                                                                                             
    "
  `);
});
