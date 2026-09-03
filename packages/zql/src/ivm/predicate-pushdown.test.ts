import {describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {SimpleCondition} from '../../../zero-protocol/src/ast.ts';
import type {NoSubqueryCondition} from '../builder/filter.ts';
import {Catch} from './catch.ts';
import {FilterEnd, FilterStart} from './filter-operators.ts';
import {FlippedJoin} from './flipped-join.ts';
import {Join} from './join.ts';
import {MemoryStorage} from './memory-storage.ts';
import {type FetchRequest, type Input, type Output} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {Skip} from './skip.ts';
import {makeSourceChangeAdd} from './source.ts';
import {consume} from './stream.ts';
import type {Stream} from './stream.ts';
import {Take} from './take.ts';
import {createSource} from './test/source-factory.ts';
import {UnionFanIn} from './union-fan-in.ts';
import {UnionFanOut} from './union-fan-out.ts';

const lc = createSilentLogContext();

const cmpEq = (column: string, value: string): SimpleCondition => ({
  type: 'simple',
  op: '=',
  left: {type: 'column', name: column},
  right: {type: 'literal', value},
});

/**
 * Wraps an Input to record every FetchRequest its `fetch` method receives.
 * Used to assert that pass-through operators preserve `req.filter`.
 */
class RecordingInput implements Input {
  readonly #wrapped: Input;
  readonly received: FetchRequest[] = [];

  constructor(wrapped: Input) {
    this.#wrapped = wrapped;
  }

  fetch(
    req: FetchRequest,
  ): Stream<ReturnType<Input['fetch']> extends Stream<infer T> ? T : never> {
    this.received.push(req);
    return this.#wrapped.fetch(req) as Stream<
      ReturnType<Input['fetch']> extends Stream<infer T> ? T : never
    >;
  }

  setOutput(output: Output): void {
    this.#wrapped.setOutput(output);
  }

  destroy(): void {
    this.#wrapped.destroy();
  }

  getSchema(): SourceSchema {
    return this.#wrapped.getSchema();
  }
}

describe('req.filter contract (pass-through operators preserve it)', () => {
  test('FilterStart introduces its condition into req.filter', () => {
    const ms = createSource(
      lc,
      testLogConfig,
      'table',
      {a: {type: 'string'}, b: {type: 'string'}},
      ['a'],
    );
    const conn = ms.connect([['a', 'asc']]);
    const recorder = new RecordingInput(conn);

    const filterCondition: NoSubqueryCondition = cmpEq('b', 'x');
    const filterStart = new FilterStart(recorder, filterCondition);
    const filterEnd = new FilterEnd(filterStart, filterStart);

    // Bare consumer fetch: req.filter is undefined.
    [...filterEnd.fetch({})];
    expect(recorder.received).toHaveLength(1);
    expect(recorder.received[0].filter).toEqual(filterCondition);

    // Consumer-provided req.filter should be AND-merged with FilterStart's
    // own condition.
    const incomingFilter: NoSubqueryCondition = cmpEq('a', 'y');
    [...filterEnd.fetch({filter: incomingFilter})];
    expect(recorder.received).toHaveLength(2);
    expect(recorder.received[1].filter).toEqual({
      type: 'and',
      conditions: [incomingFilter, filterCondition],
    });

    conn.destroy();
  });

  test('FilterStart with no condition is a no-op (forwards req.filter unchanged)', () => {
    const ms = createSource(lc, testLogConfig, 'table', {a: {type: 'string'}}, [
      'a',
    ]);
    const conn = ms.connect([['a', 'asc']]);
    const recorder = new RecordingInput(conn);

    // No condition supplied to FilterStart.
    const filterStart = new FilterStart(recorder);
    const filterEnd = new FilterEnd(filterStart, filterStart);

    const incomingFilter: NoSubqueryCondition = cmpEq('a', 'y');
    [...filterEnd.fetch({filter: incomingFilter})];
    expect(recorder.received).toHaveLength(1);
    // The FetchRequest object itself should be passed through unchanged
    // (no clone/spread needed) when no merge happens.
    expect(recorder.received[0].filter).toBe(incomingFilter);

    conn.destroy();
  });

  test('Skip preserves req.filter via spread', () => {
    const ms = createSource(
      lc,
      testLogConfig,
      'table',
      {a: {type: 'string'}, b: {type: 'string'}},
      ['a'],
    );
    const conn = ms.connect([['a', 'asc']]);
    const recorder = new RecordingInput(conn);
    const skip = new Skip(recorder, {row: {a: 'a0'}, exclusive: false});

    const incomingFilter: NoSubqueryCondition = cmpEq('b', 'x');
    [...skip.fetch({filter: incomingFilter})];
    expect(recorder.received).toHaveLength(1);
    expect(recorder.received[0].filter).toBe(incomingFilter);

    conn.destroy();
  });

  test('UnionFanOut and UnionFanIn forward req.filter to all branches', () => {
    const ms = createSource(
      lc,
      testLogConfig,
      'table',
      {a: {type: 'string'}, b: {type: 'string'}},
      ['a'],
    );
    const conn = ms.connect([['a', 'asc']]);
    const recorder = new RecordingInput(conn);

    const ufo = new UnionFanOut(recorder);
    // Two trivial branches, both reading from the UFO.
    const branch1 = new FilterEnd(new FilterStart(ufo), new FilterStart(ufo));
    const branch2 = new FilterEnd(new FilterStart(ufo), new FilterStart(ufo));
    const ufi = new UnionFanIn(ufo, [branch1, branch2]);

    const incomingFilter: NoSubqueryCondition = cmpEq('b', 'x');
    [...ufi.fetch({filter: incomingFilter})];

    // Each branch should have triggered a fetch with the same req.filter.
    expect(recorder.received.length).toBeGreaterThanOrEqual(2);
    for (const req of recorder.received) {
      expect(req.filter).toBe(incomingFilter);
    }

    conn.destroy();
  });

  test('Take preserves req.filter via spread', () => {
    const ms = createSource(
      lc,
      testLogConfig,
      'table',
      {a: {type: 'string'}, b: {type: 'string'}},
      ['a'],
    );
    const conn = ms.connect([['a', 'asc']]);
    const recorder = new RecordingInput(conn);
    const take = new Take(recorder, new MemoryStorage(), 10);

    const incomingFilter: NoSubqueryCondition = cmpEq('b', 'x');
    [...take.fetch({filter: incomingFilter})];
    expect(recorder.received).toHaveLength(1);
    expect(recorder.received[0].filter).toBe(incomingFilter);

    conn.destroy();
  });

  test('Join preserves req.filter on parent fetches', () => {
    // Join forwards `req` whole to its parent input. The child fetch path
    // (`#processParentNode`) constructs a fresh `{constraint}` request and
    // intentionally drops `req.filter` — see the contract block in
    // `operator.ts`.
    const items = createSource(
      lc,
      testLogConfig,
      'items',
      {id: {type: 'string'}, status: {type: 'string'}},
      ['id'],
    );
    const tags = createSource(
      lc,
      testLogConfig,
      'tags',
      {id: {type: 'string'}, itemID: {type: 'string'}},
      ['id'],
    );
    consume(items.push(makeSourceChangeAdd({id: 'i1', status: 'open'})));
    consume(tags.push(makeSourceChangeAdd({id: 't1', itemID: 'i1'})));

    const parentConn = items.connect([['id', 'asc']]);
    const recorder = new RecordingInput(parentConn);
    const childConn = tags.connect([['id', 'asc']]);

    const join = new Join({
      parent: recorder,
      child: childConn,
      parentKey: ['id'],
      childKey: ['itemID'],
      relationshipName: 'tagged',
      hidden: true,
      system: 'client',
    });

    const incomingFilter: NoSubqueryCondition = cmpEq('status', 'open');
    const sink = new Catch(join);
    [...sink.fetch({filter: incomingFilter})];

    // Exactly one parent fetch, and it must carry the original filter.
    expect(recorder.received).toHaveLength(1);
    expect(recorder.received[0].filter).toBe(incomingFilter);

    parentConn.destroy();
    childConn.destroy();
  });

  test('FlippedJoin spreads req (including req.filter) to parent fetches', () => {
    // parent: items keyed by id, child: tags keyed by itemID.
    const items = createSource(
      lc,
      testLogConfig,
      'items',
      {id: {type: 'string'}, status: {type: 'string'}},
      ['id'],
    );
    const tags = createSource(
      lc,
      testLogConfig,
      'tags',
      {id: {type: 'string'}, itemID: {type: 'string'}},
      ['id'],
    );

    // Seed a single child so FlippedJoin makes at least one parent fetch.
    consume(items.push(makeSourceChangeAdd({id: 'i1', status: 'open'})));
    consume(tags.push(makeSourceChangeAdd({id: 't1', itemID: 'i1'})));

    const parentConn = items.connect([['id', 'asc']]);
    const recorder = new RecordingInput(parentConn);
    const childConn = tags.connect([['id', 'asc']]);

    const flippedJoin = new FlippedJoin({
      parent: recorder,
      child: childConn,
      parentKey: ['id'],
      childKey: ['itemID'],
      relationshipName: 'tagged',
      hidden: true,
      system: 'client',
    });

    const incomingFilter: NoSubqueryCondition = cmpEq('status', 'open');
    const sink = new Catch(flippedJoin);
    [...sink.fetch({filter: incomingFilter})];

    // FlippedJoin should have invoked at least one parent fetch with
    // child-derived multiConstraints AND the original req.filter intact.
    const parentCalls = recorder.received.filter(
      r => r.multiConstraints !== undefined,
    );
    expect(parentCalls.length).toBeGreaterThanOrEqual(1);
    for (const req of parentCalls) {
      expect(req.filter).toBe(incomingFilter);
    }

    parentConn.destroy();
    childConn.destroy();
  });
});
