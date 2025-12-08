import {expect, test, vi} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Catch} from './catch.ts';
import {createSource} from './test/source-factory.ts';
import {UnionFanOut} from './union-fan-out.ts';
import {consume} from './stream.ts';

const lc = createSilentLogContext();
const mockFanIn = {
  fanOutStartedPushing() {},
  *fanOutDonePushing() {},
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
} as any;

test('push broadcasts change to all outputs', () => {
  const s = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  const connector = s.connect([['a', 'asc']]);

  const fanOut = new UnionFanOut(connector);
  fanOut.setFanIn(mockFanIn);
  const catch1 = new Catch(fanOut);
  const catch2 = new Catch(fanOut);
  const catch3 = new Catch(fanOut);

  consume(s.push({type: 'add', row: {a: 1, b: 'foo'}}));
  consume(
    s.push({type: 'edit', oldRow: {a: 1, b: 'foo'}, row: {a: 1, b: 'bar'}}),
  );
  consume(s.push({type: 'remove', row: {a: 1, b: 'bar'}}));

  expect(catch1.pushes).toMatchInlineSnapshot(`
    [
      {
        "node": {
          "relationships": {},
          "row": {
            "a": 1,
            "b": "foo",
          },
        },
        "type": "add",
      },
      {
        "oldRow": {
          "a": 1,
          "b": "foo",
        },
        "row": {
          "a": 1,
          "b": "bar",
        },
        "type": "edit",
      },
      {
        "node": {
          "relationships": {},
          "row": {
            "a": 1,
            "b": "bar",
          },
        },
        "type": "remove",
      },
    ]
  `);

  expect(catch2.pushes).toEqual(catch1.pushes);
  expect(catch3.pushes).toEqual(catch1.pushes);
});

test('setOutput adds new output to list', () => {
  const s = createSource(lc, testLogConfig, 'table', {a: {type: 'number'}}, [
    'a',
  ]);
  const connector = s.connect([['a', 'asc']]);

  const fanOut = new UnionFanOut(connector);
  fanOut.setFanIn(mockFanIn);
  const catch1 = new Catch(fanOut);

  consume(s.push({type: 'add', row: {a: 1}}));

  const catch2 = new Catch(fanOut);

  consume(s.push({type: 'add', row: {a: 2}}));

  expect(catch1.pushes).toHaveLength(2);
  expect(catch2.pushes).toHaveLength(1);
  expect(catch2.pushes[0]).toMatchInlineSnapshot(`
    {
      "node": {
        "relationships": {},
        "row": {
          "a": 2,
        },
      },
      "type": "add",
    }
  `);
});

test('getSchema returns input schema', () => {
  const s = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  const connector = s.connect([['a', 'asc']]);

  const fanOut = new UnionFanOut(connector);

  const schema = fanOut.getSchema();
  expect(schema.tableName).toBe('table');
  expect(schema.columns).toEqual({
    a: {type: 'number'},
    b: {type: 'string'},
  });
  expect(schema.primaryKey).toEqual(['a']);
});

test('fetch delegates to input', () => {
  const s = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );

  consume(s.push({type: 'add', row: {a: 1, b: 'foo'}}));
  consume(s.push({type: 'add', row: {a: 2, b: 'bar'}}));
  consume(s.push({type: 'add', row: {a: 3, b: 'baz'}}));

  const connector = s.connect([['a', 'asc']]);
  const fanOut = new UnionFanOut(connector);

  const result = [...fanOut.fetch({})];
  expect(result).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "a": 1,
          "b": "foo",
        },
      },
      {
        "relationships": {},
        "row": {
          "a": 2,
          "b": "bar",
        },
      },
      {
        "relationships": {},
        "row": {
          "a": 3,
          "b": "baz",
        },
      },
    ]
  `);
});

test('destroy destroys input only after all outputs have called destroy', () => {
  const s = createSource(lc, testLogConfig, 'table', {a: {type: 'number'}}, [
    'a',
  ]);
  const connector = s.connect([['a', 'asc']]);

  const fanOut = new UnionFanOut(connector);
  const catch1 = new Catch(fanOut);
  const catch2 = new Catch(fanOut);
  const catch3 = new Catch(fanOut);

  const destroySpy = vi.spyOn(connector, 'destroy');

  catch1.destroy();
  expect(destroySpy).not.toHaveBeenCalled();

  catch2.destroy();
  expect(destroySpy).not.toHaveBeenCalled();

  catch3.destroy();
  expect(destroySpy).toHaveBeenCalledOnce();
});

test('destroy throws error when called more times than outputs', () => {
  const s = createSource(lc, testLogConfig, 'table', {a: {type: 'number'}}, [
    'a',
  ]);
  const connector = s.connect([['a', 'asc']]);

  const fanOut = new UnionFanOut(connector);
  const catch1 = new Catch(fanOut);
  const catch2 = new Catch(fanOut);

  catch1.destroy();
  catch2.destroy();

  expect(() => fanOut.destroy()).toThrowError(
    'FanOut already destroyed once for each output',
  );
});
