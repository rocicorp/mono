import {describe, expect, test, vi} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {Catch} from './catch.ts';
import {makeAddChange} from './change.ts';
import {makeSourceChangeAdd} from './source.ts';
import {consume} from './stream.ts';
import {TakeGate, type TakeBoundProvider} from './take-gate.ts';
import {createSource} from './test/source-factory.ts';

const lc = createSilentLogContext();

describe('TakeGate', () => {
  function setupSource() {
    const source = createSource(
      lc,
      testLogConfig,
      'issue',
      {
        id: {type: 'string'},
        created: {type: 'number'},
      },
      ['id'],
    );
    for (let i = 1; i <= 5; i++) {
      consume(
        source.push(
          makeSourceChangeAdd({
            id: `i${i}`,
            created: i * 100,
          }),
        ),
      );
    }
    return source.connect([
      ['created', 'asc'],
      ['id', 'asc'],
    ]);
  }

  test('yields all rows when boundProvider is not set', () => {
    const input = setupSource();
    const gate = new TakeGate(input);
    const sink = new Catch(gate);

    const data = sink.fetch();
    expect(
      data.map(n => (n === 'yield' ? 'yield' : (n as {row: Row}).row)),
    ).toEqual([
      {id: 'i1', created: 100},
      {id: 'i2', created: 200},
      {id: 'i3', created: 300},
      {id: 'i4', created: 400},
      {id: 'i5', created: 500},
    ]);
  });

  test('yields all rows when boundProvider returns undefined', () => {
    const input = setupSource();
    const gate = new TakeGate(input);
    const getBound = vi.fn(() => undefined);
    const provider: TakeBoundProvider = {
      getBound,
    };
    gate.setBoundProvider(provider);
    const sink = new Catch(gate);

    const data = sink.fetch();
    expect(
      data.map(n => (n === 'yield' ? 'yield' : (n as {row: Row}).row)),
    ).toEqual([
      {id: 'i1', created: 100},
      {id: 'i2', created: 200},
      {id: 'i3', created: 300},
      {id: 'i4', created: 400},
      {id: 'i5', created: 500},
    ]);
    expect(getBound).toHaveBeenCalled();
  });

  test('bounds fetch stream when bound is active', () => {
    const input = setupSource();
    const gate = new TakeGate(input);
    const provider: TakeBoundProvider = {
      getBound: vi.fn(() => ({id: 'i3', created: 300})),
    };
    gate.setBoundProvider(provider);
    const sink = new Catch(gate);

    const data = sink.fetch();
    expect(
      data.map(n => (n === 'yield' ? 'yield' : (n as {row: Row}).row)),
    ).toEqual([
      {id: 'i1', created: 100},
      {id: 'i2', created: 200},
      {id: 'i3', created: 300},
    ]);
  });

  test('does not early-terminate reverse fetch (needed for Take backward scans)', () => {
    const input = setupSource();
    const gate = new TakeGate(input);
    const provider: TakeBoundProvider = {
      getBound: vi.fn(() => ({id: 'i3', created: 300})),
    };
    gate.setBoundProvider(provider);
    const sink = new Catch(gate);

    const data = sink.fetch({reverse: true});
    expect(
      data.map(n => (n === 'yield' ? 'yield' : (n as {row: Row}).row)),
    ).toEqual([
      {id: 'i5', created: 500},
      {id: 'i4', created: 400},
      {id: 'i3', created: 300},
      {id: 'i2', created: 200},
      {id: 'i1', created: 100},
    ]);
  });

  test('forwards push downstream', () => {
    const input = setupSource();
    const gate = new TakeGate(input);
    const sink = new Catch(gate);
    sink.fetch();

    const pushSpy = vi.fn();
    gate.setOutput({
      push: (change, pusher) => {
        pushSpy(change, pusher);
        return [];
      },
    });

    const addChange = makeAddChange({
      row: {id: 'i6', created: 600},
      relationships: {},
    });
    consume(gate.push(addChange, gate));

    expect(pushSpy).toHaveBeenCalledWith(addChange, gate);
  });

  test('open() bypasses bounds during fetch, close() restores bounds', () => {
    const input = setupSource();
    const gate = new TakeGate(input);
    const provider: TakeBoundProvider = {
      getBound: vi.fn(() => ({id: 'i2', created: 200})),
    };
    gate.setBoundProvider(provider);
    const sink = new Catch(gate);

    // Normally bounded to i1, i2
    expect(
      sink.fetch().map(n => (n === 'yield' ? 'yield' : (n as {row: Row}).row)),
    ).toEqual([
      {id: 'i1', created: 100},
      {id: 'i2', created: 200},
    ]);

    // When opened, yields all rows
    gate.open();
    expect(gate.isOpen()).toBe(true);
    expect(
      sink.fetch().map(n => (n === 'yield' ? 'yield' : (n as {row: Row}).row)),
    ).toEqual([
      {id: 'i1', created: 100},
      {id: 'i2', created: 200},
      {id: 'i3', created: 300},
      {id: 'i4', created: 400},
      {id: 'i5', created: 500},
    ]);

    // When closed, bounds are restored
    gate.close();
    expect(gate.isOpen()).toBe(false);
    expect(
      sink.fetch().map(n => (n === 'yield' ? 'yield' : (n as {row: Row}).row)),
    ).toEqual([
      {id: 'i1', created: 100},
      {id: 'i2', created: 200},
    ]);
  });
});
