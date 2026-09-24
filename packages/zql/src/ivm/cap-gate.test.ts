import {describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {CapGate} from './cap-gate.ts';
import {Cap} from './cap.ts';
import {Catch} from './catch.ts';
import {MemoryStorage} from './memory-storage.ts';
import {makeSourceChangeAdd} from './source.ts';
import {consume} from './stream.ts';
import {createSource} from './test/source-factory.ts';

const lc = createSilentLogContext();

describe('CapGate', () => {
  function setupSource() {
    const source = createSource(
      lc,
      testLogConfig,
      'issue',
      {
        id: {type: 'string'},
        projectID: {type: 'string'},
        created: {type: 'number'},
      },
      ['id'],
    );
    for (let i = 1; i <= 5; i++) {
      consume(
        source.push(
          makeSourceChangeAdd({
            id: `i${i}`,
            projectID: 'p1',
            created: i * 100,
          }),
        ),
      );
    }
    return source.connect(undefined);
  }

  test('yields all rows when cap is not set', () => {
    const input = setupSource();
    const gate = new CapGate(input);
    const sink = new Catch(gate);

    const data = sink.fetch();
    expect(
      data.map(n => (n === 'yield' ? 'yield' : (n as {row: Row}).row)),
    ).toHaveLength(5);
  });

  test('yields all rows when open', () => {
    const input = setupSource();
    const gate = new CapGate(input);
    const cap = new Cap(gate, new MemoryStorage(), 1);
    gate.setCap(cap);
    cap.setCapGate(gate);
    const sink = new Catch(cap);

    // Initial fetch fills cap with 1 row
    sink.fetch();

    gate.open();
    const fetchNodes = [...gate.fetch({})];
    expect(fetchNodes).toHaveLength(5);
    gate.close();
  });

  test('gates fetch to tracked PKs when cap is at capacity', () => {
    const input = setupSource();
    const gate = new CapGate(input);
    const cap = new Cap(gate, new MemoryStorage(), 1);
    gate.setCap(cap);
    cap.setCapGate(gate);
    const sink = new Catch(cap);

    // Initial fetch hydrates cap with 1 row (i1)
    const initial = sink.fetch();
    expect(
      initial.map(n => (n === 'yield' ? 'yield' : (n as {row: Row}).row)),
    ).toEqual([{id: 'i1', projectID: 'p1', created: 100}]);

    // Subsequent fetch through gate for non-PK query yields ONLY the tracked PK (i1)
    const gated = [...gate.fetch({constraint: {projectID: 'p1'}})];
    expect(
      gated.map(n => (n === 'yield' ? 'yield' : (n as {row: Row}).row)),
    ).toEqual([{id: 'i1', projectID: 'p1', created: 100}]);
  });

  test('does not gate PK point lookups', () => {
    const input = setupSource();
    const gate = new CapGate(input);
    const cap = new Cap(gate, new MemoryStorage(), 1);
    gate.setCap(cap);
    cap.setCapGate(gate);
    const sink = new Catch(cap);

    // Hydrate cap with i1
    sink.fetch();

    // A point lookup for i3 by its primary key is NOT gated even though cap holds i1
    const pointLookup = [...gate.fetch({constraint: {id: 'i3'}})];
    expect(
      pointLookup.map(n => (n === 'yield' ? 'yield' : (n as {row: Row}).row)),
    ).toEqual([{id: 'i3', projectID: 'p1', created: 300}]);
  });
});
