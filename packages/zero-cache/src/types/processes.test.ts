import {describe, expect, test} from 'vitest';
import {inProcChannel} from './processes.js';

describe('types/processes', () => {
  test('in-proc channel', () => {
    const [port1, port2] = inProcChannel();

    const messages1: unknown[] = [];
    const messages2: unknown[] = [];

    port1.on('message', data => messages1.push(data));
    port2.on('message', data => messages2.push(data));

    port1.send(['ready', 'yo']);
    port1.send(['ready', 'yoz']);

    expect(messages2).toEqual([
      ['ready', 'yo'],
      ['ready', 'yoz'],
    ]);

    port2.send(['subscribe', 'foo']);
    port2.send(['notify', 'bar']);

    expect(messages1).toEqual([
      ['subscribe', 'foo'],
      ['notify', 'bar'],
    ]);
  });
});
