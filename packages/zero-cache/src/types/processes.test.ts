import {SendHandle} from 'child_process';
import {describe, expect, test, vi} from 'vitest';
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

  type NotifyMessage = ['notify', {uuid: string}];
  type SubscribeMessage = ['subscribe', {foo: string}];

  test('onMessageType, onceMessageType', () => {
    const [port1, port2] = inProcChannel();

    const handler1 = vi.fn();
    const handler2 = vi.fn();
    const handler3 = vi.fn();

    port2
      .onMessageType<NotifyMessage>('notify', handler1)
      .onceMessageType<NotifyMessage>('notify', handler2)
      .onMessageType<SubscribeMessage>('subscribe', handler3);

    port1.send<SubscribeMessage>(
      ['subscribe', {foo: 'bar'}],
      'sendHandle' as unknown as SendHandle,
    );
    port1.send<NotifyMessage>(['notify', {uuid: 'one'}]);
    port1.send<SubscribeMessage>(['subscribe', {foo: 'baz'}]);
    port1.send<NotifyMessage>(
      ['notify', {uuid: 'two'}],
      123 as unknown as SendHandle,
    );
    port1.send<NotifyMessage>(['notify', {uuid: 'three'}]);

    expect(handler1).toHaveBeenCalledTimes(3);
    expect(handler2).toHaveBeenCalledTimes(1);
    expect(handler3).toHaveBeenCalledTimes(2);

    expect(handler1.mock.calls).toEqual([
      [{uuid: 'one'}, undefined],
      [{uuid: 'two'}, 123],
      [{uuid: 'three'}, undefined],
    ]);
    expect(handler2.mock.calls).toEqual([[{uuid: 'one'}, undefined]]);
    expect(handler3.mock.calls).toEqual([
      [{foo: 'bar'}, 'sendHandle'],
      [{foo: 'baz'}, undefined],
    ]);
  });
});
