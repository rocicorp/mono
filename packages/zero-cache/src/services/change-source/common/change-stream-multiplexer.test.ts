import {
  type MockedFunction,
  beforeEach,
  describe,
  expect,
  test,
  vi,
} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import type {
  ChangeStreamMessage,
  DownstreamStatusMessage,
} from '../protocol/current.ts';
import {ChangeStreamMultiplexer} from './change-stream-multiplexer.ts';

type CancelFn = () => void;
type ListenFn = (message: ChangeStreamMessage) => void;

describe('change-stream-multiplexer', () => {
  let stream: ChangeStreamMultiplexer;
  let cancelFn1: MockedFunction<CancelFn>;
  let cancelFn2: MockedFunction<CancelFn>;
  let listenFn1: MockedFunction<ListenFn>;
  let listenFn2: MockedFunction<ListenFn>;

  beforeEach(() => {
    cancelFn1 = vi.fn();
    cancelFn2 = vi.fn();
    listenFn1 = vi.fn();
    listenFn2 = vi.fn();

    stream = new ChangeStreamMultiplexer(createSilentLogContext(), '123')
      .addProducers({cancel: cancelFn1}, {cancel: cancelFn2})
      .addListeners({onChange: listenFn1}, {onChange: listenFn2});
  });

  test('reservations', async () => {
    // pushStatus() does not require a reservation.
    stream.pushStatus(['status', {ack: true}, {watermark: 'foo'}]);

    // However, other pushes should fail with an assert.
    expect(() =>
      stream.push(['begin', {tag: 'begin'}, {commitWatermark: '123'}]),
    ).toThrowError();

    const res1 = stream.reserve('foo');
    expect(res1).toBe('123');

    expect(stream.waiterDelay()).toBeLessThan(0);

    // Already reserved, should be a promise.
    const res2 = stream.reserve('bar');
    expect(res2).toBeInstanceOf(Promise);

    expect(stream.waiterDelay()).toBeGreaterThan(0);

    // A third request waits in line.
    const res3 = stream.reserve('baz');
    expect(res3).toBeInstanceOf(Promise);

    expect(stream.waiterDelay()).toBeGreaterThan(0);

    // The first producer releases the reservation.
    stream.release('156');
    expect(await res2).toBe('156');

    // There's still one producer in line
    expect(stream.waiterDelay()).toBeGreaterThan(0);

    stream.release('28da');
    expect(await res3).toBe('28da');

    expect(stream.waiterDelay()).toBeLessThan(0);

    // Relinquish the final reservation.
    stream.release('2e82');

    // No Promise required to reserve.
    expect(stream.reserve('foo')).toBe('2e82');
  });

  test('cancelation', () => {
    expect(cancelFn1).not.toBeCalled();
    expect(cancelFn2).not.toBeCalled();

    stream.fail(new Error('foo'));

    expect(cancelFn1).toHaveBeenCalledOnce();
    expect(cancelFn2).toHaveBeenCalledOnce();
  });

  test('listeners', () => {
    expect(listenFn1).not.toBeCalled();
    expect(listenFn1).not.toBeCalled();

    const begin: ChangeStreamMessage = [
      'begin',
      {tag: 'begin'},
      {commitWatermark: '123'},
    ];
    void stream.reserve('foo');
    void stream.push(begin);

    expect(listenFn1).toHaveBeenCalledExactlyOnceWith(begin);
    expect(listenFn2).toHaveBeenCalledExactlyOnceWith(begin);

    const commit: ChangeStreamMessage = [
      'commit',
      {tag: 'commit'},
      {watermark: '123'},
    ];
    void stream.push(commit);

    expect(listenFn1).toHaveBeenNthCalledWith(2, commit);
    expect(listenFn2).toHaveBeenNthCalledWith(2, commit);

    const status: DownstreamStatusMessage = [
      'status',
      {ack: false},
      {watermark: '124'},
    ];
    void stream.pushStatus(status);
    expect(listenFn1).toHaveBeenNthCalledWith(3, status);
    expect(listenFn2).toHaveBeenNthCalledWith(3, status);
  });
});
