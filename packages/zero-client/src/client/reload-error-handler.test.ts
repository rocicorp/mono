import {LogContext} from '@rocicorp/logger';
import {expect} from 'chai';
import sinon from 'sinon';
import {
  RELOAD_REASON_STORAGE_KEY,
  reloadWithReason,
  reportReloadReason,
} from './reload-error-handler.js';
import {TestLogSink} from './test-utils.js';

const localStorageDescriptor = Object.getOwnPropertyDescriptor(
  globalThis,
  'localStorage',
)!;

teardown(() => {
  sinon.restore();
  Object.defineProperty(globalThis, 'localStorage', localStorageDescriptor);
});

test('reloadWithReason', () => {
  const storage = {unrelated: 'foo'};
  sinon.replaceGetter(
    globalThis,
    'localStorage',
    () => storage as unknown as Storage,
  );

  const sink = new TestLogSink();
  const lc = new LogContext('debug', {foo: 'bar'}, sink);

  const reload = sinon.fake();
  reloadWithReason(lc, reload, 'my reason');
  expect(reload.calledOnce).equal(true);
  expect(storage).deep.equal({
    unrelated: 'foo',
    [RELOAD_REASON_STORAGE_KEY]: 'my reason',
  });

  reportReloadReason(lc);
  expect(sink.messages).deep.equal([
    ['error', {foo: 'bar'}, ['Zero reloaded the page.', 'my reason']],
  ]);
  expect(storage).deep.equal({unrelated: 'foo'});
});

test('reloadWithReason no localStorage', () => {
  // @ts-expect-error This isa test so we do not play along with TS
  delete globalThis.localStorage;

  const sink = new TestLogSink();
  const lc = new LogContext('debug', {foo: 'bar'}, sink);

  const reload = sinon.fake();
  reloadWithReason(lc, reload, 'my reason');
  expect(reload.calledOnce).equal(true);
  expect(sink.messages).deep.equal([
    ['error', {foo: 'bar'}, ['Zero reloaded the page.', 'my reason']],
  ]);

  reportReloadReason(lc);
  expect(sink.messages).deep.equal([
    ['error', {foo: 'bar'}, ['Zero reloaded the page.', 'my reason']],
  ]);
});
