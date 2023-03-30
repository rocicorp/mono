import {expect} from '@esm-bundle/chai';
import {LogContext} from '@rocicorp/logger';
import sinon from 'sinon';
import {
  reloadWithReason,
  RELOAD_REASON_STORAGE_KEY,
  reportReloadReason,
} from './reload-error-handler.js';
import {TestLogSink} from './test-utils.js';

test('reloadWithReason', () => {
  const storage = {unrelated: 'foo'};
  const reload = sinon.fake();
  reloadWithReason(reload, storage, 'myreason');
  expect(reload.calledOnce).equal(true);
  expect(storage).deep.equal({
    unrelated: 'foo',
    [RELOAD_REASON_STORAGE_KEY]: 'myreason',
  });

  const sink = new TestLogSink();
  reportReloadReason(new LogContext('debug', sink), storage);
  expect(sink.messages).deep.equal([
    ['error', 'Reflect reloaded the page.', 'myreason'],
  ]);
  expect(storage).deep.equal({unrelated: 'foo'});
});
