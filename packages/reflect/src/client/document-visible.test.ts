import {expect} from '@esm-bundle/chai';
import * as sinon from 'sinon';
import {LogContext} from '@rocicorp/logger';
import {waitForHidden, waitForVisible} from './document-visible.js';

let clock: sinon.SinonFakeTimers;

setup(() => {
  clock = sinon.useFakeTimers();
});

teardown(() => {
  sinon.restore();
});

class Document extends EventTarget {
  #visibilityState: DocumentVisibilityState = 'visible';
  set visibilityState(v) {
    if (this.#visibilityState === v) {
      return;
    }
    this.#visibilityState = v;
    this.dispatchEvent(new Event('visibilitychange'));
  }
  get visibilityState() {
    return this.#visibilityState;
  }
}

test('waitForVisible', async () => {
  const doc = new Document();
  doc.visibilityState = 'hidden';

  const p = waitForVisible(new LogContext(), doc);
  doc.visibilityState = 'visible';
  await p;
});

test('waitForHidden', async () => {
  const doc = new Document();
  doc.visibilityState = 'visible';

  let resolved = false;
  const p = waitForHidden(new LogContext(), doc, 1000).then(() => {
    resolved = true;
  });
  doc.visibilityState = 'hidden';
  expect(resolved).false;
  await clock.tickAsync(1000);
  expect(resolved).true;
  await p;
});

test('waitForHidden flip back to visible', async () => {
  const doc = new Document();
  doc.visibilityState = 'visible';

  let resolved = false;
  void waitForHidden(new LogContext(), doc, 1000).then(() => {
    resolved = true;
  });

  doc.visibilityState = 'hidden';
  expect(resolved).false;
  await clock.tickAsync(500);
  expect(resolved).false;

  // Flip back to visible.
  doc.visibilityState = 'visible';
  expect(resolved).false;

  // And wait a bit more.
  await clock.tickAsync(50_000);
  expect(resolved).false;
});

test('waitForHidden flip back and forth', async () => {
  const doc = new Document();
  doc.visibilityState = 'visible';

  let resolved = false;
  const p = waitForHidden(new LogContext(), doc, 1000).then(() => {
    resolved = true;
  });

  doc.visibilityState = 'hidden';
  expect(resolved).false;
  await clock.tickAsync(500);
  expect(resolved).false;

  // Flip back to visible.
  doc.visibilityState = 'visible';
  expect(resolved).false;
  await clock.tickAsync(500);
  expect(resolved).false;

  doc.visibilityState = 'hidden';
  await clock.tickAsync(500);
  expect(resolved).false;
  await clock.tickAsync(500);
  expect(resolved).true;

  await p;
});

test('waitForVisible no document', async () => {
  await waitForVisible(new LogContext(), undefined);
  // resolves "immediately"
});

test('waitForHidden no document', async () => {
  let resolved = false;
  void waitForHidden(new LogContext(), undefined, 1000).then(() => {
    resolved = true;
  });
  expect(resolved).false;
  await clock.tickAsync(1000);
  expect(resolved).false;

  await clock.tickAsync(100_000);
  expect(resolved).false;
});
