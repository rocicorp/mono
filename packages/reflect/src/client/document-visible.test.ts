import {expect} from '@esm-bundle/chai';
import * as sinon from 'sinon';
import {getDocumentVisibilityWatcher} from './document-visible.js';

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

  const {signal} = new AbortController();
  const w = getDocumentVisibilityWatcher(doc, 1_000, signal);
  const p = w.waitForVisible();
  doc.visibilityState = 'visible';
  await p;
});

test('waitForHidden', async () => {
  const doc = new Document();

  let resolved = false;
  const {signal} = new AbortController();
  const w = getDocumentVisibilityWatcher(doc, 1_000, signal);
  const p = w.waitForHidden().then(() => {
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
  const {signal} = new AbortController();
  const w = getDocumentVisibilityWatcher(doc, 1_000, signal);

  let resolved = false;
  void w.waitForHidden().then(() => {
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
  const {signal} = new AbortController();
  const w = getDocumentVisibilityWatcher(doc, 1_000, signal);

  let resolved = false;
  const p = w.waitForHidden().then(() => {
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
  const {signal} = new AbortController();
  const w = getDocumentVisibilityWatcher(undefined, 1_000, signal);

  await w.waitForVisible();
  // resolves "immediately"
});

test('waitForHidden no document', async () => {
  const {signal} = new AbortController();
  const w = getDocumentVisibilityWatcher(undefined, 1_000, signal);

  let resolved = false;
  void w.waitForHidden().then(() => {
    resolved = true;
  });
  expect(resolved).false;
  await clock.tickAsync(1000);
  expect(resolved).false;

  await clock.tickAsync(100_000);
  expect(resolved).false;
});

test('DocumentVisibleWatcher', async () => {
  const doc = new Document();

  const {signal} = new AbortController();
  const w = getDocumentVisibilityWatcher(doc, 1_000, signal);
  await w.waitForVisible();
  await w.waitForVisible();

  let resolved = false;
  const p = w.waitForHidden().then(() => {
    resolved = true;
  });
  expect(resolved).false;
  doc.visibilityState = 'hidden';
  expect(resolved).false;
  await clock.tickAsync(1_000 - 1);
  expect(resolved).false;
  await clock.tickAsync(1);
  expect(resolved).true;
  await p;

  await w.waitForHidden();
  await w.waitForHidden();

  {
    let resolved = false;
    const p = w.waitForVisible().then(() => {
      resolved = true;
    });
    expect(resolved).false;
    doc.visibilityState = 'visible';
    await clock.tickAsync(0);
    expect(resolved).true;
    await p;
  }
});

test('DocumentVisibleWatcher controller abort', async () => {
  const doc = new Document();
  doc.visibilityState = 'hidden';

  const controller = new AbortController();
  const w = getDocumentVisibilityWatcher(doc, 1_000, controller.signal);
  controller.abort();
  let resolved = false;
  void w.waitForVisible().then(() => {
    resolved = true;
  });
  doc.visibilityState = 'visible';
  await clock.tickAsync(0);
  expect(resolved).false;
  await clock.tickAsync(10_000);
  expect(resolved).false;
});
