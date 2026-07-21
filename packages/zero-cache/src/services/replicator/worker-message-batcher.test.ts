import {expect, test, vi} from 'vitest';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {WorkerMessageBatcher} from './worker-message-batcher.ts';
import type {WriteWorkerClient} from './write-worker-client.ts';

const begin = [
  'begin',
  {tag: 'begin'},
  {commitWatermark: '02'},
] satisfies ChangeStreamData;
const row = [
  'data',
  {
    tag: 'insert',
    relation: {
      schema: 'public',
      name: 'issues',
      rowKey: {columns: ['id']},
    },
    new: {id: '1'},
  },
] satisfies ChangeStreamData;
const commit = [
  'commit',
  {tag: 'commit'},
  {watermark: '02'},
] satisfies ChangeStreamData;

test('batches until transaction boundary', async () => {
  const {worker, processMessages} = mockWorker();
  const batcher = new WorkerMessageBatcher(worker, 64);

  expect(batcher.push(begin)).toBeUndefined();
  expect(batcher.push(row)).toBeUndefined();
  expect(processMessages).not.toHaveBeenCalled();

  await batcher.push(commit);

  expect(processMessages).toHaveBeenCalledTimes(1);
  expect(processMessages).toHaveBeenCalledWith([begin, row, commit]);
  expect(batcher.size).toBe(0);
});

test('flushes large batches before commit', async () => {
  const {worker, processMessages} = mockWorker();
  const batcher = new WorkerMessageBatcher(worker, 2);

  expect(batcher.push(begin)).toBeUndefined();
  await batcher.push(row);

  expect(processMessages).toHaveBeenCalledTimes(1);
  expect(processMessages).toHaveBeenCalledWith([begin, row]);
  expect(batcher.size).toBe(0);
});

test('can defer commit flush until caller finishes a stream batch', async () => {
  const {worker, processMessages} = mockWorker();
  const batcher = new WorkerMessageBatcher(worker, 64, {
    flushOnCommit: false,
  });

  expect(batcher.push(begin)).toBeUndefined();
  expect(batcher.push(row)).toBeUndefined();
  expect(batcher.push(commit)).toBeUndefined();
  expect(processMessages).not.toHaveBeenCalled();
  expect(batcher.size).toBe(3);

  await batcher.flush();

  expect(processMessages).toHaveBeenCalledTimes(1);
  expect(processMessages).toHaveBeenCalledWith([begin, row, commit]);
  expect(batcher.size).toBe(0);
});

function mockWorker() {
  const processMessages = vi.fn().mockResolvedValue(null);
  const worker: WriteWorkerClient = {
    getSubscriptionState: vi.fn(),
    processMessage: vi.fn(),
    processMessages,
    abort: vi.fn(),
    stop: vi.fn(),
    onError: vi.fn(),
  };
  return {worker, processMessages};
}
