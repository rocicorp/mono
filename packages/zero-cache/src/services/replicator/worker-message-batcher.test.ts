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
  const worker = mockWorker();
  const batcher = new WorkerMessageBatcher(worker, 64);

  expect(batcher.push(begin)).toBeUndefined();
  expect(batcher.push(row)).toBeUndefined();
  expect(worker.processMessages).not.toHaveBeenCalled();

  await batcher.push(commit);

  expect(worker.processMessages).toHaveBeenCalledTimes(1);
  expect(worker.processMessages).toHaveBeenCalledWith([begin, row, commit]);
  expect(batcher.size).toBe(0);
});

test('flushes large batches before commit', async () => {
  const worker = mockWorker();
  const batcher = new WorkerMessageBatcher(worker, 2);

  expect(batcher.push(begin)).toBeUndefined();
  await batcher.push(row);

  expect(worker.processMessages).toHaveBeenCalledTimes(1);
  expect(worker.processMessages).toHaveBeenCalledWith([begin, row]);
  expect(batcher.size).toBe(0);
});

function mockWorker(): WriteWorkerClient {
  return {
    getSubscriptionState: vi.fn(),
    processMessage: vi.fn(),
    processMessages: vi.fn().mockResolvedValue(null),
    abort: vi.fn(),
    stop: vi.fn(),
    onError: vi.fn(),
  };
}
