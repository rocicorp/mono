import {expect, test} from 'vitest';
import {guardNodeExits} from './cluster.ts';

test('returns the guarded operation result', async () => {
  const neverExits = new Promise<never>(() => {});

  await expect(
    guardNodeExits(Promise.resolve('done'), [{unexpectedExit: neverExits}]),
  ).resolves.toBe('done');
});

test('fails an in-flight operation when a node exits unexpectedly', async () => {
  let rejectExit: (reason: Error) => void = () => {};
  const unexpectedExit = new Promise<never>((_resolve, reject) => {
    rejectExit = reject;
  });
  const guarded = guardNodeExits(new Promise<never>(() => {}), [
    {unexpectedExit},
  ]);

  rejectExit(new Error('rm exited unexpectedly (code=14 signal=null)'));

  await expect(guarded).rejects.toThrow('rm exited unexpectedly');
});
