import {expect, test} from '@jest/globals';
import {lineByLineStream} from './line-by-line-stream.js';

function createReadableStreamFromText(
  ...chunks: string[]
): ReadableStream<string> {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(chunk);
      }
      controller.close();
    },
  });
}

async function expectStream<T>(stream: ReadableStream<T>, expected: T[]) {
  const actual = [];
  for await (const line of stream as unknown as AsyncIterable<T>) {
    actual.push(line);
  }
  expect(actual).toEqual(expected);
}

test('basics', async () => {
  const t = async (chunks: string[], expected: string[]) => {
    {
      const r = createReadableStreamFromText(...chunks);
      const r2 = r.pipeThrough(lineByLineStream());
      await expectStream(r2, expected);
    }
    {
      const r = createReadableStreamFromText(chunks.join(''));
      const r2 = r.pipeThrough(lineByLineStream());
      await expectStream(r2, expected);
    }
  };

  await t(['abc'], ['abc']);
  await t(['abc', 'def'], ['abcdef']);
  await t(['abc\n'], ['abc']);
  await t(['abc\n', 'def'], ['abc', 'def']);
  await t(['abc\n', 'def\n'], ['abc', 'def']);
  await t(['abc\ndef\n', 'ghi'], ['abc', 'def', 'ghi']);
  await t(['abc\ndef\n', 'ghi\n'], ['abc', 'def', 'ghi']);
  await t(['abc\ndef\n', 'ghi\n', 'jkl'], ['abc', 'def', 'ghi', 'jkl']);

  await t(['abc\n\ndef'], ['abc', '', 'def']);
  await t(['abc\n\ndef\n'], ['abc', '', 'def']);
  await t(['abc\n\n'], ['abc', '']);
  await t(['\nabc'], ['', 'abc']);
  await t(['\n', '\nabc'], ['', '', 'abc']);
  await t(['\n'], ['']);
  await t(['\n', '\n'], ['', '']);
  await t(['\n\n'], ['', '']);
});
