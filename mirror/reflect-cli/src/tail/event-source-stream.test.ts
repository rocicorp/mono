import {expect, test} from '@jest/globals';
import {EventSourceEntry, eventSourceStream} from './event-source-stream.js';

function createLineByLineStream(lines: string): ReadableStream<string> {
  return new ReadableStream({
    start(controller) {
      for (const line of lines.split('\n')) {
        controller.enqueue(line);
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
  const t = async (data: string, expected: EventSourceEntry[]) => {
    {
      const r = createLineByLineStream(data);
      const r2 = r.pipeThrough(eventSourceStream());
      await expectStream(r2, expected);
    }
  };

  await t(``, []);
  await t(`data:abc\n\n`, [{event: 'message', data: 'abc', lastEventId: ''}]);
  await t(`data:abc\n\n\n`, [{event: 'message', data: 'abc', lastEventId: ''}]);
  await t(`data:abc\nevent: foo\n\n`, [
    {event: 'foo', data: 'abc', lastEventId: ''},
  ]);
  await t(`data:abc\nevent: foo\ndata:def\n\n`, [
    {event: 'foo', data: 'abc\ndef', lastEventId: ''},
  ]);

  await t(
    `data: XYZ
data: +2
data: 10
`,
    [{event: 'message', data: 'XYZ\n+2\n10', lastEventId: ''}],
  );

  await t(
    `: test stream

data: first event
id: 1

data:second event
id

data:  third event
`,
    [
      {event: 'message', data: 'first event', lastEventId: '1'},
      {event: 'message', data: 'second event', lastEventId: ''},
      {event: 'message', data: ' third event', lastEventId: ''},
    ],
  );

  await t(
    `
data:test

data: test
`,
    [
      {event: 'message', data: 'test', lastEventId: ''},
      {event: 'message', data: 'test', lastEventId: ''},
    ],
  );
});
