// This is based on the spec at
// https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation

export type EventSourceEntry = {
  data: string;
  event: string;
  lastEventId: string;
};

function stripFirstSpace(s: string): string {
  return s.startsWith(' ') ? s.slice(1) : s;
}

function stripLastSpace(s: string): string {
  return s.endsWith(' ') ? s.slice(0, -1) : s;
}

export function eventSourceStream(): TransformStream<string, EventSourceEntry> {
  let data = '';
  let eventType = '';
  let lastEventId = '';

  return new TransformStream<string, EventSourceEntry>({
    transform(line, controller) {
      function processField(name: string, value: string) {
        switch (name) {
          case 'event':
            eventType = value;
            break;
          case 'data':
            data += value + '\n';
            break;
          case 'id':
            if (!value.includes('\0')) {
              lastEventId = value;
            }
            break;
          case 'retry':
            // ignore for now
            break;
          default:
            console.warn('Unknown field', name);
            break;
        }
      }

      function dispatchEvent() {
        if (data === '') {
          // Ignore
          return;
        }

        if (data.endsWith('\n')) {
          data = data.slice(0, -1);
        }

        const event = {
          event: eventType || 'message',
          data: stripLastSpace(data),
          lastEventId,
        };

        eventType = '';
        data = '';
        controller.enqueue(event);
      }

      if (line === '') {
        dispatchEvent();
        return;
      }

      // Comment
      if (line.startsWith(':')) {
        return;
      }

      const i = line.indexOf(':');
      if (i !== -1) {
        const field = line.slice(0, i);
        const value = stripFirstSpace(line.slice(i + 1));
        processField(field, value);
      } else {
        processField(line, '');
      }
    },
  });
}
