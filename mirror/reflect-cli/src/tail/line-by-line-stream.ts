/**
 * Converts a string stream into a stream of lines.
 */
export function lineByLineStream(): TransformStream<string, string> {
  let lineBuffer = '';
  return new TransformStream<string, string>({
    transform(chunk, controller) {
      for (;;) {
        const i = chunk.indexOf('\n');
        if (i === -1) {
          lineBuffer += chunk;
          break;
        } else {
          const line = lineBuffer + chunk.slice(0, i);
          chunk = chunk.slice(i + 1);
          lineBuffer = '';
          controller.enqueue(line);
        }
      }
    },
    flush(controller) {
      if (lineBuffer) {
        controller.enqueue(lineBuffer);
      }
    },
  });
}
