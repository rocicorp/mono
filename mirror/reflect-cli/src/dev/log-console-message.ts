import type {Protocol} from 'devtools-protocol';
import {default as color, default as picocolors} from 'picocolors';
import type {Formatter} from 'picocolors/types.js';

// Original source from https://github.com/cloudflare/workers-sdk/blob/6de3c5eced6f31a2a55f4c043e1025f4f4733ad0/packages/wrangler/src/inspect.ts

type RemoteObject = Protocol.Runtime.RemoteObject;
type PropertyPreview = Protocol.Runtime.PropertyPreview;
type ConsoleAPICalledEvent = Protocol.Runtime.ConsoleAPICalledEvent;
type Protocol = Protocol.Runtime.ConsoleAPICalledEvent;
type Type = RemoteObject['type'] | PropertyPreview['type'];
type SubType = RemoteObject['subtype'] | PropertyPreview['subtype'];

/**
 * This function converts a message serialised as a devtools event
 * into arguments suitable to be called by a console method, and
 * then actually calls the method with those arguments. Effectively,
 * we're just doing a little bit of the work of the devtools console,
 * directly in the terminal.
 */

const mapConsoleAPIMessageTypeToConsoleMethod: {
  [key in ConsoleAPICalledEvent['type']]: Exclude<keyof Console, 'Console'>;
} = {
  log: 'log',
  debug: 'debug',
  info: 'info',
  warning: 'warn',
  error: 'error',
  dir: 'dir',
  dirxml: 'dirxml',
  table: 'table',
  trace: 'trace',
  clear: 'clear',
  count: 'count',
  assert: 'assert',
  profile: 'profile',
  profileEnd: 'profileEnd',
  timeEnd: 'timeEnd',
  startGroup: 'group',
  startGroupCollapsed: 'groupCollapsed',
  endGroup: 'groupEnd',
};

const typeToFormatter: Record<string, Formatter | undefined> = {
  number: picocolors.yellow,
  bigint: picocolors.yellow,
  boolean: picocolors.yellow,
  symbol: picocolors.green,
  undefined: picocolors.gray,
  regexp: picocolors.red,
  null: picocolors.bold,
  date: picocolors.magenta,
};

function formatValue(o: {
  description?: string | undefined;
  value?: string | undefined;
  type: Type;
  subtype?: SubType;
}): string {
  const type = o.subtype ?? o.type;
  const s =
    type === 'undefined' ? type : o.description ?? o.value ?? '<unknown>';
  const f = typeToFormatter[type];
  return f ? f(s) : s;
}

export function logConsoleMessage(evt: ConsoleAPICalledEvent): void {
  const args: string[] = [];
  for (const ro of evt.args) {
    type: switch (ro.type) {
      case 'string':
        args.push(ro.value);
        break;
      case 'undefined':
        args.push(formatValue(ro));
        break;
      case 'number':
      case 'boolean':
      case 'symbol':
      case 'bigint':
        args.push(formatValue(ro));
        break;
      case 'function':
        args.push(
          color.cyan(`[Function: ${ro.description ?? '<no-description>'}]`),
        );
        break;
      case 'object':
        switch (ro.subtype) {
          case 'regexp':
          case 'date':
          case 'null':
            args.push(formatValue(ro));
            break type;
        }
        if (!ro.preview) {
          args.push(ro.description ?? '<no-description>');
        } else {
          args.push(ro.preview.description ?? '<no-description>');

          switch (ro.preview.subtype) {
            case 'array':
              args.push(
                '[ ' +
                  ro.preview.properties.map(p => formatValue(p)).join(', ') +
                  (ro.preview.overflow ? '...' : '') +
                  ' ]',
              );

              break;
            case 'weakmap':
            case 'map':
              ro.preview.entries === undefined
                ? args.push('{}')
                : args.push(
                    '{\n' +
                      ro.preview.entries
                        .map(
                          ({key, value}) =>
                            // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
                            `  ${formatValue(key!)} => ${formatValue(value)}`,
                        )
                        .join(',\n') +
                      (ro.preview.overflow ? '\n  ...' : '') +
                      '\n}',
                  );

              break;
            case 'weakset':
            case 'set':
              ro.preview.entries === undefined
                ? args.push('{}')
                : args.push(
                    '{ ' +
                      ro.preview.entries
                        .map(({value}) => formatValue(value))
                        .join(', ') +
                      (ro.preview.overflow ? ', ...' : '') +
                      ' }',
                  );
              break;
            case 'regexp':
              break;
            case 'date':
              args.push(formatValue(ro));
              break;
            case 'generator':
              args.push(ro.preview.properties[0].value || '');
              break;
            case 'promise':
              if (ro.preview.properties[0].value === 'pending') {
                args.push(`{<${ro.preview.properties[0].value}>}`);
              } else {
                args.push(
                  `{<${ro.preview.properties[0].value}>: ${formatValue(
                    ro.preview.properties[1],
                  )}}`,
                );
              }
              break;
            case 'node':
            case 'iterator':
            case 'proxy':
            case 'typedarray':
            case 'arraybuffer':
            case 'dataview':
            case 'webassemblymemory':
            case 'wasmvalue':
              break;
            case 'error':
            default:
              // just a pojo
              args.push(
                '{\n' +
                  ro.preview.properties
                    .map(p => `  ${p.name}: ${formatValue(p)}`)
                    .join(',\n') +
                  (ro.preview.overflow ? '\n  ...' : '') +
                  '\n}',
              );
          }
        }
        break;
      default:
        args.push(ro.description || ro.unserializableValue || '🦋');
        break;
    }
  }

  const method = mapConsoleAPIMessageTypeToConsoleMethod[evt.type];

  if (method in console) {
    switch (method) {
      case 'dir':
        console.dir(args);
        break;
      case 'table':
        console.table(args);
        break;
      default:
        // @ts-expect-error TS wants a tuple, but we have an array
        console[method](...args);
        break;
    }
  } else {
    console.warn(`Unsupported console method: ${method}`);
    console.warn('console event:', evt);
  }
}
