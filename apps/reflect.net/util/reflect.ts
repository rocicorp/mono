import type {MutatorDefs, ReadonlyJSONValue} from '@rocicorp/reflect';
import type {Reflect} from '@rocicorp/reflect/client';

// TODO(reflect): improve this inside Reflect.
export function closeReflect<M extends MutatorDefs>(r: Reflect<M>) {
  // Need to give any outstanding operations time to complete or else get IDB closing error.
  window.setTimeout(() => r.close(), 1000);
}

// TODO(reflect): we probably want something like this built in!
// Note: the callback receives a shared (mutable) map. With react, you will frequently
// want to clone this, ie for use with setState. We don't clone it because often you want
// some transform in addition to the clone. Do not modify the value you receive or
// terrible things will happen (✖╭╮✖).
export function watch<M extends MutatorDefs>(
  r: Reflect<M>,
  {
    prefix,
    ops,
  }: {
    prefix: string;
    ops: ('add' | 'change' | 'del')[];
  },
  callback: (result: Readonly<Map<string, ReadonlyJSONValue>>) => void,
) {
  const result = new Map<string, ReadonlyJSONValue>();
  return r.experimentalWatch(
    diff => {
      let changed = false;
      for (const change of diff) {
        if (change.op === 'add' && ops.includes('add')) {
          result.set(change.key.substring(prefix.length), change.newValue);
          changed = true;
        } else if (change.op === 'change' && ops.includes('change')) {
          result.set(change.key.substring(prefix.length), change.newValue);
          changed = true;
        } else if (change.op === 'del' && ops.includes('del')) {
          result.delete(change.key.substring(prefix.length));
          changed = true;
        }
      }
      if (changed) {
        callback(result);
      }
    },
    {prefix, initialValuesInFirstDiff: true},
  );
}
