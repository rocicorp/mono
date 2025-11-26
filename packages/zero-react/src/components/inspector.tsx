import type {CustomMutatorDefs} from '../../../zero-client/src/client/custom.ts';
import type {Zero} from '../../../zero-client/src/client/zero.ts';
import type {MutatorDefinitions} from '../../../zero-types/src/mutator-registry.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {QueryDefinitions} from '../../../zql/src/query/query-definitions.ts';
import {MarkIcon} from './mark-icon.tsx';

export default function Inspector<
  S extends Schema,
  MD extends
    | MutatorDefinitions<S, Context>
    | CustomMutatorDefs
    | undefined = undefined,
  Context = unknown,
  QD extends QueryDefinitions<S, Context> | undefined = undefined,
>({zero, onClose}: {zero: Zero<S, MD, Context, QD>; onClose: () => void}) {
  return (
    <dialog
      open
      style={{
        alignItems: 'center',
        backgroundColor: 'white',
        borderRadius: '8px 0 0 0',
        bottom: 0,
        boxShadow: '0 4px 8px rgba(0, 0, 0, 0.1)',
        color: 'black',
        display: 'flex',
        height: 'fit-content',
        marginRight: 0,
        opacity: 0.95,
        padding: '0.25em 0.5em',
        position: 'fixed',
        width: 'fit-content',
        zIndex: 1000,
      }}
    >
      <MarkIcon style={{margin: '0.5em'}} />
      <div>Zero v{zero.version}</div>
      <button onClick={onClose} style={{padding: '0.5em'}}>
        ✖︎
      </button>
    </dialog>
  );
}
