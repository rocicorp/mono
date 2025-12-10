import type {
  CustomMutatorDefs,
  DefaultContext,
  DefaultSchema,
  Schema,
  Zero,
} from '../../../zero/src/zero.ts';
import {MarkIcon} from './mark-icon.tsx';

export default function Inspector<
  S extends Schema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  Context = DefaultContext,
>({zero, onClose}: {zero: Zero<S, MD, Context>; onClose: () => void}) {
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
