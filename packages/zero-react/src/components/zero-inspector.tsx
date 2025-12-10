import {lazy, Suspense, useState} from 'react';
import type {
  CustomMutatorDefs,
  DefaultContext,
  DefaultSchema,
  Schema,
  Zero,
} from '../../../zero/src/zero.ts';
import {MarkIcon} from './mark-icon.tsx';

const Inspector = lazy(() => import('./inspector.tsx'));

export function ZeroInspector<
  S extends Schema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  Context = DefaultContext,
>({zero}: {zero: Zero<S, MD, Context>}): JSX.Element {
  const [show, setShow] = useState(false);
  return show ? (
    <Suspense fallback={<div>Loading Inspector...</div>}>
      <Inspector
        // oxlint-disable-next-line @typescript-eslint/no-explicit-any
        zero={zero as any}
        onClose={() => setShow(false)}
      />
    </Suspense>
  ) : (
    <button
      onClick={() => setShow(!show)}
      style={{
        position: 'fixed',
        bottom: 0,
        right: 0,
        zIndex: 1000,
        padding: '5px',
        color: 'white',
        backgroundColor: '#333',
        borderTopLeftRadius: '8px',
        opacity: 0.95,
      }}
    >
      <MarkIcon
        style={{
          width: '20px',
          height: '20px',
          fill: 'currentColor',
        }}
      />
    </button>
  );
}
