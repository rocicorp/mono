import {Suspense} from 'react';

export function MaybeSuspend({
  children,
  enabled,
  onReveal,
}: {
  children: React.ReactNode;
  enabled: boolean;
  onReveal?: (() => void) | undefined;
}) {
  if (enabled) {
    return (
      <Suspense>
        {children}
        <Reveal on={onReveal} />
      </Suspense>
    );
  }
  return <>{children}</>;
}

function Reveal({on}: {on?: (() => void) | undefined}) {
  on?.();
  return null;
}
