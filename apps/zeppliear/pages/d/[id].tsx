import {useEffect, useRef, useState} from 'react';
import {Zero} from 'zero-client';
import {M, mutators} from '../../frontend/mutators';
import App from '../../frontend/app';
import {UndoManager} from '@rocicorp/undo';

export default function Home() {
  const [zero, setZero] = useState<Zero<M> | null>(null);
  const undoManagerRef = useRef(new UndoManager());
  useEffect(() => {
    // disabled eslint await requirement
    // eslint-disable-next-line
    (async () => {
      if (zero) {
        return;
      }

      const [, , spaceID] = location.pathname.split('/');
      const z = new Zero({
        server: process.env.NEXT_PUBLIC_SERVER,
        userID: 'anon',
        roomID: spaceID,
        mutators,
        kvStore: 'idb',
      });

      setZero(z);
    })();
  }, [zero]);

  if (!zero) {
    return null;
  }
  return (
    <div className="repliear">
      <App zero={zero} undoManager={undoManagerRef.current} />
    </div>
  );
}
