import {useQuery} from '../../../../packages/zero-react/src/use-query.tsx';
import {useZero} from '../hooks/use-zero.ts';

export function QueryUsers() {
  const z = useZero();
  const [users, details] = useQuery(z.query.allUsers());
  if (details.type === 'unknown') {
    return <div>Loading...</div>;
  }
  if (details.type === 'error') {
    return (
      <div>
        <button onClick={details.retry}>Retry</button>
        Error: {JSON.stringify(details.error?.details ?? null)}
      </div>
    );
  }
  return (
    <div>
      Query Users Component
      <ul>
        {users?.map(u => (
          <li key={u.id}>
            {u.name} ({u.login})
          </li>
        ))}
      </ul>
    </div>
  );
}
