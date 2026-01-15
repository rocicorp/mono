import {useQuery} from '@rocicorp/zero/react';
import {queries} from '../../shared/queries.ts';

export function QueryUsers() {
  const [users, details] = useQuery(queries.allUsers());
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
