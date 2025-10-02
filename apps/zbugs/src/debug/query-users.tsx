import {useQuery} from '../../../../packages/zero-react/src/use-query.tsx';
import {queries} from '../../shared/queries.ts';

export function QueryUsers() {
  const q = queries.allUsers();
  const [users, details] = useQuery(q);
  if (details.type === 'unknown') {
    return <div>Loading...</div>;
  }
  if (details.type === 'error') {
    return (
      <div>
        <button onClick={details.refetch}>Retry</button>
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
