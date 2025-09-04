import {useQuery} from '../../../../packages/zero-react/src/use-query.tsx';
import {queries} from '../../shared/queries.ts';

export function QueryUsers() {
  const [users, details] = useQuery(queries.allUsers());
  if (details.type === 'unknown') {
    return <div>Loading...</div>;
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
