import {useZero} from 'zero-react/src/use-zero.js';
import {useQuery} from 'zero-react/src/use-query.js';
import {Schema} from '../../domain/schema.js';
import {useState} from 'react';

export default function ListPage() {
  const z = useZero<Schema>();

  const [open, setOpen] = useState(false);

  const issues = useQuery(
    z.query.issue
      .where('open', open)
      .orderBy('modified', 'desc')
      .limit(100)
      .related('labels'),
    [open],
  );

  return (
    <>
      <div>
        <button onMouseDown={() => setOpen(true)}>Open Issuees</button>
        <button onMouseDown={() => setOpen(false)}>Closed Issuees</button>
      </div>
      <table>
        <thead>
          <tr>
            <th>Issue</th>
            <th>Labels</th>
          </tr>
        </thead>
        <tbody>
          {issues.map(issue => (
            <tr key={issue.id}>
              <td align="left">{issue.title}</td>
              <td align="left">
                {issue.labels.map(label => label.name).join(', ')}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}
