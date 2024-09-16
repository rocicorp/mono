import {useZero} from 'zero-react/src/use-zero.js';
import {useQuery} from 'zero-react/src/use-query.js';
import {Schema} from '../../domain/schema.js';
import {useSearch} from 'wouter';
import {Link} from '../../components/link.js';

export default function ListPage() {
  const z = useZero<Schema>();

  const qs = new URLSearchParams(useSearch());
  const open = qs.get('open');
  const creator = qs.get('creator');

  let q = z.query.issue
    .orderBy('modified', 'desc')
    .limit(100)
    .related('labels');

  if (open !== null) {
    q = q.where('open', open === 'true');
  }

  if (creator !== null) {
    q = q.where('creatorID', creator);
  }

  const issues = useQuery(q, [open, creator]);
  const creators = useQuery(z.query.user);

  const addParam = (key: string, value: string) => {
    const newParams = new URLSearchParams(qs);
    newParams.set(key, value);
    return '?' + newParams.toString();
  };

  return (
    <>
      <div>
        <span className="mr-2">Creator:</span>
        {Array.from(creators.values()).map(creator => (
          <Link
            key={creator.id}
            href={addParam('creator', creator.id)}
            className="mr-2"
          >
            {creator.name}
          </Link>
        ))}
      </div>
      <table style={{width: '100%'}}>
        <thead>
          <tr>
            <th style={{width: '75%'}}>Issue</th>
            <th style={{width: '25%'}}>Labels</th>
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
