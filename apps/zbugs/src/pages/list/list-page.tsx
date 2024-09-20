import {useQuery} from 'zero-react/src/use-query.js';
import {useZero} from '../../domain/schema.js';
import {useSearch} from 'wouter';
import {Link} from '../../components/link.js';

export default function ListPage() {
  const z = useZero();

  const qs = new URLSearchParams(useSearch());
  const open = qs.get('open');
  const creator = qs.get('creator');
  const label = qs.get('label');

  // TODO: one should be in schema
  const labelID = useQuery(z.query.label.where('name', label ?? '').one())?.id;

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

  if (labelID !== undefined) {
    q = q.where('labelIDs', 'LIKE', `%${labelID}%`);
  }

  const issues = useQuery(q);
  const creators = useQuery(z.query.user);
  const labels = useQuery(z.query.label);

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
      <div>
        <span className="mr-2">Label:</span>
        {Array.from(labels.values()).map(label => (
          <Link
            key={label.id}
            href={addParam('label', label.name)}
            className="mr-2"
          >
            {label.name}
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
              <td align="left" className="issue-title">
                <Link href={`/issue/${issue.id}`}>{issue.title}</Link>
              </td>
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
