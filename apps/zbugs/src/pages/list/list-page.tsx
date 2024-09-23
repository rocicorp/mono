import {useQuery} from 'zero-react/src/use-query.js';
import {useZero} from '../../domain/schema.js';
import {useSearch} from 'wouter';
import {Link} from '../../components/link.js';
import Filter, {Selection} from '../../components/filter.js';
import {navigate} from 'wouter/use-browser-location';
import classNames from 'classnames';

export default function ListPage() {
  const z = useZero();

  const qs = new URLSearchParams(useSearch());
  const open = qs.get('open');
  const creator = qs.get('creator');
  const labels = qs.getAll('label');

  // TODO: this can go away once we have filter-by-subquery, you should be able
  // to filter by label.name directly.
  const creatorID = useQuery(
    z.query.user.where('login', creator ?? '').one(),
    creator !== null,
  )?.id;
  const labelIDs = useQuery(z.query.label.where('name', 'IN', labels));

  // TODO: Implement infinite scroll
  let q = z.query.issue
    .orderBy('modified', 'desc')
    .limit(100)
    .related('labels');

  if (open !== null) {
    q = q.where('open', open === 'true');
  }

  if (creatorID) {
    q = q.where('creatorID', creatorID);
  }

  for (const labelID of labelIDs) {
    q = q.where('labelIDs', 'LIKE', `%${labelID.id}%`);
  }

  const issues = useQuery(q);

  const addFilter = (
    key: string,
    value: string,
    mode?: 'exclusive' | undefined,
  ) => {
    const newParams = new URLSearchParams(qs);
    newParams[mode === 'exclusive' ? 'set' : 'append'](key, value);
    return '?' + newParams.toString();
  };

  const onDeleteFilter = (index: number) => {
    const entries = [...new URLSearchParams(qs).entries()];
    entries.splice(index, 1);
    navigate('?' + new URLSearchParams(entries).toString());
  };

  const onFilter = (selection: Selection) => {
    if ('creator' in selection) {
      navigate(addFilter('creator', selection.creator, 'exclusive'));
    } else {
      navigate(addFilter('label', selection.label));
    }
  };

  return (
    <>
      <div className="list-view-header-container">
        <h1 className="list-view-header">
          Open Issues
          <span className="issue-count">154</span>
        </h1>
      </div>
      <div className="list-view-filter-container">
        <span className="filter-label">Filtered by:</span>
        {[...qs.entries()].map(([key, val], idx) => {
          if (key === 'label' || key === 'creator') {
            return (
              <span
                className={classNames('pill', {
                  label: key === 'label',
                  user: key === 'creator',
                })}
                onMouseDown={() => onDeleteFilter(idx)}
                key={idx}
              >
                {key}: {val}
              </span>
            );
          }
          return null;
        })}
        <Filter onSelect={onFilter} />
      </div>

      <table style={{width: '100%'}}>
        <thead>
          <tr className="header-row">
            <th className="issue-column">Items</th>
            <th className="label-column">Labels</th>
          </tr>
        </thead>
        <tbody>
          {issues.map(issue => (
            <tr key={issue.id}>
              <td
                title={issue.title}
                align="left"
                className={`issue-title ${
                  issue.open ? 'issue-open' : 'issue-closed'
                }`}
              >
                <Link href={`/issue/${issue.id}`}>{issue.title}</Link>
              </td>
              <td align="right">
                <div className="issue-taglist">
                  {issue.labels.map(label => (
                    <Link
                      key={label.id}
                      className="pill label"
                      href={`/?label=${label.name}`}
                    >
                      {label.name}
                    </Link>
                  ))}
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}
