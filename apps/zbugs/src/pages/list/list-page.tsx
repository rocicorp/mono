import {useQuery} from '@rocicorp/zero/react';
import classNames from 'classnames';
import {type CSSProperties, useRef} from 'react';
import {FixedSizeList as List} from 'react-window';
import {useSearch} from 'wouter';
import {navigate} from 'wouter/use-browser-location';
import Filter, {type Selection} from '../../components/filter.js';
import {Link} from '../../components/link.js';
import {useElementSize} from '../../hooks/use-element-size.js';
import {useZero} from '../../hooks/use-zero.js';
import {mark} from '../../perf-log.js';

let firstRowRendered = false;
export default function ListPage() {
  const z = useZero();

  const qs = new URLSearchParams(useSearch());
  const status = qs.get('status');
  const creator = qs.get('creator');
  const assignee = qs.get('assignee');
  const labels = qs.getAll('label');

  // TODO: this can go away once we have filter-by-subquery, you should be able
  // to filter by label.name directly.
  const creatorID = useQuery(
    z.query.user.where('login', creator ?? '').one(),
    creator !== null,
  )?.id;
  const assigneeID = useQuery(
    z.query.user.where('login', assignee ?? '').one(),
    assignee !== null,
  )?.id;
  const labelIDs = useQuery(z.query.label.where('name', 'IN', labels));

  let q = z.query.issue.orderBy('modified', 'desc').related('labels');

  if (status === null) {
    q = q.where('open', true);
  } else if (status === 'closed') {
    q = q.where('open', false);
  }

  if (creatorID) {
    q = q.where('creatorID', creatorID);
  }

  if (assigneeID) {
    q = q.where('assigneeID', assigneeID);
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
    } else if ('assignee' in selection) {
      navigate(addFilter('assignee', selection.assignee, 'exclusive'));
    } else {
      navigate(addFilter('label', selection.label));
    }
  };

  const Row = ({index, style}: {index: number; style: CSSProperties}) => {
    const issue = issues[index];
    if (firstRowRendered === false) {
      mark('first issue row rendered');
      firstRowRendered = true;
    }
    return (
      <div
        key={issue.id}
        className="row"
        style={{
          ...style,
        }}
      >
        <Link
          className={classNames('issue-title', {
            'issue-closed': !issue.open,
          })}
          title={issue.title}
          href={`/issue/${issue.id}`}
        >
          {issue.title}
        </Link>
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
      </div>
    );
  };

  const tableWrapperRef = useRef<HTMLDivElement>(null);
  const size = useElementSize(tableWrapperRef.current);

  return (
    <>
      <div className="list-view-header-container">
        <h1 className="list-view-header">
          {/* Need to make this dynamic */}
          Open Issues
          <span className="issue-count">{issues.length}</span>
        </h1>
      </div>
      <div className="list-view-filter-container">
        <span className="filter-label">Filtered by:</span>
        {[...qs.entries()].map(([key, val], idx) => {
          if (key === 'label' || key === 'creator' || key === 'assignee') {
            return (
              <span
                className={classNames('pill', {
                  label: key === 'label',
                  user: key === 'creator' || key === 'assignee',
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

      <div className="issue-list" ref={tableWrapperRef}>
        {size && (
          <List
            className="virtual-list"
            width={size.width}
            height={size.height}
            itemSize={56}
            itemCount={issues.length}
          >
            {Row}
          </List>
        )}
      </div>
    </>
  );
}
