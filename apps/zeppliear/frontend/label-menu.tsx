import {useState} from 'react';
import type {Collections} from './app';
import {useZero} from './hooks/useZero';
import {useQuery} from './hooks/useZql';
import {getLabelColor} from './issue';
import {LabelTypeahead} from './label-typeahead';
import type {M} from './mutators';

export function LabelMenu({
  onSelectLabel,
}: {
  onSelectLabel: (label: string) => void;
}) {
  const [labelFilter, setLabelFilter] = useState<string | undefined>();
  return (
    <>
      <LabelTypeahead filter={labelFilter} onFilterChange={setLabelFilter} />
      <LabelsComponent filter={labelFilter} onSelectLabel={onSelectLabel} />
    </>
  );
}

function LabelsComponent({
  filter,
  onSelectLabel,
}: {
  filter?: string | undefined;
  onSelectLabel: (label: string) => void;
}) {
  const zero = useZero<M, Collections>();
  const query = filter
    ? zero.query.label.where('name', 'ILIKE', `%${filter}%`)
    : zero.query.label;
  const labels = useQuery(query.select('name').limit(10), [filter]);
  return (
    <>
      {labels.map((label, idx) => (
        <div
          key={idx}
          className="text-xs flex items-center h-8 px-3 text-gray focus:outline-none hover:text-gray-800 hover:bg-gray-300"
          onMouseDown={() => onSelectLabel(label.name)}
        >
          <LabelDot color={getLabelColor(label.name)} />{' '}
          <span className="truncate">{label.name}</span>
        </div>
      ))}
    </>
  );
}

function LabelDot({color}: {color: string}) {
  return (
    <span style={{color}} className="pr-2">
      ⬤
    </span>
  );
}
