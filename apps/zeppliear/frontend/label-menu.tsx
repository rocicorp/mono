import {useState} from 'react';
import type {Collections} from './app';
import {useZero} from './hooks/useZero';
import {useQuery} from './hooks/useZql';
import {LabelTypeahead} from './label-typeahead';
import type {M} from './mutators';

export function LabelMenu() {
  const [labelFilter, setLabelFilter] = useState<string | undefined>();
  return (
    <>
      <LabelTypeahead filter={labelFilter} onFilterChange={setLabelFilter} />
      <LabelsComponent filter={labelFilter} />
    </>
  );
}

const colors = [
  '#b0b0b0', // Gray
  '#ff6a6a', // Red
  '#ffd633', // Yellow
  '#7bea7b', // Green
  '#6aa9ff', // Blue
  '#7c7cff', // Indigo
  '#c978d8', // Purple
  '#ff85ad', // Pink
];

function getColor(labelName: string) {
  const charCode = labelName.charCodeAt(2) || labelName.charCodeAt(0);
  return colors[charCode % colors.length];
}

function LabelsComponent({filter}: {filter?: string | undefined}) {
  // onMouseDown={() => {
  //   // onSelectStatus(status as Status);
  //   setFilter(null);
  //   setFilterDropDownVisible(false);
  // }}
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
        >
          <LabelDot color={getColor(label.name)} />{' '}
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
