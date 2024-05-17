/**
 * Lets the user search for labels that may not be present in the dropdown
 */
import styles from './label-typeahead.module.css';

export function LabelTypeahead({
  filter,
  onFilterChange,
}: {
  filter?: string | undefined;
  onFilterChange: (filter: string) => void;
}) {
  return (
    <div className="border-b border-gray-300">
      <input
        autoFocus
        placeholder="Labels"
        type="search"
        value={filter}
        onChange={e => {
          onFilterChange(e.target.value);
        }}
        ref={e => e?.focus()}
        className={styles.input}
      />
    </div>
  );
}
