import React, {useState, useEffect, useRef} from 'react';

const Filter: React.FC = () => {
  const [isOpen, setIsOpen] = useState(false);
  const modalRef = useRef<HTMLDivElement>(null);

  const toggleDropdown = () => {
    setIsOpen(!isOpen);
  };

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (
        modalRef.current &&
        !modalRef.current.contains(event.target as Node)
      ) {
        setIsOpen(false); // Close the modal
      }
    };

    document.addEventListener('mousedown', handleClickOutside);

    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, []);

  return (
    <div className="add-filter-container" ref={modalRef}>
      <button className="add-filter" onClick={toggleDropdown}>
        <span className="plus">+</span> Filter
      </button>

      {isOpen && (
        <div className="add-filter-modal">
          <div className="filter-modal-item">
            <p className="filter-modal-label">Creator</p>
            <button
              onClick={toggleDropdown}
              className="creator-select filter-modal-button button-dropdown"
            >
              Select
            </button>
          </div>
          <div className="filter-modal-item">
            <p className="filter-modal-label">Label</p>
            <button
              onClick={toggleDropdown}
              className="label-select filter-modal-button button-dropdown"
            >
              Select
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

export default Filter;
