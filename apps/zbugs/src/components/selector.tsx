import React from 'react';

type SelectorProps = {
  isOpen: boolean;
  onClose: () => void;
};

const Selector: React.FC<SelectorProps> = ({isOpen, onClose}) => {
  return (
    <>
      {isOpen && (
        <div className="selector">
          <button className="selector-status status-open" onClick={onClose}>
            Open
          </button>
          <button className="selector-status status-closed" onClick={onClose}>
            Closed
          </button>
        </div>
      )}
    </>
  );
};

export default Selector;
