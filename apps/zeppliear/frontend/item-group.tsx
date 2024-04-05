import ArrowDropDown from '@mui/icons-material/ArrowDropDown';
import ArrowRight from '@mui/icons-material/ArrowRight';
import * as React from 'react';
import {useState} from 'react';

interface Props {
  title: string;
  children: React.ReactNode;
}
function ItemGroup({title, children}: Props) {
  const [itemsVisible, setItemsVisible] = useState(true);
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const Icon = itemsVisible ? ArrowDropDown : ArrowRight;
  return (
    <div className="flex flex-col w-full text-sm">
      <div
        className="px-2 relative w-full mt-0.5 h-7 flex items-center rounded cursor-pointer hover:bg-gray-850"
        onMouseDown={() => setItemsVisible(!itemsVisible)}
      >
        <Icon className="w-3 h-3 mr-2 -ml-1" />
        {title}
      </div>
      {itemsVisible && children}
    </div>
  );
}

export default ItemGroup;
