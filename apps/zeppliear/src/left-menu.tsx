import classnames from 'classnames';
import {noop} from 'lodash';
import {RefObject, useRef, useState} from 'react';
import AboutModal from './about-modal.jsx';
import AddIcon from './assets/icons/add.svg?react';
import HelpIcon from './assets/icons/help.svg?react';
import MenuIcon from './assets/icons/menu.svg?react';
import {getViewStatuses} from './filters.js';
import {
  useIssueDetailState,
  useStatusFilterState,
  useViewState,
} from './hooks/query-state-hooks.js';
import {useClickOutside} from './hooks/use-click-outside.js';
import useQueryState, {identityProcessor} from './hooks/useQueryState.js';
import IssueModal from './issue-modal.jsx';
import type {IssueCreationPartial} from './issue.js';
import ItemGroup from './item-group.js';
import SearchBox from './searchbox.js';

interface Props {
  // Show menu (for small screen only)
  menuVisible: boolean;
  onCloseMenu?: () => void;
  onCreateIssue: (i: IssueCreationPartial) => void;
}

function LeftMenu({menuVisible, onCloseMenu = noop, onCreateIssue}: Props) {
  const [, setView] = useViewState();
  const [, setIss] = useIssueDetailState();

  const [disableAbout] = useQueryState('disableAbout', identityProcessor);

  const ref = useRef<HTMLDivElement>() as RefObject<HTMLDivElement>;
  const [issueModalVisible, setIssueModalVisible] = useState(false);
  const [aboutModalVisible, setAboutModalVisible] = useState(false);
  const [, setStatusFilter] = useStatusFilterState();

  const classes = classnames(
    'absolute lg:static inset-0 lg:relative lg:translate-x-0 flex flex-col flex-shrink-0 w-56 font-sans text-sm border-r lg:shadow-none justify-items-start bg-gray border-gray-850 text-white bg-opacity-1',
    {
      /* eslint-disable @typescript-eslint/naming-convention */
      '-translate-x-full shadow-none': !menuVisible,
      'translate-x-0 shadow-x z-50': menuVisible,
      /* eslint-enable @typescript-eslint/naming-convention */
    },
  );

  useClickOutside(ref, () => {
    if (menuVisible && onCloseMenu) {
      onCloseMenu();
    }
  });

  return (
    <>
      <div className={classes} ref={ref}>
        <button
          className="flex-shrink-0 px-5 ml-2 lg:hidden h-14 focus:outline-none"
          onMouseDown={onCloseMenu}
        >
          <MenuIcon className="w-3.5 text-gray-50 hover:text-gray-100" />
        </button>

        {/* Top menu*/}
        <div className="flex flex-col flex-grow-0 flex-shrink-0 px-5 py-3">
          {/* Create issue btn */}
          <button
            className="inline-flex items-center px-2 py-2 mt-3 bg-gray-850  hover:bg-gray-900 border border-gray rounded focus:outline-none h-7"
            onMouseDown={() => {
              setIssueModalVisible(true);
              onCloseMenu && onCloseMenu();
            }}
          >
            <AddIcon className="mr-2.5 w-3.5 h-3.5" /> New Issue
          </button>
        </div>

        {/* Search box */}
        <div className="flex flex-col flex-shrink flex-grow overflow-y-auto mb-0.5 px-4">
          <SearchBox placeholder="Search" className="mt-5" />
          {/* actions */}

          <ItemGroup title="React Issues">
            <div
              className="flex items-center pl-9 rounded cursor-pointer group h-8 hover:bg-gray-900"
              onMouseDown={async () => {
                await Promise.all([
                  setView('all'),
                  setStatusFilter(null),
                  setIss(null),
                ]);
                onCloseMenu && onCloseMenu();
              }}
            >
              <span>All</span>
            </div>

            <div
              className="flex items-center pl-9 rounded cursor-pointer group h-8 hover:bg-gray-900"
              onMouseDown={async () => {
                await Promise.all([
                  setView('active'),
                  setStatusFilter(getViewStatuses('active')),
                  setIss(null),
                ]);
                onCloseMenu && onCloseMenu();
              }}
            >
              <span>Active</span>
            </div>

            <div
              className="flex items-center pl-9 rounded cursor-pointer group h-8 hover:bg-gray-900"
              onMouseDown={async () => {
                await Promise.all([
                  setView('backlog'),
                  setStatusFilter(getViewStatuses('backlog')),
                  setIss(null),
                ]);
                onCloseMenu && onCloseMenu();
              }}
            >
              <span>Backlog</span>
            </div>
            <div
              className="flex items-center pl-9 rounded cursor-pointer group h-8 hover:bg-gray-900"
              onMouseDown={async () => {
                await Promise.all([setView('board'), setIss(null)]);
                onCloseMenu && onCloseMenu();
              }}
            >
              <span>Board</span>
            </div>
          </ItemGroup>

          {/* extra space */}
          <div className="flex flex-col flex-grow flex-shrink" />

          {/* bottom group */}
          <div className="px-2 pb-2 text-gray-50 mt-7">
            <button
              className="inline-flex mt-1 focus:outline-none"
              onMouseDown={() => setAboutModalVisible(true)}
            >
              <HelpIcon className="w-3 mr-2 pt-1 h-4" /> About
            </button>
          </div>
        </div>
      </div>
      {/* Modals */}
      {
        <IssueModal
          isOpen={issueModalVisible}
          onDismiss={() => setIssueModalVisible(false)}
          onCreateIssue={onCreateIssue}
        />
      }
      {
        <AboutModal
          isOpen={aboutModalVisible && disableAbout !== 'true'}
          onDismiss={() => setAboutModalVisible(false)}
        />
      }
    </>
  );
}

export default LeftMenu;
