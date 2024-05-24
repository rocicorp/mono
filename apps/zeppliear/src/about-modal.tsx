import classNames from 'classnames';
import Modal from './modal.jsx';

interface Props {
  isOpen: boolean;
  onDismiss?: () => void;
}

function Title({children}: {children: string}) {
  return <div className="text-lg font-normal text-white">{children}</div>;
}

function H1({children}: {children: string}) {
  return <div className="mt-5 text-lg font-normal text-white">{children}</div>;
}

function P({
  noTopMargin,
  children,
}: {
  noTopMargin?: boolean;
  children: React.ReactNode;
}) {
  return (
    <div
      className={classNames('text-sm font-normal text-gray-100', {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        'mt-3': !noTopMargin,
      })}
    >
      {children}
    </div>
  );
}

function Feature({title, children}: {title: string; children: string}) {
  return (
    <li className="ml-3 mt-3">
      <span className="font-semibold">{title}:</span> {children}
    </li>
  );
}

function A({href, children}: {href: string; children: string}) {
  return (
    <span className="text-blue">
      <a target="_blank" rel="noreferrer" href={href}>
        {children}
      </a>
    </span>
  );
}

export default function AboutModal({isOpen, onDismiss}: Props) {
  const handleClickCloseBtn = () => {
    if (onDismiss) onDismiss();
  };

  const body = (
    <div className="flex flex-col min-h-0">
      <div className="flex items-center justify-between flex-shrink-0 py-4 px-8 border-b border-gray-800">
        <Title>About This Demo</Title>
        <div
          className="flex items-center justify-center h-7 w-7 rounded hover:bg-gray-850"
          onMouseDown={handleClickCloseBtn}
        ></div>
      </div>
      <div className="flex flex-col flex-1 px-8 pt-4 pb-8 overflow-y-auto">
        <P noTopMargin>
          Zeppliear is a loving &hearts; tribute to the{' '}
          <A href="https://linear.app/">Linear issue tracker</A> built with{' '}
          <A href="https://zerosync.dev">Zero.</A>
        </P>

        <P>
          Zero is a new way to build web apps. Instead of fetching data with
          APIs, you sync your backend database continuously directly into the
          browser. This makes development dramatically easier – you can build
          features entirely client-side, against the entire database, without
          having to deploy new APIs first.
        </P>
        <P>
          But it also makes for far better user experiences: Zero stores up to
          100MB fo the most recently used data locally in the browser, making
          ~99% of user interactions <em>instantaneous</em>. When necessary, Zero
          automatically falls back to the server.
        </P>

        <H1>Key Features</H1>
        <ul
          /* tailwind doesnt have circle option built in */
          style={{
            listStyleType: 'circle',
          }}
          className="text-sm font-normal text-gray-100"
        >
          <Feature title="Dataset">
            1GB of data comprising over 200k issues and 2M comments.
          </Feature>
          <Feature title="Spinner-free">
            Everything in the UI responds instantly, without progress bars.
          </Feature>
          <Feature title="Realtime sync">
            Any change made by one user is seen ~instantly by others. Open in
            two windows to test. Even works across views!
          </Feature>
          <Feature title="Complex filters">
            Use the filter picker in the top nav to build complex filters.
          </Feature>
        </ul>

        <H1>Learn More</H1>
        <P>
          Learn more at <A href="https://zerosync.dev">zerosync.dev</A>, or come
          visit us in <A href="https://discord.replicache.dev">Discord</A>.
        </P>
      </div>
    </div>
  );

  return (
    <Modal
      isOpen={isOpen}
      center={true}
      size="wide"
      onDismiss={onDismiss}
      className="border border-gray-800"
    >
      {body}
    </Modal>
  );
}
