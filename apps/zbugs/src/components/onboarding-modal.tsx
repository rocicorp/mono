import {Modal} from './modal.tsx';
import {Link} from './link.tsx';
import {Button} from './button.tsx';

export function OnboardingModal({
  isOpen,
  onDismiss,
}: {
  isOpen: boolean;
  onDismiss: () => void;
}) {
  return (
    <Modal
      title=""
      isOpen={isOpen}
      onDismiss={onDismiss}
      className="onboarding-modal"
    >
      <p className="opening-text">
        Welcome. This is the bug tracker for the{' '}
        <Link href="https://zero.rocicorp.dev">Zero project</Link>. It is also a
        live production Zero-based product. You can click around and get a feel
        for the performance that Zero provides.
      </p>
      <p>Some things to try:</p>

      <h2>Create an issue</h2>
      <p>Click on New Issue in the sidebar to open a new issue.</p>
      <p className="aside">
        Please note that this is our place of work so we'd appreciate if you add
        test content, to just delete afterwards.
      </p>

      <h2>Quickly navigate between issues</h2>
      <p>
        Use the <span className="keyboard-keys">J</span> &amp;{' '}
        <span className="keyboard-keys">K</span> keys. (Try holding them down
        🏎️💨)
      </p>

      <h2>Search &amp; filter</h2>
      <p>
        In the list view, press <span className="keyboard-keys">/</span> search
        or use the filter picker.
      </p>

      <h2>Test live syncing</h2>
      <p>Open two windows and watch changes sync live.</p>

      <Button
        className="onboarding-modal-accept"
        eventName="Onboarding modal accept"
        onAction={onDismiss}
      >
        Let's go
      </Button>
    </Modal>
  );
}
