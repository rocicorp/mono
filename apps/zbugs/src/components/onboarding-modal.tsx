import {Modal} from './modal.tsx';
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
        Welcome to <strong>Gigabugs</strong>, a demo bug tracker built with{' '}
        <strong>Zero</strong>.
      </p>
      <p>
        It’s populated with <strong>240 thousand issues</strong> and{' '}
        <strong>2.5 million rows</strong> so that you can see how fast Zero is,
        even as the dataset grows.
      </p>
      <p>Things to try:</p>
      <h2>Clear cache and reload</h2>
      <p>
        Gigabugs loads fast, even from a cold cache. Zero’s query-driven sync
        puts you in complete control of what gets synced, and when.
      </p>
      <h2>Instant reads</h2>
      <p>
        Tap on anything. Choose any filter. Most interactions respond instantly.
        Zero queries run locally first, and fallback to the server if necessary.
      </p>
      <h2>Infinite scroll</h2>
      <p>
        Perfectly buttery infinite scroll. Because it’s fun! (Or open and issue
        and hold down <span className="keyboard-keys">J</span> /{' '}
        <span className="keyboard-keys">K</span> 🏎️💨)
      </p>
      <h2>Instant writes</h2>
      <p>
        Create an issue or comment on an existing one (don’t worry about making
        a mess, it’s test data). All mutations in Zero are client-first
        automatically.
      </p>
      <h2>Live sync</h2>
      <p>Open two windows and watch changes sync between them.</p>

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
