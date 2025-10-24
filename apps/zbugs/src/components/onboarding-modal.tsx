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
      <p>Zero's query-driven sync enables fast loads even from a cold cache.</p>
      <h2>Instant interactions</h2>
      <p>
        Click anything. Choose any filter. Create an issue or comment. Most
        interactions respond instantly.
      </p>
      <h2>Infinite scroll</h2>
      <p>
        Perfectly buttery infinite scroll. Because it’s fun! (Or open and issue
        and hold down <span className="keyboard-keys">J</span> /{' '}
        <span className="keyboard-keys">K</span> 🏎️💨)
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
