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
        A demo bug tracker built with <a href="https://zerosync.dev">Zero</a>.
        Contains <strong>240 thousand issues</strong> and{' '}
        <strong>2.5 million rows</strong> to demonstrate real-world performance.
        Also features:
      </p>
      <h2>Fast startup</h2>
      <p>
        Clear your cache and reload. Zero provides precise control over what's
        synced, for quick starts even on an empty cache.
      </p>
      <h2>Instant UX</h2>
      <p>
        Click anything. Choose some filters. Create an issue. Zero's queries use
        previously synced data if possible, making interactions instant by
        default.
      </p>
      <h2>Infinite scroll</h2>
      <p>
        Perfect, buttery scrolling. Or open an issue and hold down{' '}
        <span className="keyboard-keys">J</span> /{' '}
        <span className="keyboard-keys">K</span> 🏎️💨.
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
