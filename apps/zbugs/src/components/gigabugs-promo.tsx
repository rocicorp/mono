import {navigate} from 'wouter/use-browser-location';
import {useIsGigabugs} from '../routes.tsx';

interface GigabugsPromoProps {
  onNavigate?: () => void;
}

export function GigabugsPromo({onNavigate}: GigabugsPromoProps) {
  const isGigabugs = useIsGigabugs();

  if (isGigabugs) {
    return null;
  }

  return (
    <p className="aside">
      Testing Zero? Try{' '}
      <a
        href="/p/roci"
        onClick={e => {
          e.preventDefault();
          onNavigate?.();
          navigate('/p/roci');
        }}
      >
        Gigabugs
      </a>{' '}
      instead.
      <br />
      Want a faster response?{' '}
      <a href="https://discord.rocicorp.dev/">Join us on Discord &rarr;</a>
    </p>
  );
}
