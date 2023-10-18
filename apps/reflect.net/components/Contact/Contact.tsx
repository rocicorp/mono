import Link from 'next/link';

export function Contact() {
  return (
    <p>
      We would be happy to answer more questions. You can contact us by{' '}
      <Link href="mailto:hi@reflect.net">email</Link>, on{' '}
      <Link href="https://twitter.com/rocicorp">Twitter</Link>, or on{' '}
      <Link href="https://discord.replicache.dev/">Discord</Link>.
    </p>
  );
}
