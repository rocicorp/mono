import Link from 'next/link';

export default function Contact() {
  return (
    <p>
      We would be happy to answer more questions. You can contact us by{' '}
      <Link href="#">email</Link>, on <Link href="#">Twitter</Link>, or on{' '}
      <Link href="#">Discord</Link>.
    </p>
  );
}
