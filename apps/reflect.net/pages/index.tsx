import {Benefits} from '@/components/Benefits/Benefits';
import {Demo} from '@/components/Demo/Demo';
import {Footer} from '@/components/Footer/Footer';
import {GetStarted} from '@/components/GetStarted/GetStarted';
import {How} from '@/components/How/How';
import {Nav} from '@/components/Nav/Nav';
import {Pricing} from '@/components/Pricing/Pricing';
import {Contact} from '@/components/Contact/Contact';
import {Testimonials} from '@/components/Testimonials/Testimonials';
import {useDocumentSize} from '@/hooks/use-document-size';
import {useIsomorphicLayoutEffect} from '@/hooks/use-isomorphic-layout-effect';
import {useVHStyleProp} from '@/hooks/use-vh-style-prop';
import {useWindowSize} from '@/hooks/use-window-size';
import styles from '@/styles/Home.module.css';
import Head from 'next/head';
import {useState} from 'react';

export default function Home() {
  const winSize = useWindowSize();
  const docSize = useDocumentSize();
  useVHStyleProp(winSize?.height ?? null);
  const [gameMode, setGameMode] = useState<boolean>(false);

  useIsomorphicLayoutEffect(() => {
    document.documentElement.classList.toggle('game-mode', gameMode);
  }, [gameMode]);

  const onSetGameMode = (gameMode: boolean) => {
    setGameMode(gameMode);
  };

  return (
    <div className={styles.container}>
      <Head>
        <title>Reflect - High-performance sync for multiplayer web apps</title>
        <meta
          name="description"
          content="60FPS sync, automatic persistence, server authority, optional offline, fine-grained auth, and more..."
        />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link
          rel="apple-touch-icon"
          sizes="180x180"
          href="/apple-touch-icon.png"
        />
        <link
          rel="icon"
          href="favicon.svg"
          type="image/svg+xml"
          media="(prefers-color-scheme: light)"
        />
        <link
          rel="icon"
          href="favicon-dm.svg"
          type="image/svg+xml"
          media="(prefers-color-scheme: dark)"
        />
        <link
          rel="icon"
          type="image/png"
          sizes="32x32"
          href="/favicon-32x32.png"
          media="(prefers-color-scheme: light)"
        />
        <link
          rel="icon"
          type="image/png"
          sizes="16x16"
          href="/favicon-16x16.png"
          media="(prefers-color-scheme: light)"
        />
        <link
          rel="icon"
          type="image/png"
          sizes="32x32"
          href="/favicon-32x32-dm.png"
          media="(prefers-color-scheme: dark)"
        />
        <link
          rel="icon"
          type="image/png"
          sizes="16x16"
          href="/favicon-16x16-dm.png"
          media="(prefers-color-scheme: dark)"
        />
        <link rel="manifest" href="/site.webmanifest" />

        {/* Facebook Meta Tags */}
        <meta property="og:url" content="reflect.net" />
        <meta property="og:type" content="website" />
        <meta property="og:site_name" content="Reflect" />
        <meta
          property="og:title"
          content="Reflect: High-performance sync for multiplayer web apps."
        />
        <meta
          property="og:description"
          content="60FPS sync, automatic persistence, server authority, fine-grained auth, and more..."
        />
        <meta
          property="og:image"
          content="https://reflect.net/reflect-og.jpg"
        />

        {/* Twitter Meta Tags */}
        <meta name="twitter:card" content="summary_large_image" />
        <meta property="twitter:domain" content="reflect.net" />
        <meta property="twitter:url" content="https://reflect.net" />
        <meta
          name="twitter:title"
          content="Reflect: High-performance sync for multiplayer web apps."
        />
        <meta
          name="twitter:description"
          content="60FPS sync, automatic persistence, server authority, fine-grained auth, and more..."
        />
        <meta
          name="twitter:image"
          content="https://reflect.net/reflect-og.jpg"
        />
      </Head>

      <div itemScope itemType="https://schema.org/WebSite">
        <meta itemProp="url" content="https://reflect.net/" />
        <meta itemProp="name" content="Reflect" />
      </div>

      <Nav gameMode={gameMode} />

      <main className={styles.main}>
        <Demo
          docSize={docSize}
          winSize={winSize}
          gameMode={gameMode}
          onSetGameMode={onSetGameMode}
        />

        <section id="benefits" className={styles.section}>
          <h2 className={styles.subheader}>Why Reflect?</h2>
          <Benefits />
        </section>

        <section id="how" className={styles.section}>
          <h2 className={styles.subheader}>How it Works</h2>
          <How />
        </section>

        <section id="get-started" className={styles.section}>
          <h2 className={styles.subheader}>Get Started Now</h2>
          <GetStarted />
        </section>

        <section id="pricing" className={styles.section}>
          <h2 className={styles.subheader}>Simple, Usage-Based Pricing</h2>
          <Pricing />
        </section>

        <section id="customers" className={styles.section}>
          <h2 className={styles.subheader}>Early Reactions</h2>
          <Testimonials />
        </section>

        <section id="contact" className={styles.section}>
          <h2 className={styles.subheader}>Contact Us</h2>
          <Contact />
        </section>
      </main>

      <Footer />
    </div>
  );
}
