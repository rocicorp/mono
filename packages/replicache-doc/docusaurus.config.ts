import type * as Preset from '@docusaurus/preset-classic';
import type {Config} from '@docusaurus/types';
import {themes as prismThemes} from 'prism-react-renderer';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const config: Config = {
  title: 'Replicache Docs',
  tagline: 'Realtime Sync for any Backend Stack',
  url: 'https://doc.replicache.dev',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },
  favicon: 'img/favicon.png',
  organizationName: 'Rocicorp', // Usually your GitHub org/user name.
  projectName: 'replicache', // Usually your repo name.

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  plugins: [
    process.env.NODE_ENV === 'production' && 'docusaurus-plugin-script-tags',
    [
      'docusaurus-plugin-typedoc',
      {
        entryPoints: ['../replicache/src/mod.ts'],
        tsconfig: '../replicache/tsconfig.json',
        exclude: ['node_modules', 'src/*.test.ts'],
        excludePrivate: true,
        excludeProtected: true,
        excludeExternals: false,
        disableSources: true,
        name: 'Replicache',
        readme: 'none',
        out: 'docs/api',
        watch: process.env.TYPEDOC_WATCH ?? false,
      },
    ],
  ].filter(Boolean),

  scripts: [
    {
      src: '/js/redirects.js',
      async: false,
    },
  ],

  themeConfig: {
    tags: {
      headTags: [
        {
          tagName: 'script',
          innerHTML: `
          (function(w,d,s,l,i){w[l]=w[l]||[];w[l].push({'gtm.start':
          new Date().getTime(),event:'gtm.js'});var f=d.getElementsByTagName(s)[0],
          j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src=
          'https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);
          })(window,document,'script','dataLayer','GTM-PTN768T');
          `,
        },
      ],
    },

    colorMode: {
      defaultMode: 'light',
      disableSwitch: true,
      respectPrefersColorScheme: false,
    },

    navbar: {
      title: 'Replicache Documentation',
      logo: {
        alt: 'Shiny Replicache Logo',
        src: 'img/replicache.svg',
      },
      items: [
        {
          href: 'https://github.com/rocicorp/mono/tree/main/replicache',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Connect',
          items: [
            {
              label: 'Email',
              href: 'mailto:hello@replicache.dev',
            },
            {
              label: 'Discord',
              href: 'https://discord.replicache.dev/',
            },
            {
              label: 'Twitter',
              href: 'https://twitter.com/replicache',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Rocicorp LLC.`,
    },
    algolia: {
      appId: 'Y3T1SV2WRD',
      apiKey: 'b71db84abfaa5d2c764e0d523c383feb',
      indexName: 'replicache',
      contextualSearch: false,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
    },
  } satisfies Preset.ThemeConfig,

  presets: [
    [
      'classic',
      {
        docs: {
          routeBasePath: '/',
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/rocicorp/mono/tree/main/replicache-doc',
        },

        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],
};

export default config;
