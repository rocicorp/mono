/** @type {import('@docusaurus/types').DocusaurusConfig} */
module.exports = {
  title: 'Replicache Docs',
  tagline: 'Realtime Sync for any Backend Stack',
  url: 'https://doc.replicache.dev',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.png',
  organizationName: 'Rocicorp', // Usually your GitHub org/user name.
  projectName: 'replicache', // Usually your repo name.
  plugins: [
    process.env.NODE_ENV === 'production' && 'docusaurus-plugin-script-tags',
    [
      'docusaurus-plugin-typedoc',

      // Plugin / TypeDoc options
      {
        entryPoints: ['../src/mod.ts'],
        tsconfig: '../tsconfig.json',
        exclude: ['node_modules', 'src/*.test.ts'],
        excludePrivate: true,
        excludeProtected: true,
        excludeExternals: false,
        disableSources: true,
        name: 'Replicache',
        readme: 'none',
        out: 'api',
        watch: process.env.TYPEDOC_WATCH ?? false,
      },
    ],
  ].filter(Boolean),
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
          href: 'https://github.com/rocicorp/replicache',
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
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          routeBasePath: '/',
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/rocicorp/replicache/tree/main/doc',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      },
    ],
  ],
};
