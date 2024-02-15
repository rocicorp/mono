const redirects = [
  [
    '/byob/remote-database#supabase-setup',
    'https://github.com/rocicorp/todo-nextjs',
  ],
  ['/howto/licensing', '/concepts/licensing'],
  ['/faq#how-does-the-client-know-when-to-sync-does-it-poll', '/byob/poke'],
  [
    '/concepts/faq#what-if-i-dont-have-a-dedicated-backend-i-use-serverless-functions-for-my-backend',
    'https://github.com/rocicorp/todo-nextjs',
  ],
  [
    '/concepts/faq#how-can-i-programmatically-prevent-replicache-from-syncing',
    '/howto/unit-test',
  ],
  [
    '/concepts/faq#unpushed',
    '/api/classes/Replicache#experimentalpendingmutations',
  ],
  ['/concepts/faq#do-you-support-collaborative-text-editing', '/howto/text'],
  [
    '/concepts/faq#what-is-a-monthly-active-profile',
    '/concepts/licensing#monthly-active-profiles',
  ],
  [
    '/concepts/faq#what-do-you-mean-by-commercial-application',
    '/concepts/licensing#pricing-exemption',
  ],
  [
    '/concepts/faq#can-you-give-me-some-billing-examples',
    '/concepts/licensing#pricing-examples',
  ],
  ['/concepts/faq#can-i-get-access-to-the-source-code', '/howto/source-access'],
];

const fullPath = [location.pathname, location.search, location.hash].join('');
for (const [from, to] of redirects) {
  if (fullPath === from) {
    location.href = to;
    break;
  }
}
