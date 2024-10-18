export const links = {
  home() {
    return '/';
  },
  issue({id, shortID}: {id: string; shortID?: number | undefined}) {
    return shortID ? `/issue/${shortID}` : `/issue/?longID=${id}`;
  },
};

export const routes = {
  home: '/',
  issue: '/issue/:id?',
} as const;
