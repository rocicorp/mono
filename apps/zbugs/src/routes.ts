export const links = {
  home() {
    return '/';
  },
  issue({id, shortID}: {id: string; shortID?: number | undefined}) {
    return shortID ? `/issue/${shortID}` : `/issue/pending/${id}`;
  },
  login(pathname: string, search: string | undefined) {
    return (
      '/api/login/github?redirect=' +
      encodeURIComponent(search ? pathname + search : pathname)
    );
  },
};

export const routes = {
  home: '/',
  issue: '/issue/:shortID?',
  pendingIssue: '/issue/pending/:id?',
} as const;
