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

export type ZbugsHistoryState = {
  listContext: ListContext;
};

export type ListContext = {
  readonly href: string;
  readonly title: string;
  readonly shortIDs: readonly number[];
};

export const routes = {
  home: '/',
  issue: '/issue/:shortID?',
  pendingIssue: '/issue/pending/:id?',
} as const;
