export const links = {
  home() {
    return '/';
  },
  issue({id, shortID}: {id: string; shortID?: number | undefined}) {
    return shortID ? `/issue/${shortID}` : `/issue/${id}`;
  },
  login(pathname: string, search: string | undefined) {
    return (
      '/api/login/github?redirect=' +
      encodeURIComponent(search ? pathname + search : pathname)
    );
  },
};

export type ListContext = {
  readonly href: string;
  readonly title: string;
  readonly params: {
    readonly open?: boolean | undefined;
    readonly assigneeID?: string | undefined;
    readonly creatorID?: string | undefined;
    readonly labelIDs?: string[] | undefined;
  };
};

export const routes = {
  home: '/',
  issue: '/issue/:id',
} as const;
