// TODO: Use exports instead of a Record
export const links = {
  home() {
    return '/';
  },
  issue({id, shortID}: {id: string; shortID?: number | null}) {
    return shortID ? `/issue/${shortID}` : `/issue/${id}`;
  },
  login(pathname: string, search: string | null) {
    return (
      '/api/login/github?redirect=' +
      encodeURIComponent(search ? pathname + search : pathname)
    );
  },
};

export type ZbugsHistoryState = {
  readonly zbugsListScrollOffset?: number | undefined;
  readonly zbugsListContext?: ListContext | undefined;
};

export type ListContext = {
  readonly href: string;
  readonly title: string;
  readonly params: {
    readonly open?: boolean | undefined;
    readonly assignee?: string | undefined;
    readonly creator?: string | undefined;
    readonly labels?: string[] | undefined;
    readonly textFilter?: string | undefined;
    readonly sortField: 'modified' | 'created';
    readonly sortDirection: 'asc' | 'desc';
  };
};

export const routes = {
  home: '/',
  issue: '/issue/:id',
} as const;
