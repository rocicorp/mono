import type {ListContext} from '../shared/queries.ts';

// TODO: Use exports instead of a Record
export const links = {
  home() {
    return '/';
  },
  issue({id, shortID}: {id: string; shortID?: number | null}) {
    return shortID != null ? `/issue/${shortID}` : `/issue/${id}`;
  },
  login(pathname: string, search: string | null) {
    return (
      '/api/login/github?redirect=' +
      encodeURIComponent(search ? pathname + search : pathname)
    );
  },
};

export type ZbugsHistoryState = {
  readonly zbugsListContext?: ListContext | undefined;
};

export const routes = {
  home: '/',
  issue: '/issue/:id',
} as const;
