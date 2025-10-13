import type {ListContext} from '../shared/queries.ts';

// TODO: Use exports instead of a Record
export const links = {
  list({projectName}: {projectName: string}) {
    return `/p/${projectName.toLowerCase()}`;
  },
  issue({
    projectName,
    id,
    shortID,
  }: {
    projectName: string;
    id: string;
    shortID?: number | null;
  }) {
    // oxlint-disable-next-line eqeqeq -- Checking for both null and undefined
    return shortID != null
      ? `/p/${projectName.toLowerCase()}/issue/${shortID}`
      : `/p/${projectName.toLowerCase()}/issue/${id}`;
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
  list: '/p/:projectName',
  deprecatedIssue: '/issue/:id', // redirected to projectIssue once project is fetched
  issue: 'p/:projectName/issue/:id',
} as const;
