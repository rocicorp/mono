import type {ListContext} from '../shared/queries.ts';
import {createContext, useContext, useMemo, useState} from 'react';
import {ZERO_PROJECT_NAME} from '../shared/schema.ts';
import {useParams} from 'wouter';

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
  issue: '/p/:projectName/issue/:id',
} as const;

interface ListContextAndSetter {
  listContext: ListContext | undefined;
  setListContext: React.Dispatch<React.SetStateAction<ListContext | undefined>>;
}

const ListContextContext = createContext<ListContextAndSetter | null>(null);

export function ListContextProvider({children}: {children: React.ReactNode}) {
  const [listContext, setListContext] = useState<ListContext | undefined>(
    undefined,
  );
  const value = useMemo(() => ({listContext, setListContext}), [listContext]);

  return (
    <ListContextContext.Provider value={value}>
      {children}
    </ListContextContext.Provider>
  );
}

export function useListContext() {
  const context = useContext(ListContextContext);
  if (!context) {
    // This error is helpful for developers, as it tells them they've
    // used the hook outside of the provider.
    throw new Error('useListContext must be used within a ListContextProvider');
  }
  return context;
}

export function useProjectName() {
  const params = useParams();
  return params.projectName ?? ZERO_PROJECT_NAME;
}

export function useIsGigabugs() {
  const projectName = useProjectName();
  return isGigabugs(projectName);
}

export function isGigabugs(projectName: string) {
  return projectName.toLocaleLowerCase() === 'roci';
}
