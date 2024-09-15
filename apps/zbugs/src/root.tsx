import {Route, Switch} from 'wouter';
import ListPage from './pages/list/list-page.js';
import IssuePage from './pages/issue/issue-page.js';
import ErrorPage from './pages/error/error-page.js';
import {FPSMeter} from '@schickling/fps-meter';

export default function Root() {
  return (
    <>
      <FPSMeter className="fixed right-0 top-0 z-50 bg-gray-800" height={40} />
      <Switch>
        <Route path="/">
          <ListPage />
        </Route>
        <Route path="/issue/:id">
          <IssuePage />
        </Route>
        <Route>
          <ErrorPage />
        </Route>
      </Switch>
    </>
  );
}
