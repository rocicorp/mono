import {Route, Switch} from 'wouter';
import ListPage from './pages/list/list-page.js';
import IssuePage from './pages/issue/issue-page.js';
import ErrorPage from './pages/error/error-page.js';
import {FPSMeter} from '@schickling/fps-meter';
import {Nav} from './components/nav.js';

export default function Root() {
  return (
    <>
      <FPSMeter className="fixed right-0 top-0 z-50 bg-gray-800" height={40} />
      <div className="flex m-8 gap-12">
        <div className="w-48 shrink-0 grow-0">
          <Nav />
        </div>
        <div>
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
        </div>
      </div>
    </>
  );
}
