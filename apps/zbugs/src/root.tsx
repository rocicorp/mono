import {Route, Switch} from 'wouter';
import ListPage from './pages/list/list-page.js';
import IssuePage from './pages/issue/issue-page.js';
import ErrorPage from './pages/error/error-page.js';
import {Nav} from './components/nav.js';

export default function Root() {
  return (
    <>
      <div className="app-container flex p-8">
        <div className="primary-nav w-48 shrink-0 grow-0">
          <Nav />
        </div>
        <div className="primary-content">
          <Switch>
            <Route path="/" component={ListPage} />
            <Route path="/issue/:id" component={IssuePage} />
            <Route component={ErrorPage} />
          </Switch>
        </div>
      </div>
    </>
  );
}
