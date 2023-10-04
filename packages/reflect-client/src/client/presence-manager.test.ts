import {LogContext} from '@rocicorp/logger';
import {expect} from 'chai';
import {PresenceManager} from './presence-manager.js';
import {Resolver, resolver} from '@rocicorp/resolver';
import type {ClientID} from 'replicache';
import {sleep} from 'shared/src/sleep.js';

test('initial only this client present', async () => {
  const clientIDPromise = Promise.resolve('c1');
  const logContextPromise = Promise.resolve(new LogContext('error'));

  const presenceManager = new PresenceManager(
    clientIDPromise,
    logContextPromise,
  );

  const {promise: presentPromise, resolve: presentResolve} =
    resolver<ReadonlySet<ClientID>>();
  presenceManager.addSubscription(present => {
    presentResolve(present);
  });
  expect(await presentPromise).to.deep.equal(new Set(['c1']));
});

test('addSubscription', async () => {
  const clientIDPromise = Promise.resolve('c1');
  const logContextPromise = Promise.resolve(new LogContext('error'));

  const presenceManager = new PresenceManager(
    clientIDPromise,
    logContextPromise,
  );

  await presenceManager.updatePresence([
    {op: 'clear'},
    {op: 'put', key: 'c1', value: 1},
    {op: 'put', key: 'c2', value: 1},
  ]);

  const resolvers1: Resolver<ReadonlySet<ClientID>>[] = [
    resolver(),
    resolver(),
  ];
  let sub1CallCount = 0;
  presenceManager.addSubscription(present => {
    resolvers1[sub1CallCount++].resolve(present);
  });

  expect(await resolvers1[0].promise).to.deep.equal(new Set(['c1', 'c2']));

  const resolvers2: Resolver<ReadonlySet<ClientID>>[] = [
    resolver(),
    resolver(),
  ];
  let sub2CallCount = 0;
  presenceManager.addSubscription(present => {
    resolvers2[sub2CallCount++].resolve(present);
  });

  expect(await resolvers2[0].promise).to.deep.equal(new Set(['c1', 'c2']));

  await presenceManager.updatePresence([{op: 'put', key: 'c3', value: 1}]);

  expect(await resolvers1[1].promise).to.deep.equal(
    new Set(['c1', 'c2', 'c3']),
  );
  expect(await resolvers2[1].promise).to.deep.equal(
    new Set(['c1', 'c2', 'c3']),
  );
});

test('calling function returned by addSubscription removes subscription', async () => {
  const clientIDPromise = Promise.resolve('c1');
  const logContextPromise = Promise.resolve(new LogContext('error'));

  const presenceManager = new PresenceManager(
    clientIDPromise,
    logContextPromise,
  );

  await presenceManager.updatePresence([
    {op: 'clear'},
    {op: 'put', key: 'c1', value: 1},
    {op: 'put', key: 'c2', value: 1},
  ]);

  const resolvers1: Resolver<ReadonlySet<ClientID>>[] = [
    resolver(),
    resolver(),
    resolver(),
  ];
  let sub1CallCount = 0;
  let sub1ErrorIfCalled = false;
  const removeSub1 = presenceManager.addSubscription(present => {
    if (sub1ErrorIfCalled) {
      throw new Error('unexpected call after remove');
    }
    resolvers1[sub1CallCount++].resolve(present);
  });
  const resolvers2: Resolver<ReadonlySet<ClientID>>[] = [
    resolver(),
    resolver(),
    resolver(),
  ];
  let sub2CallCount = 0;
  presenceManager.addSubscription(present => {
    resolvers2[sub2CallCount++].resolve(present);
  });
  expect(await resolvers1[0].promise).to.deep.equal(new Set(['c1', 'c2']));
  expect(await resolvers2[0].promise).to.deep.equal(new Set(['c1', 'c2']));

  await presenceManager.updatePresence([{op: 'put', key: 'c3', value: 1}]);

  expect(await resolvers1[1].promise).to.deep.equal(
    new Set(['c1', 'c2', 'c3']),
  );
  expect(await resolvers2[1].promise).to.deep.equal(
    new Set(['c1', 'c2', 'c3']),
  );

  removeSub1();
  sub1ErrorIfCalled = true;
  await presenceManager.updatePresence([{op: 'put', key: 'c4', value: 1}]);

  expect(await resolvers2[2].promise).to.deep.equal(
    new Set(['c1', 'c2', 'c3', 'c4']),
  );
  expect(sub1CallCount).to.equal(2);
  expect(sub2CallCount).to.equal(3);
});

test('clearSubscriptions', async () => {
  const clientIDPromise = Promise.resolve('c1');
  const logContextPromise = Promise.resolve(new LogContext('error'));

  const presenceManager = new PresenceManager(
    clientIDPromise,
    logContextPromise,
  );

  await presenceManager.updatePresence([
    {op: 'clear'},
    {op: 'put', key: 'c1', value: 1},
    {op: 'put', key: 'c2', value: 1},
  ]);

  const resolvers1: Resolver<ReadonlySet<ClientID>>[] = [
    resolver(),
    resolver(),
    resolver(),
  ];
  let sub1CallCount = 0;
  let sub1ErrorIfCalled = false;
  presenceManager.addSubscription(present => {
    if (sub1ErrorIfCalled) {
      throw new Error('unexpected call after remove');
    }
    resolvers1[sub1CallCount++].resolve(present);
  });
  const resolvers2: Resolver<ReadonlySet<ClientID>>[] = [
    resolver(),
    resolver(),
    resolver(),
  ];
  let sub2CallCount = 0;
  let sub2ErrorIfCalled = false;
  presenceManager.addSubscription(present => {
    if (sub2ErrorIfCalled) {
      throw new Error('unexpected call after remove');
    }
    resolvers2[sub2CallCount++].resolve(present);
  });
  expect(await resolvers1[0].promise).to.deep.equal(new Set(['c1', 'c2']));
  expect(await resolvers2[0].promise).to.deep.equal(new Set(['c1', 'c2']));

  await presenceManager.updatePresence([{op: 'put', key: 'c3', value: 1}]);

  expect(await resolvers1[1].promise).to.deep.equal(
    new Set(['c1', 'c2', 'c3']),
  );
  expect(await resolvers2[1].promise).to.deep.equal(
    new Set(['c1', 'c2', 'c3']),
  );

  presenceManager.clearSubscriptions();
  sub1ErrorIfCalled = true;
  sub2ErrorIfCalled = true;
  await presenceManager.updatePresence([{op: 'put', key: 'c4', value: 1}]);

  await sleep(2);
  expect(sub1CallCount).to.equal(2);
  expect(sub2CallCount).to.equal(2);
});

test('updatePresence', async () => {
  const clientIDPromise = Promise.resolve('c1');
  const logContextPromise = Promise.resolve(new LogContext('error'));

  const presenceManager = new PresenceManager(
    clientIDPromise,
    logContextPromise,
  );

  const resolvers: Resolver<ReadonlySet<ClientID>>[] = [
    resolver(),
    resolver(),
    resolver(),
  ];
  let subCallCount = 0;
  presenceManager.addSubscription(present => {
    resolvers[subCallCount++].resolve(present);
  });
  expect(await resolvers[0].promise).to.deep.equal(new Set(['c1']));

  await presenceManager.updatePresence([
    {op: 'clear'},
    {op: 'put', key: 'c1', value: 1},
    {op: 'put', key: 'c2', value: 1},
  ]);

  expect(await resolvers[1].promise).to.deep.equal(new Set(['c1', 'c2']));

  await presenceManager.updatePresence([
    {op: 'del', key: 'c2'},
    {op: 'put', key: 'c3', value: 1},
  ]);

  expect(await resolvers[2].promise).to.deep.equal(new Set(['c1', 'c3']));
});

test('updatePresence self clientID always included', async () => {
  const clientIDPromise = Promise.resolve('c1');
  const logContextPromise = Promise.resolve(new LogContext('error'));

  const presenceManager = new PresenceManager(
    clientIDPromise,
    logContextPromise,
  );

  const resolvers: Resolver<ReadonlySet<ClientID>>[] = [
    resolver(),
    resolver(),
    resolver(),
  ];
  let subCallCount = 0;
  presenceManager.addSubscription(present => {
    resolvers[subCallCount++].resolve(present);
  });
  expect(await resolvers[0].promise).to.deep.equal(new Set(['c1']));

  await presenceManager.updatePresence([
    {op: 'clear'},
    {op: 'put', key: 'c2', value: 1},
  ]);

  expect(await resolvers[1].promise).to.deep.equal(new Set(['c1', 'c2']));

  await presenceManager.updatePresence([
    {op: 'del', key: 'c1'},
    {op: 'del', key: 'c2'},
    {op: 'put', key: 'c3', value: 1},
  ]);

  expect(await resolvers[2].promise).to.deep.equal(new Set(['c1', 'c3']));
});

test('updatePresence doesnt fire subscriptions if set is equal', async () => {
  const clientIDPromise = Promise.resolve('c1');
  const logContextPromise = Promise.resolve(new LogContext('error'));

  const presenceManager = new PresenceManager(
    clientIDPromise,
    logContextPromise,
  );

  const resolvers: Resolver<ReadonlySet<ClientID>>[] = [
    resolver(),
    resolver(),
    resolver(),
  ];
  let subCallCount = 0;
  presenceManager.addSubscription(present => {
    resolvers[subCallCount++].resolve(present);
  });
  expect(await resolvers[0].promise).to.deep.equal(new Set(['c1']));

  await presenceManager.updatePresence([
    {op: 'clear'},
    {op: 'put', key: 'c1', value: 1},
    {op: 'put', key: 'c2', value: 1},
  ]);

  expect(await resolvers[1].promise).to.deep.equal(new Set(['c1', 'c2']));

  // subscription not called
  await presenceManager.updatePresence([{op: 'put', key: 'c1', value: 1}]);
  expect(subCallCount).to.equal(2);

  await presenceManager.updatePresence([
    {op: 'del', key: 'c2'},
    {op: 'put', key: 'c3', value: 1},
  ]);

  expect(await resolvers[2].promise).to.deep.equal(new Set(['c1', 'c3']));
});

test('handleDisconnect', async () => {
  const clientIDPromise = Promise.resolve('c1');
  const logContextPromise = Promise.resolve(new LogContext('error'));

  const presenceManager = new PresenceManager(
    clientIDPromise,
    logContextPromise,
  );

  const resolvers: Resolver<ReadonlySet<ClientID>>[] = [
    resolver(),
    resolver(),
    resolver(),
    resolver(),
  ];
  let subCallCount = 0;
  presenceManager.addSubscription(present => {
    resolvers[subCallCount++].resolve(present);
  });
  expect(await resolvers[0].promise).to.deep.equal(new Set(['c1']));

  await presenceManager.updatePresence([
    {op: 'clear'},
    {op: 'put', key: 'c2', value: 1},
  ]);

  expect(await resolvers[1].promise).to.deep.equal(new Set(['c1', 'c2']));

  await presenceManager.handleDisconnect();

  expect(await resolvers[2].promise).to.deep.equal(new Set(['c1']));

  await presenceManager.updatePresence([
    {op: 'clear'},
    {op: 'put', key: 'c1', value: 1},
    {op: 'put', key: 'c3', value: 1},
  ]);

  expect(await resolvers[3].promise).to.deep.equal(new Set(['c1', 'c3']));
});
