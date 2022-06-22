import {expect} from '@esm-bundle/chai';
import * as sinon from 'sinon';
import type {Puller} from './puller.js';
import type {Pusher} from './pusher.js';
import type {Poke} from './replicache.js';
import {
  expectLogContext,
  initReplicacheTesting,
  replicacheForTesting,
  tickAFewTimes,
} from './test-util.js';

initReplicacheTesting();

test('pull returning ClientStateNotFoundResponse should call onClientStateNotFound', async () => {
  const puller: Puller = async _req => {
    return {
      httpRequestInfo: {httpStatusCode: 200, errorMessage: ''},
      response: {
        error: 'ClientStateNotFound',
      },
    };
  };
  const pusher: Pusher = async _req => {
    return {httpStatusCode: 200, errorMessage: ''};
  };

  const consoleErrorStub = sinon.stub(console, 'error');
  const onClientStateNotFound = sinon.fake();

  const rep = await replicacheForTesting('new-phone', {
    puller,
    pusher,
    onClientStateNotFound,
  });

  // One pull from open

  expect(onClientStateNotFound.callCount).to.equal(1);
  expect(onClientStateNotFound.lastCall.args).to.deep.equal([
    {type: 'NotFoundOnServer'},
  ]);

  expectLogContext(
    consoleErrorStub,
    0,
    rep,
    `Client state not found, clientID: ${await rep.clientID}`,
  );

  rep.pull();
  await tickAFewTimes();

  expect(onClientStateNotFound.callCount).to.equal(2);
  expect(onClientStateNotFound.lastCall.args).to.deep.equal([
    {type: 'NotFoundOnServer'},
  ]);
  expectLogContext(
    consoleErrorStub,
    1,
    rep,
    `Client state not found, clientID: ${await rep.clientID}`,
  );
});

test('push returning ClientStateNotFoundResponse should call onClientStateNotFound', async () => {
  const pusher: Pusher = async _req => {
    return {
      httpRequestInfo: {httpStatusCode: 200, errorMessage: ''},
      response: {
        error: 'ClientStateNotFound',
      },
    };
  };

  const consoleErrorStub = sinon.stub(console, 'error');
  const onClientStateNotFound = sinon.fake();

  const rep = await replicacheForTesting('new-phone', {
    pusher,
    onClientStateNotFound,
    mutators: {
      foo: async () => {
        return 42;
      },
    },
    pushDelay: 0,
  });

  await rep.mutate.foo();

  // One push

  await tickAFewTimes();

  expect(onClientStateNotFound.callCount).to.equal(1);
  expect(onClientStateNotFound.lastCall.args).to.deep.equal([
    {type: 'NotFoundOnServer'},
  ]);

  expectLogContext(
    consoleErrorStub,
    0,
    rep,
    `Client state not found, clientID: ${await rep.clientID}`,
  );

  await rep.mutate.foo();
  await tickAFewTimes();

  expect(onClientStateNotFound.callCount).to.equal(2);
  expect(onClientStateNotFound.lastCall.args).to.deep.equal([
    {type: 'NotFoundOnServer'},
  ]);
  expectLogContext(
    consoleErrorStub,
    1,
    rep,
    `Client state not found, clientID: ${await rep.clientID}`,
  );
});

test('poke with ClientStateNotFoundResponse should call onClientStateNotFound', async () => {
  const puller: Puller = async _req => {
    return {
      httpRequestInfo: {httpStatusCode: 200, errorMessage: ''},
    };
  };

  const consoleErrorStub = sinon.stub(console, 'error');
  const onClientStateNotFound = sinon.fake();

  const rep = await replicacheForTesting('new-phone', {
    puller,
    onClientStateNotFound,
  });

  const pokeBody: Poke = {
    baseCookie: null,
    pullResponse: {error: 'ClientStateNotFound'},
  };
  await rep.poke(pokeBody);

  expect(onClientStateNotFound.callCount).to.equal(1);
  expect(onClientStateNotFound.lastCall.args).to.deep.equal([
    {type: 'NotFoundOnServer'},
  ]);
  expect(consoleErrorStub.callCount).to.equal(1);
  expectLogContext(
    consoleErrorStub,
    0,
    rep,
    `Client state not found, clientID: ${await rep.clientID}`,
  );
});
