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

suite('onClientStateNotFound', () => {
  if (DD31) {
    // In DD31 the response {error: 'ClientStateNotFound'} is used to disable
    // the client group and no callback is called.
    //
    // This is tested in replicache-{poke,pull,push}.test.ts
    return;
  }

  test('pull returning ClientStateNotFoundResponse should call onClientStateNotFound', async () => {
    // eslint-disable-next-line require-await
    const puller: Puller = async _req => {
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
      puller,
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

  test('poke with ClientStateNotFoundResponse should call onClientStateNotFound', async () => {
    const consoleErrorStub = sinon.stub(console, 'error');
    const onClientStateNotFound = sinon.fake();

    const rep = await replicacheForTesting('new-phone', {
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

  test('push with ClientStateNotFoundResponse should not call onClientStateNotFound', async () => {
    // eslint-disable-next-line require-await
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
        async noop() {
          // no op
        },
      },
    });

    // Call push
    await rep.mutate.noop();
    await tickAFewTimes();

    // SDD does not call onClientStateNotFound for push because in SDD we did
    // not look at the HTTP response
    expect(onClientStateNotFound.callCount).to.equal(0);
    expect(consoleErrorStub.notCalled).true;
  });
});
