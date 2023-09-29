import {consoleLogSink, type LogSink} from '@rocicorp/logger';
import {expect} from 'chai';
import type {DatadogLogSinkOptions} from 'datadog';
import * as sinon from 'sinon';
import {createLogOptions} from './log-options.js';
import {TestLogSink} from './test-utils.js';

let consoleLogSinkSpy: sinon.SinonSpiedInstance<LogSink>;
let datadogLogSinkSpy: sinon.SinonSpiedInstance<LogSink>;
let fakeCreateDatadogLogSink: sinon.SinonSpy<[DatadogLogSinkOptions], LogSink>;

setup(() => {
  consoleLogSinkSpy = sinon.spy(consoleLogSink);
  fakeCreateDatadogLogSink = sinon.fake((_options: DatadogLogSinkOptions) => {
    const testLogSink = new TestLogSink();
    datadogLogSinkSpy = sinon.spy(testLogSink);
    return testLogSink;
  });
});

teardown(() => {
  sinon.restore();
});

function testEnableAnalyticsFalse(server: string | null) {
  test(`server ${server}, enableAnalytics false`, () => {
    const {logLevel, logSink} = createLogOptions(
      {
        consoleLogLevel: 'info',
        server,
        enableAnalytics: false,
      },
      fakeCreateDatadogLogSink,
    );
    expect(fakeCreateDatadogLogSink.callCount).to.equal(0);
    expect(logLevel).to.equal('info');
    expect(logSink).to.equal(consoleLogSink);
  });
}

suite('when server indicates testing or local dev', () => {
  const cases: (string | null)[] = [
    null,
    'http://localhost',
    'http://localhost:8000',
    'http://127.0.0.1',
    'http://127.0.0.1:1900',
    'https://[2001:db8:3333:4444:5555:6666:7777:8888]:9000',
  ];
  for (const c of cases) {
    test(c + '', () => {
      const {logLevel, logSink} = createLogOptions(
        {
          consoleLogLevel: 'info',
          server: c,
          enableAnalytics: true,
        },
        fakeCreateDatadogLogSink,
      );
      expect(fakeCreateDatadogLogSink.callCount).to.equal(0);
      expect(logLevel).to.equal('info');
      expect(logSink).to.equal(consoleLogSink);
    });
    testEnableAnalyticsFalse(c);
  }
});

function testLogLevels(
  server: string,
  expectedServiceLabel: string,
  expectedBaseURLString: string,
) {
  test('consoleLogLevel debug', () => {
    sinon.stub(console, 'debug');
    sinon.stub(console, 'info');
    sinon.stub(console, 'error');

    const {logLevel, logSink} = createLogOptions(
      {
        consoleLogLevel: 'debug',
        server,
        enableAnalytics: true,
      },
      fakeCreateDatadogLogSink,
    );
    expect(fakeCreateDatadogLogSink.callCount).to.equal(1);
    expect(fakeCreateDatadogLogSink.getCall(0).args[0].service).to.equal(
      expectedServiceLabel,
    );
    expect(
      fakeCreateDatadogLogSink.getCall(0).args[0].baseURL?.toString(),
    ).to.equal(expectedBaseURLString);
    expect(logLevel).to.equal('debug');

    logSink.log('debug', {foo: 'bar'}, 'hello');
    logSink.log('info', {foo: 'bar'}, 'world');
    logSink.log('error', {foo: 'bar'}, 'goodbye');

    // debug not logged
    expect(datadogLogSinkSpy.log.callCount).to.equal(2);
    expect(datadogLogSinkSpy.log.getCall(0).args).to.deep.equal([
      'info',
      {foo: 'bar'},
      'world',
    ]);
    expect(datadogLogSinkSpy.log.getCall(1).args).to.deep.equal([
      'error',
      {foo: 'bar'},
      'goodbye',
    ]);

    expect(consoleLogSinkSpy.log.callCount).to.equal(3);
    expect(consoleLogSinkSpy.log.getCall(0).args).to.deep.equal([
      'debug',
      {foo: 'bar'},
      'hello',
    ]);
    expect(consoleLogSinkSpy.log.getCall(1).args).to.deep.equal([
      'info',
      {foo: 'bar'},
      'world',
    ]);
    expect(consoleLogSinkSpy.log.getCall(2).args).to.deep.equal([
      'error',
      {foo: 'bar'},
      'goodbye',
    ]);
  });

  test('consoleLogLevel info', () => {
    sinon.stub(console, 'debug');
    sinon.stub(console, 'info');
    sinon.stub(console, 'error');

    const {logLevel, logSink} = createLogOptions(
      {
        consoleLogLevel: 'info',
        server,
        enableAnalytics: true,
      },
      fakeCreateDatadogLogSink,
    );
    expect(fakeCreateDatadogLogSink.callCount).to.equal(1);
    expect(fakeCreateDatadogLogSink.getCall(0).args[0].service).to.equal(
      expectedServiceLabel,
    );
    expect(logLevel).to.equal('info');

    logSink.log('debug', {foo: 'bar'}, 'hello');
    logSink.log('info', {foo: 'bar'}, 'world');
    logSink.log('error', {foo: 'bar'}, 'goodbye');

    expect(datadogLogSinkSpy.log.callCount).to.equal(2);
    expect(datadogLogSinkSpy.log.getCall(0).args).to.deep.equal([
      'info',
      {foo: 'bar'},
      'world',
    ]);
    expect(datadogLogSinkSpy.log.getCall(1).args).to.deep.equal([
      'error',
      {foo: 'bar'},
      'goodbye',
    ]);

    expect(consoleLogSinkSpy.log.callCount).to.equal(2);
    expect(consoleLogSinkSpy.log.getCall(0).args).to.deep.equal([
      'info',
      {foo: 'bar'},
      'world',
    ]);
    expect(consoleLogSinkSpy.log.getCall(1).args).to.deep.equal([
      'error',
      {foo: 'bar'},
      'goodbye',
    ]);
  });

  test('consoleLogLevel error', () => {
    sinon.stub(console, 'debug');
    sinon.stub(console, 'info');
    sinon.stub(console, 'error');

    const {logLevel, logSink} = createLogOptions(
      {
        consoleLogLevel: 'error',
        server,
        enableAnalytics: true,
      },
      fakeCreateDatadogLogSink,
    );
    expect(fakeCreateDatadogLogSink.callCount).to.equal(1);
    expect(fakeCreateDatadogLogSink.getCall(0).args[0].service).to.equal(
      expectedServiceLabel,
    );
    expect(logLevel).to.equal('info');

    logSink.log('debug', {foo: 'bar'}, 'hello');
    logSink.log('info', {foo: 'bar'}, 'world');
    logSink.log('error', {foo: 'bar'}, 'goodbye');

    // info still logged
    expect(datadogLogSinkSpy.log.callCount).to.equal(2);
    expect(datadogLogSinkSpy.log.getCall(0).args).to.deep.equal([
      'info',
      {foo: 'bar'},
      'world',
    ]);
    expect(datadogLogSinkSpy.log.getCall(1).args).to.deep.equal([
      'error',
      {foo: 'bar'},
      'goodbye',
    ]);

    // only error logged
    expect(consoleLogSinkSpy.log.callCount).to.equal(1);
    expect(consoleLogSinkSpy.log.getCall(0).args).to.deep.equal([
      'error',
      {foo: 'bar'},
      'goodbye',
    ]);
  });
}

suite('when server is subdomain of .reflect-server.net', () => {
  const server = 'https://testSubdomain.reflect-server.net';
  testLogLevels(
    server,
    'testsubdomain',
    'https://testsubdomain.reflect-server.net/api/logs/v0/log',
  );
  testEnableAnalyticsFalse(server);
});

suite('when server is not a subdomain of .reflect-server.net', () => {
  const server = 'https://fooBar.FuzzyWuzzy.com';
  testLogLevels(
    server,
    'foobar.fuzzywuzzy.com',
    'https://foobar.fuzzywuzzy.com/api/logs/v0/log',
  );
  testEnableAnalyticsFalse(server);
});
