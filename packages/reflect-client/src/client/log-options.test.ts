import {expect} from 'chai';
import * as sinon from 'sinon';
import {createLogOptions} from './log-options.js';
import {TestLogSink} from './test-utils.js';
import {consoleLogSink, type LogSink} from '@rocicorp/logger';
import type {DatadogLogSinkOptions} from 'datadog';

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

suite('when socketOrigin indicates testing or local dev', () => {
  const cases: (string | null)[] = [
    null,
    'ws://localhost',
    'ws://localhost:8000',
    'ws://127.0.0.1',
    'ws://127.0.0.1:1900',
    'wss://[2001:db8:3333:4444:5555:6666:7777:8888]:9000',
  ];
  for (const c of cases) {
    test(c + '', () => {
      const {logLevel, logSink} = createLogOptions(
        {
          consoleLogLevel: 'info',
          socketOrigin: c,
        },
        fakeCreateDatadogLogSink,
      );
      expect(fakeCreateDatadogLogSink.callCount).to.equal(0);
      expect(logLevel).to.equal('info');
      expect(logSink).to.equal(consoleLogSink);
    });
  }
});

function testLogLevels(socketOrigin: string, expectedServiceLabel: string) {
  test('consoleLogLevel debug', () => {
    const {logLevel, logSink} = createLogOptions(
      {
        consoleLogLevel: 'debug',
        socketOrigin,
      },
      fakeCreateDatadogLogSink,
    );
    expect(fakeCreateDatadogLogSink.callCount).to.equal(1);
    expect(fakeCreateDatadogLogSink.getCall(0).args[0].service).to.equal(
      expectedServiceLabel,
    );
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
    const {logLevel, logSink} = createLogOptions(
      {
        consoleLogLevel: 'info',
        socketOrigin,
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
    const {logLevel, logSink} = createLogOptions(
      {
        consoleLogLevel: 'error',
        socketOrigin,
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

suite('when socketOrigin is subdomain of .reflect-server.net', () => {
  testLogLevels('wss://testSubdomain.reflect-server.net', 'testsubdomain');
});

suite('when socketOrigin is not a subdomain of .reflect-server.net', () => {
  testLogLevels('wss://fooBar.FuzzyWuzzy.com', 'foobar.fuzzywuzzy.com');
});
