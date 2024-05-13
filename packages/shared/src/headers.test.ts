import {test, expect} from 'vitest';
import {decodeHeaderValue, encodeHeaderValue} from './headers.js';

function testEncodeDecodeHeaderValue(value: string, expected: string): void {
  const encoded = encodeHeaderValue(value);
  expect(encoded).toEqual(expected);
  expect(decodeHeaderValue(encoded)).toEqual(value);
}

test('encodeHeaderValue/decodeHeaderValue', () => {
  testEncodeDecodeHeaderValue('basic test', 'basic test');
  // All the chars normally escaped by encodeURIComponent, but which don't
  // need to be escaped for header values
  testEncodeDecodeHeaderValue(
    ':;,/"?{}[]@<>=+#$&`|^ ',
    ':;,/"?{}[]@<>=+#$&`|^ ',
  );
  testEncodeDecodeHeaderValue(
    '% char should be escaped',
    '%25 char should be escaped',
  );
  testEncodeDecodeHeaderValue(
    '{ userId: "1245678910" }',
    '{ userId: "1245678910" }',
  );
  testEncodeDecodeHeaderValue(
    'هذا اختبار',
    '%D9%87%D8%B0%D8%A7 %D8%A7%D8%AE%D8%AA%D8%A8%D8%A7%D8%B1',
  );
});
