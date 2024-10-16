import {expect, test} from 'vitest';
import fc from 'fast-check';
import {decodeSecProtocols, encodeSecProtocols} from './connect.js';

test('encode/decodeSecProtocols round-trip', () => {
  fc.assert(
    fc.property(
      fc.record({
        initConnectionMessage: fc.tuple(
          fc.constant<'initConnection'>('initConnection'),
          fc.record({
            desiredQueriesPatch: fc.array(
              fc.oneof(
                fc.record({
                  op: fc.constant<'put'>('put'),
                  hash: fc.string(),
                  ast: fc.constant({
                    table: 'table',
                  }),
                }),
                fc.record({
                  op: fc.constant<'del'>('del'),
                  hash: fc.string(),
                }),
              ),
            ),
          }),
        ),
        authToken: fc.option(
          fc.stringOf(
            fc.constantFrom(...'abcdefghijklmnopqrstuvwxyz0123456789-_.'),
          ),
          {nil: undefined},
        ),
      }),
      ({initConnectionMessage, authToken}) => {
        const encoded = encodeSecProtocols(initConnectionMessage, authToken);
        const {
          initConnectionMessage: decodedInitConnectionMessage,
          authToken: decodedAuthToken,
        } = decodeSecProtocols(encoded);
        expect(decodedInitConnectionMessage).toEqual(initConnectionMessage);
        expect(decodedAuthToken).toEqual(authToken);
      },
    ),
  );
});
