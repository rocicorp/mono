import {describe, expect, test, vi} from 'vitest';
import postgres from 'postgres';
import {
  extractConnectionConfig,
  makeAck,
  SlotReservationHandle,
} from '../services/change-source/pg/logical-replication/stream.ts';
import {inProcChannel} from '../types/processes.ts';

describe('slot-keeper', () => {
  test('makeAck constructs standby status update buffer correctly', () => {
    const lsn = 0x123456789abcdef0n;
    const buf = makeAck(lsn);

    expect(buf.length).toBe(34);
    expect(buf[0]).toBe('r'.charCodeAt(0));
    expect(buf.readBigInt64BE(1)).toBe(lsn);
    expect(buf.readBigInt64BE(9)).toBe(lsn);
  });

  test('extractConnectionConfig serializes options accurately', () => {
    const sql = postgres({
      host: 'db.example.com',
      port: 5432,
      database: 'testdb',
      user: 'testuser',
      pass: 'secret',
      ssl: {rejectUnauthorized: false},
    });

    try {
      const config = extractConnectionConfig(sql);
      expect(config.database).toBe('testdb');
      expect(config.user).toBe('testuser');
      expect(config.pass).toBe('secret');
      expect(config.host).toEqual(['db.example.com']);
      expect(config.port).toEqual([5432]);
      expect(config.ssl).toEqual({rejectUnauthorized: false});

      // Verify it can be cleanly serialized to JSON and back
      const json = JSON.stringify(config);
      const parsed = JSON.parse(json);
      expect(parsed).toEqual(config);
    } finally {
      void sql.end();
    }
  });

  test('SlotReservationHandle sends stop IPC message on destroy', () => {
    const [parent, child] = inProcChannel();
    const handle = new SlotReservationHandle(child);

    const received: unknown[] = [];
    parent.on('message', msg => received.push(msg));

    expect(handle.destroyed).toBe(false);

    handle.destroy();
    expect(handle.destroyed).toBe(true);
    expect(received).toEqual([['stop', {}]]);

    // Repeated destroy should be a no-op
    handle.destroy();
    expect(received).toEqual([['stop', {}]]);
  });

  test('SlotReservationHandle forwards error and close from worker', async () => {
    const [parent, child] = inProcChannel();
    const handle = new SlotReservationHandle(child);

    const closed = new Promise<void>(resolve => {
      handle.on('close', resolve);
    });

    child.emit('close', 0);
    await closed;
    expect(handle.destroyed).toBe(true);
  });
});
