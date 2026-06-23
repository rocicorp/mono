import {expect, test, vi} from 'vitest';
import {Acker} from './change-source.ts';

test('acker', () => {
  const sink = {push: vi.fn()};

  let acks = 0;

  const expectAck = (expected: bigint) => {
    expect(sink.push).toBeCalledTimes(++acks);
    expect(sink.push.mock.calls[acks - 1][0]).toBe(expected);
  };

  const expectNoAck = () => {
    expect(sink.push).toBeCalledTimes(acks);
  };

  const acker = new Acker(sink);

  acker.onChange(['status', {ack: false}, {watermark: '0a'}]);
  expectAck(10n);

  acker.onChange(['begin', {tag: 'begin'}, {commitWatermark: '0b'}]);
  acker.ack('0b');
  expectAck(11n);

  acker.onChange(['status', {ack: false}, {watermark: '0c'}]);
  expectAck(12n);

  acker.onChange(['begin', {tag: 'begin'}, {commitWatermark: '0d'}]);

  // This should be dropped because we are awaiting 0d
  acker.onChange(['status', {ack: false}, {watermark: '0e'}]);
  expectNoAck();

  // Now we are awaiting 0f
  acker.onChange(['status', {ack: true}, {watermark: '0f'}]);
  acker.ack('0d');
  expectAck(13n);

  // Still not caught up, so dropped
  acker.onChange(['status', {ack: false}, {watermark: '0g'}]);
  expectNoAck();

  // Downstream is now caught up.
  acker.ack('0f');
  expectAck(15n);

  // Now that downstream is caught up, this should respond
  acker.onChange(['status', {ack: false}, {watermark: '0h'}]);
  expectAck(17n);
});

test('acker backup-gated', () => {
  const sink = {push: vi.fn()};

  let acks = 0;
  const expectAck = (expected: bigint) => {
    expect(sink.push).toBeCalledTimes(++acks);
    expect(sink.push.mock.calls[acks - 1][0]).toBe(expected);
  };
  const expectNoAck = () => {
    expect(sink.push).toBeCalledTimes(acks);
  };

  const acker = new Acker(sink);

  // Enable gating before any backup watermark is known: nothing is ACKed,
  // even once downstream confirms the commit.
  acker.setBackupWatermark('');
  acker.onChange(['begin', {tag: 'begin'}, {commitWatermark: '0d'}]);
  acker.ack('0d'); // desired=0d, but nothing is backed up yet
  expectNoAck();

  // A keepalive ahead of the backup is likewise withheld (would over-ACK the
  // slot past what is durably backed up).
  acker.onChange(['status', {ack: false}, {watermark: '0f'}]); // desired=0f
  expectNoAck();

  // Backup catches up to 0d: the slot is ACKed at exactly 0d (not the 0f head).
  acker.setBackupWatermark('0d');
  expectAck(13n);

  // Backup advances to 0e: slot follows, still clamped below the 0f head.
  acker.setBackupWatermark('0e');
  expectAck(14n);

  // Backup passes the head (0g > 0f): the ACK is capped at the desired 0f.
  acker.setBackupWatermark('0g');
  expectAck(15n);

  // Further backup progress with no new desired watermark does not re-ACK
  // (capped at the desired 0f).
  acker.setBackupWatermark('0h');
  expectNoAck();

  // A new commit lifts the desired watermark past the known backup (0h), so the
  // slot advances up to that backup immediately...
  acker.onChange(['begin', {tag: 'begin'}, {commitWatermark: '0j'}]);
  acker.ack('0j'); // desired=0j; flush clamps to backup 0h
  expectAck(17n);
  // ...and follows the backup the rest of the way once it reaches the commit.
  acker.setBackupWatermark('0j');
  expectAck(19n);
});
