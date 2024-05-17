import type {ErrorKind} from 'zero-protocol';

export class ProtocolError extends Error {
  readonly kind: ErrorKind;
  readonly msg: string;

  constructor(kind: ErrorKind, msg: string) {
    super(`${kind}: ${msg}`);
    this.kind = kind;
    this.msg = msg;
  }
}
