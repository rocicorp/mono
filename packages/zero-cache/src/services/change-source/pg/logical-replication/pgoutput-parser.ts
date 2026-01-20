// Forked from https://github.com/kibae/pg-logical-replication/blob/c55abddc62eadd61bd38922037ecb7a1469fa8c3/src/output-plugins/pgoutput/pgoutput-parser.ts
/* oxlint-disable */

// https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html

// Replaced by TypeParsers
// import {types} from 'pg';

export type TypeParser = (val: string) => unknown;

import type {TypeParsers} from '../../../../db/pg-type-parser.ts';
import {BinaryReader} from './binary-reader.ts';
import type {
  Message,
  MessageBegin,
  MessageCommit,
  MessageDelete,
  MessageInsert,
  MessageMessage,
  MessageOrigin,
  MessageRelation,
  MessageTruncate,
  MessageType,
  MessageUpdate,
  RelationColumn,
} from './pgoutput.types.ts';

export class PgoutputParser {
  _typeCache = new Map<number, {typeSchema: string; typeName: string}>();
  _relationCache = new Map<number, MessageRelation>();

  // Replaces "pg-types" library.
  #typeParsers: TypeParsers;
  constructor(typeParsers: TypeParsers) {
    this.#typeParsers = typeParsers;
  }
  #getTypeParser(typeOid: number): TypeParser {
    return this.#typeParsers.getTypeParser(typeOid);
  }
  // Replaced "pg-types" library.

  public parse(buf: Buffer): Message {
    const reader = new BinaryReader(buf);
    const tag = reader.readUint8();

    switch (tag) {
      case 0x42 /*B*/:
        return this.msgBegin(reader);
      case 0x4f /*O*/:
        return this.msgOrigin(reader);
      case 0x59 /*Y*/:
        return this.msgType(reader);
      case 0x52 /*R*/:
        return this.msgRelation(reader);
      case 0x49 /*I*/:
        return this.msgInsert(reader);
      case 0x55 /*U*/:
        return this.msgUpdate(reader);
      case 0x44 /*D*/:
        return this.msgDelete(reader);
      case 0x54 /*T*/:
        return this.msgTruncate(reader);
      case 0x4d /*M*/:
        return this.msgMessage(reader);
      case 0x43 /*C*/:
        return this.msgCommit(reader);
      default:
        throw Error('unknown pgoutput message');
    }
  }

  private msgBegin(reader: BinaryReader): MessageBegin {
    // TODO lsn can be null if origin sended
    // https://github.com/postgres/postgres/blob/85c61ba8920ba73500e1518c63795982ee455d14/src/backend/replication/pgoutput/pgoutput.c#L409
    // https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/replication/reorderbuffer.h#L275

    return {
      tag: 'begin',
      commitLsn: reader.readLsn(),
      commitTime: reader.readTime(),
      xid: reader.readInt32(),
    };
  }

  private msgOrigin(reader: BinaryReader): MessageOrigin {
    return {
      tag: 'origin',
      originLsn: reader.readLsn(),
      originName: reader.readString(),
    };
  }

  private msgType(reader: BinaryReader): MessageType {
    const typeOid = reader.readInt32();
    const typeSchema = reader.readString();
    const typeName = reader.readString();

    // mem leak not likely to happen because amount of types is usually small
    this._typeCache.set(typeOid, {typeSchema, typeName});

    return {tag: 'type', typeOid, typeSchema, typeName};
  }

  private msgRelation(reader: BinaryReader): MessageRelation {
    // lsn expected to be null
    // https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/backend/replication/walsender.c#L1342
    const relationOid = reader.readInt32();
    const schema = reader.readString();
    const name = reader.readString();
    const replicaIdentity = this.readRelationReplicaIdentity(reader);
    const columns = reader.array(reader.readInt16(), () =>
      this.readRelationColumn(reader),
    );
    const keyColumns = columns.filter(it => it.flags & 0b1).map(it => it.name);

    const msg: MessageRelation = {
      tag: 'relation',
      relationOid,
      schema,
      name,
      replicaIdentity,
      columns,
      keyColumns,
    };

    // mem leak not likely to happen because amount of relations is usually small
    this._relationCache.set(relationOid, msg);

    return msg;
  }

  private readRelationReplicaIdentity(reader: BinaryReader) {
    // https://www.postgresql.org/docs/14/catalog-pg-class.html
    const ident = reader.readUint8();

    switch (ident) {
      case 0x64 /*d*/:
        return 'default';
      case 0x6e /*n*/:
        return 'nothing';
      case 0x66 /*f*/:
        return 'full';
      case 0x69 /*i*/:
        return 'index';
      default:
        throw Error(`unknown replica identity ${String.fromCharCode(ident)}`);
    }
  }

  private readRelationColumn(reader: BinaryReader): RelationColumn {
    const flags = reader.readUint8();
    const name = reader.readString();
    const typeOid = reader.readInt32();
    const typeMod = reader.readInt32();

    return {
      flags,
      name,
      typeOid,
      typeMod,
      typeSchema: null,
      typeName: null, // TODO resolve builtin type names?
      ...this._typeCache.get(typeOid),
      // parser: types.getTypeParser(typeOid),
      parser: this.#getTypeParser(typeOid),
    };
  }

  private msgInsert(reader: BinaryReader): MessageInsert {
    const relation = this._relationCache.get(reader.readInt32());

    if (!relation) {
      throw Error('missing relation');
    }

    reader.readUint8(); // consume the 'N' key

    return {
      tag: 'insert',
      relation,
      new: this.readTuple(reader, relation),
    };
  }

  private msgUpdate(reader: BinaryReader): MessageUpdate {
    const relation = this._relationCache.get(reader.readInt32());

    if (!relation) {
      throw Error('missing relation');
    }

    let key: Record<string, any> | null = null;
    let old: Record<string, any> | null = null;
    let new_: Record<string, any> | null = null;
    const subMsgKey = reader.readUint8();

    if (subMsgKey === 0x4b /*K*/) {
      key = this.readKeyTuple(reader, relation);
      reader.readUint8(); // consume the 'N' key
      new_ = this.readTuple(reader, relation);
    } else if (subMsgKey === 0x4f /*O*/) {
      old = this.readTuple(reader, relation);
      reader.readUint8(); // consume the 'N' key
      new_ = this.readTuple(reader, relation, old);
    } else if (subMsgKey === 0x4e /*N*/) {
      new_ = this.readTuple(reader, relation);
    } else {
      throw Error(`unknown submessage key ${String.fromCharCode(subMsgKey)}`);
    }

    return {tag: 'update', relation, key, old, new: new_};
  }

  private msgDelete(reader: BinaryReader): MessageDelete {
    const relation = this._relationCache.get(reader.readInt32());

    if (!relation) {
      throw Error('missing relation');
    }

    let key: Record<string, any> | null = null;
    let old: Record<string, any> | null = null;
    const subMsgKey = reader.readUint8();

    if (subMsgKey === 0x4b /*K*/) {
      key = this.readKeyTuple(reader, relation);
    } else if (subMsgKey === 0x4f /*O*/) {
      old = this.readTuple(reader, relation);
    } else {
      throw Error(`unknown submessage key ${String.fromCharCode(subMsgKey)}`);
    }

    return {tag: 'delete', relation, key, old};
  }

  private readKeyTuple(
    reader: BinaryReader,
    relation: MessageRelation,
  ): Record<string, any> {
    const tuple = this.readTuple(reader, relation);
    const key = Object.create(null);

    for (const k of relation.keyColumns) {
      // If value is `null`, then it is definitely not part of key,
      // because key cannot have nulls by documentation.
      // And if we got `null` while reading keyOnly tuple,
      // then it means that `null` is not actual value
      // but placeholder of non-key column.
      key[k] = tuple[k] === null ? undefined : tuple[k];
    }

    return key;
  }

  private readTuple(
    reader: BinaryReader,
    {columns}: MessageRelation,
    unchangedToastFallback?: Record<string, any> | null,
  ): Record<string, any> {
    const nfields = reader.readInt16();
    const tuple = Object.create(null);

    for (let i = 0; i < nfields; i++) {
      const {name, parser} = columns[i];
      const kind = reader.readUint8();

      switch (kind) {
        case 0x62: // 'b' binary
          const bsize = reader.readUint32();
          const bval = reader.read(bsize);
          // dont need to .slice() because new buffer
          // is created for each replication chunk
          tuple[name] = bval;
          break;
        case 0x74: // 't' text
          const valsize = reader.readUint32();
          const valbuf = reader.read(valsize);
          const valtext = reader.decodeText(valbuf);
          tuple[name] = parser(valtext);
          break;
        case 0x6e: // 'n' null
          tuple[name] = null;
          break;
        case 0x75: // 'u' unchanged toast datum
          tuple[name] = unchangedToastFallback?.[name];
          break;
        default:
          throw Error(`unknown attribute kind ${String.fromCharCode(kind)}`);
      }
    }

    return tuple;
  }

  private msgTruncate(reader: BinaryReader): MessageTruncate {
    const nrels = reader.readUint32();
    const flags = reader.readUint8();

    return {
      tag: 'truncate',
      cascade: Boolean(flags & 0b1),
      restartIdentity: Boolean(flags & 0b10),
      relations: reader.array(
        nrels,
        () => this._relationCache.get(reader.readInt32()) as MessageRelation,
      ),
    };
  }

  private msgMessage(reader: BinaryReader): MessageMessage {
    const flags = reader.readUint8();

    return {
      tag: 'message',
      flags,
      transactional: Boolean(flags & 0b1),
      messageLsn: reader.readLsn(),
      prefix: reader.readString(),
      content: reader.read(reader.readUint32()),
    };
  }

  private msgCommit(reader: BinaryReader): MessageCommit {
    return {
      tag: 'commit',
      flags: reader.readUint8(), // reserved unused
      commitLsn: reader.readLsn(), // should be the same as begin.commitLsn
      commitEndLsn: reader.readLsn(),
      commitTime: reader.readTime(),
    };
  }
}
