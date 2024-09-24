import {Source} from 'zql/src/zql/ivm/source.js';
import {AST} from 'zql/src/zql/ast/ast.js';
import {CommitListener} from 'zql/src/zql/query/query-impl.js';
import {Storage} from 'zql/src/zql/ivm/operator.js';

export class ConfigZqlContext {
  getSource(_name: string): Source {
    throw new Error('Not implemented');
  }

  addServerQuery(_ast: AST): () => void {
    throw new Error('Not implemented');
  }

  createStorage(): Storage {
    throw new Error('Not implemented');
  }

  onTransactionCommit(_cb: CommitListener): () => void {
    throw new Error('Not implemented');
  }
}
