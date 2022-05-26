import * as db from '../db/mod';
import type * as dag from '../dag/mod';
import type * as btree from '../btree/mod';
import {getMetaTypedDisplayName, Meta, MetaTyped} from '../db/commit';
import {Digraph, Subgraph, Node, Edge, toDot, ISubgraph} from 'ts-graphviz';
import type {ReadonlyJSONValue} from '../json';

export class ViewerVisitor extends db.Visitor {
  private readonly _commitDotFileGraph = new Digraph();
  constructor(dagRead: dag.Read) {
    super(dagRead);
    this._commitDotFileGraph.addSubgraph(
      new Subgraph('commits', {label: 'Commits'}),
    );
  }

  get getCommitDotFileGraph(): string {
    return toDot(this._commitDotFileGraph);
  }

  override async visitCommitChunk(
    chunk: dag.Chunk<db.CommitData<Meta>>,
  ): Promise<void> {
    const commitsSubgraph = this._commitDotFileGraph.getSubgraph('commits');
    if (commitsSubgraph === undefined) {
      throw new Error('commits subgraph not found');
    }

    const currentNode = this._createOrUpdateNode(
      commitsSubgraph,
      chunk.hash.toString(),
      chunk.data.meta.type,
    );

    const parentHash = chunk.data.meta.basisHash;
    if (parentHash !== null) {
      const parentNode = this._createOrUpdateNode(
        commitsSubgraph,
        parentHash.toString(),
      );
      commitsSubgraph.addEdge(
        new Edge([parentNode, currentNode], {
          dir: 'forward',
        }),
      );
    }

    const bTreeHash = chunk.data.valueHash;
    if (bTreeHash !== null) {
      let bTreeSubGraph = this._commitDotFileGraph.getSubgraph(
        bTreeHash.toString(),
      );
      if (bTreeSubGraph === undefined) {
        bTreeSubGraph = new Subgraph(bTreeHash.toString(), {
          label: 'B+Tree',
        });
        this._commitDotFileGraph.addSubgraph(bTreeSubGraph);
      }

      const bTreeRootNode = this._createOrUpdateNode(
        bTreeSubGraph,
        bTreeHash.toString(),
      );

      commitsSubgraph.addEdge(
        new Edge([currentNode, bTreeRootNode], {
          dir: 'forward',
        }),
      );
    }

    const indexHashes = chunk.data.indexes;
    for (const indexHash of indexHashes) {
      const valueHashString = indexHash.valueHash.toString();
      let indexSubGraph = this._commitDotFileGraph.getSubgraph(valueHashString);
      if (indexSubGraph === undefined) {
        indexSubGraph = new Subgraph(valueHashString, {
          label: 'Index',
        });
        this._commitDotFileGraph.addSubgraph(indexSubGraph);
      }

      const indexNode = this._createOrUpdateNode(
        indexSubGraph,
        valueHashString,
      );
      commitsSubgraph.addEdge(
        new Edge([currentNode, indexNode], {
          dir: 'forward',
        }),
      );
    }

    return super.visitCommitChunk(chunk);
  }

  private _createOrUpdateNode(
    subGraph: ISubgraph | undefined,
    hash: string,
    metaTyped?: MetaTyped,
  ): Node {
    if (subGraph === undefined) {
      throw new Error('subGraph is undefined');
    }
    let currentNode = subGraph.getNode(hash);
    if (currentNode === undefined) {
      currentNode = new Node(hash);
    }
    const subGraphLabel = subGraph.get('label');
    if (subGraphLabel === 'B+Tree') {
      currentNode.attributes.set('shape', 'circle');
      currentNode.attributes.set('style', 'filled');
      currentNode.attributes.set('fillcolor', '#e6e6e6');
      currentNode.attributes.set('label', hash);
    } else if (subGraphLabel === 'Index') {
      currentNode.attributes.set('shape', 'triangle');
      currentNode.attributes.set('style', 'filled');
      currentNode.attributes.set('fillcolor', 'red');
      currentNode.attributes.set('label', hash);
    } else {
      currentNode.attributes.set('shape', 'box');
      currentNode.attributes.set('style', 'filled');
      currentNode.attributes.set('fillcolor', 'lightblue');
      if (metaTyped) {
        const metaDisplayName = getMetaTypedDisplayName(metaTyped);
        currentNode.attributes.set('label', metaDisplayName + '|' + hash);
      }
    }
    currentNode.attributes.set('fontsize', '6');
    currentNode.attributes.set('fontname', 'monospace');
    subGraph.addNode(currentNode);
    return currentNode;
  }

  private _createOrUpdateBTreeNode(
    subGraph: ISubgraph | undefined,
    bTreeEntry: readonly btree.Entry<ReadonlyJSONValue>[],
  ): Node | undefined {
    if (subGraph === undefined) {
      throw new Error('subGraph is undefined');
    }
    if (bTreeEntry.length === 0) {
      return undefined;
    }
    let currentNode = subGraph.getNode(JSON.stringify(bTreeEntry));
    if (currentNode === undefined) {
      currentNode = new Node(JSON.stringify(bTreeEntry));
    }
    currentNode.attributes.set('shape', 'circle');
    currentNode.attributes.set('style', 'filled');
    currentNode.attributes.set('fillcolor', '#e6e6e6');
    currentNode.attributes.set('label', JSON.stringify(bTreeEntry));
    currentNode.attributes.set('fontsize', '6');
    currentNode.attributes.set('fontname', 'monospace');
    subGraph.addNode(currentNode);
    return currentNode;
  }

  override async visitBTreeNodeChunk(
    chunk: dag.Chunk<btree.Node>,
  ): Promise<void> {
    const bTreeSubGraph = this._commitDotFileGraph.getSubgraph(
      chunk.hash.toString(),
    );
    if (bTreeSubGraph === undefined) {
      throw new Error('bTreeSubGraph is undefined');
    }
    const parentBtreeNode = bTreeSubGraph.getNode(chunk.hash.toString());
    if (parentBtreeNode === undefined) {
      throw new Error('parentBtreeNode not found');
    }
    for (const [, bTreeChild] of chunk.data.entries()) {
      if (typeof bTreeChild === 'object') {
        const valueNode = this._createOrUpdateBTreeNode(
          bTreeSubGraph,
          bTreeChild,
        );
        if (valueNode !== undefined) {
          bTreeSubGraph.addEdge(
            new Edge([parentBtreeNode, valueNode], {
              dir: 'forward',
            }),
          );
        }
      }
    }
    return super.visitBTreeNodeChunk(chunk);
  }
}
