import * as db from '../db/mod';
import type * as dag from '../dag/mod';
import type * as btree from '../btree/mod';
import {getMetaTypedDisplayName, Meta} from '../db/commit';
import {Digraph, Node, attribute, Edge, toDot} from 'ts-graphviz';
import {isInternalNode} from '../btree/node';
import type {Hash} from '../hash';

export class ViewerVisitor extends db.Visitor {
  private readonly _commitDotFileGraph = new Digraph();
  constructor(dagRead: dag.Read) {
    super(dagRead);
  }

  get commitDotFileGraph(): string {
    return toDot(this._commitDotFileGraph);
  }

  override async visitCommitChunk(
    chunk: dag.Chunk<db.CommitData<Meta>>,
  ): Promise<void> {
    const currentNode = this._createOrUpdateNode(
      chunk.hash,
      getAttributesForCommit(chunk),
    );

    const parentHash = chunk.data.meta.basisHash;
    if (parentHash !== null) {
      const parentNode = this._createOrUpdateNode(parentHash);
      this._commitDotFileGraph.addEdge(
        new Edge([currentNode, parentNode], {
          dir: 'forward',
        }),
      );
    }

    const bTreeHash = chunk.data.valueHash;
    if (bTreeHash !== null) {
      const bTreeRootNode = this._createOrUpdateNode(bTreeHash);

      this._commitDotFileGraph.addEdge(
        new Edge([currentNode, bTreeRootNode], {
          dir: 'forward',
        }),
      );
    }

    const indexHashes = chunk.data.indexes;
    for (const indexHash of indexHashes) {
      const valueHashString = indexHash.valueHash;
      const indexNode = this._createOrUpdateNode(valueHashString);
      this._commitDotFileGraph.addEdge(
        new Edge([currentNode, indexNode], {
          dir: 'forward',
        }),
      );
    }

    return super.visitCommitChunk(chunk);
  }

  private _createOrUpdateNode(
    hash: Hash,
    attributes?: Map<attribute.Node, string>,
  ): Node {
    const hashAsString = hash.toString();
    let currentNode = this._commitDotFileGraph.getNode(hashAsString);
    if (currentNode === undefined) {
      currentNode = new Node(hashAsString);
    }
    currentNode.attributes.set('fontsize', '6');
    currentNode.attributes.set('fontname', 'monospace');
    if (attributes) {
      for (const [key, value] of attributes) {
        currentNode.attributes.set(key, value);
      }
    }
    this._commitDotFileGraph.addNode(currentNode);
    return currentNode;
  }

  override async visitBTreeNodeChunk(
    chunk: dag.Chunk<btree.Node>,
  ): Promise<void> {
    const parentBTreeGraphNode = this._createOrUpdateNode(
      chunk.hash,
      getAttributesForBtreeNode(chunk),
    );
    const node = chunk.data;
    if (isInternalNode(node)) {
      for (const [, bTreeChildHash] of node[1]) {
        const childBTreeGraphNode = this._createOrUpdateNode(bTreeChildHash);
        this._commitDotFileGraph.addEdge(
          new Edge([parentBTreeGraphNode, childBTreeGraphNode], {
            dir: 'forward',
          }),
        );
      }
    }
    return super.visitBTreeNodeChunk(chunk);
  }
}
function getAttributesForCommit(
  chunk: dag.Chunk<db.CommitData<Meta>>,
): Map<attribute.Node, string> {
  const attributes = new Map();
  attributes.set('shape', 'box');
  attributes.set('style', 'filled');
  attributes.set('fillcolor', 'lightblue');
  const metaDisplayName = getMetaTypedDisplayName(chunk.data.meta.type);
  attributes.set('label', chunk.hash + ' | ' + metaDisplayName);
  return attributes;
}
function getAttributesForBtreeNode(
  chunk: dag.Chunk<btree.Node>,
): Map<attribute.Node, string> | undefined {
  const attributes = new Map();
  attributes.set('shape', 'circle');
  attributes.set('style', 'filled');
  attributes.set('fillcolor', '#e6e6e6');
  attributes.set('label', chunk.hash);
  return attributes;
}
