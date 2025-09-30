export type VirtualNode = VirtualJoin | VirtualConnection;

export type VirtualJoin = {
  id: number;
  name: string;
  left: VirtualNode;
  right: VirtualNode;
  flip?: boolean | undefined;
};

export type VirtualConnection = {
  id: number;
  name: string;
};

export function isJoin(node: VirtualNode): node is VirtualJoin {
  return (node as VirtualJoin).left !== undefined;
}

/** Build a map from node -> set of leaf ids in its subtree */
function buildLeafSets(root: VirtualNode) {
  const memo = new Map<VirtualNode, Set<number>>();
  const dfs = (n: VirtualNode): Set<number> => {
    if (memo.has(n)) return memo.get(n)!;
    let s: Set<number>;
    if (isJoin(n)) {
      const L = dfs(n.left);
      const R = dfs(n.right);
      s = new Set([...L, ...R]);
    } else {
      s = new Set([n.id]);
    }
    memo.set(n, s);
    return s;
  };
  dfs(root);
  return memo;
}

type FlipDecision = [id: number, flip: boolean];

/**
 * Decide which joins must be flipped to realize `plan` (list of connection ids).
 * Mutates `flip` on the joins; also returns the decisions.
 * Throws if no flip configuration can yield the plan.
 */
export function flipJoins(bottom: VirtualJoin, planConns: VirtualConnection[]) {
  const plan = planConns.map(c => c.id);
  const leafSets = buildLeafSets(bottom);

  // Sanity: plan must be exactly the leaves under the root
  const allLeaves = leafSets.get(bottom)!;
  if (plan.length !== allLeaves.size || plan.some(id => !allLeaves.has(id))) {
    throw new Error("Plan doesn't match the tree's leaves");
  }

  const decisions: FlipDecision[] = [];

  function solve(node: VirtualNode, subseq: number[]) {
    if (!isJoin(node)) {
      if (subseq.length !== 1 || subseq[0] !== node.id) {
        throw new Error(`Impossible: expected leaf ${node.id} here`);
      }
      return;
    }

    const Lset = leafSets.get(node.left)!;
    const Rset = leafSets.get(node.right)!;

    // Split this subtree's subsequence into left/right (preserve order)
    const leftSeq: number[] = [];
    const rightSeq: number[] = [];
    for (const id of subseq) {
      if (Lset.has(id)) leftSeq.push(id);
      else if (Rset.has(id)) rightSeq.push(id);
      else throw new Error(`Unknown id ${id} in subtree of ${node.id}`);
    }

    // Determine which side appears first in this subsequence
    const first = subseq[0];
    const firstIsLeft = Lset.has(first);
    const flip = !firstIsLeft;

    // Enforce “contiguous block” constraint: at most one side switch.
    const expected = flip
      ? [...rightSeq, ...leftSeq]
      : [...leftSeq, ...rightSeq];
    for (let i = 0; i < subseq.length; i++) {
      if (subseq[i] !== expected[i]) {
        throw new Error(
          `Impossible order at join ${node.id}: leaves from left/right are interleaved`,
        );
      }
    }

    decisions.push([node.id, flip]);
    node.flip = flip;

    // Recurse into children
    solve(node.left, leftSeq);
    solve(node.right, rightSeq);
  }

  solve(bottom, plan);
  return decisions;
}
