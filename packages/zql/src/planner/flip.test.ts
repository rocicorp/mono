import {describe, test, expect, beforeEach} from 'vitest';
import {
  flipJoins,
  isJoin,
  type VirtualConnection,
  type VirtualJoin,
  type VirtualNode,
} from './flip.ts';

function resetFlips(n: VirtualNode) {
  if (isJoin(n)) {
    n.flip = undefined;
    resetFlips(n.left);
    resetFlips(n.right);
  }
}

function collectDecisionsPreorder(n: VirtualNode): Array<[number, boolean]> {
  const out: Array<[number, boolean]> = [];
  (function dfs(x: VirtualNode) {
    if (!isJoin(x)) return;
    out.push([x.id, !!x.flip]);
    dfs(x.left);
    dfs(x.right);
  })(n);
  return out;
}

// ---------- build the exact tree from your image ----------
// Leaves
const issue: VirtualConnection = {id: 1, name: 'issue'};
const project: VirtualConnection = {id: 2, name: 'project'};
const member: VirtualConnection = {id: 3, name: 'project_member'};
const creator: VirtualConnection = {id: 4, name: 'creator'};

// J1 = project ⋈ project_member
const J1: VirtualJoin = {
  id: 101,
  name: 'project⋈project_member',
  left: project,
  right: member,
};

// J2 = issue ⋈ (project⋈project_member)
const J2: VirtualJoin = {
  id: 102,
  name: 'issue⋈(project⋈project_member)',
  left: issue,
  right: J1,
};

// J3 = (issue⋈(project⋈project_member)) ⋈ creator
const J3: VirtualJoin = {
  id: 103,
  name: '(issue⋈(project⋈project_member))⋈creator',
  left: J2,
  right: creator,
};

// convenience
const idMap = {J1: 101, J2: 102, J3: 103};

// ---------- plans ----------
const P = {
  base: [issue, project, member, creator], // issue, project, project_member, creator
  creatorFirst: [creator, issue, project, member], // creator, issue, project, project_member
  rightBlock: [project, member, issue, creator], // project, project_member, issue, creator
  swapInner: [issue, member, project, creator], // issue, project_member, project, creator
  bothRight: [member, project, issue, creator], // project_member, project, issue, creator
  rightThenAll: [creator, project, member, issue], // creator, project, project_member, issue
  allFlipped: [creator, member, project, issue], // creator, project_member, project, issue
  impossible: [project, issue, member, creator], // interleaves J2's sides -> impossible
};

// ---------- expectations per plan ----------
// decisions are returned/collected in preorder: [J3, J2, J1]
const E = {
  base: [
    [idMap.J3, false],
    [idMap.J2, false],
    [idMap.J1, false],
  ],
  creatorFirst: [
    [idMap.J3, true],
    [idMap.J2, false],
    [idMap.J1, false],
  ],
  rightBlock: [
    [idMap.J3, false],
    [idMap.J2, true],
    [idMap.J1, false],
  ],
  swapInner: [
    [idMap.J3, false],
    [idMap.J2, false],
    [idMap.J1, true],
  ],
  bothRight: [
    [idMap.J3, false],
    [idMap.J2, true],
    [idMap.J1, true],
  ],
  rightThenAll: [
    [idMap.J3, true],
    [idMap.J2, true],
    [idMap.J1, false],
  ],
  allFlipped: [
    [idMap.J3, true],
    [idMap.J2, true],
    [idMap.J1, true],
  ],
} satisfies Record<string, Array<[number, boolean]>>;

// ---------- run tests ----------
describe('flipJoins', () => {
  beforeEach(() => {
    resetFlips(J3);
  });

  test.each([
    ['base', P.base, E.base],
    ['creatorFirst', P.creatorFirst, E.creatorFirst],
    ['rightBlock', P.rightBlock, E.rightBlock],
    ['swapInner', P.swapInner, E.swapInner],
    ['bothRight', P.bothRight, E.bothRight],
    ['rightThenAll', P.rightThenAll, E.rightThenAll],
    ['allFlipped', P.allFlipped, E.allFlipped],
  ] as const)('%s', (name, plan, expected) => {
    const decisions = flipJoins(J3, plan);
    const decided = collectDecisionsPreorder(J3); // confirm mutation order == returned

    expect(decisions).toEqual(expected);
    expect(decided).toEqual(expected);

    // spot-check actual flags
    const byId = new Map(decided);
    expect(byId.get(idMap.J3)).toBe(expected[0][1]);
    expect(byId.get(idMap.J2)).toBe(expected[1][1]);
    expect(byId.get(idMap.J1)).toBe(expected[2][1]);
  });

  test('impossible plan throws', () => {
    expect(() => flipJoins(J3, P.impossible)).toThrow();
  });
});
