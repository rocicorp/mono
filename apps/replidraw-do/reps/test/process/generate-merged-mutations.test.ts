import { ClientMutation } from "../../src/types/client-mutation";
import { ClientMap } from "../../src/types/client-state";
import { generateMergedMutations } from "../../src/process/generate-merged-mutations";
import { client, clientMutation, Mocket, mutation } from "../util/test-utils";

test("generateMergedMutations", () => {
  type Case = {
    name: string;
    clients: ClientMap;
    expected: ClientMutation[];
  };
  const cases: Case[] = [
    {
      name: "empty",
      clients: new Map(),
      expected: [],
    },
    {
      name: "one mutation",
      clients: new Map([client("c1", new Mocket(), 1, mutation(1))]),
      expected: [clientMutation("c1", 1)],
    },
    {
      name: "multiple mutations across clients in order",
      clients: new Map([
        client(
          "c1",
          new Mocket(),
          1,
          mutation(1, "a", null, 3),
          mutation(2, "a", null, 4),
          mutation(3, "a", null, 9)
        ),
        client(
          "c2",
          new Mocket(),
          1,
          mutation(4, "a", null, 1),
          mutation(5, "a", null, 5),
          mutation(6, "a", null, 6)
        ),
        client(
          "c3",
          new Mocket(),
          1,
          mutation(5, "a", null, 2),
          mutation(6, "a", null, 7),
          mutation(7, "a", null, 8)
        ),
      ]),
      expected: [
        clientMutation("c2", 4, "a", null, 1),
        clientMutation("c3", 5, "a", null, 2),
        clientMutation("c1", 1, "a", null, 3),
        clientMutation("c1", 2, "a", null, 4),
        clientMutation("c2", 5, "a", null, 5),
        clientMutation("c2", 6, "a", null, 6),
        clientMutation("c3", 6, "a", null, 7),
        clientMutation("c3", 7, "a", null, 8),
        clientMutation("c1", 3, "a", null, 9),
      ],
    },
    {
      name: "ooo timestamps",
      clients: new Map([
        client(
          "c1",
          new Mocket(),
          1,
          mutation(1, "a", null, 1),
          mutation(2, "a", null, 4),
          mutation(3, "a", null, 3)
        ),
        client(
          "c2",
          new Mocket(),
          1,
          mutation(4, "a", null, 2),
          mutation(5, "a", null, 1),
          mutation(6, "a", null, 5)
        ),
      ]),
      expected: [
        clientMutation("c1", 1, "a", null, 1),
        clientMutation("c2", 4, "a", null, 2),
        clientMutation("c2", 5, "a", null, 1),
        clientMutation("c1", 2, "a", null, 4),
        clientMutation("c1", 3, "a", null, 3),
        clientMutation("c2", 6, "a", null, 5),
      ],
    },
  ];
  for (const c of cases) {
    const gen = generateMergedMutations(c.clients);
    for (const [, m] of c.expected.entries()) {
      expect(gen.next().value).toEqual(m);
    }
    expect(gen.next().done);
  }
});
