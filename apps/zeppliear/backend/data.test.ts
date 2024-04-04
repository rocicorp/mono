import { expect } from "chai";
import { Issue, Priority, Status, Comment } from "../frontend/issue";
import { setup, teardown, test } from "mocha";
import type { JSONValue } from "replicache";
import {
  createDatabase,
  delEntries,
  getEntry,
  getVersion,
  initSpace,
  putEntries,
  SampleData,
  BASE_SPACE_ID,
  getIssueEntries,
  getNonIssueEntriesInSyncOrder,
} from "./data";
import { transact, withExecutor } from "./pg";

const i1: Issue = {
  priority: Priority.HIGH,
  id: "1",
  title: "Issue 1",
  status: Status.IN_PROGRESS,
  modified: 0,
  created: 0,
  creator: "testUser1",
  kanbanOrder: "1",
};

const comment1i1: Comment = {
  id: "1",
  issueID: "1",
  created: 0,
  body: "Comment 1",
  creator: "testUser1",
};

const comment2i1: Comment = {
  id: "2",
  issueID: "1",
  created: 0,
  body: "Comment 2",
  creator: "testUser2",
};

const i2: Issue = {
  priority: Priority.MEDIUM,
  id: "2",
  title: "Issue 2",
  status: Status.IN_PROGRESS,
  modified: 0,
  created: 0,
  creator: "testUser2",
  kanbanOrder: "2",
};

const comment1i2: Comment = {
  id: "1",
  issueID: "2",
  created: 0,
  body: "Comment 1",
  creator: "testUser1",
};

const i3: Issue = {
  priority: Priority.LOW,
  id: "3",
  title: "Issue 3",
  status: Status.TODO,
  modified: 0,
  created: 0,
  creator: "testUser3",
  kanbanOrder: "3",
};

export const testSampleData: SampleData = [
  {
    issue: i1,
    description: "Description 1",
    comments: [comment1i1, comment2i1],
  },
  { issue: i2, description: "Description 2", comments: [comment1i2] },
  { issue: i3, description: "Description 3", comments: [] },
];

function getTestSyncOrder(key: string) {
  return `${key}-testSyncOrder`;
}

setup(async () => {
  // TODO: This is a very expensive way to unit test :).
  // Is there an in-memory postgres or something?
  await transact((executor) => createDatabase(executor));
});

teardown(async () => {
  await withExecutor(async (executor) => {
    await executor(`delete from entry where spaceid = $1`, [BASE_SPACE_ID]);
    await executor(`delete from space where id = $1`, [BASE_SPACE_ID]);
    await executor(`delete from entry where spaceid like 'test-s-%'`);
    await executor(`delete from space where id like 'test-s-%'`);
  });
});

test("getEntry", async () => {
  type Case = {
    name: string;
    exists: boolean;
    deleted: boolean;
    validJSON: boolean;
  };
  const cases: Case[] = [
    {
      name: "does not exist",
      exists: false,
      deleted: false,
      validJSON: false,
    },
    {
      name: "exists, deleted",
      exists: true,
      deleted: true,
      validJSON: true,
    },
    {
      name: "exists, not deleted, invalid JSON",
      exists: true,
      deleted: false,
      validJSON: false,
    },
    {
      name: "exists, not deleted, valid JSON",
      exists: true,
      deleted: false,
      validJSON: true,
    },
  ];

  await withExecutor(async (executor) => {
    for (const c of cases) {
      await executor(
        `delete from entry where spaceid = 'test-s-s1' and key = 'foo'`
      );
      if (c.exists) {
        await executor(
          `insert into entry (spaceid, key, value, syncorder, deleted, version, lastmodified) values ('test-s-s1', 'foo', $1, '${getTestSyncOrder(
            "foo"
          )}',$2, 1, now())`,
          [c.validJSON ? JSON.stringify(42) : "not json", c.deleted]
        );
      }

      const promise = getEntry(executor, "test-s-s1", "foo");
      let result: JSONValue | undefined;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let error: any | undefined;
      await promise.then(
        (r) => (result = r),
        (e) => (error = String(e))
      );
      if (!c.exists) {
        expect(result, c.name).undefined;
        expect(error, c.name).undefined;
      } else if (c.deleted) {
        expect(result, c.name).undefined;
        expect(error, c.name).undefined;
      } else if (!c.validJSON) {
        expect(result, c.name).undefined;
        expect(error, c.name).contains("SyntaxError");
      } else {
        expect(result, c.name).eq(42);
        expect(error, c.name).undefined;
      }
    }
  });
});

test("getEntry RoundTrip types", async () => {
  await withExecutor(async (executor) => {
    await putEntries(
      executor,
      "test-s-s1",
      [
        ["boolean", true, getTestSyncOrder("boolean")],
        ["number", 42, getTestSyncOrder("number")],
        ["string", "foo", getTestSyncOrder("string")],
        ["array", [1, 2, 3], getTestSyncOrder("array")],
        ["object", { a: 1, b: 2 }, getTestSyncOrder("object")],
      ],
      1
    );
    expect(await getEntry(executor, "test-s-s1", "boolean")).eq(true);
    expect(await getEntry(executor, "test-s-s1", "number")).eq(42);
    expect(await getEntry(executor, "test-s-s1", "string")).eq("foo");
    expect(await getEntry(executor, "test-s-s1", "array")).deep.equal([
      1,
      2,
      3,
    ]);
    expect(await getEntry(executor, "test-s-s1", "object")).deep.equal({
      a: 1,
      b: 2,
    });
  });
});

test("putEntries", async () => {
  type Case = {
    name: string;
    duplicate: boolean;
    deleted: boolean;
  };

  const cases: Case[] = [
    {
      name: "no duplicate",
      duplicate: false,
      deleted: false,
    },
    {
      name: "duplicate",
      duplicate: true,
      deleted: false,
    },
    {
      name: "deleted",
      duplicate: true,
      deleted: true,
    },
  ];

  await withExecutor(async (executor) => {
    for (const c of cases) {
      await executor(
        `delete from entry where spaceid = 'test-s-s1' and key = 'bar'`
      );
      await executor(
        `delete from entry where spaceid = 'test-s-s1' and key = 'foo'`
      );

      if (c.duplicate) {
        await putEntries(
          executor,
          "test-s-s1",
          [["foo", 41, getTestSyncOrder("foo")]],
          1
        );
        if (c.deleted) {
          await delEntries(executor, "test-s-s1", ["foo"], 1);
        }
      }
      const res: Promise<void> = putEntries(
        executor,
        "test-s-s1",
        [
          ["bar", 100, getTestSyncOrder("bar")],
          ["foo", 42, getTestSyncOrder("foo")],
        ],
        2
      );
      await res.catch(() => ({}));

      const qr = await executor(
        `select spaceid, key, value, deleted, version
        from entry where spaceid = 'test-s-s1' and key in ('bar', 'foo') order by key`
      );
      const [barRow, fooRow] = qr.rows;

      expect(fooRow, c.name).not.undefined;
      {
        const { spaceid, key, value, deleted, version } = fooRow;
        expect(spaceid, c.name).eq("test-s-s1");
        expect(key, c.name).eq("foo");
        expect(value, c.name).eq("42");
        expect(deleted, c.name).false;
        expect(version, c.name).eq(2);
      }
      {
        const { spaceid, key, value, deleted, version } = barRow;
        expect(spaceid, c.name).eq("test-s-s1");
        expect(key, c.name).eq("bar");
        expect(value, c.name).eq("100");
        expect(deleted, c.name).false;
        expect(version, c.name).eq(2);
      }
    }
  });
});

test("delEntries", async () => {
  type Case = {
    name: string;
    exists: boolean;
  };
  const cases: Case[] = [
    {
      name: "does not exist",
      exists: false,
    },
    {
      name: "exists",
      exists: true,
    },
  ];
  for (const c of cases) {
    await withExecutor(async (executor) => {
      await executor(
        `delete from entry where spaceid = 'test-s-s1' and key = 'bar'`
      );
      await executor(
        `delete from entry where spaceid = 'test-s-s1' and key = 'foo'`
      );
      await executor(
        `insert into entry (spaceid, key, value, syncorder, deleted, version, lastmodified) values ('test-s-s1', 'bar', '100', '${getTestSyncOrder(
          "bar"
        )}', false, 1, now())`
      );
      if (c.exists) {
        await executor(
          `insert into entry (spaceid, key, value, syncorder, deleted, version, lastmodified) values ('test-s-s1', 'foo', '42', '${getTestSyncOrder(
            "foo"
          )}',false, 1, now())`
        );
      }

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let error: any | undefined;
      await delEntries(executor, "test-s-s1", ["bar", "foo"], 2).catch(
        (e) => (error = String(e))
      );

      const qr = await executor(
        `
        select spaceid, key, value, deleted, version from entry 
        where spaceid = 'test-s-s1' and key in ('bar', 'foo') order by key
        `
      );
      const [barRow, fooRow] = qr.rows;

      expect(barRow, c.name).not.undefined;
      const { spaceid, key, value, deleted, version } = barRow;
      expect(spaceid, c.name).eq("test-s-s1");
      expect(key, c.name).eq("bar");
      expect(value, c.name).eq("100");
      expect(deleted, c.name).true;
      expect(version, c.name).eq(2);
      if (c.exists) {
        expect(fooRow, c.name).not.undefined;
        const { spaceid, key, value, deleted, version } = fooRow;
        expect(spaceid, c.name).eq("test-s-s1");
        expect(key, c.name).eq("foo");
        expect(value, c.name).eq("42");
        expect(deleted, c.name).true;
        expect(version, c.name).eq(2);
      } else {
        expect(fooRow, c.name).undefined;
        expect(error, c.name).undefined;
      }
    });
  }
});

test("initSpace", async () => {
  await withExecutor(async (executor) => {
    await executor(`delete from entry where spaceid = $1`, [BASE_SPACE_ID]);
    await executor(`delete from space where id = $1`, [BASE_SPACE_ID]);
    const testSpaceID1 = await initSpace(executor, () =>
      Promise.resolve(testSampleData)
    );
    expect(await getVersion(executor, testSpaceID1)).eq(1);
    // 3 issues
    expect((await getIssueEntries(executor, testSpaceID1)).length).eq(3);
    // 3 descriptions, and 3 comments
    expect(
      (await getNonIssueEntriesInSyncOrder(executor, testSpaceID1, "", 10))
        .entries.length
    ).eq(6);
    const testSpaceID2 = await initSpace(executor, () => {
      throw new Error("unexpected call to getSampleIssues on subsequent calls");
    });
    expect(await getVersion(executor, testSpaceID2)).eq(1);
    // 3 issues
    expect((await getIssueEntries(executor, testSpaceID2)).length).eq(3);
    // 3 descriptions, and 3 comments
    expect(
      (await getNonIssueEntriesInSyncOrder(executor, testSpaceID2, "", 10))
        .entries.length
    ).eq(6);
  });
});
