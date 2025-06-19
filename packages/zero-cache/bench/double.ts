import '../../shared/src/dotenv.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Database} from '../../zqlite/src/db.ts';

type Options = {
  dbFile: string;
};

export function double(opts: Options) {
  const {dbFile} = opts;
  const db = new Database(createSilentLogContext(), dbFile);

  const insertIssue = db.prepare(
    `INSERT INTO issue (
      id, shortID, title, open, modified, created, 
      creatorID, assigneeID, description, labelIDs, _0_version
    ) VALUES (
      @id, @shortID, @title, @open, @modified, @created, 
      @creatorID, @assigneeID, @description, @labelIDs, @_0_version
    );`,
  );
  const issueLabels = db.prepare(`SELECT * from issueLabel WHERE issueID = ?`);
  const insertIssueLabel = db.prepare(
    `INSERT INTO issueLabel (
      labelID, issueID, _0_version
    ) VALUES (
      @labelID, @issueID, @_0_version
     )`,
  );

  let newIssues = 0;
  let newIssueLabels = 1;
  for (const issue of db.prepare(`SELECT * FROM issue`).all() as any[]) {
    newIssues++;
    const id = Math.floor(Math.random() * Number.MAX_SAFE_INTEGER).toString(36);
    const newIssue = {...(issue as any), id};
    insertIssue.run(newIssue);

    for (const issueLabel of issueLabels.all(issue.id) as any[]) {
      newIssueLabels++;
      const newIssueLabel = {...issueLabel, issueID: id};
      insertIssueLabel.run(newIssueLabel);
    }
  }

  console.info(`Created ${newIssues} new issues with ${newIssueLabels} labels`);
  db.close();
}

double({dbFile: '/tmp/bench/zbugs-sync-replica.db'});
