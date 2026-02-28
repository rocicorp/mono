issue
.where('open', true)
.where(({cmp, or}) => or(cmp('visibility', 'public'), cmp(null, 'crew')))
.whereExists('project', q => q.where('lowerCaseName', 'gatewaycore'), {scalar: true})
.related('labels')
.related('viewState', q => q.where('userID', 'anon').limit(1))
.orderBy('modified', 'desc')
.orderBy('id', 'desc')
.limit(101)

we had discussed some slow query problems and documented theories in
@TERABUGS_CREATOR_CRASH_ANALYSIS.md

I now have a copy of the sqlite db locally in  
 `/tmp/zbugs-replica.db`

Please investigate our theories and the chosen query plans by
sqlite. Document your findings.

```shell
npx tsx packages/zero/src/analyze-query.ts \
 --replica-file=./tera.db \
 --schema-path=./apps/zbugs/shared/schema.ts \
 --query='issue.where("open", true).where(({cmpLit, cmp, or}) => or(cmp("visibility", "public"), cmpLit(null, "=", "crew"))).whereExists("project", q => q.where("lowerCaseName", "gatewaycore"), {scalar: true}).related("labels").related("viewState", q => q.where("userID", "anon").limit(1)).orderBy("modified", "desc").orderBy("id", "desc").limit(101)'
```

```shell
npx tsx packages/zero/src/analyze-query.ts \
 --replica-file=./tera.db \
 --schema-path=./apps/zbugs/shared/schema.ts \
 --query='issue.where("open", true).where(({cmpLit, cmp, or}) => or(cmp("visibility", "public"), cmpLit(null, "=", "crew"))).whereExists("project", q => q.where("lowerCaseName", "gatewaycore"), {scalar: true}).whereExists("creator", q => q.where("login", "aldasmith-predovic44"), {scalar: true}).related("labels").related("viewState", q => q.where("userID", "anon").limit(1)).orderBy("modified", "desc").orderBy("id", "desc").limit(101)'
```

hmm,... run zero with replica and no upstream?

- table source shouldn't re-print same plan
- should print AST of received query before table source plans
  - can marry back?
- remove queries from zbugs?
  - till offender goes away?
