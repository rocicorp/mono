INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('Kec_-qIVi1mnXy82tKGhT','y8cMnHKAiOfdv7P4gCFXn',1726270936000,'```sql
SELECT application_name, query FROM pg_stat_activity
```

looks reasonable, and is under the `max_connections` limit of 100.

<img width="1282" alt="Screenshot 2024-09-13 at 16 38 50" src="https://github.com/user-attachments/assets/2671640f-695f-4168-bf87-c540d475f760">
','ieK09sy2C_AIWE8KRkrQR'),
	 ('4GhA9f2XpQlXGoyGfwZyg','AWj8hMrcbwFLPAXUWSZ1m',1726195253000,'Fuller logs:

```
worker=syncer pid=38000 Initial snapshot at version 62oupsxs
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i clientID=5s255ctth1l5589rh9 wsID=R9Bla78BvUTK5Qr9ksQ-q cmd=initConnection initConnection {"desiredQueriesPatch":[{"op":"put","hash":"1e0n4ppfygy4o","ast":{"table":"issue","related":[{"correlation":{"parentField":"id","childField":"issueID","op":"="},"subquery":{"table":"issueLabel","alias":"labels","related":[{"correlation":{"parentField":"labelID","childField":"id","op":"="},"hidden":true,"subquery":{"table":"label","alias":"labels","orderBy":[["id","asc"]]}}],"orderBy":[["id","asc"]]}}],"limit":1000,"orderBy":[["modified","desc"],["id","asc"]]}},{"op":"put","hash":"11nt7amu58bv0","ast":{"table":"issue","related":[{"correlation":{"parentField":"id","childField":"issueID","op":"="},"subquery":{"table":"issueLabel","alias":"labels","related":[{"correlation":{"parentField":"labelID","childField":"id","op":"="},"hidden":true,"subquery":{"table":"label","alias":"labels","orderBy":[["id","asc"]]}}],"orderBy":[["id","asc"]]}}],"limit":1000,"orderBy":[["created","desc"],["id","asc"]]}},{"op":"put","hash":"2spa7ygo3r0xx","ast":{"table":"issue","related":[{"correlation":{"parentField":"id","childField":"issueID","op":"="},"subquery":{"table":"issueLabel","alias":"labels","related":[{"correlation":{"parentField":"labelID","childField":"id","op":"="},"hidden":true,"subquery":{"table":"label","alias":"labels","orderBy":[["id","asc"]]}}],"orderBy":[["id","asc"]]}}],"limit":1000,"orderBy":[["priority","desc"],["id","asc"]]}},{"op":"put","hash":"1sc79sfpqd36w","ast":{"table":"issue","related":[{"correlation":{"parentField":"id","childField":"issueID","op":"="},"subquery":{"table":"issueLabel","alias":"labels","related":[{"correlation":{"parentField":"labelID","childField":"id","op":"="},"hidden":true,"subquery":{"table":"label","alias":"labels","orderBy":[["id","asc"]]}}],"orderBy":[["id","asc"]]}}],"limit":1000,"orderBy":[["status","desc"],["id","asc"]]}},{"op":"put","hash":"215shknp2leti","ast":{"table":"issue","related":[{"correlation":{"parentField":"id","childField":"issueID","op":"="},"subquery":{"table":"comment","alias":"comments","related":[{"correlation":{"parentField":"creatorID","childField":"id","op":"="},"subquery":{"table":"member","alias":"creator","orderBy":[["id","asc"]]}}],"limit":10,"orderBy":[["id","asc"]]}},{"correlation":{"parentField":"id","childField":"issueID","op":"="},"subquery":{"table":"issueLabel","alias":"labels","related":[{"correlation":{"parentField":"labelID","childField":"id","op":"="},"hidden":true,"subquery":{"table":"label","alias":"labels","orderBy":[["id","asc"]]}}],"orderBy":[["id","asc"]]}}],"limit":500,"orderBy":[["modified","desc"],["id","asc"]]}},{"op":"put","hash":"3u6wkjf2cegfa","ast":{"table":"member","where":[{"type":"simple","op":"=","field":"name","value":"amos"}],"orderBy":[["id","asc"]]}}]}
worker=syncer pid=38000 loaded CVR @00 (25 ms)
worker=syncer pid=38000 62oupsxs => 62oupsxs: 0 changes
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i CVR (00) is behind db 62oupsxs
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i cvrVersion=00 all clients up to date. no config catchup.
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i cvrVersion=00 all clients up to date. no config catchup.
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i cvrVersion=00 sent 0 row patches
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i clientID=5s255ctth1l5589rh9 wsID=R9Bla78BvUTK5Qr9ksQ-q cmd=initConnection applying 6 query patches
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i {"name":"Error","message":"CVR@00\" does not match DB@62oupsxs","stack":"Error: CVR@00\" does not match DB@62oupsxs\n    at assert (file:///Users/ocean/roci/mono/packages/shared/src/asserts.ts:3:11)\n    at ViewSyncerService.#syncQueryPipelineSet (file:///Users/ocean/roci/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts:435:5)\n    at processTicksAndRejections (node:internal/process/task_queues:95:5)\n    at async file:///Users/ocean/roci/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts:134:11\n    at async run (file:///Users/ocean/roci/mono/node_modules/@rocicorp/lock/out/lock.js:47:16)\n    at async ViewSyncerService.run (file:///Users/ocean/roci/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts:115:9)"}
worker=syncer pid=38000 closed database connections
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i clientID=5s255ctth1l5589rh9 wsID=R9Bla78BvUTK5Qr9ksQ-q client closed with error {"name":"Error","message":"CVR@00\" does not match DB@62oupsxs","stack":"Error: CVR@00\" does not match DB@62oupsxs\n    at assert (file:///Users/ocean/roci/mono/packages/shared/src/asserts.ts:3:11)\n    at ViewSyncerService.#syncQueryPipelineSet (file:///Users/ocean/roci/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts:435:5)\n    at processTicksAndRejections (node:internal/process/task_queues:95:5)\n    at async file:///Users/ocean/roci/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts:134:11\n    at async run (file:///Users/ocean/roci/mono/node_modules/@rocicorp/lock/out/lock.js:47:16)\n    at async ViewSyncerService.run (file:///Users/ocean/roci/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts:115:9)"}
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i view-syncer stopped

/// Culprit here ///

worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i clientID=5s255ctth1l5589rh9 wsID=R9Bla78BvUTK5Qr9ksQ-q cmd=initConnection flushed CVR {"instances":3,"queries":7,"desires":6,"clients":1,"rows":0,"statements":17} in (50 ms)
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i no more clients. starting idle timer
worker=syncer pid=38000 connection clientID=5s255ctth1l5589rh9 clientGroupID=0mrtpo7idt0o3d5a6i wsID=R9Bla78BvUTK5Qr9ksQ-q errorKind=Internal Sending error on WebSocket ["error","Internal","Error: CVR@00\" does not match DB@62oupsxs"] {"name":"Error","message":"CVR@00\" does not match DB@62oupsxs","stack":"Error: CVR@00\" does not match DB@62oupsxs\n    at assert (file:///Users/ocean/roci/mono/packages/shared/src/asserts.ts:3:11)\n    at ViewSyncerService.#syncQueryPipelineSet (file:///Users/ocean/roci/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts:435:5)\n    at processTicksAndRejections (node:internal/process/task_queues:95:5)\n    at async file:///Users/ocean/roci/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts:134:11\n    at async run (file:///Users/ocean/roci/mono/node_modules/@rocicorp/lock/out/lock.js:47:16)\n    at async ViewSyncerService.run (file:///Users/ocean/roci/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts:115:9)"}
worker=syncer pid=38000 connection clientID=5s255ctth1l5589rh9 clientGroupID=0mrtpo7idt0o3d5a6i wsID=R9Bla78BvUTK5Qr9ksQ-q close
worker=dispatcher pid=37984 received request 127.0.0.1:3000 /api/canary/v0/get?id=rWUdcmHCuGD6dDV5I2rGP
```

I think we''re somehow missing the await for the initial flush.  @grgbkr 
','ieK09sy2C_AIWE8KRkrQR'),
	 ('tZ51xfpXnSWgv2JEpF8k3','AWj8hMrcbwFLPAXUWSZ1m',1726195544000,'Some promising leads with more logs:

```
worker=dispatcher pid=37984 connecting 0mrtpo7idt0o3d5a6i to syncer 13
worker=syncer pid=38000 Initial snapshot at version 62oupsxs
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i clientID=5s255ctth1l5589rh9 wsID=R9Bla78BvUTK5Qr9ksQ-q cmd=initConnection initConnection {"desiredQueriesPatch":[{"op":"put","hash":"1e0n4ppfygy4o","ast":{"table":"issue","related":[{"correlation":{"parentField":"id","childField":"issueID","op":"="},"subquery":{"table":"issueLabel","alias":"labels","related":[{"correlation":{"parentField":"labelID","childField":"id","op":"="},"hidden":true,"subquery":{"table":"label","alias":"labels","orderBy":[["id","asc"]]}}],"orderBy":[["id","asc"]]}}],"limit":1000,"orderBy":[["modified","desc"],["id","asc"]]}},{"op":"put","hash":"11nt7amu58bv0","ast":{"table":"issue","related":[{"correlation":{"parentField":"id","childField":"issueID","op":"="},"subquery":{"table":"issueLabel","alias":"labels","related":[{"correlation":{"parentField":"labelID","childField":"id","op":"="},"hidden":true,"subquery":{"table":"label","alias":"labels","orderBy":[["id","asc"]]}}],"orderBy":[["id","asc"]]}}],"limit":1000,"orderBy":[["created","desc"],["id","asc"]]}},{"op":"put","hash":"2spa7ygo3r0xx","ast":{"table":"issue","related":[{"correlation":{"parentField":"id","childField":"issueID","op":"="},"subquery":{"table":"issueLabel","alias":"labels","related":[{"correlation":{"parentField":"labelID","childField":"id","op":"="},"hidden":true,"subquery":{"table":"label","alias":"labels","orderBy":[["id","asc"]]}}],"orderBy":[["id","asc"]]}}],"limit":1000,"orderBy":[["priority","desc"],["id","asc"]]}},{"op":"put","hash":"1sc79sfpqd36w","ast":{"table":"issue","related":[{"correlation":{"parentField":"id","childField":"issueID","op":"="},"subquery":{"table":"issueLabel","alias":"labels","related":[{"correlation":{"parentField":"labelID","childField":"id","op":"="},"hidden":true,"subquery":{"table":"label","alias":"labels","orderBy":[["id","asc"]]}}],"orderBy":[["id","asc"]]}}],"limit":1000,"orderBy":[["status","desc"],["id","asc"]]}},{"op":"put","hash":"215shknp2leti","ast":{"table":"issue","related":[{"correlation":{"parentField":"id","childField":"issueID","op":"="},"subquery":{"table":"comment","alias":"comments","related":[{"correlation":{"parentField":"creatorID","childField":"id","op":"="},"subquery":{"table":"member","alias":"creator","orderBy":[["id","asc"]]}}],"limit":10,"orderBy":[["id","asc"]]}},{"correlation":{"parentField":"id","childField":"issueID","op":"="},"subquery":{"table":"issueLabel","alias":"labels","related":[{"correlation":{"parentField":"labelID","childField":"id","op":"="},"hidden":true,"subquery":{"table":"label","alias":"labels","orderBy":[["id","asc"]]}}],"orderBy":[["id","asc"]]}}],"limit":500,"orderBy":[["modified","desc"],["id","asc"]]}},{"op":"put","hash":"3u6wkjf2cegfa","ast":{"table":"member","where":[{"type":"simple","op":"=","field":"name","value":"amos"}],"orderBy":[["id","asc"]]}}]}
worker=syncer pid=38000 loaded CVR @00 (25 ms)
worker=syncer pid=38000 62oupsxs => 62oupsxs: 0 changes
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i CVR (00) is behind db 62oupsxs
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i cvrVersion=00 all clients up to date. no config catchup.
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i cvrVersion=00 all clients up to date. no config catchup.
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i cvrVersion=00 sent 0 row patches
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i clientID=5s255ctth1l5589rh9 wsID=R9Bla78BvUTK5Qr9ksQ-q cmd=initConnection applying 6 query patches

worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i 
{"name":"Error","message":"CVR@00\" does not match DB@62oupsxs",
"stack":"Error: CVR@00\" does not match DB@62oupsxs\n
    at assert (file:///Users/ocean/roci/mono/packages/shared/src/asserts.ts:3:11)\n
    at ViewSyncerService.#syncQueryPipelineSet (file:///Users/ocean/roci/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts:435:5)\n
    at processTicksAndRejections (node:internal/process/task_queues:95:5)\n
    at async file:///Users/ocean/roci/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts:134:11\n
    at async run (file:///Users/ocean/roci/mono/node_modules/@rocicorp/lock/out/lock.js:47:16)\n
    at async ViewSyncerService.run (file:///Users/ocean/roci/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts:115:9)"}

worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i {"name":"Error","message":"CVR@00\" does not match DB@62oupsxs","stack":"Error: CVR@00\" does not match DB@62oupsxs\n    at assert (file:///Users/ocean/roci/mono/packages/shared/src/asserts.ts:3:11)\n    at ViewSyncerService.#syncQueryPipelineSet (file:///Users/ocean/roci/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts:435:5)\n    at processTicksAndRejections (node:internal/process/task_queues:95:5)\n    at async file:///Users/ocean/roci/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts:134:11\n    at async run (file:///Users/ocean/roci/mono/node_modules/@rocicorp/lock/out/lock.js:47:16)\n    at async ViewSyncerService.run (file:///Users/ocean/roci/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts:115:9)"}
worker=syncer pid=38000 closed database connections
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i clientID=5s255ctth1l5589rh9 wsID=R9Bla78BvUTK5Qr9ksQ-q client closed with error {"name":"Error","message":"CVR@00\" does not match DB@62oupsxs","stack":"Error: CVR@00\" does not match DB@62oupsxs\n    at assert (file:///Users/ocean/roci/mono/packages/shared/src/asserts.ts:3:11)\n    at ViewSyncerService.#syncQueryPipelineSet (file:///Users/ocean/roci/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts:435:5)\n    at processTicksAndRejections (node:internal/process/task_queues:95:5)\n    at async file:///Users/ocean/roci/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts:134:11\n    at async run (file:///Users/ocean/roci/mono/node_modules/@rocicorp/lock/out/lock.js:47:16)\n    at async ViewSyncerService.run (file:///Users/ocean/roci/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts:115:9)"}
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i view-syncer stopped
worker=syncer pid=38000 component=view-syncer serviceID=0mrtpo7idt0o3d5a6i clientID=5s255ctth1l5589rh9 wsID=R9Bla78BvUTK5Qr9ksQ-q cmd=initConnection flushed CVR {"instances":3,"queries":7,"desires":6,"clients":1,"rows":0,"statements":17} in (50 ms)
```

Looks like there are two things going on; the initial hydration and the initConnection, and somehow the locking isn''t working the way we expect.','ieK09sy2C_AIWE8KRkrQR'),
	 ('Yb7_mpOhw8oWo180IIUeP','AWj8hMrcbwFLPAXUWSZ1m',1726196048000,'Okay, I think the assert is just wrong. If the CVR is empty, there will be no queries to add or remove, and so we won''t write CVR. I''ll fix the assert.','ieK09sy2C_AIWE8KRkrQR'),
	 ('8sBU-F6lypLygo-xk7tYI','A_5GErKdgkngmbFRyygQp',1726189802000,'Here''s a proposed schema and algorithm.

Add a new column to the change table:

```sql
CREATE TABLE "cdc.ChangeDB" (
  "commitWatermark" TEXT,
  "watermark" TEXT,
  "change" JSONB,
  PRIMARY KEY ("commitWatermark", "watermark")
);
```

For normal, non-streaming replication messages, we actually know the commit watermark when we receive the `begin` message, so we can write the all columns as the messages arrive.

To add support for streaming in-progress transactions, or other CDC protocols where the commit watermark is not known at begin time, we can write a placeholder in for the commitWatermark (derived from a constant larger than the largest LSN, like `2^64 + 1`) while the transaction is pending, and then when the final commit is known, update all rows to replace that constant with the final commit.

Finally, we also have to propagate this down to the client so that watermarks look like "commitWatermark/watermark" so that the subscriber can properly do watermark based filtering. This will make in-progress transactions a bit trickier. Perhaps we shouldn''t do forward-time filtering based on watermarks.  🤔 ','ieK09sy2C_AIWE8KRkrQR'),
	 ('MT6dy-cZdE9atwW9kdFyX','azP-Z7eh40PAVtIliJ-12',1715878457000,'Republishing unb0rks things, but they get reb0rked fairly quickly. A different error this time:

<img width="2503" alt="Screenshot 2024-05-16 at 09 53 16" src="https://github.com/rocicorp/mono/assets/132324914/a4d60d4b-b2d6-4bf8-9a4b-f34c88ab3e6d">

I suspect that we can only hold these transactions open for a certain time. Probably a knob in there somewhere.','ieK09sy2C_AIWE8KRkrQR'),
	 ('87lPZYMywqZnp6WAtoDCt','azP-Z7eh40PAVtIliJ-12',1715878795000,'`show idle_in_transaction_session_timeout;` says "1d", although I''m seeing the timeout happen before 5 minutes.','ieK09sy2C_AIWE8KRkrQR'),
	 ('k_DOlIhiOWyXKEOzLudTQ','azP-Z7eh40PAVtIliJ-12',1715885392000,'Data point: The connection seems to close fairly consistently after 4:30 minutes of inactivity.

<img width="1381" alt="Screenshot 2024-05-16 at 11 48 32" src="https://github.com/rocicorp/mono/assets/132324914/40b4ba9e-08e1-4b8f-bda0-694a57307976">
','ieK09sy2C_AIWE8KRkrQR'),
	 ('EYc8xm7ZDt3LaI3GtnvXb','EXpLtXkeB--ideoLqoopz',1715676224000,'<details>
<summary><a href="https://linear.app/roci/issue/ROC-12/zeppliear-exception-because-issue-missing-properties">ROC-12 Zeppliear: Exception because issue missing properties</a></summary>
<p>

To reproduce scroll down in Zeppliear.

[image](https://uploads.linear.app/be6d9e3c-d622-4339-b3a8-6f0d9478e889/674469ce-9dce-413c-a264-eaa5d8f8fd95/ce3d7065-a488-44ef-8731-176a21558894)

`row` is

```
{
    "id": "_0kcprVNTV",
    "issue": {
        "id": "_0kcprVNTV",
        "kanbanOrder": "0"
    },
    "labels": []
}
```

but the type of `row` is supposed to be `{issue: Issue; labels: string[]};`
</p>
</details>','pFCse65icLsHqrf1TJMuV'),
	 ('K8JrbR7vjnwFkMuA0fAbN','EXpLtXkeB--ideoLqoopz',1715677032000,'This comes from

https://github.com/rocicorp/mono/blob/a221bec5b5e6dc9e675c1ad743c39dd7113f9bdd/apps/zeppliear/frontend/app.tsx#L129

and `filteredAndOrderedQuery` comes from:

https://github.com/rocicorp/mono/blob/a221bec5b5e6dc9e675c1ad743c39dd7113f9bdd/apps/zeppliear/frontend/app.tsx#L405-L418

so it is not clear yet why the `title` is not present','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('gCzOj1YthWH_hxfr_l4yM','EXpLtXkeB--ideoLqoopz',1715678316000,'IDB has:

<img width="281" alt="image" src="https://github.com/rocicorp/mono/assets/45845/c4a63c62-dbb2-4652-9f5c-bfe5b3e8042f">

let me check what the server sent','nqYkxAGMnzk7Y5STjZryV'),
	 ('gvIHE3ROqgAlLjcZ1Qo2m','EXpLtXkeB--ideoLqoopz',1715699172000,'We''ll need the operation to filter out partial rows from a query. This was started here: https://github.com/rocicorp/mono/tree/mlaw/filter-partial

but paused since we thought Zeppliear never diverged in what queries asked for so it wasn''t a top priority for the hackfest.

It also requires schema information on the client to support `*`','tDY6IbKdVqbBlRBc3XMwF'),
	 ('vVBQ3eSgd1fu8Y4R7c0FR','Lo98nXz8eCj624qfhyfMd',1715675572000,'<details>
<summary><a href="https://linear.app/roci/issue/ROC-11/limit-is-broken">ROC-11 limit is broken</a></summary>
<p>

I was hitting this when testing Zeppliear

In Zeppliear we have a limit of 200 but we end up with a case where we get to `#limitedAddAll` where the size of the BTree is 201 (changing the limit to 10 hits a case where the data.size is 11):

[https://github.com/rocicorp/mono/blob/baff399d78101ac77813803516b06a0c84fa4209/packages/zql/src/ivm/view/tree-view.ts#L142-L144](https://github.com/rocicorp/mono/blob/baff399d78101ac77813803516b06a0c84fa4209/packages/zql/src/ivm/view/tree-view.ts#L142-L144)

I did some debugging and the problem seems to be that the we call tree `set` without going through the *limit function* so the tree size is larger than we expect.

[https://github.com/rocicorp/mono/blob/baff399d78101ac77813803516b06a0c84fa4209/packages/zql/src/ivm/source/set-source.ts#L173-L175](https://github.com/rocicorp/mono/blob/baff399d78101ac77813803516b06a0c84fa4209/packages/zql/src/ivm/source/set-source.ts#L173-L175)
</p>
</details>','pFCse65icLsHqrf1TJMuV'),
	 ('B3SXUJOYMWx-CLz2E_euS','Lo98nXz8eCj624qfhyfMd',1715675726000,'Also, swapping the order of set and delete here seems to fix it

https://github.com/rocicorp/mono/blob/a221bec5b5e6dc9e675c1ad743c39dd7113f9bdd/packages/zql/src/ivm/view/tree-view.ts#L151-L154

Which makes no sense to me! Are we mutating a shared tree somewhere?','nqYkxAGMnzk7Y5STjZryV'),
	 ('B78AJAPm3JC7JQadSuh5w','Lo98nXz8eCj624qfhyfMd',1715851935000,'#1804 ?','nqYkxAGMnzk7Y5STjZryV'),
	 ('fZNy7u3yCke33OLxfEaX2','Lo98nXz8eCj624qfhyfMd',1715855576000,'Do you know of a reliable repro? As you linked, I couldn''t repro with those tests. I also changed the b-tree recently to use the immutable variants of add/remove/delete (#1825) which would preclude any of those mutation issues.','tDY6IbKdVqbBlRBc3XMwF'),
	 ('PYu8IuKD6gO4bsdd8ONtz','Lo98nXz8eCj624qfhyfMd',1715856096000,'> I did some debugging and the problem seems to be that the we call tree set without going through the limit function so the tree size is larger than we expect.

The `source` tree and `view` tree are two distinct trees.','tDY6IbKdVqbBlRBc3XMwF'),
	 ('TZwEeHmqPoCCR_RV8gyrP','Lo98nXz8eCj624qfhyfMd',1715856425000,'I just synced main, npm i, npm run build and I still get the same error on loading zeppliear with a new "room"','nqYkxAGMnzk7Y5STjZryV'),
	 ('k-pAvnKWtW0oIEe1MpU05','Lo98nXz8eCj624qfhyfMd',1715867117000,'taking a look','tDY6IbKdVqbBlRBc3XMwF'),
	 ('gEoENDu07rGD1iUkUqKQR','Lo98nXz8eCj624qfhyfMd',1716564431000,'This ended up being fixed by #1867, correct?','tDY6IbKdVqbBlRBc3XMwF');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('cjK1amDn31-LSIMGGMYyV','Lo98nXz8eCj624qfhyfMd',1716811485000,'limit is broken but in a different way #1866

I''ll close this and open a new issue.

Subsumed by #1942','nqYkxAGMnzk7Y5STjZryV'),
	 ('gBWgtm0Tf31A-EHTnh39k','GgpFqe8eiV-A3d6_AEMk5',1715671154000,'<p><a href="https://linear.app/roci/issue/ROC-10/type-generation-for-client-api">ROC-10 Type Generation for Client API</a></p>','pFCse65icLsHqrf1TJMuV'),
	 ('VB4C4QC27NzzrLsSQhz6K','wXojICEjDrM2vZK7SXBSI',1715671049000,'<details>
<summary><a href="https://linear.app/roci/issue/ROC-9/auth">ROC-9 Auth</a></summary>
<p>

Right now we have a few paragraphs of text and a code block. We need to design and implement both authentication and authorization.
</p>
</details>','pFCse65icLsHqrf1TJMuV'),
	 ('yhlw1Dqd2hEqDu9EZr4Sr','Y3WTx0k75v5gebho0Yps0',1715670373000,'<details>
<summary><a href="https://linear.app/roci/issue/ROC-8/test">ROC-8 test</a></summary>
<p>

test
</p>
</details>','pFCse65icLsHqrf1TJMuV'),
	 ('k3-2K6-JOWeSvExGQdQcz','Y3WTx0k75v5gebho0Yps0',1715670396000,'test?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('X4CJ85HxN582eGwydHr-m','-Thbh2750u8nQUnsjkl2X',1716564465000,'fixed by https://github.com/rocicorp/mono/pull/1839','tDY6IbKdVqbBlRBc3XMwF'),
	 ('H82Zal-VdKY_CYbrrrdnp','gZw3ftjun5SZcLwQN7qre',1715013757000,'I did this originally. My thought process was that an app level ping would handle more failure modes. For example it could detect deadlocks in the locking code of the do. The cf provided ping support couldn’t do that.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('fSWWJ02AumIBzJsq1QFkV','gZw3ftjun5SZcLwQN7qre',1715066130000,'Let''s leave as is. It works.','nqYkxAGMnzk7Y5STjZryV'),
	 ('jVo8M37WmOe6HGi2WdN2T','qjceEA0PK8JwC8ZtIRheS',1714602151000,'cc @tantaman ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('rGSdSQbSRSN93fEHeJhVi','qjceEA0PK8JwC8ZtIRheS',1715192809000,'Started on this here: https://github.com/rocicorp/mono/tree/mlaw/filter-partial

but it stalled out since we need schema information on the client to deal with `*`. Once we have that I can resume this work.','tDY6IbKdVqbBlRBc3XMwF');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('3dIpKJ_cZU91sr_HuDzrR','qjceEA0PK8JwC8ZtIRheS',1718868284000,'We should only cache the entire row.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('QlqV_283cX9Sg5N4S42Ww','02kEEyJGmES9OjXXAkyBA',1715649260000,'- #1803
- #1817 

Reduce still needs to be lazy on input.

Join is lazy in coming commits.','tDY6IbKdVqbBlRBc3XMwF'),
	 ('x9mFPoH3yotbeN_71vuQM','SihBUomWCf-ksK8hBvy6p',1714484810000,'Another option is to just go ahead an implement sharing of structure. In that case, joins will only run once.

We''ll want to be a bit smart when cleaning up graph nodes after hitting 0 references and keep them around a bit in case a new query immediately shows up wanting a recently de-referenced node.','tDY6IbKdVqbBlRBc3XMwF'),
	 ('mVz5uj85nyevf_28yKd6l','AuiUsUv4Q9XFVqfbGlx9U',1714155513000,'For (1) --

The current signature is:

```ts
EntityQuery<F extends FromSet, Return = []>
```

which, in practice, looks like the following when defining functions that take queries:

```ts
function applyFilter(q: EntityQuery<{
  issue: Issue,
  label: Label
}, {
  issue: Issue,
  label: Label
}[]>) {
}
```

That''s... difficult to get right.

A potential fix is to modify the `EntityQuery` type to:

```ts
EntityQuery<F extends union of entities?, Return = MakeReturn<F>>
```

Which cleans up user defined functions (like the applyFilter example) to:

```ts
function applyFilter(q: EntityQuery<Issue | Label>) {
}
```

> note: `EntityQuery<Issue | Label>` instead of `EntityQuery<[Issue, Label]>` since order should not matter.

Which, I think, is pretty obvious. 

Does this fix the issue with `EntityQuery<{foo: Foo, bar: Bar}>` being assignable to `EntityQuery<{foo: Foo}>`?
We do not want to former to be assignable to the latter unless we force the user to always use qualified names in `where`, `on`, `having`. The reason is that the type system will allow unqualified selectors for the latter but the implementation will break if the type of query is really the former at runtime.','tDY6IbKdVqbBlRBc3XMwF'),
	 ('MqkOp0YH4150UhEabMjeH','AuiUsUv4Q9XFVqfbGlx9U',1714155630000,'For (2) --

I like the idea of leaning into sub-queries and making join as irrelevant as possible in the language. This, combined with co-located queries, should fix the problem.

Co-located queries helps to fix the problem since the return type of a query will not spread out into many components.','tDY6IbKdVqbBlRBc3XMwF'),
	 ('YGLOrqdYExwA42oWA-yVL','AuiUsUv4Q9XFVqfbGlx9U',1714155822000,'For (3) --

One option would be to default the return type of a query to the empty object rather than defaulting it to `SELECT *`.

I _think_ this would allow selects which add fields to a query to be assigned to a prior query variable.','tDY6IbKdVqbBlRBc3XMwF'),
	 ('o664USZrJY7ehTdQxuMXr','Ug-22TAa01jHLgsN5C_4B',1714064314000,'Looks like I forgot to deal with operators that have memory when it comes to processing historical data.

History requests should stop as soon as they hit an operator with memory. Although no queries are sharing structure right now so I''m a bit confused as to why we''d process history more than once through a pipeline.','tDY6IbKdVqbBlRBc3XMwF'),
	 ('FWO2USswZEyxlUOOZaydQ','Ug-22TAa01jHLgsN5C_4B',1714065768000,'ah, the source is always shared among all queries.

When removing the `queue` abstraction I removed/screwed up the code that selected the correct downstream path.','tDY6IbKdVqbBlRBc3XMwF'),
	 ('3RcLyUArWWMHxMm9jVrBD','Ug-22TAa01jHLgsN5C_4B',1714066591000,'need to clean it up but the fix is here: 6e192626b168ce4197cadf0496b75ee51a1047d6 in this draft pr: https://github.com/rocicorp/mono/pull/1640 ','tDY6IbKdVqbBlRBc3XMwF'),
	 ('CQWlehyzmflC8pBnBNk6m','Ug-22TAa01jHLgsN5C_4B',1714485062000,'The existing count issue is fixed. Zeppliear has an unrelated count issue where we''re just doing the count query incorrectly.

I.e.,
```
SELECT count(*) FROM issue JOIN ... GROUP BY issue.id 
```

That counts the count in a group, not the total count of rows.

Should be:

```
SELECT count(distinct issue.id) FROM issue JOIN ... ;
```','tDY6IbKdVqbBlRBc3XMwF');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('bjHzmmoNTypSGd459wjT1','Ug-22TAa01jHLgsN5C_4B',1714512255000,'- #1684 adds distinct','tDY6IbKdVqbBlRBc3XMwF'),
	 ('lShAYIHC6fb7vBrGAn72m','IxiKp7PlkscyxnOIJbBTi',1713189710000,'1. All events still exists in `pending` so the delete will be sent downstream
2. The `set-source` is assuming everything has a unique id so is not allowing dupes. I.e., `this.#tree.add` replaces the old value with the new one','tDY6IbKdVqbBlRBc3XMwF'),
	 ('OdiWmZ0epOPqvRU_UCknD','WnlHcMY3_QYYPJYKfvLOZ',1710489883000,'My bias is to keep it as is unless the support for old ff is really getting
in the way badly or else we are certain ~nobody’s using this old version of
FF anymore.

Keep in mind that when we make browser support choices we’re making them on
behalf of our *customers* not ourselves. This is now a market our customer
can’t target.

An advantage of Replicache is that we use pretty basic web platform APIs so
we are very very compatible.

SQLite based systems have much higher requirements. Let’s not throw away
that advantage carelessly.

a (phone)


On Thu, Mar 14, 2024 at 9:48 PM Erik Arvidsson ***@***.***>
wrote:

> https://www.mozilla.org/en-US/firefox/115.0/releasenotes/
>
> IndexedDB <https://w3c.github.io/IndexedDB/> is now also supported in private
> browsing <https://bugzilla.mozilla.org/show_bug.cgi?id=1639542> without
> memory limits thanks to encrypted storage on disk. The temporary keys to
> decrypt the information are held in RAM only and all stored information is
> purged at the normal end of a private browsing session from disk.
>
> They way Replicache deals with this is that it catches an exception and
> switches to an in memory store. With Firefox 115 this exception is no
> longer triggered. This means that we already use IDB in Firefox private
> browsing but there is room to simplify the code to remove this fallback.
>
> —
> Reply to this email directly, view it on GitHub
> <https://github.com/rocicorp/mono/issues/1476>, or unsubscribe
> <https://github.com/notifications/unsubscribe-auth/AAATUBCM4COXO74NF5H73C3YYKRTFAVCNFSM6AAAAABEXQL3MSVHI2DSMVQWIX3LMV43ASLTON2WKOZSGE4DOOJRGUYTGNY>
> .
> You are receiving this because you are subscribed to this thread.Message
> ID: ***@***.***>
>
','OeVnr1y5bEM_Yg06sUFtD'),
	 ('xLSjM_rD794r8dga63Ahg','1M1sj4szpJElpkdR4Wo0F',1708950745000,'It is not clear if `authHandler` to `onAuth` makes sense because `onAuth` implies that it gets called when something is authenticated but this thing is called to do the actual authentication.

Deferring that follow up until further discussions.','nqYkxAGMnzk7Y5STjZryV'),
	 ('tDaiUNM1sIVAqGikTq1it','GInRIPj0w4RGU9O5LZylY',1708746427000,'FYI, happened again for a different user. I''ll fix the CLI to not report this as an error.

<img width="1011" alt="Screenshot 2024-02-23 at 19 46 30" src="https://github.com/rocicorp/mono/assets/132324914/434dacda-bdeb-4738-a612-e198e39dfa9e">
','ieK09sy2C_AIWE8KRkrQR'),
	 ('J-ZDFM8rsZtUt27w0B7me','GInRIPj0w4RGU9O5LZylY',1708746800000,'Another option is to create the team if the user calls one of these functions (which would normally not happen until they publish their first app).

This might make the most sense from a dx perspective. Then `apps list` and `keys` would work.','ieK09sy2C_AIWE8KRkrQR'),
	 ('9KKnJ26n-3fEGt2X-4OmU','GInRIPj0w4RGU9O5LZylY',1709670117000,'> Another option is to create the team if the user calls one of these functions

This makes sense to me. In the future if users can be invited to teams then such users will often end up with two teams, their personal one and their work one. But this is standard with similar tools.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Srq5ynmANRlGmGc8Q6SLi','4CuOpzv26yKxhL47hizM_',1708655783000,'Yeah, weird. Looking.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('BYE0qLfyvUVBBx7EP_tzY','4CuOpzv26yKxhL47hizM_',1708656653000,'I believe this is a length issue.  loop-orchestrator-release-0-39-2024022-rocicorpreflectservices.reflect-server.net is accepted but loop-orchestrator-release-0-39-20240222-rocicorpreflectservices.reflect-server.net is not.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('I9CHKdIzXuuwN7TVU42s3','4CuOpzv26yKxhL47hizM_',1708668542000,'Nice. I''ll add an explicit check beforehand so we don''t get these orphaned custom hostnames (since that succeeds, but the subsequent dns step fails).','ieK09sy2C_AIWE8KRkrQR');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('GkqcifwlM-EdVr99Cao2t','4CuOpzv26yKxhL47hizM_',1708670195000,'Needs more experimentation to know if it’s the total host name length it doesn’t like or subdomain or what ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('9-NTyGnq6G6vfoJ2BqVoB','4CuOpzv26yKxhL47hizM_',1708714681000,'Yup.

I was unable to create loop-orchestrator-release-0-39-202402220-rocicorpreflectservices.reflect-server.dev via the dashboard, but I am able to create loop-orchestrator-release-0-39-20240222-rocicorpreflectservices.reflect-server.dev.

<img width="1268" alt="Screenshot 2024-02-23 at 10 47 38" src="https://github.com/rocicorp/mono/assets/132324914/d1d55fc1-0f02-444b-93fe-45524c572892">

It looks like loop-orchestrator-release-0-39-20240222-rocicorpreflectservices.reflect-server.net already exists, which may be why you couldn''t create it.

Next test is to see if it''s the hostname length or the full dns name length, so I tried it on replicache.dev.

 loop-orchestrator-release-0-39-202402220-rocicorpreflectservices.replicache.dev fails but  loop-orchestrator-release-0-39-20240222-rocicorpreflectservices.replicache.dev succeeds. So it''s hostname specific.

To be complete, I removed all numbers and hyphens and just tried a straight alphabetic hostname.

The 64 character hostname rocicorpreflectservicesrocicorpreflectservicesrocicorpreflectser.replicache.dev fails, but the 63 character hostname rocicorpreflectservicesrocicorpreflectservicesrocicorpreflectse.replicache.dev succeeds.

So the max hostname length is 63.
','ieK09sy2C_AIWE8KRkrQR'),
	 ('pAUj0gQwBTrR6R4KS8_2U','ISJdIl3mCFbsjixC-Cgwz',1699463406000,'(Are you able to reproduce it?)','ieK09sy2C_AIWE8KRkrQR'),
	 ('TTqpucyf_x-0ggL0DJ1Qo','4CuOpzv26yKxhL47hizM_',1708718168000,'Confirmed: https://developers.cloudflare.com/dns/manage-dns-records/reference/dns-record-types/#cname

> CNAME
>
> Name: A subdomain or the zone apex (@), which must: 
> * Be 63 characters or less
> * Start with a letter and end with a letter or digit
> * Only contain letters, digits, or hyphens (underscores are allowed but discouraged)

Also, according to https://community.cloudflare.com/t/dns-record-for-cname-is-limited/491688, the total dns name must be 255 characters or less:

<img width="771" alt="Screenshot 2024-02-23 at 11 54 37" src="https://github.com/rocicorp/mono/assets/132324914/1fd4925e-a4ef-4145-82e1-6cd2331f5af8">


At the moment, we don''t have to worry about the max dns name limit of 255 characters, but it''s good to keep in mind if/when we start supporting more variants of domain names.
','ieK09sy2C_AIWE8KRkrQR'),
	 ('QjtChf2NGuR2du9pDTv2T','4CuOpzv26yKxhL47hizM_',1708719293000,'So does this mean we should limit the app name to 63 chars too?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('2sxPCdkp9QwDs0xweGYHh','4CuOpzv26yKxhL47hizM_',1708719429000,'Yeah, we actually need to limit `{appName}-{teamLabel}` to 63 characters.','ieK09sy2C_AIWE8KRkrQR'),
	 ('hRYdpkTxKybllMCcH6g-z','4CuOpzv26yKxhL47hizM_',1708719626000,'ooooh. Two follow-up thoughts:

1. Should we put a limit on team label too?
2. And/or, should we implicitly clamp and then add a hash to user supplied appNames that would exceed the limit?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('3zaJTbU3k2eh4NRpj5-nc','4CuOpzv26yKxhL47hizM_',1708728641000,'A fix for (2) is in. The app name will be truncated and hashed if the dns label would otherwise be too long.

But yes, we would also need to limit the lengths of team names for this to be water tight. Does github limit usernames already? If not, or if it''s close to 63 chars, do you want to do a similar dns-only truncate+hash thing for the team name? 

I guess we''d also need to decide how to deal with the case when they''re both too long (i.e. decide how many characters each name gets). ','ieK09sy2C_AIWE8KRkrQR'),
	 ('eNExTZ26eNC_QfarAfy3V','4CuOpzv26yKxhL47hizM_',1708744985000,'Various places indicate that Github restricts its username length to 39:

https://gist.github.com/tonybruess/9405134
https://github.com/shinnn/github-username-regex
https://docs.github.com/en/enterprise-cloud@latest/admin/identity-and-access-management/iam-configuration-reference/username-considerations-for-external-authentication

So I will set the limiit to 40 when creating a team label for the future point at which we choosing or renaming team names.','ieK09sy2C_AIWE8KRkrQR'),
	 ('L38BTYxiSOvV75FFP8lwZ','dct7Ai7_EypFcGxEDioPU',1707851314000,'I was just coming here to file this bug as Tristan raised it on Discord https://discord.com/channels/830183651022471199/1206779893229031494/1206812591997976596
 
 :)
 
 I''ll respond with how I think we should fix.','Gg4MskWt3M-ttzzlrJ9jn');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('7ZcJjf_hh-bCjOQOzm2rr','dct7Ai7_EypFcGxEDioPU',1707853212000,'To fix:

1.  [RoomDO''s delete handler](https://github.com/rocicorp/mono/blob/2b950d45e343e84ae0dfadd47e2f9246483c07f9/packages/reflect-server/src/server/room-do.ts#L169) should close all connections.
2.  [deleteRoom handler](https://github.com/rocicorp/mono/blob/2b950d45e343e84ae0dfadd47e2f9246483c07f9/packages/reflect-server/src/server/auth-do.ts#L263) should [delete all ConnectionRecords](https://github.com/rocicorp/mono/blob/2b950d45e343e84ae0dfadd47e2f9246483c07f9/packages/reflect-server/src/server/auth-do.ts#L1253) for the deleted room.  Note that its possible that a room-do processes a delete request but the auth-do fails to update the RoomRecord and delete the ConnectionRecords (the new logic), this is because the state is spread across two dos and thus is non-transactional.  This complicates the handling in 2 and 3 below.
3. [authRevalidateConnections](https://github.com/rocicorp/mono/blob/2b950d45e343e84ae0dfadd47e2f9246483c07f9/packages/reflect-server/src/server/auth-do.ts#L895) should correctly handle deleted rooms.  For each roomID, before sending it the request, it should check if it''s RoomRecord indicates it is deleted, if it is it should delete the connection records and move to the next roomID.  If not proceed to send the request for the current connection.  If the returned responses is 410 deleted, this indicates the room was deleted but we failed to record it in the auth do''s state, so we should fix that now, we should update the RoomRecord to indicate it is deleted and we should delete the connection records for the room.
4. The auth invalidate endpoints (authInvalidateAll, authInvalidateForRoom and authInvalidateForUser) need to be updated to deal with deleted rooms.  Similarly to 2, before sending a request to a room we should check the RoomRecord for deleted, and we should also handle 410 deleted responses from the room, updating the RoomRecords and ConnectionRecords appropriately.  *We should treat a deleted room as a successful invalidation.* 

Probably the handling in 2 and 3 can be largely shared code.','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('5evTl0nheBoMd5TFRTiD1','dct7Ai7_EypFcGxEDioPU',1707853868000,'This reminds me of a related idea that I had when I encountered this non-transactional (two storage system) interaction. The idea was something like:

1. deleteRoom sets that status of the RoomRecord (in the AuthDO) to a new state called `DELETING`. For most intents and purposes, the AuthDO treats it the same as `DELETED`.
2. An Alarm is scheduled to make the call to the RoomDO to delete itself (along with any other rooms that are in the `DELETING` state). If it succeeds, the AuthDO then marks the RoomRecord as `DELETED`. If it fails, the alarm tries again later.

Would this simplify things in terms of revalidate / invalidate? ','ieK09sy2C_AIWE8KRkrQR'),
	 ('NQbJji0M4P3p6HdZgQxbJ','dct7Ai7_EypFcGxEDioPU',1707855047000,'BTW reading some comments in the code I think the original intention was that callers that wanted to delete a room would have to:
1. call close with roomID 
2. call invalidateForRoom with roomID
3. call delete with roomID

This seems overly burdensome on the caller.  Also if you fail to invalidateForRoom before you delete you can end up with dangling connections that are just getting errors cause all of the room''s state has been deleted.

We do strictly require that you [close a room before deleting it](https://github.com/rocicorp/mono/blob/2b950d45e343e84ae0dfadd47e2f9246483c07f9/packages/reflect-server/src/server/rooms.ts#L213).  I''m also not sure about this.  Why require closing before deleting?   A comment indicates that close is intentionally coded to not log out existing connections, but just to not allow new ones.... why?  ','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('skgzQ9lEKs4NIopAiSQ4u','dct7Ai7_EypFcGxEDioPU',1707856499000,'> This reminds me of a related idea that I had when I encountered this non-transactional (two storage system) interaction. The idea was something like:
> 
> 1. deleteRoom sets that status of the RoomRecord (in the AuthDO) to a new state called `DELETING`. For most intents and purposes, the AuthDO treats it the same as `DELETED`.
> 2. An Alarm is scheduled to make the call to the RoomDO to delete itself (along with any other rooms that are in the `DELETING` state). If it succeeds, the AuthDO then marks the RoomRecord as `DELETED`. If it fails, the alarm tries again later.
> 
> Would this simplify things in terms of revalidate / invalidate?

That is a nice way to ensure eventual consistency. 2 would retry if either the call to the RoomDO failed or the AuthDO RoomRecord/ConnectionRecords updates failed, correct?

I think it would allow a little simplification of revalidate/invalidate if we had been using this scheme from the beginning, but given having to deal with existing rooms I think its probably more complicated to move to this.  With this revalidate could just skip over DELETING and DELETED rooms.  However invalidate would still need to make calls to DELETING rooms (as its contract is that if the response is 200 the connections are closed, not that they will be eventually close), and would need to deal with `410 deleted` responses.

','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('LRo9vc8DH-zfyHawTdqhz','dct7Ai7_EypFcGxEDioPU',1707872116000,'Taking this off of @grgbkr ''s plate.','ieK09sy2C_AIWE8KRkrQR'),
	 ('LsVDBEb3fznSdvGyZO6K9','ISJdIl3mCFbsjixC-Cgwz',1699462620000,'Tried again. Here''s the console:

![Screenshot 2023-11-08 at 8 55 11 AM](https://github.com/rocicorp/mono/assets/132324914/c7eb7e24-5c23-4000-b58f-94b7a032d771)

And here are the logs:

![Screenshot 2023-11-08 at 8 56 24 AM](https://github.com/rocicorp/mono/assets/132324914/e13abfb8-8229-42b1-8ffc-9a240313653b)

Anything else I should try?
','ieK09sy2C_AIWE8KRkrQR'),
	 ('uFy_Yp3AHmce8sTqH9aH3','dct7Ai7_EypFcGxEDioPU',1708032856000,'As I''m working through the code, one thing I''ve noticed is that none of the invalidate methods actually clear connection state. Revalidate is the only place that does so. I assume that this is because the AuthDO doesn''t know about regular (non-invalidation) disconnects, and so it has to periodically revalidate, and thus it will eventually find out about the invalidated connections through this process.

Is there a harm in leaving it this way and only removing connections in revalidate (with the additional logic for understanding deleted rooms)? I think this would simplify things slightly by consolidating all of the bookkeeping in the revalidate step. ','ieK09sy2C_AIWE8KRkrQR'),
	 ('pvFWddkfUX1MgfLFYg56o','dct7Ai7_EypFcGxEDioPU',1708037065000,'> As I''m working through the code, one thing I''ve noticed is that none of the invalidate methods actually clear connection state. Revalidate is the only place that does so. I assume that this is because the AuthDO doesn''t know about regular (non-invalidation) disconnects, and so it has to periodically revalidate, and thus it will eventually find out about the invalidated connections through this process.
> 
> Is there a harm in leaving it this way and only removing connections in revalidate (with the additional logic for understanding deleted rooms)? I think this would simplify things slightly by consolidating all of the bookkeeping in the revalidate step.

I convinced myself that this is a correct (and elegant) way to handle this. The connection cleanup code remains largely the same, the only difference being that it handles a deleted room as having returned no connections (so that they get cleaned up), and checks that the RoomRecord is marked as deleted.','ieK09sy2C_AIWE8KRkrQR'),
	 ('s9q-q9iT4KT32-NVEVzEV','dct7Ai7_EypFcGxEDioPU',1708109132000,'> BTW reading some comments in the code I think the original intention was that callers that wanted to delete a room would have to:
> 
> 1. call close with roomID
> 2. call invalidateForRoom with roomID
> 3. call delete with roomID
> 
> This seems overly burdensome on the caller. Also if you fail to invalidateForRoom before you delete you can end up with dangling connections that are just getting errors cause all of the room''s state has been deleted.
> 
> We do strictly require that you [close a room before deleting it](https://github.com/rocicorp/mono/blob/2b950d45e343e84ae0dfadd47e2f9246483c07f9/packages/reflect-server/src/server/rooms.ts#L213). I''m also not sure about this. Why require closing before deleting? A comment indicates that close is intentionally coded to not log out existing connections, but just to not allow new ones.... why?

After thinking about this a bit, I can see use cases for:
* Closing a room and leaving the existing connections open. (maybe?)
* Closing a room and then invalidating connections but keeping the room data for archival purposes.

However, I feel like `deleteRoom` could automatically close the room (in the AuthDO) similar to how we added the auto invalidate, so that the `deleteRoom` command can stand on its own without any of the previous steps.

I guess one gotcha is that in the case of a partial failure (e.g. the call to RoomDO#delete fails), the user is left with a closed room, which could be kind of unintuitive.

I dunno. Food for thought.','ieK09sy2C_AIWE8KRkrQR'),
	 ('12B8eRDA_WgnZE7lZpubM','XW1MbnGrrr8zn8M3os47u',1707802349000,'Sorry, mischaracterized the error. Will file a new Issue.','ieK09sy2C_AIWE8KRkrQR');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('H1mneBWCGUjY1fjijb75q','8yjdh-9gaDHKm9acvePLq',1707804512000,'Actually, the request is technically incorrect because the user id is supposed to be uri encoded, so the '':'' character should be encoded as a ''%3A''.

It looks like Tristan eventually resolved the issue by changing the colon to an underscore in the user id.

<img width="1518" alt="Screenshot 2024-02-12 at 22 07 06" src="https://github.com/rocicorp/mono/assets/132324914/723031a8-77bc-4950-b25b-a0dce6149dc0">
','ieK09sy2C_AIWE8KRkrQR'),
	 ('OL0AF0Qj_ZErSipvg8qQH','8yjdh-9gaDHKm9acvePLq',1707805557000,'If you look at the discord thread, he was sending it URL encoded:

![CleanShot 2024-02-12 at 20 25 15@2x](https://github.com/rocicorp/mono/assets/80388/3d35d892-e447-4271-9c68-da258dfee032)

-- https://discord.com/channels/830183651022471199/1020392595450507304/1206779893229031494

Could some infra somewhere be decoding it before it gets to our code?

','OeVnr1y5bEM_Yg06sUFtD'),
	 ('RwjFMINclwmodRRwUncEd','8yjdh-9gaDHKm9acvePLq',1707874353000,'Yeah, I just confirmed that it happens to me too.

<img width="1364" alt="Screenshot 2024-02-13 at 17 30 41" src="https://github.com/rocicorp/mono/assets/132324914/cba4a63b-8c64-4d6d-ad02-df6e333a607b">
<img width="1230" alt="Screenshot 2024-02-13 at 17 30 19" src="https://github.com/rocicorp/mono/assets/132324914/526696d6-de0c-4928-9c1a-4fa4b9016304">


I think it''s GCP (which is based on Express) that''s uri decoding the "%3A". Sort of defeats the whole purpose.
I haven''t been successful in figuring out whether that can be turned off.','ieK09sy2C_AIWE8KRkrQR'),
	 ('-kSZ5m0IGxUZm1sceE7H9','8yjdh-9gaDHKm9acvePLq',1707877114000,'I found other folks who have encountered this problem (in Express, or IIS, which uses Express) but have yet to find a solution:

https://github.com/expressjs/express/issues/4825
https://github.com/expressjs/express/issues/1479

https://github.com/tjanczuk/iisnode/issues/217
https://github.com/tjanczuk/iisnode/issues/343
','ieK09sy2C_AIWE8KRkrQR'),
	 ('Gtzwu1uSQ2PU-IMeipah1','8yjdh-9gaDHKm9acvePLq',1707877706000,'FTR, I ran a bunch of URL encoded characters through to see which ones do and do not get decoded.

Sent:

```
connections/users/-._~%3A%2F%3F%23%5B%5D%4024%26''()*%2B%2C%3B%25%3D:invalidate
```

Received:

```
connections/users/-._~:/%3F%23%5B%5D@24&''()*+,;%25=:invalidate
```

This indicates which characters we''d have to re-encode to reverse this behavior for upstream (reflect-server) receivers.','ieK09sy2C_AIWE8KRkrQR'),
	 ('0bSE64UfN4ovHb5JDB-WX','8yjdh-9gaDHKm9acvePLq',1707878141000,'Also FTR, I checked the request headers to see if it provides the original (sent) url. It contains a partially decoded one, which is unfortunately equally useless.

```
x-forwarded-url: "/v1/apps/ln3/ddtrj/connections/users/-._~:/%3F%23%5B%5D@24&%27%28%29%2A+,;%25=:invalidate"
```','ieK09sy2C_AIWE8KRkrQR'),
	 ('VYM8CeATKl2u2cKk_IYtg','8yjdh-9gaDHKm9acvePLq',1707931953000,'@grgbkr and I consulted and agreed on the path forward being to move ids into query parameters. Working on this.','ieK09sy2C_AIWE8KRkrQR'),
	 ('y8XmB_EJb9bIXHg9M1l42','DSnV1eRo8fT1ajY-FxwPg',1706916109000,'@cesara this is due to your change in 

https://github.com/rocicorp/mono/commit/b5ee7e383ae1f2035ec7217375361f6ab2b9c541

<img width="532" alt="Screenshot 2024-02-02 at 15 21 04" src="https://github.com/rocicorp/mono/assets/132324914/2ad0088c-7201-4627-841c-931e403c8695">

Do you recall what the motivation was? 

(These errors are now triggering alerts)','ieK09sy2C_AIWE8KRkrQR'),
	 ('m_4FMWBRV3huRwsAhELsc','DSnV1eRo8fT1ajY-FxwPg',1706916806000,'@d-llama ya this was a mistake to check-in. I needed to throw the error because the exit(1) was hiding an error on a failing test','pgyTvcxh2hjmq2l4WKzK6'),
	 ('qYvAnOfKSJcBIr2tKhyrj','H6cCUgJgky0ctjrOul493',1707856154000,'Confirmed that Tanushree bumped our quota up to 10K.

<img width="725" alt="Screenshot 2024-02-13 at 12 27 57" src="https://github.com/rocicorp/mono/assets/132324914/6f77d485-dbbc-407e-8950-8e2e53b5783d">

@aboodman are we happy with this or is there more to follow up on with CF?','ieK09sy2C_AIWE8KRkrQR');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('NKIXKxEQDyiYsBtpjeoCG','H6cCUgJgky0ctjrOul493',1707857509000,'<img width="750" alt="CleanShot 2024-02-13 at 10 51 26@2x" src="https://github.com/rocicorp/mono/assets/80388/08ff83f0-0032-4dfe-ad2d-45abfc8e617f">

Booya','OeVnr1y5bEM_Yg06sUFtD'),
	 ('SjMq-p8z3qNeH07B9-Ef5','nwQPt2rxZRrSXCFAWYABs',1706622451000,'Here is what I think is happening?

reflect.net does not use auth and the handler currently requires auth. We need to make the http handler have an optional authentication header instead.

https://github.com/rocicorp/mono/blob/main/packages/reflect-server/src/server/auth-do.ts#L317','nqYkxAGMnzk7Y5STjZryV'),
	 ('vEgPJU0q0AFA5_LX4wuCl','nwQPt2rxZRrSXCFAWYABs',1707395741000,'Fixed','nqYkxAGMnzk7Y5STjZryV'),
	 ('YZGH2uZFcWka9aHYSv3OZ','K9bUxS6ifgHMMA3zpkldG',1702638011000,'https://github.com/rocicorp/mono/blob/main/packages/reflect-server/src/mod.ts#L16-L20

We should not export `ReflectServerBaseEnv` nor `createReflectServer`.','nqYkxAGMnzk7Y5STjZryV'),
	 ('d752_gCPzxUP_bdNnqPhA','03ArnhWRmiKTwsfNAIT0Q',1701396730000,'FTR, the code documentation clarifies this:

```js
(property) GlobalOptions.concurrency?: number | ResetValue | Expression<number>

Number of requests a function can serve at once.

@remarks
Can only be applied to functions running on Cloud Functions v2. 
A value of null restores the default concurrency (80 when CPU >= 1, 1 otherwise).
 Concurrency cannot be set to any value other than 1 if cpu is less than 1. 
The maximum value for concurrency is 1,000.
```','ieK09sy2C_AIWE8KRkrQR'),
	 ('GzFv9bSO07Y5e1wXORA-9','qzRb5YFzYjzZv2oLjmhTm',1701202071000,'For posterity, I manually ran the backup that failed:

```bash
mirror-cli $ npm run mirror backup-analytics ConnectionLifetimes

Running on reflect-mirror-prod

{"severity":"INFO","message":"Start date: 2023-11-19T00:00:00.000Z"}
QUERY: SELECT * FROM ConnectionLifetimes WHERE timestamp >= toDateTime(1700352000) AND timestamp < toDateTime(1700438400) ORDER BY timestamp FORMAT JSONEachRow
{"_sample_interval":1,"blob1":"1327adzgrUI","blob10":"","blob11":"","blob12":"","blob13":"","blob14":"","blob15":"","blob16":"","blob17":"","blob18":"","blob19":"","blob2":"loluf17a","blob20":"","blob3":"34c7b5fd-e508-4cff-839c-0252dd1aae55","blob4":"","blob5":"","blob6":"","blob7":"","blob8":"","blob9":"","dataset":"ConnectionLifetimes","double1":1700357802510,"double10":0,"double11":0,"double12":0,"double13":0,"double14":0,"double15":0,"double16":0,"double17":0,"double18":0,"double19":0,"double2":1700357838292,"double20":0,"double3":0,"double4":0,"double5":0,"double6":0,"double7":0,"double8":0,"double9":0,"index1":"","timestamp":"2023-11-19 01:37:18","severity":"INFO","message":"Validated first row"}
{"severity":"INFO","message":"Num results: 47"}
QUERY: SELECT * FROM ConnectionLifetimes WHERE timestamp >= toDateTime(1700438400) AND timestamp < toDateTime(1700524800) ORDER BY timestamp FORMAT JSONEachRow
{"_sample_interval":1,"blob1":"7b4eqY3OWih","blob10":"","blob11":"","blob12":"","blob13":"","blob14":"","blob15":"","blob16":"","blob17":"","blob18":"","blob19":"","blob2":"loxv9i52","blob20":"","blob3":"orch_public_d","blob4":"","blob5":"","blob6":"","blob7":"","blob8":"","blob9":"","dataset":"ConnectionLifetimes","double1":1700444186329,"double10":0,"double11":0,"double12":0,"double13":0,"double14":0,"double15":0,"double16":0,"double17":0,"double18":0,"double19":0,"double2":1700444212739,"double20":0,"double3":0,"double4":0,"double5":0,"double6":0,"double7":0,"double8":0,"double9":0,"index1":"","timestamp":"2023-11-20 01:36:52","severity":"INFO","message":"Validated first row"}
{"severity":"INFO","message":"Num results: 557"}
QUERY: SELECT * FROM ConnectionLifetimes WHERE timestamp >= toDateTime(1700524800) AND timestamp < toDateTime(1700611200) ORDER BY timestamp FORMAT JSONEachRow
{"_sample_interval":1,"blob1":"7b4eqY3OWih","blob10":"","blob11":"","blob12":"","blob13":"","blob14":"","blob15":"","blob16":"","blob17":"","blob18":"","blob19":"","blob2":"loxv9i52","blob20":"","blob3":"orch_public_e","blob4":"","blob5":"","blob6":"","blob7":"","blob8":"","blob9":"","dataset":"ConnectionLifetimes","double1":1700529467258,"double10":0,"double11":0,"double12":0,"double13":0,"double14":0,"double15":0,"double16":0,"double17":0,"double18":0,"double19":0,"double2":1700529874342,"double20":0,"double3":0,"double4":0,"double5":0,"double6":0,"double7":0,"double8":0,"double9":0,"index1":"","timestamp":"2023-11-21 01:24:34","severity":"INFO","message":"Validated first row"}
{"severity":"INFO","message":"Num results: 9337"}
QUERY: SELECT * FROM ConnectionLifetimes WHERE timestamp >= toDateTime(1700611200) AND timestamp < toDateTime(1700697600) ORDER BY timestamp FORMAT JSONEachRow
{"_sample_interval":1,"blob1":"OW06tVvTZG","blob10":"","blob11":"","blob12":"","blob13":"","blob14":"","blob15":"","blob16":"","blob17":"","blob18":"","blob19":"","blob2":"loztnkex","blob20":"","blob3":"/","blob4":"","blob5":"","blob6":"","blob7":"","blob8":"","blob9":"","dataset":"ConnectionLifetimes","double1":1700611152133,"double10":0,"double11":0,"double12":0,"double13":0,"double14":0,"double15":0,"double16":0,"double17":0,"double18":0,"double19":0,"double2":1700611221482,"double20":0,"double3":0,"double4":0,"double5":0,"double6":0,"double7":0,"double8":0,"double9":0,"index1":"","timestamp":"2023-11-22 00:00:21","severity":"INFO","message":"Validated first row"}
{"severity":"INFO","message":"Num results: 28377"}
QUERY: SELECT * FROM ConnectionLifetimes WHERE timestamp >= toDateTime(1700697600) AND timestamp < toDateTime(1700784000) ORDER BY timestamp FORMAT JSONEachRow
{"_sample_interval":1,"blob1":"OW06tVvTZG","blob10":"","blob11":"","blob12":"","blob13":"","blob14":"","blob15":"","blob16":"","blob17":"","blob18":"","blob19":"","blob2":"loztnkex","blob20":"","blob3":"/","blob4":"","blob5":"","blob6":"","blob7":"","blob8":"","blob9":"","dataset":"ConnectionLifetimes","double1":1700697598374,"double10":0,"double11":0,"double12":0,"double13":0,"double14":0,"double15":0,"double16":0,"double17":0,"double18":0,"double19":0,"double2":1700697617917,"double20":0,"double3":0,"double4":0,"double5":0,"double6":0,"double7":0,"double8":0,"double9":0,"index1":"","timestamp":"2023-11-23 00:00:17","severity":"INFO","message":"Validated first row"}
{"severity":"INFO","message":"Num results: 35038"}
QUERY: SELECT * FROM ConnectionLifetimes WHERE timestamp >= toDateTime(1700784000) AND timestamp < toDateTime(1700870400) ORDER BY timestamp FORMAT JSONEachRow
{"_sample_interval":1,"blob1":"OW06tVvTZG","blob10":"","blob11":"","blob12":"","blob13":"","blob14":"","blob15":"","blob16":"","blob17":"","blob18":"","blob19":"","blob2":"loztnkex","blob20":"","blob3":"/","blob4":"","blob5":"","blob6":"","blob7":"","blob8":"","blob9":"","dataset":"ConnectionLifetimes","double1":1700783993933,"double10":0,"double11":0,"double12":0,"double13":0,"double14":0,"double15":0,"double16":0,"double17":0,"double18":0,"double19":0,"double2":1700784000652,"double20":0,"double3":0,"double4":0,"double5":0,"double6":0,"double7":0,"double8":0,"double9":0,"index1":"","timestamp":"2023-11-24 00:00:00","severity":"INFO","message":"Validated first row"}
{"severity":"INFO","message":"Num results: 60902"}
QUERY: SELECT * FROM ConnectionLifetimes WHERE timestamp >= toDateTime(1700870400) AND timestamp < toDateTime(1700956800) ORDER BY timestamp FORMAT JSONEachRow
{"_sample_interval":1,"blob1":"OW06tVvTZG","blob10":"","blob11":"","blob12":"","blob13":"","blob14":"","blob15":"","blob16":"","blob17":"","blob18":"","blob19":"","blob2":"loztnkex","blob20":"","blob3":"/","blob4":"","blob5":"","blob6":"","blob7":"","blob8":"","blob9":"","dataset":"ConnectionLifetimes","double1":1700871580050,"double10":0,"double11":0,"double12":0,"double13":0,"double14":0,"double15":0,"double16":0,"double17":0,"double18":0,"double19":0,"double2":1700871652082,"double20":0,"double3":0,"double4":0,"double5":0,"double6":0,"double7":0,"double8":0,"double9":0,"index1":"","timestamp":"2023-11-25 00:20:52","severity":"INFO","message":"Validated first row"}
{"severity":"INFO","message":"Num results: 232"}
{"severity":"INFO","message":"Saving 134490 rows to 085f6d8eb08e5b23debfb08b21bda1eb/ConnectionLifetimes/2023-11-19~2023-11-26"}
 mirror-cli $ 
```

Interestingly, the resulting backup is quite a bit larger than that of previous weeks, at 1.7MB vs ~50kb:

![Screenshot 2023-11-28 at 12 03 07 PM](https://github.com/rocicorp/mono/assets/132324914/8d94f37a-c294-4fcf-9afb-df113ae0f6fe)

There is also an increase in size for our other table, `RunningConnectionSeconds`, but not nearly as much:

![Screenshot 2023-11-28 at 12 04 42 PM](https://github.com/rocicorp/mono/assets/132324914/a58bc436-c076-4f56-bae2-5bb92097cbf6)

Which is likely because the latter is bounded at once-per-minute entries, while `ConnectionLifetimes` produce an entry per connection and can thus increase with lots of (short) connections.

We''ll probably want to keep an eye on this and see if saving ConnectionLifetimes are worth it, given that we don''t actually use the data (due to the [overcounting issue](https://www.notion.so/replicache/Usage-Tracking-Methodologies-Limitations-e94b29188b024038bfbcc183a6d88189)).

','ieK09sy2C_AIWE8KRkrQR'),
	 ('qNUSvufDYdJcRNnZcB-bP','GY-1qG2U10bTurze8FSk7',1699987328000,'More context / ideas from @aboodman here:

https://discord.com/channels/830183651022471199/1020392595450507304/1174054443508043916','ieK09sy2C_AIWE8KRkrQR'),
	 ('5tBWMCpfx2bKy6LxaNlUO','GY-1qG2U10bTurze8FSk7',1699991926000,'I''m pretty sure that Cloudflare workers executes the code once to figure out what the DOs and fetch/alarm etc there are.

esbuild does not check for undefined bindings. This is actually something that it cannot do because someone might have added "window" as a global using `eval`.

I think the task for us is to make sure we forward the stack trace for these errors to the cli.','nqYkxAGMnzk7Y5STjZryV'),
	 ('zuQSvhvb96JWWf6cYFMmN','GY-1qG2U10bTurze8FSk7',1699992583000,'I can reproduce this with this change to a sample app:

<img width="718" alt="CleanShot 2023-11-14 at 10 09 00@2x" src="https://github.com/rocicorp/mono/assets/80388/2a84201a-1dd8-45a4-b00f-11fc4e8e5013">

<img width="1089" alt="CleanShot 2023-11-14 at 10 09 22@2x" src="https://github.com/rocicorp/mono/assets/80388/9406a5cd-e8d1-45fc-bc54-eecc20eb8596">
','OeVnr1y5bEM_Yg06sUFtD'),
	 ('GXkkrQRw7daw-jUuIpqbZ','GY-1qG2U10bTurze8FSk7',1699995115000,'Surfacing error code 10021 should be straightforward. I''ll take this.','ieK09sy2C_AIWE8KRkrQR');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('Fu3Y20uGxeyKqffYXipQi','tVxbMnV6abTPXYyJk9ar5',1699883747000,'I think handling this in the CLI and print an error, asking them to update reflect.config.json, is the right approach.

Checking and changing to src/reflect/index.{js,ts} seems like a step too far
','nqYkxAGMnzk7Y5STjZryV'),
	 ('IgfZJ5EMLc9MUnOAHuc6u','tVxbMnV6abTPXYyJk9ar5',1700032404000,'> I think handling this in the CLI and print an error, asking them to update reflect.config.json, is the right approach.

This is already happening:

<img width="1409" alt="CleanShot 2023-11-14 at 21 08 46@2x" src="https://github.com/rocicorp/mono/assets/80388/133a432a-d992-4fc9-88aa-0dbe67d1e456">

---

There are actually a few things that go wrong here though:

1. First thing is that `init` and `create` put the `reflect` dir in different places (`/refect` vs `/src/reflect`). Thus when `init` runs we end up in a confused state where we have two reflect directories (the old one is still there!).

2. If you run `npx reflect dev` at this point, then you have the wrong mutators for the current app (you''re still looking at the `create` app, which wants to call cursor-related mutators).

---

I think the right thing here is to have both apps use the `/src/reflect` directory so that this confusion can''t happen. Then it will do the right thing:

<img width="774" alt="CleanShot 2023-11-14 at 21 13 04@2x" src="https://github.com/rocicorp/mono/assets/80388/7a65055e-7f04-4074-bace-344b830b9e69">

','OeVnr1y5bEM_Yg06sUFtD'),
	 ('dRL2HlT8jOdtZHKp8g_p0','tVxbMnV6abTPXYyJk9ar5',1700178964000,'After thinking about this some, I''d like to just remove `init`. I don''t think it''s buying us much. If we can default the server entrypoint to `reflect-server/index.ts` (and allow it to be overridden via config), then I think we can get away with removing `init`.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('wy5b_5SOnrZrefw545hAW','tVxbMnV6abTPXYyJk9ar5',1700178981000,'(and overall the setup will be easier to understand because less magic)','OeVnr1y5bEM_Yg06sUFtD'),
	 ('2-MBLRoxrikG9iYZYWQzp','tVxbMnV6abTPXYyJk9ar5',1700180866000,'Actually nevermind, still wringing my hands about this.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('CisABt_IP4rHgkQlOBeKD','tVxbMnV6abTPXYyJk9ar5',1700213560000,'OK back to my original idea. Let''s remove `init`. I think it''s easier overall to walk user through creating the right files. See: https://reflect-docs-git-aa-idea-rocicorp.vercel.app/add-to-existing#sync for what I''m planning.

All we have to do here is remove the `init` subcommand. We''re. not going to refer to it in the docs anymore.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('ZLEvClvjsQPfocsad_DYo','QC3Eso1j24URE4SWY1H5N',1698799394000,'Actually, I think there will be cases in which we want to create the app without publishing (like `reflect vars set ...`). So I think our options are:

1. Figure out how to get `reflect tail` to display the more helpful text that the server returns
2. Have `reflect tail` check if the app has a `runningDeployment`.

I lean towards (1), but will defer to you.','ieK09sy2C_AIWE8KRkrQR'),
	 ('4_ShxsFJYS-Rq8eo22CKQ','QC3Eso1j24URE4SWY1H5N',1698836365000,'`reflect tail` uses a SSE from mirror-server/Firebase. These HTTP errors should be readable (we have our own custom implementation of SSE). Let''s double check that these errors are reasonably reported.

mirror-server/firebase talks to CF using a Websocket. These errors are reported using a ws message not http headers because those cannot be read by a websocket client. When these happens we forward the error to the cli using a server sent event called error.','nqYkxAGMnzk7Y5STjZryV'),
	 ('_1BP-kxqfmTulmP1XR7zS','QC3Eso1j24URE4SWY1H5N',1698856535000,'Okay, I found out where the error message is being sent; it''s in `response.text()`. Will send out a PR to surface the message.','ieK09sy2C_AIWE8KRkrQR'),
	 ('XmCawFteyEM6UM0YiVGZ_','ISJdIl3mCFbsjixC-Cgwz',1698748797000,'Yup. Here it is.

https://github.com/rocicorp/mono/blob/main/packages/reflect-server/src/server/auth-do.ts#L319

The old format was _better_ in the sense that it was shared between tail and connect.','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('-8UIN_xvj133N1mWo4k14','ISJdIl3mCFbsjixC-Cgwz',1699420616000,'Weird ... I just got this again when testing `tail` in prod, on an App running 0.37.202311060940.

Is this supposed to be fixed in that version?

In my case, the Worker existed but I had never actually run the app so the room had not yet been created.

![Screenshot 2023-11-07 at 9 14 43 PM](https://github.com/rocicorp/mono/assets/132324914/8ec8b3ae-d00e-4b6a-be3f-8294fe9d6659)','ieK09sy2C_AIWE8KRkrQR'),
	 ('Vu2YaBz3TB5mTGz6deiFN','ISJdIl3mCFbsjixC-Cgwz',1699445406000,'That is strange. Maybe needs another publish?','nqYkxAGMnzk7Y5STjZryV'),
	 ('-xcxYxwwgn_USME-VsJJ2','ISJdIl3mCFbsjixC-Cgwz',1699476971000,'I see it and I see the error in the auth-do

','nqYkxAGMnzk7Y5STjZryV'),
	 ('KRmWVrjPRMEj4E1EX7LP1','PJ9Bn1-zoLEuMR6OkwP70',1698516599000,'I found the [documentation on this](https://cloud.google.com/functions/docs/bestpractices/retries).

> When retries are not enabled for a (background) function, which is the default, the function always reports that it executed successfully, and 200 OK response codes might appear in its logs. This occurs even if the function encountered an error. To make it clear when your function encounters an error, be sure to [report errors](https://cloud.google.com/functions/docs/monitoring/error-reporting) appropriately.

So we either have to enable retries or use a different mechanism for surfacing the error (which could be error reporting or it could be log levels).

I''ll need to think about whether retries are the right thing to do.','ieK09sy2C_AIWE8KRkrQR'),
	 ('rxhXfm6_jDU1ZiJ77pOFo','PJ9Bn1-zoLEuMR6OkwP70',1698517874000,'After more research, I realize that retries may be useful for some cases, but for the purpose of alerts it''s not what we want. We''re supposed to throw an Error for transient, retryable scenarios, and _not_ throw them for non-retryable errors, which is somewhat opposite of how we want to be alerted.

I looked into error reporting, though, and it appears that these errors are already being nicely collected by the error reporter:

https://console.cloud.google.com/errors?project=reflect-mirror-prod&supportedpurview=project

![Screenshot 2023-10-28 at 11 26 43 AM](https://github.com/rocicorp/mono/assets/132324914/3645178d-f75d-4acd-a300-28753c1bfcea)

This is very nice (and I have actually used this in other companies ... just forgot about it  😉 ). It''s also catching some errors that the alerts missed, like the `Memory Limit` errors on publish.

The key will be figuring out the right process for distinguishing warnings (like the deprecation errors we return to users) from errors that we want to be alerted on. ','ieK09sy2C_AIWE8KRkrQR'),
	 ('TKWRsP7YWoDJtIDCi0s31','PJ9Bn1-zoLEuMR6OkwP70',1698518550000,'More good news. Error reporter does exactly what we want (with some roughness around the edges).

I was concerned about the deprecation errors rising to the top because I had converted those to warnings quite a while ago. It turns out that it''s a case of mis-bucketing; the error reporter is putting the `dev` errors into same bucket, and until this morning these were classified as errors.

![Screenshot 2023-10-28 at 11 37 14 AM](https://github.com/rocicorp/mono/assets/132324914/1f77d0ef-c17c-4714-bb34-9b2249f8fa7f)

Importantly, the stuff that we classify as warnings do not get surfaced to the error reporter.  🎉 

It also has some nice features like attaching bugs and setting state of errors to `Acknowledged` and `Resolved`, re-reporting as desired if something resurfaces.

I''m enabling notifications from the Error Reporter to our #mirror-prod-alerts channel. This could conceivably replace our existing error-level alerts (though I''ll keep the warning-level alerts around).

![Screenshot 2023-10-28 at 11 41 50 AM](https://github.com/rocicorp/mono/assets/132324914/a0bcb241-744f-43dd-9904-dcaa16a7432a)


','ieK09sy2C_AIWE8KRkrQR'),
	 ('frY4T-cUz5himcupahTCR','vK2pxdp1DXeMYUz6GGjag',1698458882000,'This happens even when rolling back to a previously "healthy" release. So it may be specific to `reflect-server.dev`.

Going to worker urls from the browser is fine. I''m kind of hesitant to push a new mirror server to prod though (which is currently working fine).  🤔 ','ieK09sy2C_AIWE8KRkrQR'),
	 ('xVA3FjA1oocVWIzcs3lLG','vK2pxdp1DXeMYUz6GGjag',1698459859000,'After pushing to prod, it happens there too.  😦 ','ieK09sy2C_AIWE8KRkrQR'),
	 ('lAvtwpLhwCLg8M8ZTygPD','vK2pxdp1DXeMYUz6GGjag',1698461189000,'Updating `firebase-functions` to the latest package did not help.','ieK09sy2C_AIWE8KRkrQR'),
	 ('50oGp0o80ubnqMJRK63yO','ttN93oYNQN0K-9xl_3SiO',1698976557000,'For posterity, I''ve run some experiments to verify the behavior for how Cloudflare enforces its [5KB limit on Environment Variables](https://developers.cloudflare.com/workers/platform/limits/#environment-variables). 

The limit appears to be for the value only:

This works:

```ts
   bindings: {
      vars: {
        [''E''.repeat(1024)]: ''H''.repeat(5120),
      },
```

But this results in an error:

```ts
   bindings: {
      vars: {
        [''E''.repeat(1024)]: ''H''.repeat(5121),
      },
```

```json
  code: 10054,
  error_chain: [
    {
      code: 10054,
      message: ''workers.api.error.text_binding_too_large''
    }
  ]
```

In addition, there appears to be an undocumented limit of 2712 bytes for the key name.

This works:

```ts
bindings: {
      vars: {
        [''B''.repeat(2712)]: ''A''.repeat(5120),
      },
```

But this results in an error:

```ts
bindings: {
      vars: {
        [''B''.repeat(2713)]: ''A''.repeat(5120),
      },
```


```json
code: 10100,
  error_chain: [
    {
      code: 10100,
      message: ''workers.api.error.binding_name_too_large''
    }
  ]
```

Finally, the limit does appear to apply to the UTF-8 encoded byte length. 

For two-byte characters:

```ts
// Works:
bindings: {
      vars: {
        [''M''.repeat(2712)]: ''£''.repeat(2560),
      },

// Fails:
bindings: {
      vars: {
        [''M''.repeat(2712)]: ''£''.repeat(2561),
      },
 ```

For three-byte characters:

```ts
// Works:
bindings: {
      vars: {
        [''M''.repeat(2712)]: ''文''.repeat(1706),
      },

// Fails:
bindings: {
      vars: {
        [''M''.repeat(2712)]: ''文''.repeat(1707),
      },
```','ieK09sy2C_AIWE8KRkrQR');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('hmaOkQ82dMSXydaY2YcCw','ttN93oYNQN0K-9xl_3SiO',1698976672000,'In summary, Cloudflare''s limits are:

* Max length of UTF-8 encoded variable name: 2712 bytes
* Max length of UTF-8 encoded variable value: 5120 bytes

The policy that we''ve implemented is:
* Max length of UTF-8 encoded name + value: 5120 bytes

Which falls safely within Cloudflare''s constraints.','ieK09sy2C_AIWE8KRkrQR'),
	 ('m7fZUk2wcym3-DTRUUfmQ','d7OdJV18QzZwnX7k-3v23',1698114183000,'Actually, perhaps can just ignore it on the server side. Then we don''t have to add extra logic to the cli. ','ieK09sy2C_AIWE8KRkrQR'),
	 ('FhYoLpmheqj2bYdF6cbnz','y0Ns_753Y19-y5yz6d4zH',1698013559000,'Maybe consider `ERR_RUNTIME_FAILURE` too?

Going to lump these into the same issue. Feel free to separate if that''s more appropriate.

https://console.cloud.google.com/monitoring/alerting/incidents/0.n3pfgkk59jih?project=reflect-mirror-prod

![Screenshot 2023-10-22 at 3 24 46 PM](https://github.com/rocicorp/mono/assets/132324914/d2cf4763-6a82-4ec2-8bce-781a939066f0)
','ieK09sy2C_AIWE8KRkrQR'),
	 ('rCndtHshpDpJH4o4na5sJ','YVZr9JZJcaGHoKizfAmaP',1697645373000,'@cesara ','nqYkxAGMnzk7Y5STjZryV'),
	 ('e5_7G1zkE6k4TsK2XS8AF','YPP0y4PmxYWaq-DRFLqI6',1697190425000,'Closing. Didn''t show any measurable perf gain.','nqYkxAGMnzk7Y5STjZryV'),
	 ('-5jn_ANePUqEVaqhNqVkH','L5S1gvDaQKHQz0xaXxWAG',1681172301000,'Moving this to a checkbox on existing bug (#350 )','OeVnr1y5bEM_Yg06sUFtD'),
	 ('tJpR3AWjePRS6SeA7ek1o','lxNic7aS6Z8ip9Jdc24Gr',1680638935000,'And if we decide to soften offline, then #367 can go back to beta.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Oef5NG3S89X8HL29JM_3F','56TpUjT6eLFKe2Of8w5_t',1681462993000,'IIRC, the code supports this behavior but the TS types do not.','nqYkxAGMnzk7Y5STjZryV'),
	 ('dhj4YEIhVvtIsdX_zw75i','pkY5VRD91YWXX0AYIT8Qt',1680568206000,'Note that this really wants `warn` to exist on our logging package. There have been a few cases of this. We should add it.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('-mI3wgN6B1-tqsGfK3s2y','AUuDgcjnoIuEq-aT1fSaW',1696999779000,'FWIW, I thought that the problem might be due to the large number of HTTP requests that `npm install` performs during a `reflect create`, so I tried a similar scenario with a fresh `reflect init`, but does not result in the same issue.

```
 analytics-test $ rm -rf node_modules reflect.config.json package-lock.json 
 analytics-test $ node ~/roci/mono/mirror/reflect-cli/out/index.mjs --stack=sandbox init
Installing @rocicorp/reflect
npm WARN deprecated sourcemap-codec@1.4.8: Please use @jridgewell/sourcemap-codec instead

added 531 packages, and audited 532 packages in 25s

75 packages are looking for funding
  run `npm fund` for details

found 0 vulnerabilities

You''re all set! 🎉

To start the Reflect dev server:

npx @rocicorp/reflect dev
 analytics-test $ 
 ```','ieK09sy2C_AIWE8KRkrQR');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('t7I3quTMUOzJEy4k5lHHS','1kcQ0SFauB8RyMVhHYpoR',1696318268000,'We need to be deliberate if we want server support. The code we had was using localStorage to do the broadcasting and that is not available on servers either.

FWIW, server environments are starting to support more and more of the browser APIs.

### BroadcastChannel
- https://nodejs.org/api/worker_threads.html#class-broadcastchannel-extends-eventtarget
- https://docs.deno.com/deploy/api/runtime-broadcast-channel
- https://bun.sh/blog/bun-v0.7.2


### localStorage
- nothing for nodejs
- https://docs.deno.com/runtime/manual/runtime/web_storage_api
- nothing for bun','nqYkxAGMnzk7Y5STjZryV'),
	 ('9kHNqG8JNubdvnKl5s_s3','1kcQ0SFauB8RyMVhHYpoR',1696319609000,'Fair. But does the bug report of BroadcastChannel not found on Safari 15.4 make sense to you? Noam is even saying he sees this in Safari 16.6 somehow.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('W9gMzvsLKws-r9USavCY2','1kcQ0SFauB8RyMVhHYpoR',1696319626000,'(I''m getting more information, just wondering right now if this jogs any ideas for you)','OeVnr1y5bEM_Yg06sUFtD'),
	 ('0ou9hZo7KZzSJLb26AfWv','1kcQ0SFauB8RyMVhHYpoR',1696328160000,'No ideas why Safari would not have it.

I''m thinking we could not broadcast the message if BroadcastChannel is not available. We currently use a channel for 2 things:

1. In case there is a newer client group so that the other tab can reload. This is so that offline usage can sync through IDB.
2. Informing ohter tabs that persist is done. This is once again to allow other tabs to pick up the changes faster.

If we have a noop channel for these in problematic browsers I think everything will continue to work but the multiple tab scenario sync will be slower.

@grgbkr What do you think?','nqYkxAGMnzk7Y5STjZryV'),
	 ('WvFAjSFBGE3PvbQvu4HcV','1kcQ0SFauB8RyMVhHYpoR',1696492634000,'This seems like a reasonable compromise to me.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('NcudtmRx0_g8cJstVnDXg','kGjZqdn3tW-YsT3rKyfEM',1695945454000,'Landed with helpful feedback / discussion with Aaron in https://rocicorp.slack.com/archives/C013XFG80JC/p1695926653293939

TL;DR, dist-tags are:
* `@latest`
* `@rec`: minimum non-deprecated version
* `@sup`: minimum supported version

<img width="677" alt="Screenshot 2023-09-28 at 1 10 37 PM" src="https://github.com/rocicorp/mono/assets/132324914/8f749859-7925-4eb7-89d3-cbfcc0544f8e">

','ieK09sy2C_AIWE8KRkrQR'),
	 ('y46KvnpLhxTIGNEseJbgV','jJd3-dXu2nheL40kaBR3x',1695301351000,'Two options:

1. Remove the CJS modules from replicache 13. They do not work
2. Update all the deps to have both esm and cjs

My vote is to do 1.','nqYkxAGMnzk7Y5STjZryV'),
	 ('ZsP1Hed6RuyXu2sy4Nhid','jJd3-dXu2nheL40kaBR3x',1695301430000,'Let''s try 1 for a bit and see how it flies.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('6-nLNsTRRMr_3HqpwH1s3','X3l7WaurF55GePP0sLJmh',1695188457000,'Another relevant thread is https://rocicorp.slack.com/archives/C013XFG80JC/p1695078215793509, where we consider a push model in which RoomDOs with connections periodically push their connection sets to the AuthDO via an Alarm. This would obviate the ping-and-wait-for-wake-up fuzziness.

I think technically this would spread out the revalidations and cause the AuthDOs to be awake more, but I imagine it''s a negligible increase in total execution time when the RoomDOs are active, and execution time will still zero out when there are no active connections.','ieK09sy2C_AIWE8KRkrQR'),
	 ('afgyu_LWXc-ZRRqp1MJNh','X3l7WaurF55GePP0sLJmh',1695190065000,'And as a general principle, we should strive for designs/protocols that minimize the I/O (and execution variability) that happens in a lock.  (Reminds me of #567 as another example of how protocol design can streamline critical sections)','ieK09sy2C_AIWE8KRkrQR');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('bJgvOuvIbSuWl7LXPqdri','X3l7WaurF55GePP0sLJmh',1695321052000,'> And as a general principle, we should strive for designs/protocols that minimize the I/O (and execution variability) that happens in a lock. (Reminds me of #567 as another example of how protocol design can streamline critical sections)

Agreed.  I was aware at the time of the short comings of the locking in AuthDO, but I was satisficing on the design.   I agree that the timestamp based approach in https://github.com/rocicorp/mono/issues/567 is the way to go. ','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('uJynmmyXWavdgS5Guj6Ot','x_yzltL4WLH7N2KwJ4_Cv',1694159645000,'I think subscribe is the wrong abstraction. Maybe watch is a better one since there is not `body` to execute here.','nqYkxAGMnzk7Y5STjZryV'),
	 ('2_PY5sUr7ECCxscjRmWCA','x_yzltL4WLH7N2KwJ4_Cv',1709537043000,'We actually did this!','OeVnr1y5bEM_Yg06sUFtD'),
	 ('OBTzq9EcSRL8OfwXjndsY','OMiIbH3YrOa2RT9B4HI2c',1693511148000,'We should definitely do a pass over how we expose errors. Right now we have a lot of unhandled exceptions that are surfaced.','nqYkxAGMnzk7Y5STjZryV'),
	 ('gGBuxH0oj9r9LL1fdKt94','OMiIbH3YrOa2RT9B4HI2c',1693511338000,'It looks like we are limited to 99 domain records:

https://dash.cloudflare.com/085f6d8eb08e5b23debfb08b21bda1eb/reflect-server.net/dns/records?recordsPage=2

<img width="777" alt="Screenshot 2023-08-31 at 12 47 41 PM" src="https://github.com/rocicorp/mono/assets/132324914/6120028d-ef26-48b8-aed8-fcbb4ff8dfc8">

I didn''t find a place in the dashboard where the limit can be increased.','ieK09sy2C_AIWE8KRkrQR'),
	 ('86blJCv-2hyvLMZCmm-JP','OMiIbH3YrOa2RT9B4HI2c',1693514510000,'I will loop in cloudflare.

On Thu, Aug 31, 2023 at 9:49 AM d-llama ***@***.***> wrote:

> It looks like we are limited to 99 domain records:
>
>
> https://dash.cloudflare.com/085f6d8eb08e5b23debfb08b21bda1eb/reflect-server.net/dns/records?recordsPage=2
> Screenshot.2023-08-31.at.12.47.41.PM.png (view on web)
> <https://github.com/rocicorp/mono/assets/132324914/6120028d-ef26-48b8-aed8-fcbb4ff8dfc8>
>
> I didn''t find a place in the dashboard where the limit can be increased.
>
> —
> Reply to this email directly, view it on GitHub
> <https://github.com/rocicorp/mono/issues/902#issuecomment-1701690289>, or
> unsubscribe
> <https://github.com/notifications/unsubscribe-auth/AAATUBAUXXTFOTP6VGYEHMDXYDTDJANCNFSM6AAAAAA4GR2TG4>
> .
> You are receiving this because you were mentioned.Message ID:
> ***@***.***>
>
','OeVnr1y5bEM_Yg06sUFtD'),
	 ('j3T7nNGPzYrgPHB5GUYUS','pkY5VRD91YWXX0AYIT8Qt',1681462932000,'I''m not sure why we cannot throw in the case of nested transaction? Maybe the problem is to detect that we are nesting them in an async context?','nqYkxAGMnzk7Y5STjZryV'),
	 ('T29WOH_D0ADtzu0yCh_CG','OMiIbH3YrOa2RT9B4HI2c',1693514539000,'+Aaron Boodman ***@***.***>

On Thu, Aug 31, 2023 at 10:41 AM Aaron Boodman ***@***.***>
wrote:

> I will loop in cloudflare.
>
> On Thu, Aug 31, 2023 at 9:49 AM d-llama ***@***.***> wrote:
>
>> It looks like we are limited to 99 domain records:
>>
>>
>> https://dash.cloudflare.com/085f6d8eb08e5b23debfb08b21bda1eb/reflect-server.net/dns/records?recordsPage=2
>> Screenshot.2023-08-31.at.12.47.41.PM.png (view on web)
>> <https://github.com/rocicorp/mono/assets/132324914/6120028d-ef26-48b8-aed8-fcbb4ff8dfc8>
>>
>> I didn''t find a place in the dashboard where the limit can be increased.
>>
>> —
>> Reply to this email directly, view it on GitHub
>> <https://github.com/rocicorp/mono/issues/902#issuecomment-1701690289>,
>> or unsubscribe
>> <https://github.com/notifications/unsubscribe-auth/AAATUBAUXXTFOTP6VGYEHMDXYDTDJANCNFSM6AAAAAA4GR2TG4>
>> .
>> You are receiving this because you were mentioned.Message ID:
>> ***@***.***>
>>
>
','OeVnr1y5bEM_Yg06sUFtD'),
	 ('C9K-rn2mewSLAUtl0vg0L','OMiIbH3YrOa2RT9B4HI2c',1694564907000,'FYI, I think both we and Tanushree misunderstood the problem here. (The hint was that she referred to some kind of domain limit *per worker*, which was not our problem).

I think the limit of 100 Custom domains is just part of our Free plan:

https://developers.cloudflare.com/pages/platform/limits/#custom-domains

> Custom domains
> Based on your Cloudflare plan type, a Pages project is limited to a specific number of custom domains. This limit is on a per-project basis.
> 
> Free | Pro | Business | Enterprise
> -- | -- | -- | --
> 100 | 250 | 500 | 500
> 

(I realize that this is part of the "Pages" documentation but my hunch is that this is where the limit comes from.)

 Upgrading to Pro or Business would cost $20 and $200 per month, respectively:

<img width="1336" alt="Screenshot 2023-09-12 at 5 20 29 PM" src="https://github.com/rocicorp/mono/assets/132324914/865e9840-1978-43cf-a0f0-a9971ff1496f">

The silver lining here is that we should be able to overcome our limit by upgrading our plan, so we''re not blocked on migrating to Workers for Platforms.

I''m still digging into mapping out a game plan for WfP, but it won''t change the fact that we''ll need to pay for more domains, as WfP has similar Plan-based limits on hostnames:

https://developers.cloudflare.com/cloudflare-for-platforms/cloudflare-for-saas/plans/

<img width="1328" alt="Screenshot 2023-09-12 at 5 24 39 PM" src="https://github.com/rocicorp/mono/assets/132324914/649f7457-014d-4eae-9556-37a21094fe14">


 ','ieK09sy2C_AIWE8KRkrQR'),
	 ('pwwhR6guuc05dsroLlh4x','OMiIbH3YrOa2RT9B4HI2c',1694631544000,'After playing around with this, it turns out that I was wrong.

* reflect-server.dev (Free Plan, Rocicorp DEV account): Max of 100 worker custom domains
* reflect-server.net (Free Plan, Rocicorp LLC account): Max of 300 worker custom domains
* replicache.dev (Enterprise Plan, Rocicorp LLC account): Max of 300 worker custom domains

So indeed our limit is what Tanushree bumped us too, even for the zone that''s officially on the "Enterprise Plan".

So moving to Workers for Platforms should indeed allow us to scale to many more hostnames. The first 100 are free, and the default max is 5000, but Enterprise customers can remove that 5000 limit by contacting sales:

https://developers.cloudflare.com/cloudflare-for-platforms/cloudflare-for-saas/plans/','ieK09sy2C_AIWE8KRkrQR');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('8QjFpEUmEKYY0XEM4CFO9','OMiIbH3YrOa2RT9B4HI2c',1697222471000,'Problem is understood now. The migration to WFP addresses this issue.','ieK09sy2C_AIWE8KRkrQR'),
	 ('8XfMoZ_UUcTOyq2mjGi0y','PxFcGtMEWEjB09ecJKtPx',1692382992000,'Two things seem to work, though neither of them ideal.

1. Reference a different version for `@rocicorp/reflect` in the `mirror-cli/package.json`. Then npm picks up the code from the registry.
2. Download the tarball for the package and reference it as `file:reflect-...tgz` in `mirror-cli/package.json`. This allows you to use the version that''s in the repo, while using the code actually published in npm. But it takes a bit of work.

It would be nice to have a magic solution that makes `mirror-cli` always reference the canonical npm package.  🤔 ','ieK09sy2C_AIWE8KRkrQR'),
	 ('db5HfNuaPKt3IYoEuVkyL','PxFcGtMEWEjB09ecJKtPx',1692848163000,'I think a way to do this could be to download the tarball from npm directly and extract it: https://stackoverflow.com/questions/33530978/download-a-package-from-npm-as-a-tar-not-installing-it-to-a-module','OeVnr1y5bEM_Yg06sUFtD'),
	 ('BXRqyVb4mwqZQdP8kHctz','PxFcGtMEWEjB09ecJKtPx',1692907314000,'Let''s download the file from npm','nqYkxAGMnzk7Y5STjZryV'),
	 ('Q4OjkF-h80ockXkV_Cx7L','PxFcGtMEWEjB09ecJKtPx',1694778540000,'I looked at this a bit. Downloading the tarball is fine, but then when we try to build from it we need to `npm install` because of the deps. Not a big deal since this is only for our internal usage.','nqYkxAGMnzk7Y5STjZryV'),
	 ('5oCs56AEO8AD4Auxvy9o6','PxFcGtMEWEjB09ecJKtPx',1694779516000,'...and if have to use `npm install` to get the deps we might as well use npm add `@rocicorp/reflect` to get the files.','nqYkxAGMnzk7Y5STjZryV'),
	 ('GEki90huQOuI6UDpOcW5M','PxFcGtMEWEjB09ecJKtPx',1694810971000,'One thing that I have in my client is an option to build from source (to be able to push non-published servers for development or debugging). It''s basically a flag that asserts that the path does _not_ have /node_modules in it, as well as a fake version to upload the module as.

It would be nice to be able to preserve that capability if possible. I''ll send you a PR so that you have a better idea.','ieK09sy2C_AIWE8KRkrQR'),
	 ('wZzeiREJQusGVqLZ1hsUx','rdusQ_QfKBY14U2RAr04X',1691542302000,'See also #367, #808, #807.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('DcQjo1yPsMlRTLzWHvJIX','H1CChLY9exEX7igHpZMaX',1691466519000,'Actually now that I think of it, I remember that Erik and I decided that Valita was fast enough to have on by default, but just that we could add an "escape hatch" flag for disabling it if user really wanted to go as fast as possible. I can''t remember if this decision applied to both client and server or if we actually added the hatch.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('rmu327Gg-Nj4kGhoFZwvE','H1CChLY9exEX7igHpZMaX',1691478872000,'That seems to fit with what I remember too.

We did not add the escape hatch yet.

I think the next step is to identify where validation is happening and decide what knobs to provide.','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('fuzqbOqc_Qd1ieQmyw6Oi','_4bceppFYIYK_8dy_Fhgq',1691396851000,'Thanks','nqYkxAGMnzk7Y5STjZryV'),
	 ('z0WAP8tco0s6XrYZ5FUjD','Yfoz9WZxoQFBshtfKs-0g',1690585245000,'Thanks @grgbkr 

With the ability for sandbox to have its own env vars, sandbox.reflect.net can use the `reflect-mirror-staging` (perhaps renamed to `reflect-mirror-sandbox`) FIrebase project so that reflect-cli + cloud-function development does not require running a local instance of the login page.','ieK09sy2C_AIWE8KRkrQR'),
	 ('frMvMzjyui0Xnj_qvbk6I','0PGwUHDk4LqigVDlDaMJd',1690493573000,'cc @arv @grgbkr ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('REKdjbhxdLD7k4ODHlsxm','0PGwUHDk4LqigVDlDaMJd',1710163847000,'fixed by https://github.com/rocicorp/mono/pull/1463','tDY6IbKdVqbBlRBc3XMwF'),
	 ('DTC0RuJAEjloUKfRc7PJO','jevoTlhabxVRkXuDbPVjK',1709545367000,'I believe closely related to #754','OeVnr1y5bEM_Yg06sUFtD'),
	 ('O3QFEBrldj_Z0jZ3VdgvZ','jevoTlhabxVRkXuDbPVjK',1709577933000,'~~Looks like this is already fixed (see console output in the screenshot below) --~~

![Screenshot 2024-03-04 at 1 43 00 PM](https://github.com/rocicorp/mono/assets/1009003/83d5656e-cd15-401e-a1a7-a72d0c699f67)

~~Assuming that the correct behavior is to throw away null cookies, which it must be since `null` indicates the _first_ cookie: https://doc.replicache.dev/reference/server-pull#cookie~~

Ignore me. Was able to repro in a new tab.
','tDY6IbKdVqbBlRBc3XMwF'),
	 ('X8aRrY-sSMjYRM3RbhhVP','nX6Ty8TT2ggpam2E1pqad',1689238462000,'I like the idea of deleting all local state in debug mode!','nqYkxAGMnzk7Y5STjZryV'),
	 ('FesVi2pyxpYuZQu5eSCG1','nX6Ty8TT2ggpam2E1pqad',1689266714000,'Why doesn''t refreshing fix it?  The new client should not get assigned to the disabled client group, but perhaps we have a bug here https://github.com/rocicorp/mono/blob/main/packages/replicache/src/persist/clients.ts#L508.  

','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('ecwCuyN-6UdpMiOw30ehf','nX6Ty8TT2ggpam2E1pqad',1689277403000,'I didn''t understand that''s what this code is trying to do. I don''t think it''s what I''m seeing though, will confirm.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('fzbEBe7IIG4PKCLcnEeQR','eLdyfZP3th1jJmzZxS_R-',1689238713000,'We used to use localStorage as a fallback. We removed it to make things simpler. There is no reason we cannot add back that fallback path.','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('onBrbIQmrr6jo-ia6dmnv','Is9thv8wlN9pyAK3J6KAf',1689882914000,'- [ ] Mirror server generates a REFLECT_AUTH_API_KEY when an app is created. This key gets sent to the client when the app is created and printed to the console. It is also stored in firebase in the apps collection so that we can set the secret when we publish to cloudflare.
  - [ ] Should we store this in the app config (reflect.config.json)?
- [ ] Provide a way to reset/get a new REFLECT_AUTH_API_KEY in case the key has been compromised.
- [ ] For dev mode we can use a dummy REFLECT_AUTH_API_KEY.
- [ ] We should remove the authentication for calls from the main worker to the DOs since these are safe and can only come from the same CF worker script.','nqYkxAGMnzk7Y5STjZryV'),
	 ('9DZb9wZYo_dJPtPXB_ZU8','Is9thv8wlN9pyAK3J6KAf',1695410765000,'I had a conversation with Greg about this a while back, and the preference we concluded was to handle this value as a secret and avoid storing it insecurely (which includes storing it in plainly in Firestore, as that data can be exposed in leaked backups, etc.).

What I''d prefer to avoid, however, is storing a lot of secrets in the Secret Manager because it''s extra datastore management and is [relatively expensive](https://cloud.google.com/secret-manager/pricing) (at least, compared to the other GCP costs,  which for our usage is pretty much free).

The rough idea I had in mind is to store a single master key in the Secret Manager, and then store a random plaintext in Firestore for each app. The REFLECT_AUTH_API_KEY for the app would be the plaintext encrypted with the master key. On that scheme we can implement key replacement by replacing the plaintext, or key rotation by storing multiple plaintexts (and having reflect accept multiple keys).

The downside to this approach is that leaking the master key puts everyone''s keys at risk, but only if the plaintexts are also exposed. I think the way to address this is to have each plaintext be associated with the master key version, and if the latter is leaked, we would create a new master key/version and rotate in a new plaintext with that version. Then we''d notify everyone to switch to their new resulting API_KEY.

This is not a high priority at the moment, but I wanted to jot down my thoughts so that I don''t forget.','ieK09sy2C_AIWE8KRkrQR'),
	 ('CWWJUOiybzWe0zd5wjWjH','Is9thv8wlN9pyAK3J6KAf',1698424739000,'I''ll take this as it it has some synergy with #1150','ieK09sy2C_AIWE8KRkrQR'),
	 ('SWg1vqFfMze0simufRRcu','Is9thv8wlN9pyAK3J6KAf',1698759114000,'I feel like this is not done.

We do not yet have a way to get the REFLECT_AUTH_API_TOKEN so that we can invoke the REST API.

Straw proposal:

```
npx reflect api-token 
npx reflect api-token --rotate
```

I also think we might want to expose the actual REST endpoints as conveniences on the reflect commands. See https://github.com/rocicorp/reflect-todo/blob/main/doc/server-api.md for a list of the existing endpoints','nqYkxAGMnzk7Y5STjZryV'),
	 ('7sr7EbSp5R0MUi3B_yol_','Is9thv8wlN9pyAK3J6KAf',1698765042000,'Good point. For real down-time free rotation, we would technically need to add `reflect-server` support for two keys (old and new) while clients are updating, but we can probably get away with one-key-at-a-time rotation.','ieK09sy2C_AIWE8KRkrQR'),
	 ('CQ9Q1vP3MBlV4AAmorYxL','Is9thv8wlN9pyAK3J6KAf',1698765285000,'Also, if we did want to rename the header, now would be the time to do it. 

 @grgbkr @arv what do you think?','ieK09sy2C_AIWE8KRkrQR'),
	 ('CoKHQi-VABpUsrsuxMJxC','Is9thv8wlN9pyAK3J6KAf',1698879323000,'Yes. Let''s rename it ','nqYkxAGMnzk7Y5STjZryV'),
	 ('VFTddOpPbfZnbu-lX7wJj','ZhUktojn1c_KNQY7PFqTX',1687814590000,'The problem is that we were using `[string, string][]` for the HTTP headers. The fetch spec allows this but it seems like React Native is having trouble with this. It isn''t clear if they have fixed this or not (their .d.ts includes the tuple form).

I changed the license code to use `Record<string, string>` in `replicache` but we would need a release for this to work out of the box.','nqYkxAGMnzk7Y5STjZryV'),
	 ('tO6PKRekvrCzA6K_ntrHR','vGmjjI13s36VXwdf36Dzx',1687497970000,'To unblock deploys I''m changing the build command  from:
`npm run build --prefix=../.. && ./publish-if-production.sh`
to:
`npm run build --prefix=../..`

https://github.com/rocicorp/mono/pull/639


We should try to get this working again.  For now we will need to manually wrangler publish from our machines.','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('z-xqgw_f0HGzRtatRnCcU','vGmjjI13s36VXwdf36Dzx',1687545765000,'im also sometimes seeing this error when publishing from my machine.  ','Gg4MskWt3M-ttzzlrJ9jn');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('KvyfGCvMCVCzfznulHZiI','vGmjjI13s36VXwdf36Dzx',1687556528000,'I think this may have started with my change that pulls in the datadog libraries, which increased the code size to 2000+ kb. According to [Worker startup time](https://developers.cloudflare.com/workers/platform/limits/#worker-startup-time):

```
 Script size can impact startup because there’s more code to parse and evaluate.
```

The amount of actual code that we use in the libraries is not actually that large though, so I''m wondering if we''re not effectively tree-shaking the new code.

I tried adding the [`commonjs()`](https://github.com/rollup/plugins/tree/master/packages/commonjs#readme) plugin to our rollup config, but that didn''t help (perhaps because rollup is only used for the d.ts files). Maybe @arv can figure out whether we can improve the tree shaking with the commonjs libraries we''re pulling in.

The other option, of course, is to roll back the datadog lib change and handroll the api / monitoring code, but if we can solve this at the toolchain level it would improve our ability to pull in 3rd party libs.','ieK09sy2C_AIWE8KRkrQR'),
	 ('We3ILj2MwueGdrgC4_fPa','pkY5VRD91YWXX0AYIT8Qt',1681487381000,'We want it to be possible to open overlapping transactions. Like if a user click rapidly and each click fires a mutation, since they are async, this can easily lead to overlapped writes. What we don''t want is transactions to be waiting on each other in a cycle. But we have no way to know which tx are waiting on which AFAICT.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('F-0C9FUeMqOAtP9GeDhX4','vGmjjI13s36VXwdf36Dzx',1687772186000,'```
$ cd apps/reflect.net
$ wrangler publish --dry-run --outdir distx
$ ls -l distx/
total 14096
-rw-r--r--  1 arv  staff      117 Jun 26 11:00 README.md
-rw-r--r--  1 arv  staff  2788488 Jun 26 11:00 index.js
-rw-r--r--  1 arv  staff  4421961 Jun 26 11:00 index.js.map
```

Going back to the change before 897ceacffdb964bd4d96706d459acca411e6401a:

```
$ git co 897ceacffdb964bd4d96706d459acca411e6401a~1
$ npm run build
$ cd apps/reflect.net
$ wrangler publish --dry-run --outdir distx2
$ ls -l distx2/
total 2728
-rw-r--r--  1 arv  staff      117 Jun 26 11:16 README.md
-rw-r--r--  1 arv  staff   262344 Jun 26 11:16 index.js
-rw-r--r--  1 arv  staff  1125319 Jun 26 11:16 index.js.map
```

The server code size increased 10x 

Cloudflare claims the code size limit is 10MB and 1MB on free accounts https://developers.cloudflare.com/workers/platform/limits/#worker-size

There is also a [startup time limit of 200ms](https://developers.cloudflare.com/workers/platform/limits/#worker-startup-time). It seems plausible that those 2.5MB of code the datadog library takes too long to parse and initialize.

Next step... Try some dead code elimination','nqYkxAGMnzk7Y5STjZryV'),
	 ('nZeDxZnWL2FM_V675Yop5','87zPmfDfX5GQVTh4NS0VX',1687500317000,'https://codemirror.net/examples/collab/','OeVnr1y5bEM_Yg06sUFtD'),
	 ('usy6Y6XhoFVx5KkAfUXQW','EdU6LLE7VatzZZXmMFnna',1687213365000,'<img width="918" alt="image" src="https://github.com/rocicorp/mono/assets/19158916/c3fb0f8a-3f3f-4b5f-ba7b-51c5b4d8b69c">
','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('04tpVMBcPf_M5TZCJjhnO','EdU6LLE7VatzZZXmMFnna',1687247873000,'> 4. A suspicion: Is it possible you are missing waiting on a promise somewhere, and the mutator is actually completing before the call to tx.get call in rehashBlockPaths that is leading to the ChunkNotFoundError?

I was thinking the same thing.

Are we missing a check for is `closed` somewhere along the path?','nqYkxAGMnzk7Y5STjZryV'),
	 ('vuYn5i4uFh54M1Jow1qYe','EdU6LLE7VatzZZXmMFnna',1687248457000,'It would also be important to know if they are using more than one instance of `Replicache`. All the issues we have seen in the past have been due to us not keeping things alive correctly related to persist/refresh.','nqYkxAGMnzk7Y5STjZryV'),
	 ('n3k3g2cRVnegOxii92qDn','EdU6LLE7VatzZZXmMFnna',1687366990000,'Meeting with the customer that reported this, Julian Benegas, today.','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('aQMtyYJUOYhXqTjCfb4EP','EdU6LLE7VatzZZXmMFnna',1687398822000,'From talking with Julian I have learned that.

- The issue is not a missing await on a promise, the ChunkNotFoundError is happening before the mutator''s returned promise resolves.
- Customer was encountering ChunkNotFoundError''s in Replicache 12.0.1 as well.
- In this repro with deleteBlocks, actually several calls to tx.get are hitting ChunkNotFoundErrors, but they are being collapsed to one error by Promise.all. 
- The issue seems to be related with doing parallel work in mutators.  The customer has encountered ChunkNotFoundErrors in a few mutators and has found that replacing `Promise.all` with a for loop that awaits sequentially often avoids the ChunkNotFoundError.  In this specific repro with deleteBlocks, the error can be avoided by replacing
``` 
await Promise.all([
   ...(isBlockWithChildren(block)
     ? block.value.children.map(async (c) => {
         await deleteBlockAndNestedBlocks(tx, {
           id: c.id,
           idSeed,
           isoNow,
           path: path + ''/'' + c.id,
           skipRehashing, 
         })
       })
     : []),
 ])
```
  with 
```
  if (isBlockWithChildren(block)) {
    for (const child of block.value.children) {
      await deleteBlockAndNestedBlocks(tx, {
        id: child.id,
        idSeed,
        isoNow,
        path: path + ''/'' + child.id,
        skipRehashing, 
      })
    }
  }
```

The code for the deleteBlocks mutator that leads to this error is below.

```
/* -------------------------------------------------------------------------------------------------
 * DELETE
 * -----------------------------------------------------------------------------------------------*/

export type DeleteBlockParams = {
  id: string
  path: string
  isoNow: string
  idSeed: string
  skipRehashing?: boolean
}

export const deleteBlock = async (
  tx: WriteTransaction,
  { id, path, isoNow, idSeed, skipRehashing }: DeleteBlockParams
) => {
  const normalizedPath = normalizeStringPath({ path, edgeId: id })
  const blocksInPath = [...normalizedPath]

  const rootBlockId = blocksInPath[0]
  invariant(rootBlockId)
  const blockId = blocksInPath.pop()
  const parentBlockId = blocksInPath.pop()

  if (!blockId || !parentBlockId) throw new Error(''Invalid path'')

  const [thisBlock, parentBlock] = await Promise.all([
    blockBaseOps.get(tx, blockId),
    blockBaseOps.get(tx, parentBlockId),
  ])
  if (!thisBlock) throw new Error(`Block with id ${blockId} not found`)
  if (!parentBlock || !isBlockWithChildren(parentBlock)) {
    throw new Error(`Parent block not found or not valid`)
  }

  if (isComponentBlock(thisBlock)) {
    await detachInstancesOfComponent(tx, {
      isoNow,
      idSeed,
      blockId: thisBlock.id,
      path,
    })
  }

  await deleteBlockAndNestedBlocks(tx, {
    id,
    path,
    isoNow,
    idSeed,
    skipRehashing,
  })

  // if parent block is component, need to re-sync all instances of it
  if (isComponentBlock(parentBlock)) {
    await syncAllComponentInstances(tx, {
      componentBlockId: parentBlock.id,
      idSeed,
      isoNow,
      rootBlockId,
    })
  }
}

/**
 * Self explanatory.
 *
 * **IMPORTANT:** Assumes you''re not passing a child key and then its parent.
 * You''ll need to handle that filtering before passing the blockIds here, else you''ll break stuff.
 * See the tree primitive for an example implementation on how selected keys are filtered by parent/child relationship.
 */
export async function deleteBlocks(
  tx: WriteTransaction,
  {
    rootId,
    blockIds,
    isoNow,
    idSeed,
  }: { rootId: string; blockIds: string[]; isoNow: string; idSeed: string }
) {
  console.log(''starting deleteBlocks'')
  const allBlocks = await blockBaseOps.list(tx)
  // 1. build tree
  const treeManager = await buildTree(tx, rootId, allBlocks)

  // 2. get paths for each block
  const blockPaths = blockIds.map((bId) => {
    const block = treeManager.getNode(bId)
    if (!block) throw new Error(`Block with id ${bId} not found`)
    const path = treeManager.getPathForKey(bId, ''string'')
    return { path, bId }
  })

  // 3. call `deleteBlockOnPath` on each one.
  // unfortunately, this needs to be synchronous, as we need to delete the blocks in order
  for (const { bId, path } of blockPaths) {
    await deleteBlock(tx, {
      id: bId,
      path,
      isoNow,
      idSeed,
    })
  }
  console.log(''returning from deleteBlocks'')
}

/**
 * Deletes block and all its nested blocks, without worrying about paths or hashes.
 * WARNING: This function should be paired with another function that updates the parent block''s hash and value.
 */
export async function deleteBlockAndNestedBlocks(
  tx: WriteTransaction,
  {
    id,
    path,
    isoNow,
    idSeed,
    skipRehashing,
  }: {
    id: string
    path: string
    isoNow: string
    idSeed: string
    skipRehashing?: boolean
  }
) {
  const normalizedPath = normalizeStringPath({ path, edgeId: id })
  const block = await blockBaseOps.get(tx, id)
  if (!block) throw new Error(`Block with id ${id} not found`)

  if (isComponentBlock(block)) {
    await detachInstancesOfComponent(tx, {
      isoNow,
      idSeed,
      blockId: block.id,
      path,
    })
  }

  // 1. delete
  await blockBaseOps.delete(tx, id)
  // deleting test

  // 2. rehash (remove reference from parent)
  if (!skipRehashing) {
    // root/test/a
    // root/test/b
    // root/test/c
    await rehashBlockPaths(tx, { paths: [normalizedPath.join(''/'')] })
  }

  // 3. delete orphan children
  await Promise.all([
    ...(isBlockWithChildren(block)
      ? block.value.children.map(async (c) => {
          await deleteBlockAndNestedBlocks(tx, {
             id: c.id,
             idSeed,
             isoNow,
            path: path + ''/'' + c.id,
             skipRehashing, 
           })
         })
      : []),
  ]);
}

/* -------------------------------------------------------------------------------------------------
 * Re-hash Paths
 * -----------------------------------------------------------------------------------------------*/

export const rehashBlockPaths = async (
  tx: WriteTransaction,
  { paths }: { paths: string[] }
) => {
  const normalizedPaths = paths.map((p) => {
    return normalizeStringPath({
      path: p,
      format: ''array'',
    })
  })

  // merge these paths into the shortest possible quantity
  // for example, if we have [''a'', ''a/b'', ''a/b/c'', ''a/b/c/d'']
  // we can only have [''a/b/c/d''], and that should cover all the re-hashing we need to do
  const mergedPaths = normalizedPaths.reduce<string[][]>((acc, path) => {
    if (acc.length === 0) {
      acc.push(path)
      return acc
    }

    const existingPathIndex = acc.findIndex((p) =>
      path.join(''/'').startsWith(p.join(''/''))
    )

    if (existingPathIndex !== -1) {
      acc.splice(existingPathIndex, 1, path)
    } else {
      acc.push(path)
    }

    return acc
  }, [])

  // store old hashes, to decide if we send an update
  const blockHashMap = new Map<string, string | null>()
  // store all blocks that will get updated
  const blockCache = new Map<string, Block | null>()

  const getBlockOrCache = async (id: string) => {
    if (blockCache.has(id)) {
      return blockCache.get(id) ?? null
    }
    const block = await blockBaseOps.get(tx, id)
    blockCache.set(id, block ?? null)
    if (!blockHashMap.has(id)) blockHashMap.set(id, block?.hash ?? null) // store old hash first time
    return block ?? null
  }

  await Promise.all(
    mergedPaths.map(async (path) => {
      await Promise.all(
        path.map(async (id) => {
          await getBlockOrCache(id)
        })
      )
    })
  )

  // re-hash all blocks (but don''t update yet, cause some blocks might be re-hashed more than once! e.g; root block)
  for (let index = 0; index < mergedPaths.length; index++) {
    const path = mergedPaths[index]
    invariant(path)

    let previousBlock: { id: string; hash: string } | undefined = undefined
    let previousBlockDeleted = false

    while (path.length > 0) {
      const currentBlockId = path.pop()
      invariant(currentBlockId)
      const block = await getBlockOrCache(currentBlockId)

      if (block && isBlockWithChildren(block) && previousBlock) {
        if (previousBlockDeleted) {
          // remove the reference to the previous block
          block.value = {
            ...block.value,
            children: block.value.children.filter(
              (child) => child.id !== previousBlock?.id
            ),
          }
        } else {
          // update the hash of the child reference to the previous block
          block.value = {
            ...block.value,
            children: block.value.children.map((child) => {
              if (child.id === previousBlock?.id) {
                return { ...child, hash: previousBlock.hash }
              }
              return child
            }),
          }
        }
      }

      if (!block) {
        // block was deleted
        // so parent will need to remove its reference to this block
        previousBlockDeleted = true
        previousBlock = { id: currentBlockId, hash: '''' }
      } else {
        previousBlockDeleted = false
        // else normal case: hash it, and store it as the previous block
        const hash = hashBlock(block)
        block.hash = hash
        blockCache.set(block.id, block)
        previousBlock = { id: block.id, hash }
      }
    }
  }

  // update all blocks that have changed
  const voidPromises: Promise<void>[] = []
  for (const [id, block] of blockCache.entries()) {
    if (!block) continue
    if (blockHashMap.get(id) !== block.hash) {
      voidPromises.push(blockBaseOps.update(tx, block))
    }
  }

  await Promise.all(voidPromises)
}
```','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('6oHZRFq8TjCNUQiMBNvuT','EdU6LLE7VatzZZXmMFnna',1687399009000,'I have been trying to create a reduced repro by writing mutators that do similar things to the above (a mix of deletes and reads done in parallel), but so far have not had luck.

','Gg4MskWt3M-ttzzlrJ9jn');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('_KbOb9hpjUKWFaCuqR5V8','EdU6LLE7VatzZZXmMFnna',1687420952000,'Did you figure out if they have multiple Replicache instances?','nqYkxAGMnzk7Y5STjZryV'),
	 ('B-hl6fyuC6j7ywk6rnd7v','EdU6LLE7VatzZZXmMFnna',1687861844000,'Here is a reproducible test: https://github.com/rocicorp/mono/pull/657. We are getting a `_splice` of a mutable node during the `get`.

Possible solutions:

### No mutable nodes

The motivation of allowing nodes to be mutable was for performance and memory usage. If we can prove that it is safe to mutate the Node then a `_replaceChild` (for example) is `O(1)` instead of `O(n)` where `n` is the number of nodes at that level. If we have these as immutable we have to copy the entries and create new Nodes which puts more pressure on the GC.

### Read Write Lock

Right now the BTreeWrite has a lock on the write operations. For read, we tried to prevent having an RWLock by checking if the tree changed and then start over in the case of a change.
','nqYkxAGMnzk7Y5STjZryV'),
	 ('G86ocXSrPRIheyOqlyI_h','EdU6LLE7VatzZZXmMFnna',1687881882000,'I benchmarked things with isMutable always false. This means that we never mutate existing nodes and create new ones for partition etc.

https://docs.google.com/spreadsheets/d/1UPeZX3Xy84ZPkfMYDPAE-305PtAynotZJI7uZdBGjGM/edit?usp=sharing

[populate tmcw](https://docs.google.com/spreadsheets/d/1UPeZX3Xy84ZPkfMYDPAE-305PtAynotZJI7uZdBGjGM/edit#gid=0&range=B33) and [other populate](https://docs.google.com/spreadsheets/d/1UPeZX3Xy84ZPkfMYDPAE-305PtAynotZJI7uZdBGjGM/edit#gid=0&range=B10:B11) tests are impacted a lot.','nqYkxAGMnzk7Y5STjZryV'),
	 ('COFRrePQePB8HW8kdgXdL','EdU6LLE7VatzZZXmMFnna',1687965787000,'#657 has 2 different approaches. Both have negative impact on the populate performance tests

My thinking is that we should make the nodes immutable because that gives me confidence that things are working correctly.

To alleviate the performance regression we can actually remove the lock in `put` (and `delete`/`clear`). The lock on `put` is there because:

```ts
  /**
   * This rw lock is used to ensure we do not mutate the btree in parallel. It
   * would be a problem if we didn''t have the lock in cases like this:
   *
   * ```ts
   * const p1 = tree.put(''a'', 0);
   * const p2 = tree.put(''b'', 1);
   * await p1;
   * await p2;
   * ```
   *
   * because both `p1` and `p2` would start from the old root hash but a put
   * changes the root hash so the two concurrent puts would lead to only one of
   * them actually working, and it is not deterministic which one would finish
   * last.
   */
```

What we can do instead is that we can detect if the root hash changed and if it did we start over. That approach is already used in `scan`. The case where this gets slower is when you do a lot of parallel `put`s.','nqYkxAGMnzk7Y5STjZryV'),
	 ('S4cq8HPFtfuXmD4kVqLyn','pZ11EgiQRZFsu-WEK6Det',1709536236000,'I don''t think we should do this anymore, because we should have first-class search instead.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('S7MVPgpflDgUd8QQD0jpt','wDYB9lsmTFlpp8a-3Rhqk',1686860093000,'Below is the code for drawing which Noam shared with me.
What initially jumps out at me is the cost of the calls to validateSchema and concat grow linearly with the size of the drawing.

```
export async function drawLine(tx: WriteTransaction, { id, point }: { id: string; point: Point }): Promise<void> {
  const lastLine = await getDrawing(tx, id);
  if (lastLine) {
    // add point
    lastLine.points = lastLine.points.concat([point.x, point.y]);
    await putDrawing(tx, { id, drawing: lastLine });
  }
}
export async function getDrawing(tx: ReadTransaction, id: string): Promise<Drawing | null> {
  const jv = await tx.get(key(id));
  if (!jv) {
    console.log(Specified shape ${id} not found.);
    return null;
  }
  return validateSchema(drawingSchema, jv);
}
export function putDrawing(tx: WriteTransaction, { id, drawing }: { id: string; drawing: Drawing }): Promise<void> {
  const next = { ...drawing as ReadonlyJSONObject, lastModifiedTimestamp: getUnixTimestampUTC() };
  return tx.put(key(id), next);
}
```','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('B7TaDwIkyOkEctANx8DKF','w23SLgSbmbNHaYm3j_L-C',1686824634000,'Probably just cargo culture?','nqYkxAGMnzk7Y5STjZryV'),
	 ('YDQOgVkdp1IMBGLsgb8c1','w23SLgSbmbNHaYm3j_L-C',1686840666000,'Removed in https://github.com/rocicorp/mono/pull/613','nqYkxAGMnzk7Y5STjZryV'),
	 ('84whsJWMQerdY34FKegtN','XrL0gir52YcWLkKodQHBg',1686606438000,'There really needs to be a sad trombone reaction emoji.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('s9bpTXfKQnEUZYpUlrXDH','f4qnSWqFPW-vSm0KhfD3i',1686564053000,'We have the issue in the unified package.','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('9-AtHooQVw4cAmOz_1Uwo','88r3PFTk8Ztr5C8Opry1P',1686841266000,'Done with 3ad1befafb7ea041aa25ccbd0ee615eebc022156','nqYkxAGMnzk7Y5STjZryV'),
	 ('DG9grs6Zwf-5xYeFEw_94','CNaWciBKu4nMCavPO4Ldo',1685005348000,'This sounds fantastic to me.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('6MOI53Be6LqAH77uPyRoJ','CNaWciBKu4nMCavPO4Ldo',1686240532000,'Epic. So excited to try this.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('kAcJ30CBE1QvqzFKq6Apj','YGeybqYWwm2-fg0bFkUin',1690882968000,'Strawperson:

* Add notion of special reserved "system" keyspaces (this would also be useful for other theorized features, such as local-only keys)
* The system keyspace starts with `"-/"`. (This is a breaking change but whatevs)
* The initially supported system keyspace is the "presence" keyspace: `"-/p/<client-id>"`.
* The Reflect system maintains two invariants for the presence keyspace:
  1. Client C1 can always access its own presence keys (online, offline, whatever)
  2. Client C2 can only access C1''s presence keys when C1 is connected

In other words, the server only sends C1''s presences keys to C2 when C1 is connected. When C1 is disconnected, the server sends deletes to C2 for C1''s presence keys. But the server always sends C1''s presence keys to C1.

---

Let''s test this strawperson against the goals:

> associate state with clients/users that are connected

Presence state is associated with clients by definition. Per-user state would have to be implemented by app code. Presence state could indicate which user it is for, and then some counter or timestamp could be used to track which client a user is currently at.

We could use this same pattern to provide user presence in the future if necessary.

> automatically delete this state when clients disconnect

Yes. And further, doesn''t delete it locally which is required for cursors to work consistently while disconnected.

> doesn''t get confused by mutation recovery

@grgbkr will have to verify this, but I think we are good.

Mutations to presence state sent by mutation recovery *will write* to presence state for disconnected clients on the server, as normal. However, writes to presence state for disconnected clients won''t be visible to other clients.

> integrates naturally with persisted state

This namespace idea is originally nate white''s, and although using strings in the keys feels a little hacky, it integrates beautifully with all of the existing API.

Also note that this presence state as proposed here *is normal Reflect state*. It gets persisted to IDB as normal, it gets written to durable objects, normally, etc. This means that cross-tab presences while offline will just automatically work.

The deletion of presence state when a client disconnects isn''t a function of the state being ephemeral on the server, it''s a function of a specific delete process that runs when the client disappears.

It is true that presence state often doesn''t need to be written so aggressively, both on client and server, but that can be handled separately...

> don''t bother persisting this state locally

We do persist the state locally. And as above, maybe this is a good thing (so cross-tab presence works).

> don''t bother resending changes related to this state when reconnecting from offline

We would send when reconnecting. However, sending too much unneeded data when reconnecting can be handled separately by #769. Almost all presence mutations would typically be droppable under evenflow, but we still preserve the ability to have non-droppable presence mutations.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('nbciSFfwAD9MFsFskJ-5l','YGeybqYWwm2-fg0bFkUin',1690883205000,'Open question:

Should the server enforce access control to presence? If we do not, it seems like an easy thing for developers to screw up. I believe that we can enforce that after a given clientID is written, it is only mutated by the same userID that originally wrote it.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('YAsXgoP8Zy2iInw-h-SXl','YGeybqYWwm2-fg0bFkUin',1690917539000,'Possible additional goal:
- When a client is offline, it should not see presence state of other clients (with possibly the exception of other local tabs in the same profile, i.e. clients in the same client group).   If my client is offline, it cannot have accurate up to date information about the presence of other clients, better to not show any presence info than to show stale/inaccurate presence info.','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('EhIJ1dGxJZndrGZS-PzXT','YGeybqYWwm2-fg0bFkUin',1690933559000,'In retrospect I don''t think this works perfectly with DD31, but I''m not sure if the idea is salvageable. ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('vPXjvQ6HBw5Xo8J1I6a_u','YGeybqYWwm2-fg0bFkUin',1691089812000,'> Possible additional goal:
> 
> * When a client is offline, it should not see presence state of other clients (with possibly the exception of other local tabs in the same profile, i.e. clients in the same client group).   If my client is offline, it cannot have accurate up to date information about the presence of other clients, better to not show any presence info than to show stale/inaccurate presence info.

I think this makes sense ideally, but it doesn''t seem like a very high priority. There will likely be other UI changes apps want to make to run offline, this just being one. They can detect when they are offline and hide the presence UI if they want to already. I think we should skip for v1 of presence.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('_zNnnTOlsTu2VhTSpLqKC','YGeybqYWwm2-fg0bFkUin',1693246905000,'> Open question:
> 
> Should the server enforce access control to presence? If we do not, it seems like an easy thing for developers to screw up. I believe that we can enforce that after a given clientID is written, it is only mutated by the same userID that originally wrote it.

Or even stricter, a mutation can write `-/p/<clientIdX>`, iff the mutation''s clientID is `clientIDX`.','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('SZO5p6zTXnfBnnnuoasrH','YGeybqYWwm2-fg0bFkUin',1693247066000,'> In retrospect I don''t think this works perfectly with DD31, but I''m not sure if the idea is salvageable.

I really like this proposal and spent a lot of time this weekend thinking about how to salvage it from the complexity of shared client state via client groups... and I''ve got nada.','Gg4MskWt3M-ttzzlrJ9jn');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('W9pPwMn0wZKb4B-CaVAIz','YGeybqYWwm2-fg0bFkUin',1693252371000,'Actually here is an alternative proposal: https://www.notion.so/replicache/Present-Clients-7deb6e93cba0435a82feab0a8bd3bdce','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('FSpod3TR-VdXEkq20_WPf','YGeybqYWwm2-fg0bFkUin',1693253668000,'lol

On Mon, Aug 28, 2023 at 9:53 AM Greg Baker ***@***.***> wrote:

> Actually here is an alternative proposal:
> https://www.notion.so/replicache/Present-Clients-7deb6e93cba0435a82feab0a8bd3bdce
>
> —
> Reply to this email directly, view it on GitHub
> <https://github.com/rocicorp/mono/issues/526#issuecomment-1696306240>, or
> unsubscribe
> <https://github.com/notifications/unsubscribe-auth/AAATUBHPUJ477KENT3XQSNTXXTZJ3ANCNFSM6AAAAAAYKEXGYM>
> .
> You are receiving this because you authored the thread.Message ID:
> ***@***.***>
>
','OeVnr1y5bEM_Yg06sUFtD'),
	 ('9RMIEdHot7CphkMc7WuMP','7SOdJwXzzf4JBDHBd_aBc',1684480160000,'@arv can you do this as part of the connection loop work?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Od9-E7dQH37-B5lI_pFhr','7SOdJwXzzf4JBDHBd_aBc',1685447865000,'There is also the concept of we got disconnected and now we are reconnecting. At the moment we do not call `onOnlineChange` when this happens, we only call it after we have failed once.

If we have a tristate (disconnected, connecting and connected) it would only seem fair that we report the state as connection during a reconnect.

But let''s think about what we actually want to report:

State | ConnectionState | Error Count | What we want to report
-- | -- | -- | --
Startup | disconnected | 0 | online
Connecting | connecting | 0 | previous state
Connected | connected | 0 | online
Auth Error | disconnected, connecting | 0 | previous state
Auth Error, repeated | disconnected, connecting | 1 | offline
Connect timed out | disconnected | 1 | offline
Ping timed out | disconnected | 1 | offline
Tab hidden (with timeout) | disconnected | 0 | offline
Tab shown | connecting | 0 | previous state?

I think we could keep things the way they are with slight tweak. The value of online can be `!tabHidden && errorCount === 0`. We would also "set" this when we set the connectionState to Connecting. That means that when we startup we would be online. When we come back from a hidden tab we will also treat that as being online.

','nqYkxAGMnzk7Y5STjZryV'),
	 ('1ivVLYCqw8FfJatEQ7g5X','7SOdJwXzzf4JBDHBd_aBc',1685448047000,'https://github.com/rocicorp/mono/issues/503

I think if we expose the connecting state, the most honest thing would be to only expose the current `connectingState` but I think that would lead to bad UI.

We could have `onOnlineChange` take 2 arguments, the "online" heuristic as described in the previous comment as well as the `connectedState`.','nqYkxAGMnzk7Y5STjZryV'),
	 ('2qCcjwqIIFRHeGaXj1mEV','CAWmB8ZfnsGYd2xF51NGc',1684041952000,'cc @d-llama @aboodman ','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('mb51IbHfR6WneKawRQhJA','CAWmB8ZfnsGYd2xF51NGc',1684042213000,'“Except for the tests” is a red flag. We should think critically about what
the tests are really doing for us and be prepared to abandon them where
necessary.

We have metrics, and we have our own site to test on. Also with js it’s
easier to test at higher abstraction levels.

Be bold! Let’s write the code the right way and not let the testing tail
wag the dog.

On Sat, May 13, 2023 at 7:26 PM Greg Baker ***@***.***> wrote:

> cc @d-llama <https://github.com/d-llama> @aboodman
> <https://github.com/aboodman>
>
> —
> Reply to this email directly, view it on GitHub
> <https://github.com/rocicorp/mono/issues/505#issuecomment-1546811349>, or
> unsubscribe
> <https://github.com/notifications/unsubscribe-auth/AAATUBFG3EVEH7UFFQ44SSTXGBUGXANCNFSM6AAAAAAYA53LVM>
> .
> You are receiving this because you were mentioned.Message ID:
> ***@***.***>
>
-- 
a (phone)
','OeVnr1y5bEM_Yg06sUFtD'),
	 ('qzSxHPunJNoboW8xD4txt','CAWmB8ZfnsGYd2xF51NGc',1684042263000,'Fixing this also feels like something that will pay big dividends in
velocity.

I’m fine if we have some short bustage in exchange.

On Sat, May 13, 2023 at 7:29 PM Aaron Boodman ***@***.***>
wrote:

> “Except for the tests” is a red flag. We should think critically about
> what the tests are really doing for us and be prepared to abandon them
> where necessary.
>
> We have metrics, and we have our own site to test on. Also with js it’s
> easier to test at higher abstraction levels.
>
> Be bold! Let’s write the code the right way and not let the testing tail
> wag the dog.
>
> On Sat, May 13, 2023 at 7:26 PM Greg Baker ***@***.***>
> wrote:
>
>> cc @d-llama <https://github.com/d-llama> @aboodman
>> <https://github.com/aboodman>
>>
>> —
>> Reply to this email directly, view it on GitHub
>> <https://github.com/rocicorp/mono/issues/505#issuecomment-1546811349>,
>> or unsubscribe
>> <https://github.com/notifications/unsubscribe-auth/AAATUBFG3EVEH7UFFQ44SSTXGBUGXANCNFSM6AAAAAAYA53LVM>
>> .
>> You are receiving this because you were mentioned.Message ID:
>> ***@***.***>
>>
> --
> a (phone)
>
-- 
a (phone)
','OeVnr1y5bEM_Yg06sUFtD'),
	 ('rFh_E8pmZ9euUw4GZ11Rm','RoWgbnz3DPcuui1NG_5cG',1685448060000,'Closing in favor of https://github.com/rocicorp/mono/issues/517','nqYkxAGMnzk7Y5STjZryV'),
	 ('zgrTh4_SQixTXPHJ1qOPY','PG_082z3sXSzgke7wIADG',1684606504000,'I looked at this, should be quite easy to extract a base class out of the current MetricManager that is shared btwn client and server. Don''t typically like class inheritance but this seems like a nice place for it because the current pattern of having the individual metrics be fields of MetricsManager so that lifetime and access are tied together is nice.

Once this exists, there is a separate question of reporting. Currently everytime the client sends metrics to the server, the server dumbly turns right around and forwards to datadog. This won''t last long (#189). But if we add server metrics in a naive way then we''ll double the number of metrics RPC from our server to datadog instantly.

We should probably fix #189 at same time as this bug and queue up both client and server metrics in the server for awhile before sending.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('GkWeii4vcMFt8_Cg81Mht','PG_082z3sXSzgke7wIADG',1684606713000,'Also note: a good place to actually add the code to tickle the metric is by extending `timed` shared utility to optionally take a Gauge as an argument. It is probably common that a thing we wanted to time for logging should also have a metric.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('J8wurArgd7Drcmxm6knOr','PG_082z3sXSzgke7wIADG',1684960866000,'> I looked at this, should be quite easy to extract a base class out of the current MetricManager that is shared btwn client and server. Don''t typically like class inheritance but this seems like a nice place for it because the current pattern of having the individual metrics be fields of MetricsManager so that lifetime and access are tied together is nice.
> 

Is it possible that the server can use the current MetricsManager class out of the box (no subclassing / inheritance) by just supplying an appropriate `MetricsManagerOptions.reporter`? Trying to confirm my reading of the code ...','ieK09sy2C_AIWE8KRkrQR'),
	 ('0vcVwFmnyTGv4gEmd8WWM','PG_082z3sXSzgke7wIADG',1684962336000,'The problem is that the `MetricsManager` class has hard-coded into it specific metrics -- metrics which make sense on the client but not server.

You could reuse the existing class, perhaps by having it contain a union of metrics needed by client and server, but that feels sort of odd to me. I guess there is no major harm to it I can think of.

The idea of subclassing is only to allow the client and server to have distinct set of metrics (and subclassing in particular for no particular reason -- composition would also work).','OeVnr1y5bEM_Yg06sUFtD'),
	 ('1c-Gnby-BoH4kUrGx4wzF','PG_082z3sXSzgke7wIADG',1684966074000,'Okay, I see the calls to `this._register()`. So we would need abstract that out of the class and either configure them via inheritance (baked into the class) or by composition (options). Got it. ','ieK09sy2C_AIWE8KRkrQR'),
	 ('psQeHp8aml0XXrBgflaUy','PG_082z3sXSzgke7wIADG',1684976985000,'> Also note: a good place to actually add the code to tickle the metric is by extending `timed` shared utility to optionally take a Gauge as an argument. It is probably common that a thing we wanted to time for logging should also have a metric.

Next question: I see that we use a Gauge for recording connection times. While it at first seemed odd to me, I understand now that we''re trying to track two things at the same time: (1) the number of open connections along with (2) the time they took to connect.

However, for timing of short-lived events like a LogFunction or auth_time, I was thinking that it makes more sense as a Distribution, at least according to the [DD docs](https://docs.datadoghq.com/metrics/#metric-types-and-real-time-metrics-visibility). Does that make sense or am I thinking about this the wrong way? (I recall rpc timing metrics at our previous companies being distributions.)','ieK09sy2C_AIWE8KRkrQR'),
	 ('PoYCvnFaMd-e4g678ecve','PG_082z3sXSzgke7wIADG',1684983407000,'Here are some things that Fritz wrote on this subject:

https://www.notion.so/replicache/metrics-0b848461ffce4312bdb645d65adb7da2

https://github.com/rocicorp/mono/issues/186

I think the decision to use a gauge was driven by what datadog has api support for. But also an interesting consideration is we want to be super careful to not overcount in situations like retry loops. That is why we have this setup where we store the _last value_ for some measure and report that every time period. This makes a lot of sense to me and gives me confidence in what we are seeing for the short time we''ve had metrics but I am a complete novice here.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('swfloWKkvaAOhHCyXyEpL','PG_082z3sXSzgke7wIADG',1685039985000,'Got it. This is great context. Thank you!','ieK09sy2C_AIWE8KRkrQR'),
	 ('dMfZw62aSCpdT5cZhzv18','pkY5VRD91YWXX0AYIT8Qt',1681739913000,'I think you are missing my point. We would like to prevent a transaction from trying to open another transaction. More specifically a read transaction cannot open a write transaction and a write transaction cannot open another read or write transaction.

The thing that makes this hard to detect is that transactions are async. Potentially we can use a custom PromiseLike and wrap the then resolve/reject callbacks with a context but it requires some research.','nqYkxAGMnzk7Y5STjZryV'),
	 ('kozDf7qii5DN7C6m9mqxg','Hu_H_-IzMPB5qZXo8SvN7',1683652936000,'In a previous project we used Firebase [custom claims](https://firebase.google.com/docs/auth/admin/custom-claims) to attach user-level roles (such as `admin` and `readonly-admin`) to privileged accounts for debugging user issues. These claims are accessible (after authentication) both from the client-side and the server-side, as they are encoded in the user''s JWT.

As such, it would be elegant to have the application pass the same user `context` object (containing `userID`, `claims`, and whatever else the application desires to make its authorization decisions) into the initialization of the client, and from the auth response of the `authenticateAndAuthorize` callback in the worker. Then the mutator logic can be the same on the client and server side (the former of which assumes what behavior is allowed, and the latter of which does the actual enforcement). In this way replicache / reflect provide a conduit for arbitrary information from the application''s auth code to its mutation code, in both the client and server environments.

Is that the general idea?','ieK09sy2C_AIWE8KRkrQR'),
	 ('S3lwgJ4dUA3lMpvU1vrEJ','Hu_H_-IzMPB5qZXo8SvN7',1683656219000,'Yes! We discussed enabling customers to pass this `Context` into the Reflect constructor to enable the mutator logic to be "isomorphic". I go back and forth on whether this is a net win for dx or features or not, I think we''d have to try it.

On the one side of course it sounds good to let the mutators do the same thing both client side and server side.

OTOH, it''s trivial to write:

```ts
async function fooButOnlyIfAdmin(tx: WriteTransaction) {
  if (tx.context?.isAdmin) {
    await foo();
  }
}
```

The nice thing about Reflect is because it has authoritative server the client doesn''t _need_ to do the same thing as the server. If this is an edge case that should not be triggered except by malicious users, then it doesn''t matter what the ux is.

I think we should start by just exposing context on the server-side and see how it feels to write authenticated code.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('a3HsAkHGdqDo3UvbuW9C7','Hu_H_-IzMPB5qZXo8SvN7',1683657737000,'Cool. I''m still reading up on docs and haven''t gotten to actual client code or deployment examples, but I assume that a customer will need to separately (1) deploy their mutators into Cloudflare (whether that be onprem or managed by us) and (2) pass their mutators into a Reflect client. So what I hear you saying is that, while being able to use the same mutator code in both places is convenient, it is not necessary because there are separate management paths for code running in the client and code running in the server. ','ieK09sy2C_AIWE8KRkrQR'),
	 ('ZTi2e-L0AP_Mfgi_Y9izh','Hu_H_-IzMPB5qZXo8SvN7',1683658224000,'They do need to pass their mutators to both the client and server. But what I''m saying is different: since the Replicache/Reflect is an authoritative server system, the mutators are allowed to do something different on the server. You can have a mutator that access to additional information on the server (ie auth info) and it simply computes a different answer than it did on the client. This is a feature. We don''t need to bend over backwards to have the mutators always compute the exact same thing on the client, they don''t have to be pure.

This might actually help assimilate the core of the sync protocol: https://doc.replicache.dev/concepts/how-it-works#the-replicache-sync-model. It discusses some of these ideas in more detail.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('81zeW_9rfVCdkmokjAP_x','Hu_H_-IzMPB5qZXo8SvN7',1683681342000,'I do appreciate the fact that Replicache/Reflect provide an authoritative server system, and that developers have the option to do something different on the server than on the client.

I also think, though, that part of the elegance of the Replicache/Reflect design, with mutators run in both environments, is that developers _can_ write their code without thinking about where it will be run. The dx win, to me, is an isomorphic API for server-run and client-run mutators (but certainly not requiring isomorphic implementations).

Just one dev''s opinion though.  😄 
','ieK09sy2C_AIWE8KRkrQR'),
	 ('qYks71GgLU2UqM2pnYw9v','Hu_H_-IzMPB5qZXo8SvN7',1683685278000,'Thanks a lot for the feedback. Let''s just try it! I''ve learned *not* to trust my instincts on this kind of thing... I am often surprised how bad my guesses are, when just using it makes it clear what feels right and doesn''t.

I think that providing `.context` only on the server-side is strictly less work than providing it on client and server. We can write some code that requires authorization in samples and I bet we will pretty quickly realize if it is not working.

WDYT?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('dnyiRFgRhTY6pGZ4t023Y','Hu_H_-IzMPB5qZXo8SvN7',1683693171000,'For sure! Sorry, I was just weighing in on what your were "[going back and forth on](https://github.com/rocicorp/mono/issues/492#issuecomment-1540653342)" and not trying to imply that we _must_ do it one way or the other. Certainly, the server-side functionality is the only prerequisite for the desired functionality. And after learning more about DD31, I can imagine that adding a Context to the Reflect constructor could complicate client grouping (e.g. what do we do for clients with the same "name" but different Contexts?).','ieK09sy2C_AIWE8KRkrQR'),
	 ('_T1NasT1oiCaBlks_sTP9','Hu_H_-IzMPB5qZXo8SvN7',1686093873000,'API proposal:

```ts
interface WriteTransaction {
  // ...
  readonly auth?: AuthData|undefined;
  // ...
};

type AuthData = {
  readonly userID: string;
} & ReadonlyJSONObject;
```','OeVnr1y5bEM_Yg06sUFtD'),
	 ('98cvmXfWDp-AEcddDzZQ8','hsr9sWG_TMiESLo9gCCxr',1683231143000,'The real error is: 

"Closing socket with error, {kind=VersionNotSupported, message=unsupported version}"

The "accepting connecting to send error" are completely explained by this.  Both have the same number of occurrences and occur in pairs (in the code we expect one to be logged immediately after the other).

One issue is:
"accepting connection to send error" is logged at level error, while "Closing socket with error, {kind=VersionNotSupported, message=unsupported version}" is logged at info.  Both should be logged at info.','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('2NekmyzUyQh1LM8h0dEZ_','hsr9sWG_TMiESLo9gCCxr',1683231879000,'I recall monday had a spike and slow fade out of these VersionNotSupported errors last time they updated versions as well.  ','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('aNZLDD2XCU_KQvjLVgCbs','hsr9sWG_TMiESLo9gCCxr',1683238312000,'I do not believe this is a real problem.  Monday has said they only soft users to update when there is a version mismatch.  I expect these errors to subside as users refresh their pages.','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('POUAFVPuKomSXUNMc_hvE','hsr9sWG_TMiESLo9gCCxr',1683579930000,'Closing this LMK if you disagree @grgbkr ','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('TpXaVIsaWc94LrZAkAbqR','D4sNwXulmL2z6gSqgzc7d',1683246659000,'Most are logged by the Worker, fewer are logged by the AuthDO, and very few are logged by the RoomDO.

<img width="1408" alt="image" src="https://user-images.githubusercontent.com/19158916/236356847-25cd385c-6b37-4687-919d-edcbdd014ef0.png">
','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('yhAeHLF2u98gZdVZ7DZw1','tgTx3g8nLTtBXZO1Z3C0S',1682920872000,'#454 caused this regression. It was reverted in https://github.com/rocicorp/mono/pull/480 as a temporary fix.

We still need to re-land 454.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('pkZIT-tzMTVuUKEf3ScEv','-f91Mdf0XQuX8XPPi9swV',1684744555000,'@grgbkr I vaguely remember we consciously decided to leave this for some reason. Do you remember the details?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('pF4b3FOr_98RVDoG1Ma_n','pkY5VRD91YWXX0AYIT8Qt',1681758688000,'Ah, I think we''re saying the same thing in different words. I agree with your wording of what we are trying to prevent.

I did not even consider that it was possible to engineer something to prevent this using promises. But I guess that it should actually be since at the end of the day this is a promise chain and we can restructure to say that what we are trying to prevent is a chains like `{(waiting on a write tx from rep 1) ... (waiting on another write tx from rep 1)}`.

Neat idea.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('1g9O9Z35lhggexQyP5-9v','qyhnHQo5MX_O15GeAOoEY',1680568034000,'This turned out to be a misunderstanding of how to use the API. Turning this bug into a doc bug: #456.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('masYu05ypxV3x5sT1Tjxr','O_X2gNFEjvX4_m8Sh0jA0',1680336047000,'cc @arv - can you please review this and tell me which parts are right and wrong?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('_CdloiChGwWfB0kYImNyb','O_X2gNFEjvX4_m8Sh0jA0',1680343340000,'ISSUE 1 is not correct. I forgot that it''s the client that measures ping timeouts not the server.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('XkvpcMC48YIZs4jj5L5z3','O_X2gNFEjvX4_m8Sh0jA0',1680510756000,'ISSUE 4 is also not correct as `visibilityWatcher` actually keeps watching while the ping is outstanding. The next time you wait on it, it resolves immediately if already in that state.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('aHtAxjJrog95BnE5gMMjm','O_X2gNFEjvX4_m8Sh0jA0',1680510787000,'https://github.com/rocicorp/mono/pull/454 demonstrates ISSUE 2 and 3 and also that 4 is *not* present.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('s14u9HSmWaNRIJjGn0YBl','O_X2gNFEjvX4_m8Sh0jA0',1680683818000,'The goal of `#nextMessageResolver` was to abort the ping timeout when we get a valid message. In other words, no need to send pings when we just received a message. Changing this to use an [abort signal on the `sleep`](https://github.com/rocicorp/mono/blob/ef6f15feae567541267df8ae4ad3109c98f9fa88/packages/replicache/src/sleep.ts#L10) function might make more sense.

ISSUE 3: You are right that we are not dealing with invalid/error messages after sending ping, waiting for pong. I don''t have a suggestion at this point.

ISSUE 2: Agreed. We do not disconnect on invalid messages and unexpected exceptions but we incorrectly increment the error count and set online to false. Ignoring these errors seems better.','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('cj3K-pwxm3yaCWSR3OHFt','wFOe5fd5CIwU46Pggrsoy',1680341644000,'Fixed by #451 ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('4NjDQo35EbbxnMb4l3Bvk','WCJgLUfx56evqh-hajX5j',1680568370000,'@arv can you flesh this out a little more it''s not clear what this bug is about.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('uz5g9IJAVV_1SBv1-RVPB','WCJgLUfx56evqh-hajX5j',1680598126000,'This is all related to refresh being broken.

#30 #434 ','nqYkxAGMnzk7Y5STjZryV'),
	 ('-7kG4U3-KWKZdskVQxabR','imzYzehAVGxYIWIt0QzTo',1679821705000,'Erik can you take this - you can see how to add tags to metrics in #440. We should do a request to some `/canary` http endpoint that the server exposes concurrent with connection, and include its status in the metrics tags (plus if it errors, log the result at error level or success at debug level).','OeVnr1y5bEM_Yg06sUFtD'),
	 ('rhjvcwhBpO2aYDeZp1h9L','imzYzehAVGxYIWIt0QzTo',1681148214000,'@aboodman do we want the worker to answer the canary request or do we want it to route to the roomDO ?  If to the roomDO should the canary request do implicit room creation? If we allow them to do implicit room creation then they need to be authenticated, right?','pgyTvcxh2hjmq2l4WKzK6'),
	 ('_yBmQsjh8tsqbL6SsZAv2','imzYzehAVGxYIWIt0QzTo',1681167754000,'The purpose of the canary request is to compare http connectivity to socket connectivity. Again, the concrete case we had was one where the ssl certificate wasn''t configured properly and the http request had a clear error. So I think just handling the request by the worker is fine.

Please log the result of the request either way (at debug level).','OeVnr1y5bEM_Yg06sUFtD'),
	 ('22uH84zsTexsFT2FHSFKI','vc6NI-cMUz74BXqgBTqLb',1679611909000,'cc @grgbkr -- I checked this out with Jesse. For some reason with current trunk builds, and only on --local mode, mutations stay pending forever. The server never decides to run them.

If you reboot the server then it *does* find the mutations and run them.

This doesn''t happen with current npm build, nor does it happen in trunk builds without --local.

Smells very much related to clock changes to me. Updated wrangler but didn''t help. We tried running --experimental-local, but it doens''t seem to be working at all right now (server crashes at startup with some npm inssue).

We will have to figure out something for alpha because we just decided to recommend people use --local so we don''t have to fix https://github.com/rocicorp/mono/issues/388#issuecomment-1476942996.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('wFtXrr0-6Tf1d1Xx6lWlV','vc6NI-cMUz74BXqgBTqLb',1679613807000,'Ok, here''s the repro branch: https://github.com/rocicorp/reflect-todo/tree/mono-issue-436-repro

To reproduce, pull this branch then:

- `cp .env.example .env` (if you have no `.env`)
- `cp .dev.vars.example .dev.vars` (if you have no `.dev.vars`)
- `npm i`
- `npm run dev-worker`

Then in a separate console

- `./create-room.sh`
- `npm run dev`
- open http://localhost:5173/

Note that you can create todos, and they don''t appear until the server confirms them ([this is the commit that forces server confirmation](https://github.com/rocicorp/reflect-todo/commit/e565f700cfb310ea97eae440b82d88b298e1ae0f)).
Now to reproduce the issue, stop both consoles, then

- `npm run dev-worker-local`

and in a new console,

- `./create-room.sh`
- `npm run dev`

Now open http://localhost:5173/ and see that when you try to add a TODO it isn''t created.

Observe that the mutator is sent to the server, it just doesn''t run it. 

<details>
  <summary>Sample server logs from one such run</summary>

```
handling message ["push",{"timestamp":5971.400000095367,"clientGroupID":"a2c67419-a064-4a4e-95cd-d1d3a6531d8a","mutations":[{"timestamp":5969,"id":2,"clientID":"355e72ec-d18b-4bd1-a0aa-77fec40bd345","name":"createTodo","args":{"id":"TqfJ6NK11kL8JPVaoTP_S","text":"test","completed":false}}],"pushVersion":1,"schemaVersion":"","requestID":"355e72ec-d18b-4bd1-a0aa-77fec40bd345-ceabd626-0"}] waiting for lock
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=66ej5mmxtra received lock
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=66ej5mmxtra msgType=push requestID=355e72ec-d18b-4bd1-a0aa-77fec40bd345-ceabd626-0 handling push {"timestamp":5971.400000095367,"clientGroupID":"a2c67419-a064-4a4e-95cd-d1d3a6531d8a","mutations":[{"timestamp":5969,"id":2,"clientID":"355e72ec-d18b-4bd1-a0aa-77fec40bd345","name":"createTodo","args":{"id":"TqfJ6NK11kL8JPVaoTP_S","text":"test","completed":false}}],"pushVersion":1,"schemaVersion":"","requestID":"355e72ec-d18b-4bd1-a0aa-77fec40bd345-ceabd626-0"}
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=66ej5mmxtra msgType=push requestID=355e72ec-d18b-4bd1-a0aa-77fec40bd345-ceabd626-0 inserted 1 mutations, now there are 2 pending mutations.
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=66ej5mmxtra handling processUntilDone
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=66ej5mmxtra already processing, nothing to do
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processNext - starting turn at 1679613135160 - waiting for lock
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw received lock at 1679613135160
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw process pending
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing 0 of 2 pending mutations with 0 forced misses
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing room clientIDs [[0,"355e72ec-d18b-4bd1-a0aa-77fec40bd345"]]  pendingMutations []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw currentVersion 10
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw pokes from fastforward []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing frame - clients ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw prevVersion 10 nextVersion 11
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processed 0 mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw Checking for disconnected clients. Currently connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"] Stored connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw built pokes 0
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw clearing pending mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processPending took 0 ms
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processNext - starting turn at 1679613135160 - waiting for lock
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw received lock at 1679613135160
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw process pending
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing 0 of 2 pending mutations with 0 forced misses
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing room clientIDs [[0,"355e72ec-d18b-4bd1-a0aa-77fec40bd345"]]  pendingMutations []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw currentVersion 10
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw pokes from fastforward []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing frame - clients ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw prevVersion 10 nextVersion 11
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processed 0 mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw Checking for disconnected clients. Currently connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"] Stored connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw built pokes 0
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw clearing pending mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processPending took 0 ms
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processNext - starting turn at 1679613135160 - waiting for lock
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw received lock at 1679613135160
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw process pending
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing 0 of 2 pending mutations with 0 forced misses
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing room clientIDs [[0,"355e72ec-d18b-4bd1-a0aa-77fec40bd345"]]  pendingMutations []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw currentVersion 10
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw pokes from fastforward []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing frame - clients ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw prevVersion 10 nextVersion 11
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processed 0 mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw Checking for disconnected clients. Currently connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"] Stored connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw built pokes 0
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw clearing pending mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processPending took 0 ms
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processNext - starting turn at 1679613135160 - waiting for lock
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw received lock at 1679613135160
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw process pending
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing 0 of 2 pending mutations with 0 forced misses
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing room clientIDs [[0,"355e72ec-d18b-4bd1-a0aa-77fec40bd345"]]  pendingMutations []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw currentVersion 10
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw pokes from fastforward []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing frame - clients ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw prevVersion 10 nextVersion 11
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processed 0 mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw Checking for disconnected clients. Currently connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"] Stored connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw built pokes 0
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw clearing pending mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processPending took 0 ms
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processNext - starting turn at 1679613135160 - waiting for lock
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw received lock at 1679613135160
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw process pending
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing 0 of 2 pending mutations with 0 forced misses
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing room clientIDs [[0,"355e72ec-d18b-4bd1-a0aa-77fec40bd345"]]  pendingMutations []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw currentVersion 10
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw pokes from fastforward []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing frame - clients ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw prevVersion 10 nextVersion 11
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processed 0 mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw Checking for disconnected clients. Currently connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"] Stored connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw built pokes 0
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw clearing pending mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processPending took 0 ms
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processNext - starting turn at 1679613135160 - waiting for lock
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw received lock at 1679613135160
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw process pending
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing 0 of 2 pending mutations with 0 forced misses
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing room clientIDs [[0,"355e72ec-d18b-4bd1-a0aa-77fec40bd345"]]  pendingMutations []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw currentVersion 10
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw pokes from fastforward []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing frame - clients ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw prevVersion 10 nextVersion 11
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processed 0 mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw Checking for disconnected clients. Currently connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"] Stored connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw built pokes 0
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw clearing pending mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processPending took 0 ms
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processNext - starting turn at 1679613135160 - waiting for lock
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw received lock at 1679613135160
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw process pending
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing 0 of 2 pending mutations with 0 forced misses
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing room clientIDs [[0,"355e72ec-d18b-4bd1-a0aa-77fec40bd345"]]  pendingMutations []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw currentVersion 10
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw pokes from fastforward []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing frame - clients ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw prevVersion 10 nextVersion 11
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processed 0 mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw Checking for disconnected clients. Currently connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"] Stored connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw built pokes 0
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw clearing pending mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processPending took 0 ms
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processNext - starting turn at 1679613135160 - waiting for lock
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw received lock at 1679613135160
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw process pending
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing 0 of 2 pending mutations with 0 forced misses
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing room clientIDs [[0,"355e72ec-d18b-4bd1-a0aa-77fec40bd345"]]  pendingMutations []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw currentVersion 10
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw pokes from fastforward []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing frame - clients ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw prevVersion 10 nextVersion 11
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processed 0 mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw Checking for disconnected clients. Currently connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"] Stored connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw built pokes 0
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw clearing pending mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processPending took 0 ms
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processNext - starting turn at 1679613135160 - waiting for lock
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw received lock at 1679613135160
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw process pending
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing 0 of 2 pending mutations with 0 forced misses
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing room clientIDs [[0,"355e72ec-d18b-4bd1-a0aa-77fec40bd345"]]  pendingMutations []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw currentVersion 10
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw pokes from fastforward []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing frame - clients ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw prevVersion 10 nextVersion 11
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processed 0 mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw Checking for disconnected clients. Currently connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"] Stored connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw built pokes 0
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw clearing pending mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processPending took 0 ms
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processNext - starting turn at 1679613135160 - waiting for lock
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw received lock at 1679613135160
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw process pending
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing 0 of 2 pending mutations with 0 forced misses
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing room clientIDs [[0,"355e72ec-d18b-4bd1-a0aa-77fec40bd345"]]  pendingMutations []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw currentVersion 10
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw pokes from fastforward []
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processing frame - clients ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw prevVersion 10 nextVersion 11
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw processed 0 mutations
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw Checking for disconnected clients. Currently connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"] Stored connected ["355e72ec-d18b-4bd1-a0aa-77fec40bd345"]
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw built pokes 0
RoomDO doID=000000018710ae9d518d94b5482913caf0f2f5fe4efc5aeaf1d06b1b0e4d0cda roomID=room-id requestID=ami2kf3dej clientIP=127.0.0.1 wsid=ii0oe6T_F5rN9e2BaWOIc client=355e72ec-d18b-4bd1-a0aa-77fec40bd345 wsid=ii0oe6T_F5rN9e2BaWOIc msg=hsobsokdgiw clearing pending mutations
```
</details>
','_4MJeHV4T-qZb52fyNwQ9'),
	 ('JFsTO8h3-ssvXJsu7Hjc2','vc6NI-cMUz74BXqgBTqLb',1679616473000,'From those logs it is clear that the clock is frozen (all the timestamps are 1679613135160).   I''m not sure we can work around this.','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('vZrzeOuRx2hg4z4K1jcgO','vc6NI-cMUz74BXqgBTqLb',1679619025000,'I think it may be more feasible to address: #388   We need to do #388 if we do https://github.com/rocicorp/mono/issues/178, because then clients can end up ahead of the server in production.','Gg4MskWt3M-ttzzlrJ9jn');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('gC_QbeCiX9XkpJNMu4cfe','vc6NI-cMUz74BXqgBTqLb',1679624776000,'Another option is to get --experimental-local working. It''s hard to believe they''re just shipping it completely broken, we must be missing something.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('FYAy9e4F2-1IbQae2x7Ce','vc6NI-cMUz74BXqgBTqLb',1679677136000,'I was able to get past the npm error with --experimental-local by clearing my npm cache.  However, then I hit an error that persisted DOs are not supported.


```
greg replidraw-do [grgbkr/dd31-60fps]$ npm cache clean --force
npm WARN using --force Recommended protections disabled.
greg replidraw-do [grgbkr/dd31-60fps]$ wrangler dev --experimental-local
 ⛅️ wrangler 2.9.1 (update available 2.13.0)
------------------------------------------------------
Your worker has access to the following bindings:
- Durable Objects:
  - roomDO: RoomDO
  - authDO: AuthDO
[NPXI] @miniflare/tre not available locally. Attempting to use npx to install temporarily.
[NPXI] Installing... (npx --prefer-offline -y -p @miniflare/tre@3.0.0-next.8)
[NPXI] Installed into /Users/greg/.npm/_npx/f763b2efd540e32a/node_modules.
[NPXI] To skip this step in future, run: npm install --save-dev @miniflare/tre@3.0.0-next.8
✘ [ERROR] local worker: DurableObjectsError [ERR_PERSIST_UNSUPPORTED]: Persisted Durable Objects are not yet supported

      at Object.getServices
  (/Users/greg/.npm/_npx/f763b2efd540e32a/node_modules/@miniflare/tre/dist/src/index.js:5289:13)
      at #assembleConfig
  (/Users/greg/.npm/_npx/f763b2efd540e32a/node_modules/@miniflare/tre/dist/src/index.js:6221:45)
      at async #init
  (/Users/greg/.npm/_npx/f763b2efd540e32a/node_modules/@miniflare/tre/dist/src/index.js:6070:20)
      at async Mutex.runWith
  (/Users/greg/.npm/_npx/f763b2efd540e32a/node_modules/@miniflare/tre/dist/src/index.js:2296:16)
      at async startLocalWorker
  (/Users/greg/github/replidraw-do/node_modules/wrangler/wrangler-dist/cli.js:124794:11) {
    code: ''ERR_PERSIST_UNSUPPORTED'',
    cause: undefined
  }
```','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('iC-Fwg8IdkX9bKuOb2Moj','vc6NI-cMUz74BXqgBTqLb',1679683211000,'FWIW, I''ve also been seeing (and I believe it may have been like this since I started on this project) that `Script modified, context reset` will run when I change files that are not dependencies of `worker/index.ts` - this means that when I change any frontend file, the reflect db will be wiped, and baseCookie will be unrecognized.

This was less of a blocker when doing pure FE iteration, but when working on bots, it makes it pretty rough. I''ve also been seeing exceptions on reconnect, so most of the time even reloading will wipe the db locally.

I''ve seen some other reports of this (https://community.cloudflare.com/t/script-modified-context-reset-during-developement/384304) and believe it to be on cloudflare''s side, and I haven''t put any time into cleanly reproducing the reload crash, so I don''t think anything here is directly actionable, just mentioning it so we all know what the DX is at the moment.

My workaround for now is to just use the reflect libs from npm in dev, and use the tarballs in prod, since things are ok there, so I''m not blocked.','_4MJeHV4T-qZb52fyNwQ9'),
	 ('KK6HCngP9hagiY3oruasy','vc6NI-cMUz74BXqgBTqLb',1679684568000,'> However, then I hit an error that persisted DOs are not supported.

Ah I should have guessed this. I looked into the open source impl of the worker platform and it also doesn''t support persistent DOs. So this makes sense. So that path is dead for now.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('V0XkFhAewDlVECbxoS5FV','vc6NI-cMUz74BXqgBTqLb',1680164278000,'Since we decided not to do anything here, closing this.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('AYRwmGNv80UWq3J5U6S-6','Qrjv44_PJ1zw2-Ly9kGuH',1680558408000,'We have some issues related to splitting the persist and refresh implementaions over multiple transactions.

Both refresh and persist has some issues in case of transactions failing. Since we are splitting the logic over multiple transactions a rollback on failure does not work and we end up in invalid state.

Refresh:
- perdag write
- memdag write
- perdag write

Persist should be safe because it does:
- perdag read
- memdag read
- memdag write (in one of the two branches)
- perdag write

This is not what we are seeing in the reproduced test case.



','nqYkxAGMnzk7Y5STjZryV'),
	 ('dGQasp76G7Lmkn_qaSiUW','Qrjv44_PJ1zw2-Ly9kGuH',1680558810000,'What we are seeing is that we have interleaved persist/refresh.

https://www.notion.so/replicache/ChunkNotFound-Repro-ddeb6e1db3684c59bfd3d2163cb3eeff#dc4b1b3bcc614a268eeb42d178c18340','nqYkxAGMnzk7Y5STjZryV'),
	 ('Cw4QeKWUkFPSYX8ZIXueo','Qrjv44_PJ1zw2-Ly9kGuH',1680728025000,'One thing I thought would work was to wrap persist and refresh in a exclusive lock. But even with that we get:

```
4713.js:280245  name=reflect-anon-jTxPHFi6N1IgssS7AAw7k Error during refresh from storage ChunkNotFoundError: Chunk not found 82120de598a74cb083e8a1a354b75df9000000006595
    at mustGetChunk (5fbb21d5-abd1abff75732ec5.js:4187:9)
    at async GatherNotCachedVisitor.visitBTreeNode (5fbb21d5-abd1abff75732ec5.js:4273:19)
    at async Promise.all (:3000/index 27)
    at async GatherNotCachedVisitor._visitBTreeInternalNode (5fbb21d5-abd1abff75732ec5.js:4286:5)
    at async GatherNotCachedVisitor.visitBTreeNodeChunk (5fbb21d5-abd1abff75732ec5.js:4281:7)
    at async GatherNotCachedVisitor.visitBTreeNode (5fbb21d5-abd1abff75732ec5.js:4276:5)
    at async GatherNotCachedVisitor.visitCommitChunk (5fbb21d5-abd1abff75732ec5.js:4213:5)
    at async GatherNotCachedVisitor.visitCommit (5fbb21d5-abd1abff75732ec5.js:4209:5)
    at async 5fbb21d5-abd1abff75732ec5.js:6415:9
    at async using (5fbb21d5-abd1abff75732ec5.js:3162:12)
log @ 326-ae18cf0b689d4713.js:2
```

```
4713.js:280245  name=reflect-anon-jTxPHFi6N1IgssS7AAw7k Error during persist ChunkNotFoundError: Chunk not found 82120de598a74cb083e8a1a354b75df9000000003635
    at mustGetChunk (5fbb21d5-abd1abff75732ec5.js:4187:9)
    at async BTreeWrite.getNode (5fbb21d5-abd1abff75732ec5.js:2334:22)
    at async InternalNodeImpl.set (5fbb21d5-abd1abff75732ec5.js:1979:26)
    at async 5fbb21d5-abd1abff75732ec5.js:2629:24
    at async run (326-ae18cf0b689d4713.js:280178:16)
    at async Write.put (5fbb21d5-abd1abff75732ec5.js:3845:5)
    at async WriteTransactionImpl.put (5fbb21d5-abd1abff75732ec5.js:1432:5)
    at async addSplatter (index-8d0d173e8edba1d6.js:2708:9)
    at async rebaseMutation (5fbb21d5-abd1abff75732ec5.js:4340:3)
    at async rebaseMutationAndPutCommit (5fbb21d5-abd1abff75732ec5.js:4344:14)
log @ 326-ae18cf0b689d4713.js:2
```

One take away from this still failing when we put a single lock around them is that it is not the interleaving of persists/refreshes that causes the trouble.

It could still be the interleaving of the memdag with mutations and pull...

','nqYkxAGMnzk7Y5STjZryV'),
	 ('XjLzIoyT6Tc3yx13NqAHB','Z4cdvgUqkwEMoHFYxCh24',1683333564000,'We''re now deciding to leave this in case subset wants it, until we completely fix every last correctness issue.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('ZVnjJbcO6WMZ-h3oN5LMZ','snYqI7sUWdElbDgtcgl34',1679430593000,'Is this for replidraw-do? I thought I tried that one already.','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('A_XdlAn9TFYClaqi9B8kc','snYqI7sUWdElbDgtcgl34',1679430920000,'yeah replidraw-do, maybe i need to rebase.

On Tue, Mar 21, 2023 at 1:30 PM Erik Arvidsson ***@***.***>
wrote:

> Is this for replidraw-do? I thought I tried that one already.
>
> —
> Reply to this email directly, view it on GitHub
> <https://github.com/rocicorp/mono/issues/427#issuecomment-1478540004>, or
> unsubscribe
> <https://github.com/notifications/unsubscribe-auth/AESFPBHES3Q76DHHPLMXYBLW5IFUZANCNFSM6AAAAAAWC5EV6U>
> .
> You are receiving this because you authored the thread.Message ID:
> ***@***.***>
>
','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('AcDsQfmbINF4WMaxS9aA8','snYqI7sUWdElbDgtcgl34',1679490347000,'I was able to reproduce this using `npm run build`. There is some webpack bug triggering this.
- Changing from swc to babel did not help
- Changing esbuild for reflect to not minimize helped/
- Changing esbuild to treat `@badrap/valita` helped.

## Plan

Change the following dependencies to be external and real dependencies:
- [x] Move build.js to shared
- [x] Update reflect to use build.js 
- [x] Make the following external
  - `@badrap/valita`
  - `@rocicorp/logger`
  - `@rocicorp/resolver`
  - `@rocicorp/lock`
- [x] Update package.json to mark these as dependencies','nqYkxAGMnzk7Y5STjZryV'),
	 ('bAh5h7Ns8LkJdKjmBVsuK','snYqI7sUWdElbDgtcgl34',1680036741000,'Jesse is still seeing this on paint-fight.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('E1f4Df8NuZ_5wJV7Tx5Fo','QE2eFvZrV3g9bN9hUzalE',1679303817000,'Duplicating the code might also have semantic issues. Like instanceof not working as expected etc.','nqYkxAGMnzk7Y5STjZryV'),
	 ('USLLmB4lTY3ICU0Nitda4','QE2eFvZrV3g9bN9hUzalE',1679322842000,'Here is some code from the compiled bundle of replidraw-do:

```
;// CONCATENATED MODULE: ./node_modules/@rocicorp/reflect/out/reflect.js
// ../../node_modules/@rocicorp/logger/out/logger.js
var TeeLogSink = class {
  constructor(sinks) {
    this._sinks = sinks;
  }
  log(level, ...args) {
    for (const logger of this._sinks) {
      logger.log(level, ...args);
    }
  }
  async flush() {
    await Promise.all(this._sinks.map((logger) => logger.flush?.()));
  }
};
```

```
// ../replicache/out/replicache.js
var Xt = class {
  constructor(e) {
    this.qe = e;
  }
  log(e, ...n) {
    for (let r of this.qe)
      r.log(e, ...n);
  }
  async flush() {
    await Promise.all(this.qe.map((e) => e.flush?.()));
  }
};
```

```
;// CONCATENATED MODULE: ./node_modules/@rocicorp/logger/out/logger.js
/**
 * A [[LogSink]] implementation that logs to multiple sinks.
 */
class logger_TeeLogSink {
    constructor(sinks) {
        this._sinks = sinks;
    }
    log(level, ...args) {
        for (const logger of this._sinks) {
            logger.log(level, ...args);
        }
    }
    async flush() {
        await Promise.all(this._sinks.map(logger => logger.flush?.()));
    }
}
```

- One copy comes from the replicache bundle
- One copy comes from the reflect bundle
- And one final copy from replicache-do','nqYkxAGMnzk7Y5STjZryV'),
	 ('1JKCXHu4ra0eI48dlpQr8','MZwjPkwQNzpkrw57gVWcr',1709536329000,'Not necessary as it''s in rails.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('8nZj6saVDtlWKe5qmk-dQ','94ByP81RTZCxqhxKzLDFO',1680568644000,'I kind of prefer it simpleminded as it is. Let''s way and see if anyone complains.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('9FCGy4kjpcRqGfcskebxV','eYGW6TomsSBHX1TpqwMne',1678698590000,'Really? I thought we had a test for this... checking...

https://github.com/rocicorp/mono/blob/main/packages/replicache/src/replicache-subscribe.test.ts#L428

It is not as fine grained as other subscriptions since we cannot determine if the emptiness changed based on the diff. We always call these subscription bodies.

We could improve this be doing the emptiness check outside the subscription body to determine if that changed.','nqYkxAGMnzk7Y5STjZryV'),
	 ('4LwSOndLb6WwkbaXS4F4A','eYGW6TomsSBHX1TpqwMne',1678735158000,'Huh, I think my test case was wrong. I am seeing that it works now too. Weird.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('BZqhu8XMTeY9iF4FHUmMV','6EhERUKvk2Az2oQwoSAt9',1678579085000,'@grgbkr thoughts on this?','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('VNiw7PKR_552XodrUz3P0','6EhERUKvk2Az2oQwoSAt9',1679619266000,'Yes I think we should do this.','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('_9OCyU7Zbwv7lJkSkFPEj','6EhERUKvk2Az2oQwoSAt9',1683341969000,'The `userID` field from the client is passed in the connection string to the server, so we should be able to fairly easily match it up against what comes back from the auth handler.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('a-DoLV_BhfeB4_Ax6_Elg','6EhERUKvk2Az2oQwoSAt9',1683402496000,'Fixed by https://github.com/rocicorp/mono/commit/2c1a49a822e5c3d5cbe3f13d2851191dd48af3a1','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('CtzkIex-zlLtSBlLqc6Bl','zc-cGtvCOmXYIHUJrKSqN',1678717752000,'https://github.com/rocicorp/mono/pull/394','nqYkxAGMnzk7Y5STjZryV'),
	 ('usQlA8ADamiM_IcxMg18h','zc-cGtvCOmXYIHUJrKSqN',1679211662000,'I think we probably want to dump local state when this occurs. It will be quite common due to #363 and dev mode, and makes the dx terrible.

This can create lost writes which is also bad, but during the alpha period I think it is more important to demonstrate the promise than to be perfectly robust.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Gv2OseMf2rHImR7JDF8U0','zc-cGtvCOmXYIHUJrKSqN',1679303403000,'> I think we probably want to dump local state when this occurs

What other part of the local state would be useful here? Pending commits? The BTree? The Client object?','nqYkxAGMnzk7Y5STjZryV'),
	 ('toxUFCrBiYekFQpVoDAWF','zc-cGtvCOmXYIHUJrKSqN',1679304007000,'Oh sorry, what I mean is delete/drop local state. Basically delete all the Replicache data 😬.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('3bKuD73H6zR18qbkBEmbZ','zc-cGtvCOmXYIHUJrKSqN',1679343285000,'Big picture here, the goal is that on something like `reflect-todo`:

1. we can change the example to just instantiate Reflect with a hard coded room and it will work (that''s #363)
2. even if you kill the dev worker and reboot it, it will work

Right now I believe that step 2 will print an error to the JS console telling you to clear cache which is OK, but better for the alpha would be to just drop localstate and start over. I think?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('oLP-bJ20-XqLzxAoioLcU','zc-cGtvCOmXYIHUJrKSqN',1679344028000,'It is tricky to do this transparently under the cover (i.e. close the
replicache client being wrapped, delete local replicache state, and create
a new replicache client, transparent to the user of the reflect client).
In particular its hard to get the subscribe/watch behavior correct.

I think roughly the best we can do is to delete local replicache state and
call a callback that by default reloads the page.  We could maybe do one
better, by transparently closing/deleting/reopening, if no watches have
been fired, but it seems likely to be buggy.




On Mon, Mar 20, 2023 at 1:14 PM Aaron Boodman ***@***.***>
wrote:

> Big picture here, the goal is that on something like reflect-todo:
>
>    1. we can change the example to just instantiate Reflect with a hard
>    coded room and it will work (that''s #363
>    <https://github.com/rocicorp/mono/pull/363>)
>    2. even if you kill the dev worker and reboot it, it will work
>
> Right now I believe that step 2 will print an error to the JS console
> telling you to clear cache which is OK, but better for the alpha would be
> to just drop localstate and start over. I think?
>
> —
> Reply to this email directly, view it on GitHub
> <https://github.com/rocicorp/mono/issues/388#issuecomment-1476870559>, or
> unsubscribe
> <https://github.com/notifications/unsubscribe-auth/AESFPBHJHCYD2FYEMDELD43W5C3EDANCNFSM6AAAAAAVV4ZJZI>
> .
> You are receiving this because you are subscribed to this thread.Message
> ID: ***@***.***>
>
','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('A_lIbYebxb4aVPp2LGtS7','zc-cGtvCOmXYIHUJrKSqN',1679346795000,'I wonder if instead we should just recommend `wrangler --local --persist`. Development is the main problem. @jesseditson how has that mode been for you?','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('mxYq9vkjX5Euhr5z2aNsy','zc-cGtvCOmXYIHUJrKSqN',1679352409000,'That mode has worked fine! The only caveat is that it prints a recommendation to use an experimental new flag that does not work for me.','_4MJeHV4T-qZb52fyNwQ9'),
	 ('YSa_it4pBUe4MSGja98UV','zc-cGtvCOmXYIHUJrKSqN',1679359407000,'OK let''s just do that for now. @arv nothing to do here for now.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Mk7HFO3yZXDePCbi-RljS','FXgRjIRgQbxf3BvxuWL9M',1678309036000,'Another way to ensure this is to build-dts and look for DD31 in there (and SDD)','nqYkxAGMnzk7Y5STjZryV'),
	 ('2mhiRWJwu4ySeD-VDEpfL','859py59MYOCv4BPBfdL_y',1678309104000,'I added a known issue regarding this to the last release note FYI:
https://www.notion.so/replicache/reflect-0-13-1-reflect-server-0-22-0-7ebcdf937978409285d31b8eb1e80f2d?pvs=4#28f01adb761b41769f02962dbdce8257

On Wed, Mar 8, 2023 at 7:06 AM Greg Baker ***@***.***> wrote:

> When reconnecting the CPU is pegged by rebasing mutations. This is
> because, the pusher logic pushes up mutations individually and then they
> come down in a series of pokes. Resulting in something like
> 1000 pending, poke contains 50, rebasing 950
> 950 pending, poke contains 50, rebasing 900
> 900 pending, poke contains 50, rebasing 850
> ... and so on
>
> This is improved somewhat by the 60fps buffering and playback logic, as
> the mutations from multiple of the reflect pokes above will often get
> merged into a single replicache poke.
>
> This is related to: #378 <https://github.com/rocicorp/mono/issues/378>
>
> —
> Reply to this email directly, view it on GitHub
> <https://github.com/rocicorp/mono/issues/384>, or unsubscribe
> <https://github.com/notifications/unsubscribe-auth/AAATUBB46FANUKFMYPP6QATW3C4AFANCNFSM6AAAAAAVUAV7VQ>
> .
> You are receiving this because you are subscribed to this thread.Message
> ID: ***@***.***>
>
','OeVnr1y5bEM_Yg06sUFtD'),
	 ('vL08Ygl3tMsCpiHuEPZlz','kFxkx4JRIedt24BpowFC3',1678270133000,'Why is it annoying? Composable API seems better than specialized APIs.','nqYkxAGMnzk7Y5STjZryV'),
	 ('RIjFlCX4KVyuXeKuv1a2L','vSSKL90GWgRgqFRVAbk6F',1678136763000,'I think we should still hook the `online` event and use that as a hint to reconnect too, as even 5s is an annoying few beats to wait when you know you''ve just reconnected (especially in demos).','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Yr4NN-FaOjb63iMHMu3LJ','qR--IkCBAIebGeeZi8FGx',1678222257000,'Before you do this check with Aaron for last check about whether we''re really doing it.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('5KaFjVSgsgqMU__ogCUpj','qR--IkCBAIebGeeZi8FGx',1684868566000,'Raising priority since we also want this for the next Replicache release.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('fa44dgpBUZ4wW3Ku5a-n2','q7joMFz9UkevLyWkubjC2',1686045440000,'A confusing thing I just ran into:

```
import {version} from ''@rocicorp/reflect'';
console.log(version);
```

prints `13.0.0-beta.0` because that is the version `replicache` exports.

','nqYkxAGMnzk7Y5STjZryV'),
	 ('oIgTB7hBkBAZi_HIIki4Y','9LHQncTcD5cVGrQMf7jAS',1677789465000,'cc @arv ','Gg4MskWt3M-ttzzlrJ9jn');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('8kBwW_OrcsJR77qNgW9_i','fQ5N6benWwiTYuaktIhXi',1690343382000,'This is working now.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('4fjQ9XGFn8-thlXb3C0gs','fQ5N6benWwiTYuaktIhXi',1690363763000,'✅ ','nqYkxAGMnzk7Y5STjZryV'),
	 ('dFTW9Mf4tOhXAbbef9OZq','a0P8t3TjA4959txnuLzty',1677762278000,'@aboodman @grgbkr ','nqYkxAGMnzk7Y5STjZryV'),
	 ('JlJ48R4vm6P-H8MSImgHx','a0P8t3TjA4959txnuLzty',1677783512000,'The jurisdiction flag that is passed into Reflect applies to the _room_. It is saying "I would like the data for _this room_ to be in EU". it''s solving the problem of "some of my (Monday.com''s) customers are EU entities and they need their data to be housed only in EU".

There is an interesting separate question of the auth DO. The AuthDO is shared among all rooms for a single customer (ie Monday) and the data within it is Monday''s data, not Monday''s customer''s data. Monday might someday ask us "hey I''m an EU state. I want my data to only be in EU.". This would have minor performance tradeoffs because it would put the auth do further away from some cutomers than it would otherwise have to be.

So far nobody has asked for this feature though.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('OFcmyM8dgox8JII78RJPU','a0P8t3TjA4959txnuLzty',1677789788000,'I was worried that the auth data might be considered user data.','nqYkxAGMnzk7Y5STjZryV'),
	 ('1qrZp9pFYSnBSnIgawKYW','nmXzY4Jjr9jGDde0PxxKE',1680639014000,'We should do a build at end of milestone and get these two customers onto it!','OeVnr1y5bEM_Yg06sUFtD'),
	 ('q9sQ6E2D9L3S0VtuHLYQM','B6Jpm-sbLu8QN8s2qZ-ky',1678321587000,'We discovered another thing that should be available globally beside `env`: the roomID. There are probably going to be a few of these, we should probably define like a `StartParams` or similar that gets passed to `createReflectServer` which we can add things to over time.

See: https://rocicorp.slack.com/archives/C013XFG80JC/p1678321518575779?thread_ts=1678315633.516129&cid=C013XFG80JC','OeVnr1y5bEM_Yg06sUFtD'),
	 ('H33sncnJWNzI5LNxoj2jX','B6Jpm-sbLu8QN8s2qZ-ky',1678322773000,'Here is an idea I started playing with earlier and is coming back to me again:

```ts
async function createReflectServer(opts: CreateReflectServerOptions) {
  const rs = new ReflectServer({
    // Different signature and use from the `auth` field of `Reflect` but similar idea.
    // Too clever to overload?
    auth: async () => {
    },
    mutators,
    logLevel,
    logSink,
  });
  
  // rs is a fully functioning Reflect-like thing 🤯.
  // - you can call `mutate.foo` on it (it will get queued in the game loop)
  // - you can call `subscribe` and get notifs when things change (ie to sync with
  //   external systems, maintain computed state, whatever)
  // - you can set timers or call fetch in the global scope and call mutators later!
  // - you can use libraries against it that are designed for the `Reflect` or
  //   `Replicache` interface.

  // The runtime arranges to call your mutators when messages come in.
  // The return type of `createReflectServer` is `extends ReflectServer<T>`, so
  // you can also *extend* Reflect and add your own state. 
  return rs;
}
```

We could implement this incrementally by having the return type of `createReflectServer` be as in https://github.com/rocicorp/mono/issues/352#issue-1605585020, and later add `ReflectServer` which happens to implement that interface later.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('TPhFMYSg914Vj89zHHc8R','B6Jpm-sbLu8QN8s2qZ-ky',1678348901000,'An interesting thing here is the way features compose. Example: we do not need `onDisconnect` to take a `WriteTransaction` (and then to deal with the fact that we don''t have a mutationID). We can just have `onDisconnect` be a normal callback and the user can do anything they want in there. If they want to mutate data, they just call `rs.mutate.disconnect(...)`.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('fjDAjogaPfgaa4D5pWDan','B6Jpm-sbLu8QN8s2qZ-ky',1678354623000,'I like exposing things like query and mutate but I am a bit concerned about making things less clear. For the server, these mutations are very different from the mutations the client makes. These do not have a client id and they do not have a mutation id etc. They do not get rebased and how do you order/queue these with the mutations coming from the clients? It just adds a lot of new concepts that have to be well thought through and that we need to teach.','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('1UGlSNi3-0kSFgzqUHVvq','B6Jpm-sbLu8QN8s2qZ-ky',1678380355000,'> An interesting thing here is the way features compose. Example: we do not need `onDisconnect` to take a `WriteTransaction` (and then to deal with the fact that we don''t have a mutationID). We can just have `onDisconnect` be a normal callback and the user can do anything they want in there. If they want to mutate data, they just call `rs.mutate.disconnect(...)`.

But then the disconnect mutator is passed a WriteTransaction. Does that WriteTransaction have a mutationID?','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('fSBvdb7kZxT2WPURMXCIE','B6Jpm-sbLu8QN8s2qZ-ky',1678381738000,'> These do not have a client id and they do not have a mutation id etc.
> But then the disconnect mutator is passed a WriteTransaction. Does that WriteTransaction have a mutationID?

We could give them the special clientID `server` or similar then give them mutationIDs as normal.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('lYfTaLn1igVcMhu2B-ZWF','B6Jpm-sbLu8QN8s2qZ-ky',1679305830000,'> Another thing to check into is whether in CF, the env can change without restarting the context. I bet that it cannot. But if it can then we need to make the env field in the constructor a getter so it can return latest value (or maybe a function to make it clear it''s dynamic).

[The bindings assigned to the Worker. As long as the environment has not changed, the same object (equal by identity) is passed to all requests.](https://developers.cloudflare.com/workers/runtime-apis/fetch-event/#parameters:~:text=The%20bindings%20assigned%20to%20the%20Worker.%20As%20long%20as%20the%20environment%20has%20not%20changed%2C%20the%20same%20object%20(equal%20by%20identity)%20is%20passed%20to%20all%20requests.)

So it seems like they can change or at least that CF wants to leave this open to allow it to change in the future.','nqYkxAGMnzk7Y5STjZryV'),
	 ('zI8npZSNFAe-n4kiHz96e','B6Jpm-sbLu8QN8s2qZ-ky',1679313468000,'There are a lot of parts here. Let me focus on the `createReflectServer` part. This is what our user calls today. In a future setup with a saas we will restructure this for better ergonomics, but for now, our customer is calling us and then "handing" the `worker` to CF. That means `createReflectServer` cannot take an "env". Instead we inverse the flow slightly to take a function instead.

```ts
export function createReflectServer<
  Env extends ReflectServerBaseEnv,
  MD extends MutatorDefs,
>(
  getOptionsFunc: (env: Env) => ReflectServerOptions<MD>,
): {
  worker: ExportedHandler<Env>;
  RoomDO: DurableObjectCtor<Env>;
  AuthDO: DurableObjectCtor<Env>;
}
```

and an example usage:

```ts
const {worker, RoomDO, AuthDO} = createReflectServer(env => {
  console.log(env);
  return {
    mutators,
    authHandler,
  };
});
export {worker as default, RoomDO, AuthDO};
```

We can use a `WeakMap` to store the options per Env so we do not call this more than once per isolate and env.','nqYkxAGMnzk7Y5STjZryV'),
	 ('2gG3PkdkM2gHawKZ_i8na','B6Jpm-sbLu8QN8s2qZ-ky',1679358919000,'https://github.com/rocicorp/mono/issues/352#issuecomment-1476093117 LGTM.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('rO0XwLICkqqt7ff3FWpwO','B6Jpm-sbLu8QN8s2qZ-ky',1679389617000,'For the global, we could do a global function `getEnv(): Promise<Env>` but I''m a little bit worried of "dead locks". Conceptually I think the isolate could have different Envs so a global might not be a good fit.','nqYkxAGMnzk7Y5STjZryV'),
	 ('H0ogmDYkxOxqYi5IgLWlq','B6Jpm-sbLu8QN8s2qZ-ky',1679466312000,'> We can use a WeakMap to store the options per Env so we do not call this more than once per isolate and env.

@arv I don''t think we need the WeakMap. I think it is totally fine and expected to call `getOptions` once per construction of `Reflect`. I would find it very confusing if this didn''t happen actually.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('0Xk0-NTJvqvuH83fNmHeH','B6Jpm-sbLu8QN8s2qZ-ky',1679466393000,'It should just be:

```
createReflectServer(env => {
  // Gets called once when the server starts.
  return {
    ...
  };
});
```','OeVnr1y5bEM_Yg06sUFtD'),
	 ('s3c-6qa6nMhOhE65Pmnt7','B6Jpm-sbLu8QN8s2qZ-ky',1679475043000,'> // Gets called once when the server starts.

It cannot be called when the server starts. It gets called from `fetch` and `scheduled` gets called by CF worker as well as for when the RoomDO and AuthDO gets instantiated.

For typical usage it gets called 4 times if we do not have a WeakMap.

I agree that it should get called once per createReflectServer. Let me see what I can do.','nqYkxAGMnzk7Y5STjZryV'),
	 ('-bZc487LBIYUEZ0fufO39','B6Jpm-sbLu8QN8s2qZ-ky',1679478287000,'Argh fetch. This CF API is so frustrating.

In this case I understand why you did it that way.

> I agree that it should get called once per createReflectServer. Let me see what I can do.

I was being lazy/imprecise with my writing when I said: `// Gets called *once* when the server starts.` Sorry that''s happened a few times, I''ll try to be more precise in the future since we''re async.

Often, the worker, roomDO, and authDO can be in separate isolates or on different machines. So in that case, it is not possible for the options callback to get called only once per `createReflectServer`.

What i really meant was "Gets called once per worker/DO, the first time the worker/DO needs the options".

Given all this i think what you did is the best balance of forces. I''ll revert this part of my recent PR.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('dkNRYMIDeXFfI3ZJrbDdV','B6Jpm-sbLu8QN8s2qZ-ky',1679478357000,'And I don''t think any other work is necessary here by you right now - let me know if I''m still confused.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('9O9qsOZ3X6cxQFHkBXe-U','B6Jpm-sbLu8QN8s2qZ-ky',1679482783000,'I got a PR that does this once per call to createReflectServer.

What isn''t clear to me is how this interacts with isolates. If it wasn''t for isolates the options object would be shared between these 4 cases. The options object is part of a closure. I assume CF has to call createReflectServer once per isolate and that it uses one isolate for the worker fetch, one for worker scheduled, one for RoomDO and one for AuthDO.

','nqYkxAGMnzk7Y5STjZryV'),
	 ('GnXiVWACH-5fNvHnvFxXf','B6Jpm-sbLu8QN8s2qZ-ky',1679485292000,'I instrumented the code to see how often this got called. The caching I put in place in #430 allows reusing the options between fetch calls.

We get one isolate for Worker, RoomDO and AuthDO respectively','nqYkxAGMnzk7Y5STjZryV'),
	 ('v_kg6j6deKeOKmZucz5lh','B6Jpm-sbLu8QN8s2qZ-ky',1679487755000,'CF uses one isolate per machine/script/version. In CF nomenclature a "script" is the thingy that wrangler.toml describes. So each unique version of one of those on a machine has its own isolate.

In the simple case where there''s one just user then yes, the worker, authdo, and roomdo will all be in same isolate.

But if there are multiple users in a room, the worker and roomdo can easily be in different isolates since CF will spawn a new worker close to where user 2 is, even if user 1 already has their own worker.


','OeVnr1y5bEM_Yg06sUFtD'),
	 ('NxzJSedSiZCVyOEEMEJSJ','B6Jpm-sbLu8QN8s2qZ-ky',1679490008000,'Calling this done with #430 ','nqYkxAGMnzk7Y5STjZryV'),
	 ('J0rzBt25uGEZRTthtXDhs','Va0Urkmmr0hvVdA1KlEBV',1685126377000,'Here is a useful datapoint. It took about 15s to connect to puzzle-000000:

https://github.com/rocicorp/mono/assets/80388/c7ed8942-7351-4dca-a08b-4b8e98f98d9e

','OeVnr1y5bEM_Yg06sUFtD'),
	 ('KzRhrTHMxMrt0s3iyH8ZD','M8mfjAnsA7XC4SkPHC1U1',1678401450000,'Cesar is going to do this!','OeVnr1y5bEM_Yg06sUFtD'),
	 ('9EbsKFE7_9iKImkyItgAC','M8mfjAnsA7XC4SkPHC1U1',1680634271000,'@cesara these demos don''t currently work on iphone. I don''t see any message in the server console, and receiver doesn''t play either.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('4-2SO54ivNJJY4bh3WnSC','M8mfjAnsA7XC4SkPHC1U1',1681819114000,'Closing this in favor of burn down notion.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('KD1y9spYb9Fqezcu8tmCT','M7AYjBEI0o2ou09qiMklf',1677671726000,'@grgbkr wdyt?','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('nWSCayk_YlRf_rhtH8AnU','M7AYjBEI0o2ou09qiMklf',1709536376000,'I think maybe we implemented this @arv ?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('x0nyR3sHRJzvFJNuENrxr','M7AYjBEI0o2ou09qiMklf',1709546003000,'No. I don''t see anything in `close` that waits for persist.','nqYkxAGMnzk7Y5STjZryV'),
	 ('HDYoCGuOxhnxwC471gm8s','M7AYjBEI0o2ou09qiMklf',1709580453000,'Agree this makes sense, and am a little surprised we aren''t doing this
already.

Should we also be doing it on visibilitychange?

On Mon, Mar 4, 2024 at 2:53 AM Erik Arvidsson ***@***.***>
wrote:

> No. I don''t see anything in close that waits for persist.
>
> —
> Reply to this email directly, view it on GitHub
> <https://github.com/rocicorp/mono/issues/348#issuecomment-1976173593>, or
> unsubscribe
> <https://github.com/notifications/unsubscribe-auth/AESFPBFTGEMQKSCATOAW2L3YWRACBAVCNFSM6AAAAAAVL6NGR2VHI2DSMVQWIX3LMV43OSLTON2WKQ3PNVWWK3TUHMYTSNZWGE3TGNJZGM>
> .
> You are receiving this because you were mentioned.Message ID:
> ***@***.***>
>
','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('cohsIPhY_B5MX_KyCvzj0','ooi7QtL1QIEfDAg7fJ2Ll',1677617489000,'Fixed in #344','nqYkxAGMnzk7Y5STjZryV'),
	 ('F5PsfVenGB7wG7z05AEER','N_zoG-OQkTLODLx233FoP',1677694982000,'Greg''s current idea: https://rocicorp.slack.com/archives/C013XFG80JC/p1677560755693459?thread_ts=1677555317.057609&cid=C013XFG80JC

my current idea is:
add schemaVersion to ReflectServerOptions, if the schemaVersion differs from what is stored on startup call a customer provided schemaVersionChangeHandler passing it a WriteTransaction (or a WriteTransaction factory).  Don''t accept connections until the schemaVersionChangeHandler completes
add schemaVersion as a param to socket connect, and reject connections if the schemaVersion doesn''t match server''s current version
keep the existing behavior on the client of giving new schemaVersion''s a new idb so they do a full sync','OeVnr1y5bEM_Yg06sUFtD'),
	 ('8UFJ5u2xGEBvXPbOzJYcf','N_zoG-OQkTLODLx233FoP',1677867863000,'We should also consider if we should support undo-migration/down-migration/migration-rollback (see https://flywaydb.org/documentation/tutorials/undo, https://www.prisma.io/docs/guides/database/developing-with-prisma-migrate/generating-down-migrations).

In addition or alternatively we could support snapshotting before a migration, and allow rollback to the snapshot (if a migration ends up being destructive, undo-migration wont be able to recover the data).   ','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('PvHvqF04OCsOmcuRtrR9f','vQ_s0b4lFLLy1kXejIR1p',1677706276000,'Duplicate of #178, I''m losing it.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('dD7ZwN1XLZa99aNExmoY2','txL1WKD6dr9TjTsDuUu8D',1677036195000,'Next steps:

* Verify this still happens on trunk / latest wrangler. If it does happen with latest wrangler, then:
  * File a bug with cf. Unhandled rejections should be printed to console in dev mode.
* Check whether this error goes to logpush in production. If it doesn''t, file error with CF. Unhandled rejections should go to logpush.
* Check whether this error goes to `unhandledrejection` event handler (and has a defined `reason`). If it goes there, add such global handler to our code and send to `lc.error`. If it does not, file bug with CF.

* On 0.21.1 branch, check whether this goes to logpush. If it does *not*, add a try/catch around this block and update build that monday is going to deploy.
','OeVnr1y5bEM_Yg06sUFtD'),
	 ('D0axFpyvqysHxRAS8wfy8','txL1WKD6dr9TjTsDuUu8D',1677071506000,'I''m seeing the following:

```
ERR RoomDO doID=f9623b0ceef5dc8b0b0593bad751d8be97f31c32e798009f9ae47cad753c4cb4 roomID=mDmS3P Unhandled promise rejection: XXX
```

And here is the diff:

```diff
diff --git a/packages/reflect-server/src/server/room-do.ts b/packages/reflect-server/src/server/room-do.ts
index 14c5695b..23e846ee 100644
--- a/packages/reflect-server/src/server/room-do.ts
+++ b/packages/reflect-server/src/server/room-do.ts
@@ -106,6 +106,10 @@ export class BaseRoomDO<MD extends MutatorDefs> implements DurableObject {
       .addContext(''doID'', state.id.toString());
     this._lc.info?.(''Starting server'');
     this._lc.info?.(''Version:'', version);
+
+    addEventListener(''unhandledrejection'', event => {
+      this._lc.error?.(''Unhandled promise rejection:'', event.reason);
+    });
   }

   private _initRoutes() {
@@ -387,6 +391,7 @@ export class BaseRoomDO<MD extends MutatorDefs> implements DurableObject {
   }

   private async _processNext(lc: LogContext) {
+    void Promise.reject(''XXX'');
     lc.debug?.(
       `processNext - starting turn at ${Date.now()} - waiting for lock`,
     );
```','nqYkxAGMnzk7Y5STjZryV'),
	 ('qknt_hK7I3uj6En8aBD0u','txL1WKD6dr9TjTsDuUu8D',1677085149000,'On what version of reflect/wrangler?','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('_piyNsDR6Zbor1rBpxWHF','txL1WKD6dr9TjTsDuUu8D',1677152697000,'@arv - in addition to the code change you have in progress, can you do the following:

1. Can you tell me whether, without any code changes, does trunk Reflect reproduce this bug (does it fail to send the unhandled rejection to console output)?
  - If so, we should file a bug, but I''m not sure actually where to do that. Will ask.
2. Can you tell me whether, without any code changes, does trunk Reflect send this unhandled rejection to logpush?
  - If not, we should file a bug
3. Can you tell me whether, without any code changes, does v0.21.1 Reflect send this unhandled rejection to logpush?
  - If not, we probably need to make a patch release of 0.21.1 that includes this error handling','OeVnr1y5bEM_Yg06sUFtD'),
	 ('NB-d67Rr4LUMAW4YFvGKC','txL1WKD6dr9TjTsDuUu8D',1677162217000,'Here are my observations:

1. Unhandled rejections are not logged by default in a DO
2. Unhandled rejections are not logged by default in a Worker
3. Nothing is reported in logpush

I checked tail (on CF dashboard and locally) as well as logpush (on DD)','nqYkxAGMnzk7Y5STjZryV'),
	 ('_OsJ2rV-TWPbilJyF6DnN','txL1WKD6dr9TjTsDuUu8D',1677618223000,'Added code on our side to handle unandled rejections:

9c8cfb703dd9efee911f20e18c711cdbee343296

I haven''t followed up with cloud flare what the intended behavior is and if they could expose this in their logs at least.','nqYkxAGMnzk7Y5STjZryV'),
	 ('vBJZqAohgvRep5jLxG_CT','txL1WKD6dr9TjTsDuUu8D',1677670632000,'Posted question to Cloudflare''s Discord.','nqYkxAGMnzk7Y5STjZryV'),
	 ('EeCfbA-zY2ytnTDr-P84O','txL1WKD6dr9TjTsDuUu8D',1677670785000,'Kenton said that things that are more toward the side of bug like this could be better posted in https://github.com/cloudflare/workerd.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('VgYFGXRo-pgymAOPD6Fc9','txL1WKD6dr9TjTsDuUu8D',1677698244000,'@arv once you post this to cf issue tracker we can consider this closed!','OeVnr1y5bEM_Yg06sUFtD'),
	 ('5O6tV3PS5Cqe-Rq1UfmVV','txL1WKD6dr9TjTsDuUu8D',1677754391000,'https://github.com/cloudflare/workerd/issues/412','nqYkxAGMnzk7Y5STjZryV'),
	 ('D32C0NL4fLp6QRv_0GHnF','QuEOZYMU6SWNxI6IVXMAK',1677790410000,'FWIW here''s what I do now: https://github.com/rocicorp/reflect.net/commit/1fae51626983a171869d2702289a133c02c954af','_4MJeHV4T-qZb52fyNwQ9'),
	 ('3vge-FHG47h4F5yDD1vlE','ltLoJ9hPd_fHUh4SsnySd',1677002734000,'@arv do you agree?  if so I can make this change quick.','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('5Q3W3PGBWWPkQXsYQaN4L','ltLoJ9hPd_fHUh4SsnySd',1677065352000,'I agree','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('XhKptQ-mgGTJDjbqkQrFA','ltLoJ9hPd_fHUh4SsnySd',1678115377000,'When passing `--platform=neutral`, `nanoid` ends up importing nodejs specific modules and bundling fails.

We didn''t have this kind of problem in replicache since it didn''t have any runtime dependencies.','nqYkxAGMnzk7Y5STjZryV'),
	 ('VoyDekK9Mj2-iQ5Xf2lZC','4o--59eZVIs9CbHfxg-GE',1677164918000,'Done in https://github.com/rocicorp/mono/commit/b25e7940af349190effa21e9b82009e9dd7e24ed','nqYkxAGMnzk7Y5STjZryV'),
	 ('aE1XD_bLmrRAWvJNYnYsP','4o--59eZVIs9CbHfxg-GE',1677165206000,'Not sure this is working.

It says:

```
• Remote caching enabled
```

```
@***/reflect:check-format: cache miss, executing 902c3594961702cf
replicache:check-format: cache miss, executing 8973c6d29bd20ec4
@***/reflect-server:check-format: cache miss, executing 9b514e8f3872a890
```

Let''s check on this later','nqYkxAGMnzk7Y5STjZryV'),
	 ('HXGh9Mfpt5dzJewlPCalZ','j7YYm4ELeHkMM-t3H5NsV',1676581227000,'I think there was a `nodeConsoleLogSink` somewhere for this purpose, since we want to have a version that doesn''t stringify for environments that are fancy (ie browsers).

But on second thought maybe it''s better to just do this in one place and accept that it won''t be perfectly optimal in browsers.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('tNB3Yez8ZfzmzRtJ23Y21','j7YYm4ELeHkMM-t3H5NsV',1676645118000,'Yeah, maybe it is not worth it.
','nqYkxAGMnzk7Y5STjZryV'),
	 ('JBNlrw8eNpZk2QfgK-lJG','j7YYm4ELeHkMM-t3H5NsV',1677695694000,'Agree let''s simplify and just not have the fancy expandy logging in browsers so that we can have just one console logger.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Km6jYi-sdtYhT5wVy6IUW','UC7o_pd6dFhaIb8qoDGtK',1677698286000,'The logs part of this has been done. The metrics part will roll into the metrics bug.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('I9Ay32iTUcA35i6Y3Sx5C','TBUpMPVad3qM1Cy8nJRbc',1677666133000,'**delete** service does not seem right to me','nqYkxAGMnzk7Y5STjZryV'),
	 ('SVBafoIOOUs5LCg-qrp2k','TBUpMPVad3qM1Cy8nJRbc',1677666987000,'1. I verified that migrate works.
2. Deleted the worker
3. `wrangler publish` again

Everything seems to work fine.','nqYkxAGMnzk7Y5STjZryV'),
	 ('NOdocJb-HPYxn3uec9--l','Ti6CuXTnxrY_W_f1iev_r',1677695758000,'Seems like this is internal and has no customer impact. If so can we take out of the milestone @arv?','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('pTrKiDAB8yXAF6f784JFs','Ti6CuXTnxrY_W_f1iev_r',1677695775000,'Taking out optimistically, LMK if you disagree.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('m_tkNOffJFK--jZjh5XIW','eT2QXxyPgBpJ2mvKe9OVx',1677695806000,'I think this is done @grgbkr ?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('cLzrQg55wIYafPbq4HOyz','eT2QXxyPgBpJ2mvKe9OVx',1677752055000,'Yup. In 0a2cb5bea263de38480be8d8dbbd646a8d3cb246','nqYkxAGMnzk7Y5STjZryV'),
	 ('6QC1sJ7WFcE-uqyIB4dJ8','MsrceOpfLM_rpGQw44AfK',1677696386000,'Punting from milestone until someone has time to think about. We won''t get kicked out of bed for this deficiency.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('M_8AdAsrP_BXSFPFKOBjw','MsrceOpfLM_rpGQw44AfK',1677700836000,'Factored just the first item out into https://github.com/rocicorp/mono/issues/352.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('271oxuhYdf-EcYFeGWvTX','MsrceOpfLM_rpGQw44AfK',1684746343000,'See more details of idea here: https://github.com/rocicorp/mono/issues/352#issuecomment-1461092522','OeVnr1y5bEM_Yg06sUFtD'),
	 ('GmGiPLChp6liOttlILuge','Ry3LRznUiySX-I4N1j3N6',1677696434000,'Nice to have but not required for beta.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('PpbhqUk66dkmuO0uyMH5j','Ry3LRznUiySX-I4N1j3N6',1683855049000,'I think we need to redesign the server-side API more broadly, but for now, for consistency with other events, I suppose we should call this `roomStartHandler` 😕','OeVnr1y5bEM_Yg06sUFtD'),
	 ('MainmeUJXUMSl2oj9Zk_q','Ry3LRznUiySX-I4N1j3N6',1684355253000,'Woo, this will be great for next release.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('UbteLGCNRnq_qLPLrsP2s','yQ24dH-V3rH-JjZwiSW1i',1677696442000,'Nice to have but not required for beta.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('nF1PoGU1AHAQ_9VlflGJc','yQ24dH-V3rH-JjZwiSW1i',1683485986000,'I''m told that @grgbkr has a draft of this somewhere.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('DC_YVzzG4G6Cr5ghf9jzS','yQ24dH-V3rH-JjZwiSW1i',1683679684000,'Here is an example use case that came up in the ALIVE demo:

We want to shuffle the demo on load so the user gets a nice initial state.

We could wait until we get the initial state, delay displaying anything, and if there are no non-bot users shuffle. But there is a small chance that there is some other user also joining at that moment, and that user will see the state suddenly shuffle.

To fix that issue we could do the shuffle server-side. So: we wait to display anything, do a mutation that shuffles the demo, but only if nobody is present, and wait for that mutation to round-trip through the server. But in that case, we have to wait for the mutation to round trip, delaying startup.

The best place to do this shuffle would be on the server, right as a user is connecting. We can see at that moment if they are the only one present and if they they are we shuffle the demo just before they sync their initial state.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('aynf9kk34sHm0EYxYcTG3','yQ24dH-V3rH-JjZwiSW1i',1683764770000,'> Exceptions from the connect handler should prevent the connection from being accepted (and result in an error back to the client).

This last requirement differs from the existing behavior and can be used in interesting ways; I want to flesh out my thoughts and confirm that we''re all on the same page.

Currently, a connection can be closed early (in `handleConnection()`) due to errors originating from the contents of the connect request: bad request format, invalid connection group id, invalid lmid, invalid base cookie. Importantly, a client was always able to fix its error and retry the connect.

Closing a connection due to an error from the `connectHandler` introduces a new scenario in which the connection can be closed due to situations outside of the control of the client. I can see this as a desired feature (e.g. disallowing connections for rooms that are too full), but it is, I believe, a new class of behavior and I wanted to confirm is intended.

One of the reasons I ask is that @grgbkr''s initial implementation (granted, done before @aboodman listed these more detailed requirements) plumbs the `connectHandler` down into a callback where it''s not straightforward to close the connection (i.e. [`processFrame()`](https://github.com/rocicorp/mono/pull/494/files#diff-6821aedf9ef09ff065dbf002dc72a7927e474df1876b02677ac20b81aba73480R47)). I presume he did this to be symmetric with the `disconnectHandler`, but I also imagine that he didn''t have this close-connection-on-error behavior in mind.

In summary, two questions:
* @aboodman: Can you confirm that we want the `connectHandler` to be able reject incoming connections due to non-connection-related scenarios? (fwiw, it makes for an interesting capability but does add a bit more complexity to the code when compared to simply logging/ignoring the errors)
* @grgbkr: If so, can you advise as to the best place to invoke the `connectHandler` transaction? Doing it synchronously in [`handleConnection()`](https://github.com/rocicorp/mono/blob/7f6331652b2177acd4abac792f3f9e8aba70e20f/packages/reflect-server/src/server/connect.ts#L136), before the call to `putClientId()`, makes it easiest to close the connection on errors, but it also circumvents all of the poke-sending logic that in the `processPending()` -> `processRoom()` -> `processFrame()` callpath.
','ieK09sy2C_AIWE8KRkrQR'),
	 ('JKvTafmLdVLlNFsIv5paE','yQ24dH-V3rH-JjZwiSW1i',1683768465000,'The reason I put this requirement in is that I feel it would be very difficult to reason about the system if `connectHandler` was not guaranteed to run (and succeed) before a connection was accepted.

The main purpose of `connectHandler` that I envision is ensuring certain state exists for a connection before it proceeds. If we ignore errors and proceed then this invariant is destroyed.

You''re right to point out that it''s not symmetrical with `disconnectHandler`. We cannot offer the same level of guarantee that `disconnectHandler` completes, unless we are willing to kill the entire rooms forever if `disconnectHandler` fails. Honestly part of me is tempted to do that. I really like invariants :). But it feels like something users would often hit and hate.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('0zc7-aBOS4XWv698Ct1Ah','yQ24dH-V3rH-JjZwiSW1i',1683768652000,'This is often the point where @grgbkr will point out some consequence of my design choices that I didn''t anticipate which will cause me to rethink. @grgbkr wdyt?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('h4K8bkfhjkwUbsYT1h95S','yQ24dH-V3rH-JjZwiSW1i',1683768817000,'Agreed, I like the behavior from an API / invariance perspective. I''ll figure out how to best rework the code to make this possible (and hopefully clean), given that currently the mutate/send-poke logic and the connect/reject logic are in different layers.  ','ieK09sy2C_AIWE8KRkrQR'),
	 ('V9cfXaa2_KS9ZKGldYDQ1','yQ24dH-V3rH-JjZwiSW1i',1683776315000,'After covering more of the code, I see that there is precedent for closing connections after `handleConnect()` has succeeded, so I think this can in fact be done fairly cleanly.','ieK09sy2C_AIWE8KRkrQR'),
	 ('_ITn9idbe_lFvRsKJ6iXF','yQ24dH-V3rH-JjZwiSW1i',1683777507000,'Hmmm ... I guess it depends. If we run the `connectHandler` in the `processPending` loop after the connection has been accepted, I think it''s technically possible for a client to connect, push mutations, and then get disconnected due to a `connectHandler` error, but those pushed mutations would still have been accepted, thereby violating our desired invariant.

So it may be that the best way to prevent any client-mutations in the face of an error-returning `connectHandler` is to synchronously run the connectHandler in `handleConnect()` before accepting the connection and registering the websocket. Will pow-wow with @grgbkr when he''s back.','ieK09sy2C_AIWE8KRkrQR'),
	 ('0Mtiu389u004PqHpqSQ2o','yQ24dH-V3rH-JjZwiSW1i',1683837359000,'Closing the connection (before we send any pokes to it) is straightforward.

However, not processing a client''s mutations until its connectHandler succeeds is complicated.   

A client''s mutations can be pushed by other clients (either via them sharing a client group, or via mutation recovery).  ','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('A4pBWURruE04VV6JlL22S','yQ24dH-V3rH-JjZwiSW1i',1683854047000,'Summary of our huddle:
* Connection-rejecting connectHandler may result in unintuitive semantics given that clients can send each other''s mutations with DD31
* We can accomplish @aboodman''s intended use case better with [onRoomStart](https://github.com/rocicorp/mono/issues/174)
* This `connectHandler` may be reincarnated in some different form for handling Presence. Aaron and I will write a design doc.  ','ieK09sy2C_AIWE8KRkrQR');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('La6TSQSRBflPvmkCccsKj','NtisAKbNZloKdZCHzXArH',1677696460000,'I think this may have been fixed @grgbkr ?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('75yYZ8JoVqfimvuatacc0','NtisAKbNZloKdZCHzXArH',1677697213000,'Not fixed yet.  

My current thinking is to keep fastforward in turn processing (its efficient to do it as a batch when a DO restarts, and has a bunch of clients reconnecting).  I think new connections should cause a turn to run (just as mutations and disconnects do).   This structure can all onConnect (https://github.com/rocicorp/mono/issues/175 ) to be implemented in basically the same way onDisconnect is today ','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('SBIXgIvdO0C7rs1ydelkE','NtisAKbNZloKdZCHzXArH',1677698737000,'Duplicate of #293','OeVnr1y5bEM_Yg06sUFtD'),
	 ('gsqe59QW4uTBYRnCqbfU5','xAyzOlNdHfgU_sYkdnn3x',1683332950000,'I think this has been done, right @grgbkr ?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Y3AFxZO2Jrf8Yjqa8WXSC','vkpq6VNfOq-R1mUng83op',1675934779000,'Erik says: "We should also probably wait 10s (or 5s, or whatever) before doing this to allow cycling through tabs"','OeVnr1y5bEM_Yg06sUFtD'),
	 ('zCLuJZseOMyU4V6LEjyiG','7m5jBEv2Sn9XycVaM1q4Y',1675762685000,'This one I feel it is sufficient to just rename the mutators when you want a breaking change, like in Replicache?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('A1InwaT1FBnaR2ZSbxq90','7m5jBEv2Sn9XycVaM1q4Y',1675785018000,'SGTM','nqYkxAGMnzk7Y5STjZryV'),
	 ('z6OrZC3YdHD8ygcNWs1CB','I6hEvwp3lwm2b4b5jKcIq',1675761725000,'For Reflect we could probably collapse **PullVersion** + **PushVersion** into a single **ProtocolVersion**.','nqYkxAGMnzk7Y5STjZryV'),
	 ('b65MmirlvxjhhaaCqWSRF','I6hEvwp3lwm2b4b5jKcIq',1675762777000,'Agreed - only the connection should be versioned in Reflect, not individual messages flowing over it.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('pmQCen99X2xOeLfZp4mdU','I6hEvwp3lwm2b4b5jKcIq',1675934022000,'> How does this overlap with PullVersion and PushVersion used in Replicache?

PushVersion should be removed from the protocol. It is just taking up useless space in the messages.

> SchemaVersion

SchemaVersion should also be moved to the connect message. It doesn''t make sense in push. But we should have a separate bug for schema management.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('FbuH5Vobic_W0Kbk7p1ca','I6hEvwp3lwm2b4b5jKcIq',1675934490000,'> The client sends its protocol version when it tries to connect.

As a tiny note, I think it would be nice and consistent if the protocol version was sent by way of the API path -- /api/1/connect, or whatever.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('WwerGFqhybgDXuPm37uan','I6hEvwp3lwm2b4b5jKcIq',1676315183000,'We will need this for DD31 as it involves breaking protocol changes.  ','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('DHVl773ZM6jtTeZ7hvBia','Tckft3fqCqXBy9sTzI0F_',1675136072000,'To add one addendum, I''m pretty happy with how this low tech solution turned out -- it was super easy to add a new metric type (State) and to build a dashboard for it once I understood how datadog handles the concepts. I think it''s a decent (though maybe not ideal) foundation on which to build understandability for reps and reflect server. I think the next steps would be to address the scale issue described above by adding aggregation and then to develop the observability/monitoring/alerting plan for reflect (and later, reps) which is some combination of the poorly described and factored https://github.com/rocicorp/reflect-server/issues/193 and https://github.com/rocicorp/reflect-server/issues/60. I think that with a couple of engineer-weeks one could:
- define the minimal set of metrics one would like to keep for reflect-server (eg request error rate or ws message error rate, ws msg latency, request count, storage write latency distribution, auth failures rate, game loop lag, etc/whatever)
- add whatever new metric types one needs for these (likely: rate, count, maybe set for clientids) and integrate with reflect-server
- add basic dashboards
- prototype alerting by adding some alerts on the above to whatever the most trafficked demo app that there is. 

This would provide a pretty good guide for monitoring reps when the time comes. 

','yJ5hiysWE-LBcDfT44lR8'),
	 ('q9YmYCPHbut4xiDqipm4g','Tckft3fqCqXBy9sTzI0F_',1679818208000,'Fixed by #437 ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Q6zvMwTLSF1u3rH7JWGAk','SNR_aE9fn7b5Wzpn0-Qmb',1674548850000,'omg yes, please.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('T9hKinkto014POLS7nd_O','SNR_aE9fn7b5Wzpn0-Qmb',1674548927000,'This would simplify tons of code, all over the place. As far as waiting for persistent storage, we could have a testing-only `ready()` api or something, but why do the test need to know when storage is available?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('5HmMI8HF6LjDDwdR-YBCq','SNR_aE9fn7b5Wzpn0-Qmb',1674548970000,'Before SDD we used to reuse the `clientID` but with both SDD and DD31 we always create a new `clientID`.','nqYkxAGMnzk7Y5STjZryV'),
	 ('Nw6E8RfPn_46yTkK_p-Lq','SNR_aE9fn7b5Wzpn0-Qmb',1674549789000,'> why do the test need to know why storage is available?

I think it is mostly useful for unit tests to reduce nondeterministic behavior.','nqYkxAGMnzk7Y5STjZryV'),
	 ('yGt-jGomjKIwVkx-ruYe1','OGe1tnwdeE_3IYZKNqE-i',1674493837000,'This part of Replicache was always a bit wonky. In retrospect, I''m not sure why we need the retry mechanism. Would something like:

```ts
class Reflect {
  constructor(options:{auth: string});
  // I think you guys were already working on a mechanism for "structured errors", this is just that mechanism
  // with a code for auth.
  onClose: (reason: "auth"|..., details: string);
}
```

work? User could hook `onClose` + `auth` to just recreate Reflect and try again. True there''s no backoff here, but  I''m not sure that really matters and every one of these indirections in common case makes Reflect a little harder to use.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('pFetkNQGwVjcNgMHC643t','OGe1tnwdeE_3IYZKNqE-i',1674558627000,'I like the idea of closing the Reflect instance on auth error. Let''s try it!

~~I don''t like bundling this into `onClose` because it is not clear if `onClose` should happen if `close()` is called. I''d rather be more explicit and use `onAuthError`.~~

I realized that there are a few non auth close with error so using `onClose(ok: boolean, kind: string, details: string)` seems reasonable.','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('HDMKeb4inf9J_aSxHLSpe','OGe1tnwdeE_3IYZKNqE-i',1674588975000,'Picking up from https://github.com/rocicorp/reflect/pull/83#issuecomment-1402468754...

I see that it is awkward to special case this one class of error behavior. Sorry for not thinking this through. I change my mind: all classes of server errors should leave Reflect "open" so the app can continue working, and it should try and retry in the background.

I still feel a little sad about this though:

```ts
const rep = new Reflect({
  apiKey,
  roomID,
  mutators,
  authToken: () => myConstantAuthToken
})
```

... for the very common case where user doesn''t handle reauth. It''s just a tiny bit of friction that makes the dx feel less inviting when people are first trying the product.

A few simple fixes I can imagine:

1. authToken can be a string or a function. In the string case a function returning the string is implied.
2. There''s a separate reauth API
3. Aaron is being unnecessarily prissy

I guess of these I like 1 the best.

For the question of retrying, I think we can integrate retrying auth into the normal exponential backoff?

If the client fails auth on the server-side, we send an error and close the connection. The client sees that the error is auth related and internally clears the auth token. Exponential backoff happens normally. On next connection, client sees auth token is null and calls `authToken()` function.

WDYT? (also @phritz)','OeVnr1y5bEM_Yg06sUFtD'),
	 ('KfzDauApuixwCjhxMQj4y','OGe1tnwdeE_3IYZKNqE-i',1674600291000,'I''m not sure about the exponential backoff for reauth. I will have to think a bit more about it tomorrow...','nqYkxAGMnzk7Y5STjZryV'),
	 ('tHfWlM4XgfgNtiDj4PndX','OGe1tnwdeE_3IYZKNqE-i',1675888734000,'> I''m not sure about the exponential backoff for reauth. I will have to think a bit more about it tomorrow...

I ended up using a backoff but the first auth error is tried immediately.
','nqYkxAGMnzk7Y5STjZryV'),
	 ('ROSO-_CpdvrDdF-g4QPTA','p7wxY5YNUecj_HG39MVYl',1683340809000,'Update: I forgot about this concerned and increased the rate significantly to 1 ping every 5s :). So maybe this is more of an issue now.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('9zKB96kEEXnG2Aps26Hm9','A8IJYzC_gzC6ejrn1RFKv',1674056958000,'https://discord.com/channels/830183651022471199/1063387649462775828','nqYkxAGMnzk7Y5STjZryV'),
	 ('27fuIU4wJ8DZXHgdmO0ig','A8IJYzC_gzC6ejrn1RFKv',1674061546000,'I think this is covered by rocicorp/mono#43 , no?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('QJoOBZsTGCyUdMKPPehsi','A8IJYzC_gzC6ejrn1RFKv',1674119543000,'Yup. Closing in favor of rocicorp/mono#43 ','nqYkxAGMnzk7Y5STjZryV'),
	 ('a5QmWHRqfTHq87lwBG942','wBXewIj2c81V7yXL_eWa2',1677696997000,'I no longer think we should do this.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('-GSG6_nqLJ8E_De9sAUOB','4d-w78fngkUNR2nd284MI',1673630311000,'For custom attributes, say doID or something, do we have to do something to get datadog to recognize them (eg, add a pipline that maps them to a tag or something), or does that happen automatically if they are passed in the context? 

Generally, whatever we need to do to get datadog to recognize and search or join on the additional context in the log we should do. ','yJ5hiysWE-LBcDfT44lR8'),
	 ('XtNofOHxWNtaxZhUbTqlx','4d-w78fngkUNR2nd284MI',1673948299000,'We do not need to do anything to get Datadog to recognize these. We can filter and add columns etc

Filter:
<img width="931" alt="Screenshot 2023-01-17 at 10 33 48" src="https://user-images.githubusercontent.com/45845/212861996-335079bf-bb33-47f7-8229-133b48a79039.png">

Columns:

<img width="595" alt="Screenshot 2023-01-17 at 10 37 07" src="https://user-images.githubusercontent.com/45845/212862647-ebc14247-1f23-49fb-ba29-117e4ba995c6.png">

Datadog does have a feature that allows you to map names to other names so you can unify things like `clientID` and `client_id` etc.

','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('EApvI4qeRshNSOG5NFQOH','4d-w78fngkUNR2nd284MI',1677696950000,'Still think we should do this. @arv it''s not clear to me if this requires a change in `LogContext` or just `DataDogLogSink`. ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('CdQCeH2J90cUG_tr-qjCg','4d-w78fngkUNR2nd284MI',1677752344000,'My thinking was that the `LogSink` interface would get a new optional method:

```ts
logWithContext?(level: LogLevel, context: Context, ...args: unknown[]): void;
```

and the `LogContext` impl would use that if present instead of adding the context as string args.','nqYkxAGMnzk7Y5STjZryV'),
	 ('jX3DPmamQM1uOez8YiHjz','CaK0J5eeX10pPH0LmvND6',1673470151000,'aaaah.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('o5zVPS3kyXJT46X4DlG89','CaK0J5eeX10pPH0LmvND6',1673470239000,'But... this all started due to @jesseditson putting a large `UInt8Array` into DO. `UInt8Array`s, like almost anything else are type safe from a JSONValue perspective. It is just that they get serialized as `{"0":0,"1":1, ... }` which is very inefficient and also does not round trip.

In debug mode we could warn about usage of typed array (`ArrayBuffer.isView(value)`) but it seems strange to have a deny list... thinking if it is possible to have an allow list instead...','nqYkxAGMnzk7Y5STjZryV'),
	 ('j-enI0NCMawEhmdCrZRib','CaK0J5eeX10pPH0LmvND6',1673470820000,'Realize this may be a non-desirable can of worms, but perhaps for an allowlist, rather than asserting, replicache could just perform a standardized encoding/decoding?','_4MJeHV4T-qZb52fyNwQ9'),
	 ('bsliuLhRfiVnEiiErOSQF','CaK0J5eeX10pPH0LmvND6',1673470937000,'> Realize this may be a non-desirable can of worms, but perhaps for an allowlist, rather than asserting, replicache could just perform a standardized encoding/decoding?

We had this but it is too expensive. Instead we try to assert in debug mode and do nothing in release mode.
','nqYkxAGMnzk7Y5STjZryV'),
	 ('70tK5U914KOPfxLA3MQOf','h_P5TxjqbvTCfqnduTxlm',1673447801000,'Looking at Figma WS network request. They do not use `Sec-WebSocket-Protocol`','nqYkxAGMnzk7Y5STjZryV'),
	 ('UGGr68NxCbxlDQnrzrTxI','h_P5TxjqbvTCfqnduTxlm',1673453586000,'yes for sure, auth via the header like we do seems potentially a contributor: https://github.com/rocicorp/mono/issues/198. since you have more info here i will close that one. ','yJ5hiysWE-LBcDfT44lR8'),
	 ('0Y-g05oeno_MUPEYFD-44','h_P5TxjqbvTCfqnduTxlm',1673453707000,'hehe, like minds think alike','nqYkxAGMnzk7Y5STjZryV'),
	 ('8W6FBYiz9M5x0Vz3FTZkp','h_P5TxjqbvTCfqnduTxlm',1673453709000,'Note the other issue suggests we should wait until we can measure the impact of any change here before making it (ie, after rocicorp/reflect-server#254)','yJ5hiysWE-LBcDfT44lR8');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('ON1Q4D89h1iYIcPHBnuxO','h_P5TxjqbvTCfqnduTxlm',1675861935000,'Some background reading related to how auth works

https://www.notion.so/replicache/Invalidating-Auth-732e9f9abb6a4806b5461c87dfde580f
https://www.notion.so/replicache/Reflect-Authentication-Authorization-and-Security-bf2b1a2e31844efca73c4aa453fdecdb','nqYkxAGMnzk7Y5STjZryV'),
	 ('AFo10CELY3pYPSO5G-W_g','h_P5TxjqbvTCfqnduTxlm',1677697408000,'I don''t think this project itself should be part of the beta, rather "fix connectivity" should be and if this is the solution so be it.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('PTHooLr83xRxui80FkOk3','CmTCA2KniOlv8idFsiTbP',1677698594000,'We will not get kicked out of bed for this. Feel free to fix if it bothers you, but definitely not beta blocker.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('iB1ZtX6hduFNUzGBFC81J','WDpWkLYwqh5-ivAe6JUB6',1673398046000,'I think we should look at this this week and triage it.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('FvIEyrUMZ4uCzfPRBtEDr','WDpWkLYwqh5-ivAe6JUB6',1684746081000,'This is probably `sizeOfValue` that you''re working on current right @arv ?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('0XiN56V-b3o_b-mfF06f8','WDpWkLYwqh5-ivAe6JUB6',1684746615000,'Most likely due to the fact that we were computing the size of large Uint8Array (as a Record) in every getNode.','nqYkxAGMnzk7Y5STjZryV'),
	 ('LzAbED8C1B6mUw7dB70dy','U9L_QRq8r0p27ZcMYlQ7H',1673354131000,'We need to ask ourselves: Who are these errors for?

In the sample above it looks like our code is not handling the closed state correctly during ClientGC.','nqYkxAGMnzk7Y5STjZryV'),
	 ('7j4prihxu6j-9gy5ElmLK','U9L_QRq8r0p27ZcMYlQ7H',1675759371000,'The thing fritz referenced above looks like an actual error an engineer should look like to me.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('eBdU0VxKpniJWnHROMJbZ','D1hkYdF4W_e2afBO8Sq36',1673055023000,'Although I can trivially do myself too, mainly just writing down to remember.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('95MqgHfNSLXPQF_dFrDGG','PYfC_yMiXwWgXzO2V0ncZ',1684746121000,'Lol @grgbkr ','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('KvWlw6ZLRYl9fGWNIgRXN','dYjfk3Efw8tZFr_fc7K9y',1672952034000,'We may want to add a "chunk event" logging mode to debug this.  This involves events across tabs (i.e. the tab that gc''d the chunk may not be the tab that tries to read the missing chunk). We could keep a separate db that we use in this mode that is a map from chunk id to tuples of (time, tab, action={WRITTEN, GCD, etc}) or similar. ','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('Ms3Q2wJBqv-6xwmz7YuqU','dYjfk3Efw8tZFr_fc7K9y',1672952842000,'note this error co-occurred with another error "Error during refresh from storage Error: invalid value" which makes it sound like we wrote a chunk that we could not read out. 

![image (2)](https://user-images.githubusercontent.com/157153/210879945-7a257e6e-fc59-4eee-b4f8-5f3d50f998c7.png)

one more bit is that it remained broken across refreshes
','yJ5hiysWE-LBcDfT44lR8'),
	 ('WdoWgELYb1icQU7Djo9oP','dYjfk3Efw8tZFr_fc7K9y',1681242528000,'Closing this in favor of #434','nqYkxAGMnzk7Y5STjZryV'),
	 ('76Vsy4DxOnPaMFmuCocb7','NFSnRiWXM86tpWC35kIut',1673453612000,'closing as dup of https://github.com/rocicorp/mono/issues/193 ','yJ5hiysWE-LBcDfT44lR8'),
	 ('75MzzP0fdFUWMlkWevhzX','0K2fsQ-qp2Q1Uk5oltpM9',1672900094000,'Yeah I suppose the version in `connect` should mean the version of the entire protocol that flows over that socket, right? That''s nice.

I think there are two potential behaviors for a version mismatch for connect:

1. Server rejects old protocols (page must reload with new client). This is nice because server doesn''t have to support old protocols.
2. Server supports old protocols

(1) is only really an option for on-prem, though.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('G-KRwP1ffo-2rdRXDEjHi','0K2fsQ-qp2Q1Uk5oltpM9',1675934424000,'This is a duplicate of rocicorp/mono#183 ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('9VgBhu6xjS4SZYuNT_aWT','0K2fsQ-qp2Q1Uk5oltpM9',1675934534000,'Oh wait, `createRoom` still needs to get done.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('i-mXdWcieMQbi65QtXIDz','cdWsuUKZ5sgu9TahRetSK',1674101793000,'See also comments in https://github.com/rocicorp/reflect/pull/74.','yJ5hiysWE-LBcDfT44lR8'),
	 ('WfIGF5Lj1JOILY5HcGs5g','cdWsuUKZ5sgu9TahRetSK',1674162530000,'I wonder if as part of this we should add an auth-over-ws path to client and server and have a switch in the client (eg in options) that can be set to toggle back and forth. That way we could get monday to try it out without having to get them a new binary...','yJ5hiysWE-LBcDfT44lR8'),
	 ('mvrki5KyXT2LJ-0si_Sya','cdWsuUKZ5sgu9TahRetSK',1677698094000,'Almost there. In order to call this done, let''s just do these last two:

<img width="883" alt="Screen Shot 2023-03-01 at 9 14 25 AM" src="https://user-images.githubusercontent.com/80388/222241239-24fa111e-bdb4-437a-8aa4-026a753833de.png">

The other unchecked items are represented by other bugs which are tracked and prioritized separately.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('H-yR9VJLzIg_Xg3Il4dLK','cdWsuUKZ5sgu9TahRetSK',1677698135000,'Removing P1 as remaining things not represented by other bugs are lower priority but we should still do for beta.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('b58v553RTUUr2RXMSpIJ0','cdWsuUKZ5sgu9TahRetSK',1679343338000,'@cesara is this done now?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('COqAupJMcdKz2QMTWC_3l','cdWsuUKZ5sgu9TahRetSK',1679343887000,'> Almost there. In order to call this done, let''s just do these last two:
> 
> <img width="883" alt="Screen Shot 2023-03-01 at 9 14 25 AM" src="https://user-images.githubusercontent.com/80388/222241239-24fa111e-bdb4-437a-8aa4-026a753833de.png">
> 
> The other unchecked items are represented by other bugs which are tracked and prioritized separately.

This task is finished according to the above. Maybe we open a separate issue for any remaining items.','pgyTvcxh2hjmq2l4WKzK6'),
	 ('S9xkBBCp_3CK_6qUmDM_i','YIfRqAwXdGv8V_ZB99s_5',1675146857000,'redundant ','yJ5hiysWE-LBcDfT44lR8'),
	 ('M2tCUVBLnNBsYpdywnNbA','bu5Gu8BI5Izq-DNSjvZVG',1675129249000,'@arv i think you fixed this yes?','yJ5hiysWE-LBcDfT44lR8'),
	 ('NVEsCwzbIAPXJMtISLakf','bu5Gu8BI5Izq-DNSjvZVG',1675153969000,'Sure. I have a plan to make clientID sync but it will have to wait for DD31
','nqYkxAGMnzk7Y5STjZryV'),
	 ('zS_NXHrtlVT1Q5i5QRUgf','eQvf5NTwA7IrQ5Lm2Raps',1673450581000,'DataDogBrowserLogSink is clearly not doing what I want:

```js
  const sink = new DataDogBrowserLogSink();
  sink.log("info", "test info", { a: 42 });
```

<img width="618" alt="Screenshot 2023-01-11 at 16 21 22" src="https://user-images.githubusercontent.com/45845/211844867-215771e9-14bb-43df-b945-ac66e821c83a.png">
','nqYkxAGMnzk7Y5STjZryV'),
	 ('Redqawje2VmkRvEMu1JNF','eQvf5NTwA7IrQ5Lm2Raps',1673521974000,'We should figure out where these DataDog loggers should live. Is it a new npm package?','nqYkxAGMnzk7Y5STjZryV'),
	 ('1EkiIte0xWb_x_nzEVXXo','eQvf5NTwA7IrQ5Lm2Raps',1673537935000,'> We should figure out where these DataDog loggers should live. Is it a new npm package?

I will have some datadog metric tools that I want to be shared across reflect or customer app and reflect-server. They don''t have to go in the same place, but certainly would naturally fit into a datadog-tools or similar repo. 

','yJ5hiysWE-LBcDfT44lR8'),
	 ('pJ9AkmH4wFYjPLVEmAC50','eQvf5NTwA7IrQ5Lm2Raps',1673550677000,'Thank you for doing this, all these little quality of life things really
help when debugging these production issues. Here is another one: in this screen shot, `doID` and `req` are our contextual attributes. But aren''t those supposed to show up in DD as "event attributes" so that we can filter by them using the first-class UI? It''s weird they show up as part of the log message string, I don''t think it''s intentional.

<img width="774" alt="Screen Shot 2023-01-12 at 9 07 49 AM" src="https://user-images.githubusercontent.com/80388/212160242-75b314e1-d759-4c95-8544-62b2d9d855c7.png">

https://docs.datadoghq.com/logs/log_configuration/parsing/?tab=matchers says:

<img width="828" alt="Screen Shot 2023-01-12 at 9 10 53 AM" src="https://user-images.githubusercontent.com/80388/212159217-5c5150c6-b8b0-4c91-9122-023efb928150.png">

They don''t have a picture of what the parsing is supposed to result in, but I don''t think the current behavior is right.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('bDkInhzh0oOnstH_vBt8A','eQvf5NTwA7IrQ5Lm2Raps',1673552877000,'There is also this old issue: https://github.com/rocicorp/replicache/issues/991','nqYkxAGMnzk7Y5STjZryV'),
	 ('-IrlcLJ6Y1bWSxkV8r0T2','eQvf5NTwA7IrQ5Lm2Raps',1673557588000,'> doID and req are our contextual attributes. But aren''t those supposed to show up in DD as "event attributes" 

I don''t recall if we taught datadog to grok doID, requestID and similar? If we did, maybe the pipeline broke or needs to be updated. In any case, there is a lot of related work we have to make reflect client and server easier to understand, including teaching DD about these and potentially other fields. I added a new comment to this issue and included this task there so we don''t forget: https://github.com/rocicorp/reflect-server/issues/60#issuecomment-1380991408','yJ5hiysWE-LBcDfT44lR8'),
	 ('KYyX63XBnSU_skQcPteRI','eQvf5NTwA7IrQ5Lm2Raps',1673602817000,'We never connected the dots between LogContext contexts and DD attributes','nqYkxAGMnzk7Y5STjZryV'),
	 ('plDLstQCBFOJJGj_GqIOD','eQvf5NTwA7IrQ5Lm2Raps',1673948373000,'Closing this in favor of https://github.com/rocicorp/mono/issues/191','nqYkxAGMnzk7Y5STjZryV'),
	 ('7RI3NjbzWQs5TZWPnSSyB','w5UIbcmzHvudFnxpX3lk0',1674058631000,'Closing this.

We should move the LogContext addContext handlers to the router middleware but that can be done in the future.','nqYkxAGMnzk7Y5STjZryV'),
	 ('jGMB_7mBEr8rbLzPnY9R1','9-3eWCzqz3iLkO1e4YdZP',1672894848000,'Should probably also address https://github.com/rocicorp/mono/issues/199 as part of this.','yJ5hiysWE-LBcDfT44lR8'),
	 ('DddfTnUvhoYqaBCbd1wsR','9-3eWCzqz3iLkO1e4YdZP',1675934579000,'Is this done @cesara ?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('vvjw7tiBPrsYVWVvFKOzj','9-3eWCzqz3iLkO1e4YdZP',1675952330000,'yes,  the top two are done. I believe fritz did the cors'' portion. I think Erik addressed rocicorp/mono#199. ','pgyTvcxh2hjmq2l4WKzK6'),
	 ('RHGPK_dIOHSDxclJLPaAU','9-3eWCzqz3iLkO1e4YdZP',1675958545000,'I didn''t do rocicorp/mono#199. I was looking into it as part of rocicorp/mono#193 ','nqYkxAGMnzk7Y5STjZryV'),
	 ('HelFGpbVzUKxvVF1qpgJZ','9-3eWCzqz3iLkO1e4YdZP',1675974632000,'OK well we already have a bug for rocicorp/mono#199 so closing this.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('SYHopOuAVlpIP6gnnkZ2F','c5nwL8-1RIOtD-7nhZ8K9',1672869220000,'See rocicorp/mono#210. From latest spreadsheet, I think that disconnect on blur and reconnect on focus should be sufficient. That will be a better ux too.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('vysm4tDJFdY47jUr3sQkX','c5nwL8-1RIOtD-7nhZ8K9',1672935359000,'Disconnecting on `blur` seems like the wrong signal. Disconnection on [visibilitychange](https://developer.mozilla.org/en-US/docs/Web/API/Document/visibilitychange_event) to hidden seems like a more reasonable signal. `blur` seems wrong because having two tabs side by side would then disconnect one of those tabs.

We should also probably wait 10s (or 5s, or whatever) before doing this to allow cycling through tabs','nqYkxAGMnzk7Y5STjZryV'),
	 ('Dqo35y3RyjCfYg1R6airy','c5nwL8-1RIOtD-7nhZ8K9',1675934835000,'This bug is a duplicate, mostly of rocicorp/mono#180 ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('WWQPD5IuvDlApKZ3ruseO','VAL6IFc07pvHccWmY00pg',1677705372000,'I think due to typescript / intellisense we could actually get away with *not* having reference docs 🤯 for playable beta.

But we do need a one-pager getting started doc.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('iTy89qaIHf7EbBczMC72h','kEj282IgquWxEIwD1U_p1',1672869361000,'> I think we should at the very least set allowConcurrency to false in the auth DO, and get rid of the manual locking there.

I don''t think it''s that simple. There are places that an input gate would not help us because there is a race with incoming requests while no storage operation is in flight, eg when updating a room. If someone wants to rip the lock out for reasons of perceived complexity then they need to do a careful analysis of what the locks are doing and determine which can be replaced.

However, personally, I''m *far* more comfortable reasoning about traditional locks than I am about the input gate. It is totally not clear to me that we will have fewer bugs and spend less time on the code if we try to eliminate locking in favor of gates. 

Greg and I had a discussion about the locking in the DOs and while neither of us was thrilled with having it in there we were convinced we needed it for correctness and that if it became an actual problem for throughput or complexity then we would revisit. So the current state is intentional, not accidental. ','yJ5hiysWE-LBcDfT44lR8'),
	 ('fob5o8JkAJ5say60xMgWk','kEj282IgquWxEIwD1U_p1',1672872718000,'OK this makes sense. I also have an easier time reasoning about the locking. Nevermind this.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('BmXibkKGmu6b-_1AurADH','znyGxse2L5-lR5-NUrixH',1671673820000,'We should definitely:

- Add an "idle room" metric -- length of time a room is running, but not doing anything (no mutations coming in)

Ideas for making metric go down:

1. Clients disconnect themselves on tab hide seems pretty simple, but not completely bulletproof (clients could miss the event but stay connected somehow)
2. Clients disconnect themselves if they''ve not sent any mutations (or received pokes?) for awhile
3. Server disconnect all clients when it hasn''t done anything except process pings for 5m is interesting, but we''d have to make the clients not to immediately try to reconnect themselves!','OeVnr1y5bEM_Yg06sUFtD'),
	 ('v3Ofsd9umNpWLzGbwJRfO','znyGxse2L5-lR5-NUrixH',1675935364000,'So many stupid duplicate bugs. rocicorp/mono#180 ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('y1fW-lfdeTe5HhhNps0SW','znyGxse2L5-lR5-NUrixH',1676297289000,'Reopeneing due to:

- Clients disconnect themselves if they''ve not sent any mutations (or received pokes?) for awhile
- Server disconnect all clients when it hasn''t done anything except process pings for 5m is interesting, but we''d have to make the clients not to immediately try to reconnect themselves!','nqYkxAGMnzk7Y5STjZryV'),
	 ('VpGCFJx1y4FixP62ncoUw','kYQF1eM1NXqzSISKYjoU5',1671672120000,'This seems related to rocicorp/mono#276 and rocicorp/mono#225 but not exactly the same. I feel like the immediate tasks are:

- confirm this metric by looking at datadog data -- can we find examples of clients that took > 1min to connect in that data?
- debug those cases and see if we can determine something that went wrong','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('gAhsdlcPTsPmhMnyr32hp','kYQF1eM1NXqzSISKYjoU5',1671691523000,'I do find a few examples of this. From a dataset in the neighborhood of 2022-12-15.

This client took 9m to startup:

<img width="1595" alt="Screen Shot 2022-12-21 at 8 22 52 PM" src="https://user-images.githubusercontent.com/80388/209070629-51ea203b-38a2-4e3d-a3f7-d432824a4ac1.png">

However we cannot rule out that this client was legitimately offline because at this time we didn''t have the `isOnline` log line, and the browser was open for awhile before. Perhaps network blip or something:

<img width="1482" alt="Screen Shot 2022-12-21 at 8 25 23 PM" src="https://user-images.githubusercontent.com/80388/209071022-5e4cfb6b-7e57-4943-b3ab-7a361714dd5e.png">

Here is a different one that was offline for 2m:

<img width="1593" alt="Screen Shot 2022-12-21 at 8 31 29 PM" src="https://user-images.githubusercontent.com/80388/209071772-d9ca71e0-ed4f-43f1-82f1-4e7f5148f9aa.png">

I don''t see anything wrong with these logs. It just seems to take awhile for the request to make it to the server sometimes. These are the two most egregious examples from this log set, but that same roomID `G1LcejpAqDj2vE4voT2Cgzd-2Xi4lX_x` from last example has two other cases where client took 40s to connect:

<img width="661" alt="Screen Shot 2022-12-21 at 8 35 08 PM" src="https://user-images.githubusercontent.com/80388/209072274-66158c7f-76cc-43d9-bbc8-194bad5770ac.png">

Interestingly we also see that same HK user from rocicorp/mono#276 (room `ka8iwY3wtfbQ4M6t8KYPWA8bQ4f9xBWd`) show up here with some connections that take ~14s:

<img width="693" alt="Screen Shot 2022-12-21 at 8 40 05 PM" src="https://user-images.githubusercontent.com/80388/209073077-51c3adb3-33ec-42ec-8de5-26cba782f131.png">

Another really interesting pattern is that these logs span: `2022-12-15T21:29:33.124Z` to `2022-12-16T02:29:57.425Z`, but the longest example of connections seem to cluster around `~2022-12-15T22:00:00Z`:

<img width="609" alt="Screen Shot 2022-12-21 at 8 42 11 PM" src="https://user-images.githubusercontent.com/80388/209073398-7a2c42e4-f262-43f6-9329-90e6f6ed5829.png">

Perhaps something was pushed around that time and all the DOs restarted?

There were a lot of server starts then, but also other times, but we don''t see the long connection times at other places where server starts were higher:

<img width="1604" alt="Screen Shot 2022-12-21 at 8 44 09 PM" src="https://user-images.githubusercontent.com/80388/209073674-0497e674-c261-4ac9-a051-fdcc549c141c.png">

Unclear. Perhaps something was happening at Cloudflare at that time.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('4UXFkTDuoCpEknlXnL8tT','kYQF1eM1NXqzSISKYjoU5',1671692279000,'However, from this sample, the percentages are far lower than Noam observed:

- 8/934 (0.8%) > 10s
- 2/934 (0.2%) > 60s
- 2/934 (0.2%) > 120s
- 1/934 (0.1%) > 360s

I think the next step on this bug is:

0. We need to include the clientID in every message up to the server. The fact that the "connecting..." message sometimes doesn''t have the clientID makes this analysis difficult.
1. @noamackerman - can you check the graph on Cloudflare under workers > metrics > summary and see if you can see any increase in errors around 2022-12-15T22:00:00Z ?
2. I should do the same log analysis on the data that overlaps where @noamackerman saw lots of long connections and see if I see the same thing.
3. We need a metric on datadog to track how long connections take','OeVnr1y5bEM_Yg06sUFtD'),
	 ('dncdeexOfXIIokQEPjXRq','kYQF1eM1NXqzSISKYjoU5',1673054090000,'I cannot run the analysis on the log data ourselves because we don''t have a join key between the `Connecting...` and `Connected` log line. I tried just finding the "Connecting..." log line for the same room before each "Connected" but sometimes it appears that the "Connected" line gets logged with a slightly lower timestamp than the Connecting" line. I''m not sure why.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('wFC-UiOwHvkC_gu8Part3','kYQF1eM1NXqzSISKYjoU5',1673055039000,'Need to fix logging to make this analysis possible: https://github.com/rocicorp/mono/issues/196','OeVnr1y5bEM_Yg06sUFtD'),
	 ('aAYrro6JvXqT-zBgaB2_V','kYQF1eM1NXqzSISKYjoU5',1683763876000,'Closing this now as our metrics don''t support it.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('A_1jE1JAPOhwW2z7BDxJ7','Nx3NmxamoB7soTtyMrcfE',1672874329000,'We should try it out and make sure that it catches:

(1) unhandled exceptions
(2) ooms (just make a bad worker that allocates forever)
(3) kind of curious what happens if you make a worker that enters a busy loop. I assume they eventually kill it. Does that show up in the logpush log?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('8U31OpOqTXQxLKxk4uXNc','Nx3NmxamoB7soTtyMrcfE',1672887885000,'the process of adding log collection to our reflect worker and DOs is kind of lost in the mists of time for me. @aboodman @arv i think this means we could replace https://github.com/rocicorp/reflect-server/blob/578c3ab3f83dde7fc96638721cd4eb99f70d7074/src/util/datadog-log-sink.ts with the built-in CF logpush to datadog? 

also: there must''ve been some reason that we didn''t use the node datadog library (https://github.com/DataDog/datadog-api-client-typescript) on the server? 

asking because we want to start collecting custom metrics on the client and server and https://www.npmjs.com/package/datadog-metrics on the server would be less work than rolling our own API client. ','yJ5hiysWE-LBcDfT44lR8'),
	 ('UF4vT2arlzCpdH78RHYHm','Nx3NmxamoB7soTtyMrcfE',1672888710000,'Yeah they didn''t use to have logpush for workers so we had to do the in-process thing.

I believe that the datadog client library didn''t work inside the worker env for some reason.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Qa8CkiVVaTT7Ij2TnsWxc','Nx3NmxamoB7soTtyMrcfE',1672890144000,'And for browser-side logging, we didn''t use https://github.com/DataDog/browser-sdk maybe because... too heavyweight? 

And didn''t use https://github.com/DataDog/datadog-api-client-typescript because maybe it assumes node and is incompatible with running in an actual browser? ','yJ5hiysWE-LBcDfT44lR8'),
	 ('oZQd_H2EkHQk2PvNXsC9d','Nx3NmxamoB7soTtyMrcfE',1672897858000,'For the browser we *do* use their library:
https://github.com/rocicorp/replidraw-do/blob/main/src/frontend/data-dog-browser-log-sink.tsx#L2

On Wed, Jan 4, 2023 at 5:42 PM Phritz ***@***.***> wrote:

> And for browser-side logging, we didn''t use
> https://github.com/DataDog/browser-sdk maybe because... too heavyweight?
>
> And didn''t use https://github.com/DataDog/datadog-api-client-typescript
> because maybe it assumes node and is incompatible with running in an actual
> browser?
>
> —
> Reply to this email directly, view it on GitHub
> <https://github.com/rocicorp/mono/issues/212>,
> or unsubscribe
> <https://github.com/notifications/unsubscribe-auth/AAATUBHN3YTBBF7OK7JJKPDWQY7KXANCNFSM6AAAAAATGHV3WU>
> .
> You are receiving this because you were mentioned.Message ID:
> ***@***.***>
>
','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('1SsXn90bxs0YdpwwT-g3o','Nx3NmxamoB7soTtyMrcfE',1672914505000,'@phritz: IIRC, CF workers could not use the node library due to dependencies on node specific modules.','nqYkxAGMnzk7Y5STjZryV'),
	 ('syuHbndj3uc4lzbctbVzS','Nx3NmxamoB7soTtyMrcfE',1673132434000,'FYI @aboodman logpush is [only available for enterprise customers](https://developers.cloudflare.com/logs/about). While that''s fine for us, doubt it''s going to be something that a tire kicker or small scale customer is going to be excited about needing.','yJ5hiysWE-LBcDfT44lR8'),
	 ('CTeFV-I5osSONNLTHxPPx','Nx3NmxamoB7soTtyMrcfE',1673133328000,'Darn. I''m very concerned about not having visibility into crashes in
datadog or metrics. This seems like something we need and wrapping all the
top level entrypoints doesn''t seem like a substitute to me because I worry
about missing events like oom restarts.

Thankfully we *will* see such events from client pov (in some ways even
better) once we have client-side metrics. Hm.

On Sat, Jan 7, 2023 at 1:00 PM Phritz ***@***.***> wrote:

> FYI @aboodman <https://github.com/aboodman> logpush is only available for
> enterprise customers <https://developers.cloudflare.com/logs/about>.
> While that''s fine for us, doubt it''s going to be something that a tire
> kicker or small scale customer is going to be excited about needing.
>
> —
> Reply to this email directly, view it on GitHub
> <https://github.com/rocicorp/mono/issues/212>,
> or unsubscribe
> <https://github.com/notifications/unsubscribe-auth/AAATUBE37Z3AJGLILQLQ64DWRHYR3ANCNFSM6AAAAAATGHV3WU>
> .
> You are receiving this because you were mentioned.Message ID:
> ***@***.***>
>
','OeVnr1y5bEM_Yg06sUFtD'),
	 ('iHX0JpsbmXN9uHj2-27G4','Nx3NmxamoB7soTtyMrcfE',1673134266000,'Just kidding! I asked for clarification in their discord. Logpush for *workers* is generally available, it''s logpush for other stuff (pages etc I guess) that is enterprise. 

But again it''s not a panacea -- there are a zillion reasons why a DO could be shut down with no exception delivered. We don''t even know if the exceptions that you are worried about (OOM, etc) are reliably delivered. I share the top level concern tho, and yes we should use logpush, but with logpush we still have scope for the exactly same problem, namely missing events like restarts. We need some metrics to actually understand if we are.','yJ5hiysWE-LBcDfT44lR8'),
	 ('cgH8tvenAHo8L64mcTTVb','VujlSbbYch9U_P1ZyauT8',1672991478000,'OK this other activity at same time was different user. Noam gave the user who could not connect''s account ID and it''s different than the concurrent activity. So my suspicion isn''t supported.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('3L-sQtjsm5TXBVIOG6Ufl','UFJjZiGCSP8QdFMe_0t39',1672875280000,'Note: might want to do https://github.com/rocicorp/reflect-server/issues/153 as it will have impact on the error messages. Also should probably wait for https://github.com/rocicorp/mono/issues/205 to land before doing this one because it''ll be easier to work with a single routing implementation.','yJ5hiysWE-LBcDfT44lR8'),
	 ('yBMYbI2tO8ifA5DE3MUIh','UFJjZiGCSP8QdFMe_0t39',1673468175000,'Somehow we allowed @jesseditson to try and tx.put a `UInt8Array`: https://rocicorp.slack.com/archives/C013XFG80JC/p1673465473373249?thread_ts=1673055269.485049&cid=C013XFG80JC. This should not be allowed. Runtime validation should have caught this at least in dev mode.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('bB-Qi_WooraXa7o3rVZkC','UFJjZiGCSP8QdFMe_0t39',1673470847000,'Depends on rocicorp/mono#75','nqYkxAGMnzk7Y5STjZryV'),
	 ('gKIFtkQRKSoEqs6vBfYhm','UFJjZiGCSP8QdFMe_0t39',1673988965000,'After DD31','nqYkxAGMnzk7Y5STjZryV'),
	 ('GL25OgfzRDCW9K-8qnrlE','UFJjZiGCSP8QdFMe_0t39',1678376872000,'Blocked by #307','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('NjoYFLBFujSEraXk3N83d','xDzOur0W1r62K-h5A9wYX',1684746367000,'This bug is a subset of #173 ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('089sQ55ru8TqZGTQcvsTV','o3zV2bl5vZlygIoOV-ToO',1677009186000,'You should be able to just say:

```ts
new Reflect({
  userID,
  roomID,
  auth,
  jurisdiction,
});
```

... and away you go, without needing to first call the `createRoom` endpoint.

Right now, `/connect` returns an error if room hasn''t been created already. This implies either client should call `createRoom` before every `connect` or else `connect` should carry enough information so that it can internally `/createRoom` first.

I favor the latter for latency reasons -- one roundtrip to connect vs two.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('aFKrR3yBoOKrs9brAemUn','o3zV2bl5vZlygIoOV-ToO',1677668244000,'There is a subtle and unlikely edge case if we bring this back, which I''m treating as a separate bug. See rocicorp/mono#232 .','OeVnr1y5bEM_Yg06sUFtD'),
	 ('QwClu2vzoynhfjQOq457R','4q0T060LnThGKHr_XhvYt',1677704576000,'Closing in favor of #76 now that we are in monorepo! yay!','OeVnr1y5bEM_Yg06sUFtD'),
	 ('nwk_Eh-ilqiXnOQlBl1A8','Fccuabzq0hVES_GDqKxEt',1672874902000,'This needs API design. The obvious thing is a field like:

```
environment: "client"|"server"
```

But we may also want to distinguish rebases and I''m not sure how to do that without making it a lot more obscure.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('AVxlk2slCk6ODYBNEXsOv','Fccuabzq0hVES_GDqKxEt',1677784113000,'Thinking about this more , let''s not overload the noun ''environment''. Let''s use:

```ts
location: "client"|"server",
```

In the future we can also add:

```ts
reason: "initial"|"authoritative"|"rebase"
```

That way if you only care about client vs server (common) you just use `location`. If for some reason you want to know the difference btwn initial and rebase, you can do that too.

@jesseditson - you have been using this part of the API, opinion on these two?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('WaAaOE-D1Na609HMRy7xu','Fccuabzq0hVES_GDqKxEt',1677784295000,'I think we should make this change in Replicache. In Replicache we will only set `client` (and `initial`|`rebase`) but in Reflect we will also set `server` and `authoritative`.

Later, we could aditionally modify https://github.com/rocicorp/replicache-transaction to specify `server` and `authoritative` as a bonus.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('PbguLPknnMC0iZG-tK4AU','Fccuabzq0hVES_GDqKxEt',1677784340000,'@cesara I think you can take this one. Please do not prioritize the work for `replicache-transaction`. Let''s just do that if there''s time.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('gcXrn10U0G8h3SjuO3l8v','Fccuabzq0hVES_GDqKxEt',1677790101000,'`location` would work for me, I''d be a bit concerned about people using this to detect, for instance, browser features - which wouldn''t be a good long-term design. Perhaps could imply a more specific meaning using:

```
context: "client" | "worker"
```

to differentiate between this and a physical location and ambiguity between reflect workers and customer servers (AFAIK there''s no reason not to think someone will run a reflect client on a server in the future, or that we could provide information related to a physical location to help with latency compensation or something).

That''s a nit. I''m good with it landing as described above, and it would make sense for my use case.','_4MJeHV4T-qZb52fyNwQ9'),
	 ('XVLwVYLOhet13SrVpNGSN','Fccuabzq0hVES_GDqKxEt',1677829991000,'I was trying to avoid the generic words _environment_ or _context_, but I give up. Let''s go back to:

```ts
environment: "client"|"server"
reason: "initial"|"authoritative"|"rebase"
```','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('t59S5Qx06DP_5ziWd5ioN','2n0GzU5ohobtwhs6BHAiY',1677781526000,'Uh, actually this is already done I think. We wrap Replicache, not inherit it. So we didn''t automatically inherit indexes. My bad!','OeVnr1y5bEM_Yg06sUFtD'),
	 ('kOS_b7-UvFOkrdaR0si0N','HBtkfiTiA7jZ9CSovC7br',1672737797000,'Fixed with rocicorp/reflect-server#241','nqYkxAGMnzk7Y5STjZryV'),
	 ('lSmjpjHdhkFDUsY4qtMS3','D206pQI7AMjje3NeFWqDe',1670962793000,'Probably need to accommodate not just backoff but also:
- does the reconnect logic make sense? need something similar to what arv added to replicache. 
- does the logging make sense?
- what about if DD31 is in and the user is expected to be offline for a long time?','yJ5hiysWE-LBcDfT44lR8'),
	 ('D2GCV6HhDX54GU3hWkf73','D206pQI7AMjje3NeFWqDe',1670982311000,'`onOnlineChange` should only fire when *reconnect* fails, not just because of passing socket drops.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('9r9x4L4HqqI6rdkzIx9vF','D206pQI7AMjje3NeFWqDe',1671059598000,'ensure that anything that throws (eg, connect) logs an error (eg top-ish level error handling)','yJ5hiysWE-LBcDfT44lR8'),
	 ('7xeXU3BeNlQCJCsBd3JJM','D206pQI7AMjje3NeFWqDe',1671609712000,'* disconnect on tab blur
* reconnect on tab focus','OeVnr1y5bEM_Yg06sUFtD'),
	 ('IZhhit86_wIKsjflTCdBB','D206pQI7AMjje3NeFWqDe',1671644819000,'> disconnect on tab blur
> reconnect on tab focus

Guess it makes sense for this issue to be the holistic set of things we need to do around dis/connect. So also for consideration is disconnecting from an inactive room and having a mechanism to reconnect when there is activity.','yJ5hiysWE-LBcDfT44lR8'),
	 ('NO6XoXqTkJgMfq1WP6Ljn','D206pQI7AMjje3NeFWqDe',1672882887000,'Closing in favor of https://github.com/rocicorp/mono/issues/200 which has more detail','yJ5hiysWE-LBcDfT44lR8'),
	 ('Mtcj2h9osoBpRBAWSQZBE','JpaKRaTCbaBever3R1Ucc',1677670788000,'Code is gone','nqYkxAGMnzk7Y5STjZryV'),
	 ('-NgdR8fNDt3n-buG-R_Pn','0SYi2rS6-eptSx9Jli9MG',1676316403000,'I can imagine doing this at least two ways:

* the easy hacky way
* the good way :)

The easy way is to provide some kind of language (jq?) to select keys to sync at any moment in time. This filter can be changed at runtime.

The good way is to provide a predicate function that does the same.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('VrSPPaRllvP_yDRJEw64x','t8TfRQn4UePo91vNUTSAl',1670644492000,'I think this will involve cleaning up the storage abstractions. Right now, `Replicache` directly uses IDB for the databases db. We need it to use supplied kv implementation instead, in the case that e.g., the environment doesn''t support idb (react native has this problem).

I think we need to introduce `kv.Factory` and `experimentalKVStore` => `storeFactory` or something. Then we would create an instance of the kv.store for the databases database, and also one for the storage backing the perdag.

Memory ( rocicorp/mono#43 ) would also implement the same interface.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('MvZsYDkrxjchgUeQ0fW42','t8TfRQn4UePo91vNUTSAl',1675958063000,'`experimentalKVStore` is flawed. We need to use a factory to support the database of databases.','nqYkxAGMnzk7Y5STjZryV'),
	 ('sAJkTCGkgVxU2GK6CWvK9','t8TfRQn4UePo91vNUTSAl',1675958215000,'With `experimentalCreateKVStore` we might want to export the MemoryStore or provide a separate open source repo for it since it is non trivial with the locking.','nqYkxAGMnzk7Y5STjZryV'),
	 ('ei0eKpTJiBiIQ0wAdQ-9g','t8TfRQn4UePo91vNUTSAl',1676200176000,'@arv assuming you are good with my last two PRs, I believe this can be de-experimentalified now, at least on trunk.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Y7IPmqTkad5X7jccHZYVi','t8TfRQn4UePo91vNUTSAl',1676281127000,'Proposed API:

```ts
store?: (name: string) => Store | undefined;
``` 

@aboodman suggested also allowing `''memory''` as a short for `name => new MemStore(name)`. I do feel like maybe that is better covered by `persistence?: boolean` because if we know we are not using persistence the strategy changes a bit. No need for  LazyStore and no need for a lot of the background processes.

Also rocicorp/mono#43','nqYkxAGMnzk7Y5STjZryV'),
	 ('zPSl2rfRwR4yXFul598S2','t8TfRQn4UePo91vNUTSAl',1676282213000,'Let''s wait and see how reflect develops then. No hurry here.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('bN-5havhZ4RmDPKJdYms2','0MqbIfE1OsaggQS9QCZSg',1670573422000,'I don''t think we need to use workspaces. All that is really needed is to:
-  change the name in package.json 
- `npm install` to update package-lock.json
- Change the build rule. Can be done in code by checking the name in package.json
- `npm publish`
- Change the name back

I guess this can be done on a branch or by using a script?','nqYkxAGMnzk7Y5STjZryV'),
	 ('1bdiNVOvCqXJu7YK7yatr','0MqbIfE1OsaggQS9QCZSg',1670573715000,'Oh yeah good idea, that''s easy enough. Thanks.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('hpm8jGKQvceGZyQtgoEfA','0MqbIfE1OsaggQS9QCZSg',1670574190000,'I think a branch is the simplest solution. The only thing that changes are the name field in package.json and package-lock.json which should be easy to maintain.','nqYkxAGMnzk7Y5STjZryV'),
	 ('NB77xLMkR32wPWTzRmB4S','mDWurE1JZULLC5izuPPy2',1675935496000,'Duplicate of rocicorp/mono#208 ','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('7zKl4i2vKaY32vFr1pl3C','TSwpUWX58yW4--9Q1HVmf',1673991910000,'This should happen as part of rocicorp/mono#200 ','yJ5hiysWE-LBcDfT44lR8'),
	 ('G8bg7VltYZafceqUsoIGj','TSwpUWX58yW4--9Q1HVmf',1675933426000,'This is done.','nqYkxAGMnzk7Y5STjZryV'),
	 ('Dsnht7SM9MY60Zp9TEm9D','cexhW1awUBKv-mrwl75H1',1670960823000,'also we should clean up and upintegrate these log line changes: https://github.com/rocicorp/reflect-server/pull/213','yJ5hiysWE-LBcDfT44lR8'),
	 ('6BGiSD-dnWmvNuGttlYfZ','cexhW1awUBKv-mrwl75H1',1672875147000,'This issue should also include looking at all commits on early christmas and if they are logging improvements integrate them into main.','yJ5hiysWE-LBcDfT44lR8'),
	 ('aptZfrZhI5CgIKWJeoUR5','cexhW1awUBKv-mrwl75H1',1673067889000,'We should have a structured error sent down over the ws for each of these errors that originate on the server: https://www.notion.so/replicache/metrics-0b848461ffce4312bdb645d65adb7da2#4a97339d439a414eb0090270d53c4706

We''ll need them to add the metrics described in that doc. ','yJ5hiysWE-LBcDfT44lR8'),
	 ('KVd7LKSX9lhIn6evWhbOq','cexhW1awUBKv-mrwl75H1',1673715650000,'The ''make errors structured'' part of this should also include https://github.com/rocicorp/reflect-server/blob/520f1a2f5ed35a5e6b5dc5eb018995b20d9ffeb4/src/server/auth-invalidate.ts#L12 which should be differentiated from the initial auth failure. ','yJ5hiysWE-LBcDfT44lR8'),
	 ('ujIU-VE_nuWD2mnX_EDSN','cexhW1awUBKv-mrwl75H1',1675129229000,'@arv i think you can close yes?','yJ5hiysWE-LBcDfT44lR8'),
	 ('zNvULJ-rvnhukmXqwinSB','G5xQd15DHqZMJI-6RpEOX',1677008998000,'On second thought not really a duplicate:

This bug is about changing existing protocol to detect when rooms are reused across instances of the server. Right now this works 99.99% of the time, but it should work all the time. This is a protocol correctness change that is orthogonal from API.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('ey1cXMrN6a55Lyqdl0_2u','G5xQd15DHqZMJI-6RpEOX',1678209020000,'BTW I think fritz''s [proposed solution](https://github.com/rocicorp/mono/issues/232#issue-1595660179) 

> having the room choose and persist a random value when it is created and return it to a client the first time the client connects. the client passes this in future connections and if it ever does not match what the server has then the server errors the client out.

only works when rooms are completely lost.

It is also common to just lose state changes for some recent versions.  This is exactly what happens if you start wrangler dev, access an existing room, make some changes, and then restart wrangler dev. 

To address both think we could change cookies to be a {id: uuid, version: number}, and keep a history of cookies on the server. Then when a client with existing state for a room connects, the room-do can validate if the cookie it is connecting with is in its cookie history.
','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('8WSiAeuzAR-503xlczbXT','G5xQd15DHqZMJI-6RpEOX',1678216730000,'Let''s call one running instance of the RoomDO for a given room ID, a _room instance_.

Room versions are monotonically increasing so it''s sufficient to store only the highest version reached for each room instance. If the version in a received cookie is greater than this highest version reached for the corresponding room instance, then the server has lost state and any pending mutations from that client should be dropped (or perhaps replayed, breaking causal consistency).','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('OWu6Kfq6FPT98dy9--zp6','G5xQd15DHqZMJI-6RpEOX',1678216805000,'Note this bug now relates closely to #330. When #330 is implemented, the occurrences of lost server writes will increase, causing same issue described by https://github.com/rocicorp/mono/issues/232#issuecomment-1458532241.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('n6h7Lv_C0YaeJ35zso5j7','G5xQd15DHqZMJI-6RpEOX',1678218615000,'> Let''s call one running instance of the RoomDO for a given room ID, a _room instance_.
> 
> Room versions are monotonically increasing so it''s sufficient to store only the highest version reached for each room instance. If the version in a received cookie is greater than this highest version reached for the corresponding room instance, then the server has lost state and any pending mutations from that client should be dropped (or perhaps replayed, breaking causal consistency).

But since the version is just a counter, it is possible that state is lost, and then other clients bump the version back up to a higher value.  ','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('odJNSzVX6mCAcWef5BbxP','G5xQd15DHqZMJI-6RpEOX',1678218811000,'Is it possible for that to happen in any other way than the room restarting? It seems like if that can happen it''s a CF bug.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('2EadKUjbQo9RJQNnbJcRz','G5xQd15DHqZMJI-6RpEOX',1678224497000,'It can happen with
1. dev mode (which loses state when you restart it)
2. the room restarting if the output gate is off, or if we are not flushing writes before sending pokes to clients','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('ENch9MORj3vux7DDkIQnL','G5xQd15DHqZMJI-6RpEOX',1678226629000,'What I''m saying is that each time `RoomDO` starts up it chooses for itself a new unique ID. This is its "room instance ID". So in 1, when you restart, you''d get a new instance ID. Same with 2. I''m probably missing something.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('iuwHFVhVIbvQ36g4oO57s','G5xQd15DHqZMJI-6RpEOX',1678227406000,'I see. I was missing that the id changes on restart.  Yes, that would allow
for storing a much ''smaller'' history, so we wouldn''t need to worry about
gcing history in some way.  I like it.

On Tue, Mar 7, 2023 at 3:04 PM Aaron Boodman ***@***.***>
wrote:

> What I''m saying is that each time RoomDO starts up it chooses for itself
> a new unique ID. This is its "room instance ID". So in 1, when you restart,
> you''d get a new instance ID. Same with 2. I''m probably missing something.
>
> —
> Reply to this email directly, view it on GitHub
> <https://github.com/rocicorp/mono/issues/232#issuecomment-1458935630>, or
> unsubscribe
> <https://github.com/notifications/unsubscribe-auth/AESFPBC73ZEJI2ZROBI4XNLW26WFBANCNFSM6AAAAAAVEWVVDM>
> .
> You are receiving this because you commented.Message ID:
> ***@***.***>
>
','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('uZNwHuErLGGLyPmpJbYWA','PzmGfX0MN6GFIRq9XcrHC',1670496475000,'WIP Notes: https://www.notion.so/replicache/Monday-Incident-Out-of-Order-Poke-a889ea29bcd7469d8feec679804ce2bd','OeVnr1y5bEM_Yg06sUFtD'),
	 ('gj9WtKo7MPnhz6c1WYla6','PzmGfX0MN6GFIRq9XcrHC',1671037168000,'After we fixed rocicorp/shared-monday#3, this mostly went away, but it still occurs at a much lower rate:

<img width="1384" alt="Screen Shot 2022-12-14 at 6 35 22 AM" src="https://user-images.githubusercontent.com/80388/207653949-7f4548c9-a2b3-42d1-b810-94f513cd7de3.png">

These aren''t just one-shot out-of-order pokes (ooop) either. It appears that clients still do loop on them. Here''s one:

<img width="869" alt="Screen Shot 2022-12-14 at 6 36 58 AM" src="https://user-images.githubusercontent.com/80388/207654341-5a683d2f-a3c3-471d-b56b-c94618225192.png">

This particular room has an interesting history. It was first created dec 5:

<img width="1394" alt="Screen Shot 2022-12-14 at 6 50 18 AM" src="https://user-images.githubusercontent.com/80388/207657353-8c8df178-7a69-40bf-9d99-c43caec414de.png">

Only two client IPs have ever accessed, but both from frankfurt, same UA, so I''m guessing same user:

<img width="198" alt="Screen Shot 2022-12-14 at 6 51 56 AM" src="https://user-images.githubusercontent.com/80388/207657661-f83964f5-1c27-4975-92c7-e6bef7eddf9b.png">

No server messages have ever been generated for this room. However there hasn''t been any activity since the recent change that added logging.

On Dec 13, there was a rash of "web socket error: no userData" messages:

<img width="1398" alt="Screen Shot 2022-12-14 at 6 58 09 AM" src="https://user-images.githubusercontent.com/80388/207659110-a4e49d49-99a6-4914-83c0-db74782ca631.png">

This happens in other rooms too so unsure if related:

<img width="1407" alt="Screen Shot 2022-12-14 at 6 58 51 AM" src="https://user-images.githubusercontent.com/80388/207659228-2882ad2a-7f8e-48ba-8299-c5de2ddb7eee.png">

','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Cy-PhkbWZoxT8chWu9d-p','PzmGfX0MN6GFIRq9XcrHC',1671681520000,'This still happens but much much lower frequency, and it doesn''t seem to repeat. It shouldn''t be possible with the current protocol to ever see this, so it''s still a bug.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('fX_TgDLM9c1carFGGFD5b','PzmGfX0MN6GFIRq9XcrHC',1672880975000,'I don''t have any information about OOP but the "401: no userData" happens when their customer''s auth handler returns a falsey userData or a userData with no userID. This could indicate an auth failure or transient auth problem on their end. Note code here is in early christmas branch, not yet integrated into main: https://github.com/rocicorp/reflect-server/blob/af65727174e9030746dcb3ac1bcacfa813e8fca5/src/server/auth-do.ts#L179. (It should be upintegrated as part of https://github.com/rocicorp/reflect-server/issues/206.)','yJ5hiysWE-LBcDfT44lR8');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('Y7TQY5QK_jK6QQFopqAXU','UCN2XOVUVt6_-_TR7gy2v',1670532688000,'<deleted previous messages, I was half-asleep and they didn''t make much sense>','OeVnr1y5bEM_Yg06sUFtD'),
	 ('RaKJML4cHLm8hmnv4i6cn','UCN2XOVUVt6_-_TR7gy2v',1670536422000,'I have updated https://www.notion.so/replicache/Monday-Some-users-don-t-ever-connect-b89e9f770dbf4d518281d5ada8c47c69 with my notes on this. There are some clear next steps. Serializing state to switch to different Monday bug 😅.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('TJ8CBD9HA3cFwTn8v1fyL','UCN2XOVUVt6_-_TR7gy2v',1670555661000,'https://github.com/rocicorp/reflect-server/pull/205 has the logging improvements. ','yJ5hiysWE-LBcDfT44lR8'),
	 ('QbIzIogHnyajCE7_9JaEf','UCN2XOVUVt6_-_TR7gy2v',1670924898000,'Hm, these log messages do not show up in data dog:

<img width="1512" alt="Screen Shot 2022-12-12 at 11 18 23 PM" src="https://user-images.githubusercontent.com/80388/207277327-c2ecfa69-6a6b-4243-8822-b010d54ce0bf.png">

I confirmed by manual inspection that the client includes the logging code:

<img width="800" alt="Screen Shot 2022-12-12 at 11 19 35 PM" src="https://user-images.githubusercontent.com/80388/207277586-376624ad-876a-48e3-8919-2b80fcd45cfb.png">

... and I did manually test that these logs showed up (in the console, not in datadog) when I added the patch.

Confirming the theory that these events just aren''t happening, there are some relevant server-side logs that also aren''t happening (I confirmed correct server version running too):

<img width="1512" alt="Screen Shot 2022-12-12 at 11 22 12 PM" src="https://user-images.githubusercontent.com/80388/207278194-b0e378d4-9535-4a88-819f-9a891d221a68.png">

<img width="1512" alt="Screen Shot 2022-12-12 at 11 23 02 PM" src="https://user-images.githubusercontent.com/80388/207278380-239f41a2-8100-4309-8b4e-0c846e583ecd.png">

However, it certainly seems the reconnect loops are still happening. If we search for "disconnecting" and count by client IP:

<img width="914" alt="Screen Shot 2022-12-12 at 11 26 40 PM" src="https://user-images.githubusercontent.com/80388/207279298-d6809deb-fb6a-4992-9874-3a21d615c30e.png">

`198.203.181.181` is having almost 10x as much trouble as anyone else over the past day.

This client had two different documents open for a total of 6 hours today and reconnected every 2s the entire time:

<img width="1512" alt="Screen Shot 2022-12-12 at 11 31 15 PM" src="https://user-images.githubusercontent.com/80388/207280368-09a7f238-a6f7-4393-8b08-4386f50ffea0.png">

Neither of these rooms has *any* server log entries at all:

<img width="1511" alt="Screen Shot 2022-12-12 at 11 32 56 PM" src="https://user-images.githubusercontent.com/80388/207280658-23a98e82-bce3-480a-813e-235e8d1088ae.png">

<img width="1512" alt="Screen Shot 2022-12-12 at 11 33 57 PM" src="https://user-images.githubusercontent.com/80388/207280846-248a4183-9cf8-4061-97dc-30035e9a6e31.png">

It looks to me like this client genuinely could not connect. Perhaps they were offline this entire time and the datadog client queues up the messages to send later?

But it''s suspicious to me that there is not a *single* message from either of these rooms. The user was online at one time enough to get the code for the app. But they could never connect to the server.

Nothing looks particularly odd about their UA:

<img width="778" alt="Screen Shot 2022-12-12 at 11 36 56 PM" src="https://user-images.githubusercontent.com/80388/207281635-ede0c18e-4a97-4bb5-a6f1-67b40a0bf379.png">

Let''s look at the next most common client sending "disconnecting" messages. The next most client is almost 1/10 the frequency:

<img width="1109" alt="Screen Shot 2022-12-12 at 11 39 38 PM" src="https://user-images.githubusercontent.com/80388/207282652-0aa81231-3fae-4ada-8b96-1d1243e8d2d6.png">

This pattern looks much healthier, the gaps between "connected" and "disconnecting" are much longer:

<img width="1507" alt="Screen Shot 2022-12-12 at 11 42 40 PM" src="https://user-images.githubusercontent.com/80388/207283256-c8b93b7e-6eb2-4628-bbae-d601227ae536.png">

Also in this case there are server logs!

<img width="1512" alt="Screen Shot 2022-12-12 at 11 45 58 PM" src="https://user-images.githubusercontent.com/80388/207284001-c6f9977e-f4e0-4941-a37d-e696daab9e29.png">

The third example is back to the bad pattern though. Reconnect loop:

<img width="1511" alt="Screen Shot 2022-12-12 at 11 46 44 PM" src="https://user-images.githubusercontent.com/80388/207284144-13bb4bfd-bb4b-425d-90f2-058a7fde4d71.png">

No server logs:

<img width="1512" alt="Screen Shot 2022-12-12 at 11 47 15 PM" src="https://user-images.githubusercontent.com/80388/207284269-484f04e3-f7be-4036-954b-9b31a85f775b.png">

The UA again seems like a big org:

<img width="492" alt="Screen Shot 2022-12-12 at 11 47 45 PM" src="https://user-images.githubusercontent.com/80388/207284376-65e8559a-cdb6-4bb3-a23b-dff00efb6205.png">

Could they be blocking sockets?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('XHfLHWJVkrQt3lSmjXvUp','UCN2XOVUVt6_-_TR7gy2v',1671045707000,'Fritz added a bunch of logging to confirm whether these users are ever connecting to our server at all:

https://github.com/rocicorp/reflect-server/pull/213

These new logs *do* show up in datadog:

<img width="1402" alt="Screen Shot 2022-12-14 at 8 49 13 AM" src="https://user-images.githubusercontent.com/80388/207685923-afa6d89d-5d50-42f3-8360-e59bd5f15369.png">

However, the client with the most number of "disconnecting..." messages in the last 4 hours is still not generating any server logs:

<img width="1400" alt="Screen Shot 2022-12-14 at 8 51 46 AM" src="https://user-images.githubusercontent.com/80388/207686534-07ed1091-23f7-4770-9ac2-da205472c718.png">

<img width="1509" alt="Screen Shot 2022-12-14 at 8 53 14 AM" src="https://user-images.githubusercontent.com/80388/207686737-0822d920-2518-43e1-9694-54fe17160a75.png">

<img width="1390" alt="Screen Shot 2022-12-14 at 8 54 21 AM" src="https://user-images.githubusercontent.com/80388/207687223-2f9eedda-2c1a-4639-baa2-40a579a8879d.png">

Confirming, other rooms *do* show server logs:

<img width="1403" alt="Screen Shot 2022-12-14 at 8 56 32 AM" src="https://user-images.githubusercontent.com/80388/207687573-1c7b09ad-fd59-46ef-8594-37a2a036cbb9.png">

So it seems that we still have some clients who never generate a single log message from our server. It''s not perfectly clear to me how common this is because a client in this state will generate "disconnecting" messages continuously. But let''s try a few more.

Second most client in disconnecting messages:

<img width="1401" alt="Screen Shot 2022-12-14 at 8 59 51 AM" src="https://user-images.githubusercontent.com/80388/207688198-427f016e-f72e-4f39-a398-aa613eac7441.png">

Interesting thing here the server *does* log messages for this client. It''s a fairly consistent pattern:

<img width="1403" alt="Screen Shot 2022-12-14 at 9 03 04 AM" src="https://user-images.githubusercontent.com/80388/207689228-132e04f9-4592-4485-bfd2-d6e4451d2f74.png">

Here''s an example of the pattern. These are all log lines for room `9E4L_hoi1n9HQlfXq6kRSNxmaN-8xCsA` associated with the `ts` querystring field `10213662`.

The first entry is actually from the auth DO:

<img width="743" alt="Screen Shot 2022-12-14 at 9 14 29 AM" src="https://user-images.githubusercontent.com/80388/207692103-52714030-1037-415f-82f8-dd668af87827.png">

I think this is just because all the workers are separate concurrent processes, so the order across workers is not realtime. Anyway, next one is the worker:

<img width="734" alt="Screen Shot 2022-12-14 at 9 09 59 AM" src="https://user-images.githubusercontent.com/80388/207691248-de52ca37-4a97-43bb-ae9e-7f7f93205808.png">

The roomdo receives the request:

<img width="742" alt="Screen Shot 2022-12-14 at 9 10 32 AM" src="https://user-images.githubusercontent.com/80388/207691344-eddc922f-44e0-4734-bfa4-eddc3f1bae50.png">

The roomdo finds a prev socket for this client so closes the old one 🤔

<img width="754" alt="Screen Shot 2022-12-14 at 9 11 54 AM" src="https://user-images.githubusercontent.com/80388/207691673-40e7fe80-4a6a-4f25-8fa2-f3f7baee9db3.png">

Room do notices the close:

<img width="745" alt="Screen Shot 2022-12-14 at 9 12 33 AM" src="https://user-images.githubusercontent.com/80388/207691751-3e0799cf-7045-48e5-9f7b-a356543ca735.png">

Then the pattern repeats with the authdo receiving a new request with a different timestamp:

<img width="752" alt="Screen Shot 2022-12-14 at 9 17 17 AM" src="https://user-images.githubusercontent.com/80388/207692615-ac75adad-c28c-44fd-8f02-4ec1fe3a0ed2.png">

The client with the third most number of "disconnecting" messages is seeing the second pattern. Log messages on server, but connection doesn''t last long.

<img width="1401" alt="Screen Shot 2022-12-14 at 9 20 36 AM" src="https://user-images.githubusercontent.com/80388/207693185-de62727a-b5b4-4218-a0a9-24ce4b537fce.png">

Same with fourth:

<img width="1383" alt="Screen Shot 2022-12-14 at 9 21 21 AM" src="https://user-images.githubusercontent.com/80388/207693317-444bcb7a-4450-4626-b8c3-9eb80f5f5bf8.png">','OeVnr1y5bEM_Yg06sUFtD'),
	 ('23QNPsepsk6SMGww4-wUh','UCN2XOVUVt6_-_TR7gy2v',1671046836000,'I feel like stepping back here, I really want to know how common these phenomena as a percent of entire client population. It feels common but I don''t really know. There has to be a way to ask datadog this question, just have to figure out how.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('36m_8Vt_NDuyjwBbjhJaM','UCN2XOVUVt6_-_TR7gy2v',1671053901000,'OK here''s part of the answer:

Out of 496 unique client IPs in last 24h, 473 have "Connected" once. So ~4.6% do not ever emit a "Connected" message.

<img width="1168" alt="Screen Shot 2022-12-14 at 11 36 42 AM" src="https://user-images.githubusercontent.com/80388/207720050-d3d072cc-df38-479d-b29d-6b76c770cdad.png">

<img width="1164" alt="Screen Shot 2022-12-14 at 11 37 14 AM" src="https://user-images.githubusercontent.com/80388/207720194-fbda407e-00d5-4c00-9a9a-8fa04a539459.png">

This 4.6% would encompass both patterns observed above -- no server messages and server messages, but client never connects.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('OCcBeB1PeosLeRweA6YOu','UCN2XOVUVt6_-_TR7gy2v',1671092081000,'I downloaded a bunch of logs from datadog in order to do offline-processing. The code is in https://github.com/rocicorp/monday-log-processing.

I processed logs from 2022-12-14T0500 to 2022-12-14T2100 HST (2022-12-14T1700 to 2022-12-15T0900 Israel).

Here''s the summary:

* 405 distinct client IPs
* 233 client IPs experienced at least one disconnect [0]
* 17 (4.2%) client IPs never connected ("non-connecting IPs")
* 9 distinct *rooms* never connect ("non-connecting rooms") [1]
* 4 non-connecting rooms have zero server logs [2]
* 2 non-connecting rooms are missing their web socket upgrade header [3]
* 3 non-connecting rooms are still a mystery

[0] Remember that disconnects in of themselves are not unexpected or problematic, it only matters when they are not reconnecting or thrashing.

[1] For this dataset the server doesn''t have client IPs and the client doesn''t always have client IDs, so there is no way to tie the client/server logs together other than room. Generally, I think, each user is in their own room. So the fact that there are fewer non-connecting rooms than IPs indicates to me that some users who could not connect moved IPs. So perhaps the real number of non-connecting users is more like 9/405 (~2%).

[2] This indicates that the web socket request never made it to the server at all. Perhaps these users were genuinely offline, or some other networking related situation is happening.

[3] This indicates that some system between the browser and the server (ie a corp proxy) is messing with the connection

Here are the relevant roomIDs in case anyone wants to dig further:

```
ips that never connect (17): [
  ''207.236.13.73'',
  ''93.93.216.236'',
  ''207.236.13.84'',
  ''180.208.59.157'',
  ''161.69.114.29'',
  ''103.143.8.126'',
  ''2a00:23c5:7e1d:d901:897b:9607:adf3:a829'',
  ''2a02:a212:c0:a500:e5d3:3234:c46d:3f37'',
  ''2a02:c7c:6e5a:2000:1879:4c16:79eb:5485'',
  ''198.154.191.158'',
  ''2a02:5080:2d03:7f00:d5b0:d013:ead5:1d0b'',
  ''2a00:23c5:cd9f:b601:2595:ce3e:cdb1:d85a'',
  ''68.129.143.233'',
  ''2607:fea8:be5f:900:38c2:ad27:6400:bc64'',
  ''108.166.141.122'',
  ''65.56.144.146'',
  ''73.73.24.212''
]
rooms that never connect (9): [
  ''96Dme_K9e7DUXVZ4uDbI4kJbFwRttvkz'',
  ''o7jpcRjKr6D9O9ImQ9WDEBAMr8UFuto-'',
  ''ka8iwY3wtfbQ4M6t8KYPWA8bQ4f9xBWd'',
  ''QrhVlciHlrspPIDBI1-ciskTgEAu-dQt'',
  ''NnLfKqhCZ44tlPLbGe-qJuN_5Fg-Oohs'',
  ''fZwWkZH4sPumVgSKhef903mQAB7H4IrC'',
  ''WZ14G6J7gBAk5GnRujDBEb_VwLWLJa4l'',
  ''il-ZtBddMLcM7rLi3Hi1uEnCdGpFcn04'',
  ''7W2qErt6IJhSWTIdHOxJ1Z8qN-xtGgZp''
]
non-connecting rooms without server logs (4): [
  ''QrhVlciHlrspPIDBI1-ciskTgEAu-dQt'',
  ''NnLfKqhCZ44tlPLbGe-qJuN_5Fg-Oohs'',
  ''WZ14G6J7gBAk5GnRujDBEb_VwLWLJa4l'',
  ''7W2qErt6IJhSWTIdHOxJ1Z8qN-xtGgZp''
]
non-connecting rooms with missing upgrade header (2): [
  ''96Dme_K9e7DUXVZ4uDbI4kJbFwRttvkz'',
  ''o7jpcRjKr6D9O9ImQ9WDEBAMr8UFuto-''
]
```

Next steps:

* Re-do this analysis but in terms of clientIDs
* Fix the logging of navigator.online to confirm user is online when these happen
* debug remaining three non-connecting rooms
* Add to analysis grouping by UA/corp proxy
* Research socket connection success rates of other similar products (ie figma, notion)
* ([separate bug](https://github.com/rocicorp/mono/issues/225)) Understand connection uptime','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Auuy_qtJfrJLOnxpIyRLM','UCN2XOVUVt6_-_TR7gy2v',1671224917000,'OK, armed with the new logging (thanks @noamackerman) I re-ran this analysis in terms of unique clientID and cient IP addresses.

These logs were from 20221215T1200 to 20221215T1630 HST.

* unique client IP addresses: 98
* unique client IDs: 225
* IPs addresses that never connect: 4 (~4%)
* client IDs that never connect: 12 (~5%)

Most of the clientIDs are spurious results. They are examples of a pattern where extra Reflect instances are created by the app and immediately destroyed. The same IP address does connect concurrently with a different Reflect instance.

See for example clientID: 13599658

There is only one occurrence of a log line (client or server for this client ID):

<img width="1376" alt="Screen Shot 2022-12-16 at 9 51 11 AM" src="https://user-images.githubusercontent.com/80388/208177963-0c164f0a-b4b4-405e-8559-dabdd7380ac9.png">

The reason is because the very first few log lines from a client unfortunately don''t have their client ID associated. However, if we filter on their IP address in this window, we can get a sense for what''s going on:

<img width="1377" alt="Screen Shot 2022-12-16 at 9 53 53 AM" src="https://user-images.githubusercontent.com/80388/208178405-88732d08-7f54-4348-a953-eb8b51808407.png">

There are two connections in quick succession in the same room from same IP for some reason. The second one gets through and connects normally. The first one never has its request hit the server at all. My guess is a React thing, this looks like the useEffect that instantiates Reflect happening twice, like what would happen in dev. Do ya''ll have a developer in NA using opera 😆? If not maybe under some circumstances Monday instantiates Reflect twice in succession? I feel like I''ve seen this elsewhere in the logs form time to time.

Anyway long story short, this client is not problematic. Most of the clientIDs that don''t connect fit this pattern.

So it''s actually more useful to look at the IP addresses. There are four that don''t connect.

*180.208.59.157*

This is the same one we saw the other day in China. Fascinatingly this same user (by way of room ID) connected later from Hong Kong and succeeded!

So this IP legit can''t use web sockets.

*177.244.53.34*

This user is in Mexico. Their cable provider doesn''t seem weird to me. Their requests make it to the server but seem to be terminated moments later each time.

*131.125.11.1*

This user is in New Jersey using business internet. We only ever see three logs from their room, all client-side. It appears they cannot make socket requests.

<img width="1391" alt="Screen Shot 2022-12-16 at 11 01 28 AM" src="https://user-images.githubusercontent.com/80388/208188305-0a6ddae3-e325-4f17-9dc7-9ab32a8e4ec9.png">

*46.117.249.75* 

This user is in Tel Aviv. They appear to not be able to connect. The connection succeeds from server perspective but never from client. 30m later, the server gets a close event 🤔.

So I think the takeaways are:

* From this dataset, 4 IPs of 98 could not connect.
* We should do this analysis with ~1 week of data to see if this 4% figure holds.
* Roci should check if this 4% figure matches others using sockets.
* Roci should implement an HTTP fallback if these figures hold.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('WdiB-Bv3uMhwJh6YenLD4','UCN2XOVUVt6_-_TR7gy2v',1683763941000,'Closing this as we have our own metrics now.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('Hu5rJvnF1mkOjMVu4D-YA','Oi3tH-OXx2Xk1JttpznoR',1670499358000,'Done','nqYkxAGMnzk7Y5STjZryV'),
	 ('CPDWS7EFJq16kuP8UgrSx','xmEjX4ev_l1sVcqZRLNOR',1676052071000,'External tracking bug: https://github.com/rocicorp/replicache/issues/1029','OeVnr1y5bEM_Yg06sUFtD'),
	 ('j0bJyoig9PG-N6rWueSHV','uUS71yCwTy-mMs2Gs1Kp7',1669918286000,'See https://discord.com/channels/830183651022471199/1047647135774036041 for context.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('kixnH-44vI__SVx6D-JAh','uUS71yCwTy-mMs2Gs1Kp7',1669949112000,'Update @grgbkr says this does work now and the issue that placemark is seeing is that v11-12 should have been a format change (since hash format changed).','OeVnr1y5bEM_Yg06sUFtD'),
	 ('iQqmac6BBtzIYxW_Bc5gz','lsleH54u3UHhe1yORfDs0',1675129206000,'we fixed this','yJ5hiysWE-LBcDfT44lR8'),
	 ('s3gFxl70Zr1L3tQ29ZFP1','ezbhPEJI1y-hMhjqjB4Yr',1669152926000,'@grgbkr WDYT?','nqYkxAGMnzk7Y5STjZryV'),
	 ('U3mzXuybGJDVaOfML2rbd','ezbhPEJI1y-hMhjqjB4Yr',1669949218000,'I think that making hashes less strict doesn''t help us because it won''t let users rollback to v11 unless they first have the newest v11 that relaxes the check.

I think instead we should increase the format version in 12.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('6yC4VlZM1Ot0R14aK-TtM','ezbhPEJI1y-hMhjqjB4Yr',1669973514000,'It does let them roll back to the latest 11','nqYkxAGMnzk7Y5STjZryV'),
	 ('HdGQOy1EyBcyscfONFPK_','ezbhPEJI1y-hMhjqjB4Yr',1669973566000,'See rocicorp/replicache-internal#425 on v11 branch','nqYkxAGMnzk7Y5STjZryV'),
	 ('hkjGHcwcgn9lLlCRGBype','ezbhPEJI1y-hMhjqjB4Yr',1670499378000,'Done','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('TqU0hHDFsSA-LS4_7fQ0f','FhcBxO3pSA8N_22_ekl-f',1671588866000,'I am not certain that such a thing is needed once we have offline support. It''s easy to open two reflect instances and copy data?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('STE3GQutsgbGcqSng60dI','UQcxWkedGQT8ZMwmBMCCv',1709536564000,'I think we''ve done this, please reopen if false @arv .','OeVnr1y5bEM_Yg06sUFtD'),
	 ('HOSj0DN1TgjrH9ByXiuc-','FoZDjD9_skSMC392IvUBH',1669056272000,'By entrypoints I mean:

- any place user JS calls into Replicache
- any place we parse an http response from a user endpoint
- any place we call user code and handle their result','OeVnr1y5bEM_Yg06sUFtD'),
	 ('pzPijEbRRV8KDCNCyqNfd','ZPt81ToAqtVOCfUGdOxje',1668951314000,'Meant issue to be public. Replaced with https://github.com/rocicorp/replicache/issues/1035.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('EfRzZ5W0qFeNBi2iu_Jsq','sOF6posuPS3lQ3-Y3WTYy',1675129452000,'> a strategy for managing auth and room storage schema, i don''t want anything complicated but we have to know how to make changes

Don''t overlook this one. Seems like there needs to be a way to undo pushing a new, incompatible reflect server version that interacts with storage. If rooms are small (<25M) maybe we can get away with just copying data to a new storage path for the new version when it needs to migrate and deleting version n-2 when we push n? If not then backing rooms up to R2 ugh. ','yJ5hiysWE-LBcDfT44lR8'),
	 ('gy4x4Xtw_MNzmBhsmXrLg','sOF6posuPS3lQ3-Y3WTYy',1677704469000,'I don''t think we need this stuff for playable beta. The playable beta is beta, it''s not necessary to be rock solid. I think these are things we probably need to do before GA though so adding a GA label.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Qb_fyyhxQdV10FvNub2Vr','AN6TTWBD46q_E1tCS4LyC',1668424256000,'Why is it likely developers will only test in release?

I like option b. Option b is a superset or a. A is what happens when you run replicsche in release mode without the flag and pass something with undefined.

put a different way, b is a but we try to help users detect/avoid this to the extent we can.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('A2WFPAj26SbpSH6mIocya','AN6TTWBD46q_E1tCS4LyC',1668548736000,'Going with b for now.','nqYkxAGMnzk7Y5STjZryV'),
	 ('SSgNTr8JMLY0AtyUu-QMk','NRrzGEnN_0Ya4PX8QKRqO',1668314163000,'Assigning @arv as master-of-replicache and because he wakes up first Monday :). Also cc @grgbkr since it appears it was your PR that regressed this.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('6TM88ItWt4FdP3htofsZi','NRrzGEnN_0Ya4PX8QKRqO',1668314242000,'To reproduce:

```bash
cd replicache-internal
git checkout main
npm pull
npm pack
cd ../replicache-todo
git checkout main
npm pull
npm install
npm add ../replicache-internal/<tarball-you-just-packed>
npm run dev
```
','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('yI7iOTIZcu9POy82s68l-','R3o_bo6z1J9azyjTK7cHf',1668083240000,'```
  expect(deleteCount).to.equal(2);
```

we are getting 3

It seems like the extra one could be coming from a persist of a refresh.','nqYkxAGMnzk7Y5STjZryV'),
	 ('MuDE7Y4v6HhKle7Ss9r2b','W37d7VfBqh58X2Mdq71qw',1668083119000,'```
  expect(store.write.callCount).to.equal(0);
```

We get 1 here.','nqYkxAGMnzk7Y5STjZryV'),
	 ('qZ-z2vl1XttanRQqWAzXv','yFQwxM7cOFO5508Fmjl1w',1667864246000,'related: https://github.com/rocicorp/reflect-server/pull/20','yJ5hiysWE-LBcDfT44lR8'),
	 ('pagF70L2Zr4bseLr_nUaC','0MrPG9OT6P3cWTVT7hCL9',1667677814000,'@arv can we do this for 12, super annoying for docs.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('QgMmNJ9QmQxRnhZU9kJj9','0MrPG9OT6P3cWTVT7hCL9',1667812163000,'Should be straight forward.

How do we explain `name`?','nqYkxAGMnzk7Y5STjZryV'),
	 ('eVfanADR4BP4XghvSHetN','0MrPG9OT6P3cWTVT7hCL9',1668548036000,'I wanted to get this into v12.

How about only having a `user`? In other words just rename the option and the property?

@aboodman ^^^^','nqYkxAGMnzk7Y5STjZryV'),
	 ('kcMojHgbzXXUQBGl2_l4R','0MrPG9OT6P3cWTVT7hCL9',1668564216000,'I think we need `user` and either `name` or a new `space` option.  Otherwise the common space use case will require passing `${userID}:${spaceID}` as the value for `user` which is awkward.   ','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('HBcNeqAi6scyQVFg0YDXv','0MrPG9OT6P3cWTVT7hCL9',1668656016000,'I think really it should be `user` and `space` and we should embrace being opinionated, and embrace spaces. But I don''t have enough cycles to think through whether I''m missing something there, so `user` and `name` is more conservative.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Kl04Z3rFrNe0mw2fQsRof','0MrPG9OT6P3cWTVT7hCL9',1668671599000,'Moving this to v13 due to the issues with `makeIDBName` not generating unique names and `IndexedDBDatabase` needing changes.','nqYkxAGMnzk7Y5STjZryV'),
	 ('t10tl7V7h54E5R9AEJv8x','Ch_t1N6EnWEtPeiEpOPgR',1667677729000,'Looks like we can maybe do this with a customer reporter:
https://jestjs.io/docs/configuration#reporters-arraymodulename--modulename-options
https://github.com/facebook/jest/blob/main/packages/jest-reporters/src/types.ts
','yJ5hiysWE-LBcDfT44lR8');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('gxXKBnGAPDLtYB3S0eUgB','Ch_t1N6EnWEtPeiEpOPgR',1668120645000,'To run only one test we can use jest''s `.only`','yJ5hiysWE-LBcDfT44lR8'),
	 ('cvpit326PVWl5QFeUqBOA','Ch_t1N6EnWEtPeiEpOPgR',1672740003000,'You can change the log sink to `consoleLogSink` to emit errors. I agree it would be nice to automatically spew in the case of failing tests.

To run a single test from CLI, I use: ` npm run test -- -t ''roomStatusByRoomID''   `','OeVnr1y5bEM_Yg06sUFtD'),
	 ('HVEU7Ne0cjKFqymifOMl7','ciXzk53Xm-XiKsrpdr0yw',1667502721000,'@phritz can you enqueue this after what you''re currently doing, before resuming RAAS.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('6CmyGm7uER9WsjPr7AhCZ','ciXzk53Xm-XiKsrpdr0yw',1667554006000,'> What does this stack correspond to? Can we use the trick that @arv just did in Replicache to demangle the stack and see where this is coming from?

We do not have Noam''s sourcemap :''(','nqYkxAGMnzk7Y5STjZryV'),
	 ('8oekoafHlf6nuW6zv8fNz','ciXzk53Xm-XiKsrpdr0yw',1667679986000,'Can we get the ts definition of his mutator, `changeElements`? 

Is it possible that the type of something the mutator reads from storage changed in the mutator in a way that is incompatible with the type that exists in storage? ','yJ5hiysWE-LBcDfT44lR8'),
	 ('uOzeuVg-pDq939THxMzEN','ciXzk53Xm-XiKsrpdr0yw',1667731251000,'OK @noamackerman says that it''s just storing data with a field that is undefined which causes, such as:

```ts
  value: {
    type: ''textBlock'',
    id: ''2nl5UfoVBhT3lIF8f6dF7'',
    x: 786,
    y: 588,
    fill: ''#000000'',
    fontSize: 36,
    width: 300,
    height: 43,
    cursorPosition: 1,
    attachedConnectors: {},
    textPosition: { x: 0, y: 0 },
    align: ''center'',
    zIndexLastChangeTime: 1667730876888,
    fontProps: 0,
    lastModifiedTimestamp: 1667730879924,
    text: undefined
  }
```

We should validate writes not just reads.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('-ee60owCIkvWRLO_XYBOG','ciXzk53Xm-XiKsrpdr0yw',1672874240000,'Duplicate of rocicorp/mono#164 ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('BX9nK8shPDYoujwST_NqZ','Ohdwwu22-vX_ImbuTrFv3',1667810719000,'Done','nqYkxAGMnzk7Y5STjZryV'),
	 ('EuzWk2eokg8Xksjb6QE3c','YcbGG99oyIW41HwRc09v5',1666297567000,'To make this easier we need a way to get the sourcemaps for releases. We could potentially do this as GH action that uploads a "release" when we add a git tag.','nqYkxAGMnzk7Y5STjZryV'),
	 ('rXB8kSKdn0XtqAsJtzCwp','YcbGG99oyIW41HwRc09v5',1667400345000,'Now we upload the `.map` files when we do a release (tag and git push --tags)

<img width="1310" alt="image" src="https://user-images.githubusercontent.com/45845/199520409-4b289058-df39-40ed-994b-2e9be8107b32.png">

Deleting the temporary tag now.
','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('EwQhED7PaqniLt8WwHTjc','YcbGG99oyIW41HwRc09v5',1667401673000,'You can now download the sourcemap using `gh` (Github CLI)

```
gh release download v11.3.3 -p ''*.map''
```

and deobfuscate the stacktrace using:

```
npx stacktracify replicache.mjs.map --file stacktrace.txt
```','nqYkxAGMnzk7Y5STjZryV'),
	 ('_TgF698loG_zgjzJdkWDq','YcbGG99oyIW41HwRc09v5',1667554870000,'But will this work if the stack trace came from a build that compiled Replicache into some other single js file :-/. Seems like same thing would happen as with that stack from Noam in reflect-server.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('du6oBHout7dA1TkxzkJQT','YcbGG99oyIW41HwRc09v5',1667642449000,'Yeah. That is a serious problem. The only way that can work is if we get their sourcemaps and the compiled their source with our sourcemaps which is close to 0%.

In tmcw''s case he did not bundle replicache into his own bundle so we were able to deobfuscate the stack trace.

In most cases the simplest solution would be to give them the original code :''(','nqYkxAGMnzk7Y5STjZryV'),
	 ('Sw8MfL5iWum8iAuJkB7Qu','YcbGG99oyIW41HwRc09v5',1668292546000,'OK then if the sourcemap is not a real solution then let''s not add complexity for something that doesn''t work most of the time. This just seems like more build goop that''s not a real help to us.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('ETjbE2O-nc0Cp1XoZEN7C','YcbGG99oyIW41HwRc09v5',1668292680000,'Or maybe the complete solution is to tell users to not bundle Replicache or something if they want debugability. Could we experiment with that in our sample apps, say Repliear? I''m not sure how difficult it is. If it is easy, we could have a doc telling people to do that and that would be a good solution to this bug.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('8t1EALAE1Co90KFc1Kx9W','YcbGG99oyIW41HwRc09v5',1668294043000,'I think probably what we should actually do here is make it easy for paying/selected customer to use the non-minified (source) builds. Our license already requires that they keep such code in confidence and I''m not really concerned about it as long as it''s with a controlled population.

Is the right way to do that with npm private packages?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('A5rnrvzaU-EAOIlQh54P7','YcbGG99oyIW41HwRc09v5',1672746969000,'I just published https://www.npmjs.com/package/@rocicorp/replicache

To publish the private package:

```bash
git checkout rocicorp-replicache

git merge main

# Verify that the only diff is the name and the sourcemap
git diff main

git push

npm publish
```','nqYkxAGMnzk7Y5STjZryV'),
	 ('enuwaNNrvqRSEi6ty0zTJ','YcbGG99oyIW41HwRc09v5',1677874911000,'I wonder if it is possible to do this for Reflect for alpha. If not can be beta. Not critical for alpha but would really help our early serious tire kickers (like subset) to have a good experience.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('vnd_-Lax8LrHXkz7SnUKT','f9HpKYDyeknjCeeqiB8Op',1666118708000,'This we have to wait for the defork','nqYkxAGMnzk7Y5STjZryV'),
	 ('yhmjkyhg22mJlKevFfQQq','f9HpKYDyeknjCeeqiB8Op',1667899453000,'I think we can remove these in v12 but I''m not sure. Mutation recovery uses v11 commits but we only replay Local mutation commits and IndexChange commits are ignored.','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('_EQN9eZQfZghwYQEUhGgU','f9HpKYDyeknjCeeqiB8Op',1670499417000,'Cannot get rid of this until we stop recovering old mutations.','nqYkxAGMnzk7Y5STjZryV'),
	 ('Wq_xVtqR7ZarOuHCjZft-','8tyDj9FUJWQ5qd2JEP3KS',1665587852000,'cc @arv @grgbkr ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('TfkIj8PZGQQpLdgesdodT','8tyDj9FUJWQ5qd2JEP3KS',1666297667000,'Step one is to create a perf tests that does ~100 mutations','nqYkxAGMnzk7Y5STjZryV'),
	 ('IcL2gDewKfPRnCVDxYMPu','vuUo7EbodLwWIfEiDk24K',1686065391000,'It''s interesting why this comes up. It''s not really that you actually ever want/need multiple params. I feel like in real code using a single object param will be common.

But when playing around/demoing, it''s faster to type/read:

```ts
async (tx, foo: string, bar: number) {

}
```

than:

```ts
async (tx, {foo: bar}: {foo: string, bar: number}) {

}
```','OeVnr1y5bEM_Yg06sUFtD'),
	 ('iZiYaZW5cmrBwEF8d64uc','tgbUKd8vRsIZIvVOEIyh8',1664958452000,'It doesn''t make much sense to me either ;-)

The type of invokeResult is `true | false | ''throw''`

```ts
    invokeResult?: boolean | ''throw'';
```

Let''s just change this the test to log ''true'', ''false'' and ''throw''. Not sure why I wanted things to be more concise. It is confusing.','nqYkxAGMnzk7Y5STjZryV'),
	 ('jOtwjJgF4qbw8ZCyhTDa4','iTnEytlglorlzOde51wrS',1664839652000,'We have narrowed this down to [a commit in the run up to Replicache 10](https://github.com/rocicorp/replicache-internal/commit/345df2b3594352dcd6cab64b58956711473892ee), which ended up in Reflect 0.4. We can reproduce the jank in Replidraw, but only when the console is open. Unknown why it''s so much more pronounced in Canvas.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('aab4cARH-UxKVkd2UjBvi','iTnEytlglorlzOde51wrS',1664839748000,'Also I don''t think we have a solution for doing what 345df2b3594352dcd6cab64b58956711473892ee was originally trying to do some other way yet.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Ow94fsWs5tKfeOwYjDch1','iTnEytlglorlzOde51wrS',1664845610000,'OK I''ve been working through the history here. Some notes:

- The `persistPullLock` was added at 345df2b3594352dcd6cab64b58956711473892ee.
- This was done because of https://app.slack.com/client/TMQQ9DWPQ/C013XFG80JC/thread/C013XFG80JC-1651685554.718029. Repliear was hitting a check in `maybeEndPull()` that was checking the sync head had not moved since pull began.
- This check dates all the way back to the first impl of pull! In Rust! https://github.com/rocicorp/repc/blob/273101caffa6fc389957c9fa24df828e9afe89a6/src/sync/pull.rs#L221
- The check makes sense in context: Back then, the sync (and main!) heads were shared across tabs. and the way rebase worked, it would gather a list of commits that needed to be rebased (from main head) that weren''t rebased yet on sync head, then return them to the JS to rebase. When returning the lock on IDB was released. This check prevents `maybe_end_try_pull` from continuing if some other pull in a different tab had begun in the meantime and changed the sync head.
- But in the context of SDD, I think the check stopped being a real necessary thing for correctness and became more of an internal assert/sanity check. Because in SDD the sync head was a part of memdag and so by cannot be accessed or modified by some other process.
- Except that then `persist()` was added and actually tripped the sanity check. Because the goal of persist is to transform temporary hashes to permanent hashes, and persist can happen in the gap between `beginPull()` and `maybeEndPull()`. This modifies the hash in the sync head, triggering the error.

So it seems to me we can and should remove both the `persistPullLock` and the sanity check here https://github.com/rocicorp/replicache-internal/blob/main/src/sync/pull.ts#L641. Once we remove both, we should test the case in Repliar that originally motivated this change and see whether it still works: https://rocicorp.slack.com/archives/C013XFG80JC/p1664829629149959?thread_ts=1664815353.385429&cid=C013XFG80JC

Alternately, we could remove temp hashes from SDD. Then we can remove the `persistPullLock` and the sanity check should keep working even when persist and pull interleave.

Separately I think we need to do a group code review of sync. Reading through this I feel like this has gotten a bit grotty through many refactorings and I worry that parts of it no longer make sense. Perhaps this should happen after DD31 lands and the SDD branches are removed.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('JtRUk2FxOeHwADPa5XjA5','iTnEytlglorlzOde51wrS',1664847598000,'There is also the question of why this mutex causes the behavior we see. The behavior we saw was that pokes never get processed during dragging because `await req.json()` on a DOM `Request` object doesn''t return for awhile.

I do not know how this mutex could affect the DOM `Request.json()` method. But if the json method didn''t return for awhile (for other reasons, such as my task prioritization theory) then that would hold the  `persistPullLock` open which would prevent persist from happening. Not sure what the affect of that would be.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Rlsdq2UdTLsEtfC3mNoUU','jhDamM9QgeCgxNsfIKeH0',1666297831000,'Haven''t seen this lately','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('-vuGoJaI6CLQLE29pRP8D','N983dAQkVWxJwlWi8nb_N',1664533632000,'Not clear why this autoclosed?','nqYkxAGMnzk7Y5STjZryV'),
	 ('Q8kCIdmG2c0swdo-h-uPT','N983dAQkVWxJwlWi8nb_N',1665350932000,'Fixed in rocicorp/replicache-internal#296','nqYkxAGMnzk7Y5STjZryV'),
	 ('l3WWSNhbcDCGiZNHJuKJy','Q53sfE7H1E6Je7ZvYJ3x7',1672741721000,'I''m happy with this: https://docs.google.com/spreadsheets/d/1d6xCMg6c9_oKso-124gFkfuKsY1aJXEdRqo_MX8yzk4/edit#gid=2131158829, but it would be good for @phritz to validate it.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('VlDSSSsqRyDkMwnfboxmx','MAAYQvduKVZK6RYRd38Ly',1666298022000,'Is there anything left to do except to change the name?','nqYkxAGMnzk7Y5STjZryV'),
	 ('xmqE2FaWJNq-K0d4qXAeX','MAAYQvduKVZK6RYRd38Ly',1669010849000,'Yes there is. Here are the API changes I''d like to do:

- Add the rest of the features from scan(). The options argument to watch should be the same type as the argument to scan.
- Remove the `initialValuesInFirstDiff`. I can''t imagine a use case where you''d not want the initial values. And if there is one, users can just ignore the first callback. At the very least, we should flip the default of the flag since I think wanting the initial values is overwhelmingly more common.
- Add a convenience to get the list of values not just the diff (identity should be maintained).','OeVnr1y5bEM_Yg06sUFtD'),
	 ('KdBoE0rnxzIvUynXmWbWG','UVfTqd4_ev9vol4smUp9d',1663900581000,'@aboodman any chance we''d get this for free via https://github.com/rocicorp/reflect-server/issues/149?','yJ5hiysWE-LBcDfT44lR8'),
	 ('bTAr85xjfWZPRSgT24QfG','UVfTqd4_ev9vol4smUp9d',1663900883000,'We would but I was in there anyway so I just exposed it for now. Subset was asking for it.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('DCicSoxfW5q68pR55_-QI','UVfTqd4_ev9vol4smUp9d',1663900932000,'Fixed via https://github.com/rocicorp/reflect/commit/12418f91feb37257fa60432dc600660eaca2cba2.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('YC8AGhfll2Ra0C6kM-o22','SZ8UieCHMznEsf_Q6EybI',1663744504000,'> we have in the past been enamored of the idea that we could use mutation timestamps from the sending client to have perfect replay. the mechanism by which the sending client, server, and receiving client clocks are aligned is not clear and seems complicated, my guess is we could start with maybe server receive timestamp (or better, frame number) and see if it works

Agree with all except this one. My bet is it''s simpler and going to look a lot better to use the source timestamps.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('XO3TW4UtxBFsqeP082t_2','SZ8UieCHMznEsf_Q6EybI',1663745033000,'> Agree with all except this one. My bet is it''s simpler and going to look a lot better to use the source timestamps.

Fair enough. 

Small digression: I wonder if it is easier to think about these things if we talked about _frame numbers_ instead of timestamps? Like the sending client, server, and receiving client all keep a sequential counter of what frame they are in, independently. For some reason aligning on that level of granularity makes it easier to reason about for me. ','yJ5hiysWE-LBcDfT44lR8');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('fVdOE89J-NfKLWmfpt6fg','SZ8UieCHMznEsf_Q6EybI',1663745513000,'I started sketching out the algorithm on the receiver. I was wrong, it''s harder than using the server timestamps :). I''m OK trying the server ones to start, but I''m worried it won''t look good without using the source.

I think the source is possible the only little tricky bit is that a badly behaved source client could hold up the show and there has to be some heuristic to prevent that.

>  I wonder if it is easier to think about these things if we talked about frame numbers instead of timestamps? Like the sending client, server, and receiving client all keep a sequential counter of what frame they are in, independently. For some reason aligning on that level of granularity makes it easier to reason about for me.

That''s a cool idea, but what resolution would you use? Some devices have 120 fps now. Is this just a different sort of clock whose ticks are arbitrary 16 2/3ms long? Also how would you even keep the count? Because there''s no inherent notion of a frame counter in browsers. You could just do the math I guess just for purposes of having an easier to reason about number.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('E53XaNhuI9rIyUqX5uuu0','SZ8UieCHMznEsf_Q6EybI',1663796023000,'> That''s a cool idea, but what resolution would you use? Some devices have 120 fps now. Is this just a different sort of clock whose ticks are arbitrary 16 2/3ms long? Also how would you even keep the count? Because there''s no inherent notion of a frame counter in browsers. You could just do the math I guess just for purposes of having an easier to reason about number.

I was thinking just increment a counter every 16.6 ms, doesn''t have to be super precise. Doing math on the timestamp could work too and would probably be better. The important thing for me was that it was easier to think about frame numbers instead of timestamps for some reason -- even if it happens to be a timestamp under the hood.','yJ5hiysWE-LBcDfT44lR8'),
	 ('6oH9Yn9nLmBZFQiPO2Q4q','SZ8UieCHMznEsf_Q6EybI',1663807593000,'> I was thinking just increment a counter every 16.6 ms

I don''t think we have any timers that precise either on the client or server. ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Mz_AJc0HBDNpEn6CHX2zn','SZ8UieCHMznEsf_Q6EybI',1663807728000,'> > I was thinking just increment a counter every 16.6 ms
> I don''t think we have any timers that precise either on the client or server.

Then how could we use precise timing information from the sending client? Or from the server for that matter? Happy to switch to slack if easier to discuss.','yJ5hiysWE-LBcDfT44lR8'),
	 ('RFVIQpMfUO6XFegEgvVfJ','SZ8UieCHMznEsf_Q6EybI',1663808101000,'We have `performance.now()` (https://developer.mozilla.org/en-US/docs/Web/API/Performance/now) on the client which is super precise. On the server we only have `Date.now()` and CF hobbles it, so it''s way less precise.

But both are just a way to ask what time it is, they don''t schedule code to run. If you wanted to increment a counter, you need to run code on a timer. For that we have either `setTimeout()` which can be a little wobbly, or `requestAnimationFrame()` which is more precisely the next frame.

But we wouldn''t want to run either of those constantly to just count, because battery issues. Our users would hate us.

We can grab the time when an event happens and translate it to some other coordinate system, but we can''t run a timer loop just to count frames.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('b_GQg2YbwdHhmXjrIqwCh','SZ8UieCHMznEsf_Q6EybI',1663808307000,'> We can grab the time when an event happens and translate it to some other coordinate system,

This is what I was imagining. 

But also, we _currently_ try to run a setinterval every 16ms on the server. I would expect it to not be precise, but if we did it every 4 frames or whatever then that lack of precision matters less. ','yJ5hiysWE-LBcDfT44lR8'),
	 ('dS5Q2Rz2bO7y8MnC2UyWV','SZ8UieCHMznEsf_Q6EybI',1663808882000,'Right, but we stop when the events stop coming. I think we need to stop when there''s no new input. And especially we can''t loop on client. Sounds like we''re aligned!','OeVnr1y5bEM_Yg06sUFtD'),
	 ('qIL7JPpQK5cFw_uJ74Xr0','SZ8UieCHMznEsf_Q6EybI',1672870059000,'More context:

- from @aboodman: this Jan''23 thread in slack: https://rocicorp.slack.com/archives/C013XFG80JC/p1672866903559879?thread_ts=1672866826.489229&cid=C013XFG80JC
- docs ingar produced: 
  - https://www.notion.so/replicache/Reflect-Batched-Writes-c99c237c0e0e472d9999c5188bd5b34d
  - https://www.notion.so/replicache/Mutation-batching-and-passthrough-client-timestamps-cef2fed007004b029e2fe5e78d14ec1a
- please note: fixing this issue requires a design sketch
- please note: do not overlook the N^2 communication complexity of poke-per-mutation which adds up really really quick. Eg 30 users in a room with 10% sending a mutation per frame implies at the server an incoming mutation rate of 3 mutations per frame which results in 3*30=90 outgoing messages per frame. That''s 90 messages * 60 frames per second = 5400 outgoing messages per second which would consume a huge and undesirable amount of cpu (see above, but rate of <=2000 is more prudent)
','yJ5hiysWE-LBcDfT44lR8'),
	 ('1iYqYHYaJRxlE0E4YkdMk','SZ8UieCHMznEsf_Q6EybI',1672871724000,'I just read through all this and these notes are surprisingly still current and useful!

A few follow-up notes. I think 4kb per poke is way too high of an estimate. The typical poke is a mouse movement update, it''s going to be tiny. Here''s a sample from replidraw:

```
["poke",{"baseCookie":147,"cookie":148,"lastMutationID":260,"patch":[{"op":"put","key":"client-state-5026c476-d373-4376-82d3-9f26875b78ab","value":{"overID":"","selectedID":"teBpwxAVPo-6_j0RmXBqB","userInfo":{"avatar":"🐣","name":"Chick","color":"#f94144"},"cursor":{"x":1185,"y":274}}}],"timestamp":1672870745932}]
```

This is 314 bytes. If we say 512 bytes is more typical then maybe we can send 4kb/512*2000 = 8000 messages per second. In that case our 30 users in a room example above works!

And there''s obviously a lot of room to reduce the size of this message! Just adding snappy compression might get it to 200 bytes!

===

Also - even if we do one poke-per-server-frame, we might not end up sending that much less data. If we are assuming that multiple clients are moving in each frame then a change from two active clients would look like:

```
["poke",{"baseCookie":147,"cookie":148,"lastMutationID":260,"patch":[{"op":"put","key":"client-state-5026c476-d373-4376-82d3-9f26875b78ab","value":{"overID":"","selectedID":"teBpwxAVPo-6_j0RmXBqB","userInfo":{"avatar":"🐣","name":"Chick","color":"#f94144"},"cursor":{"x":1185,"y":274}}}],"timestamp":1672870745932},{"op":"put","key":"client-state-5026c476-d373-4376-82d3-9f26875b78ab","value":{"overID":"","selectedID":"teBpwxAVPo-6_j0RmXBqB","userInfo":{"avatar":"🐣","name":"Chick","color":"#f94144"},"cursor":{"x":1185,"y":274}}}],"timestamp":1672870745932}]
```

~560 bytes.

If the main cost of sending data over the socket is just total bytes sent (if there''s no overhead per-message) then we don''t win much by sending one poke per server-frame.

===

I continue to be open to the idea of starting out doing one-poke-per-server-frame and seeing how that goes. The server timer will flex a bit but hopefully not too badly. Also because of the way we are constrained to replay messages in the order they were processed by the server there is a natural limit to how much client-side timestamps can help.

If a source client glitches badly and delivers a message to the server very late, then other mutations will run before it, and the receive clients will *have* to play those pokes in that order no matter how much buffering they do.

===

I could see either of these approaches working. I think slight bias for poke-per-mutation because (a) no reliance on server clock, (b) don''t see data size being significantly less + (c) assuming there''s no difference between sending more data in less messages or less data in more messages.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('UKrAJAppOEQtFIFR3FfUp','SZ8UieCHMznEsf_Q6EybI',1672872251000,'@aboodman as a reminder the cpu consumed to send the message is primarily a function of _number of messages sent_, not their size. It''s the i/o system calls that are costly, not copying the bytes around. ','yJ5hiysWE-LBcDfT44lR8');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('EkBW8rkBvDc-0-PXH1-fp','SZ8UieCHMznEsf_Q6EybI',1672872582000,'Ah ok, I forgot about that. Then your argument makes a lot of sense.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('PorSgy6NdMu3fCwHfd0mW','SZ8UieCHMznEsf_Q6EybI',1672872848000,'Oh one more thing: have to factor in offline or recovered mutations, how do they play with the new loop.','yJ5hiysWE-LBcDfT44lR8'),
	 ('EKN8BZiXL3wupaUDRUKuo','SZ8UieCHMznEsf_Q6EybI',1672897041000,'I had one last thought here -- if we''re only sending pokes every 4 frame, we can batch together all 4 pokes destined for one client into a single websocket message. I believe this gives us an effective safe rate of 8k messages outbound per second?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('8erNFCkdtg8T1gFhs7yht','SZ8UieCHMznEsf_Q6EybI',1672897415000,'Wait, in that case couldn''t we do the same trick with the poke-per-mutation? If the cost is the syscalls to send messages, and we know we can do 2k 4kb messages per second.

Say we have 10 clients moving continuously at 60fps. So we need to send `60*10*10` ~= 6k pokes per second. But actually we can group all pokes that need to go to a single client together into a single message every 4 frames.

So every four frames, each client will receive 10 pokes for each of the four frames as one socket message. So the DO is really only sending `6000/40` ~= 150 messages / second (!?)

If the average poke really is 500b, this means in this scenarios, these socket messages will be fat!: 500b*40 = 20kb! But I think that''s probably fine, maybe even better than what we''re doing. Even with 30 clients moving continuously the messages are 60kb each. And we can always optimize the messages if that starts to become an issue.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('8DdHNZ2EQHegl97as73oF','SZ8UieCHMznEsf_Q6EybI',1672898563000,'> couldn''t we do the same trick with the poke-per-mutation

i think yes, the implication is that we should do that or something like that. which is why this task feels to me like rewriting the game loop, as opposed to a fundamentally different thing. but whatever about the nomenclature it seems like it should work at the scale we are talking about (even in the extreme example you give which we know is pushing the perf edge like a vercel conf experience). will be interesting to see how the client replay logic pans out, there are a lot of interesting edge cases (delayed source client sends followed by realtime sends, delayed receipt by the receiving client followed by bursts of realtime mutations, etc). Server authoratativeness ensuring causal consistency is a really nice bedrock to build on for this. ','yJ5hiysWE-LBcDfT44lR8'),
	 ('LNzNMjYRC9DtGZtPxlP88','SZ8UieCHMznEsf_Q6EybI',1673293929000,'@aboodman a few questions about things you said in https://rocicorp.slack.com/archives/C013XFG80JC/p1672866903559879?thread_ts=1672866826.489229&cid=C013XFG80JC, you said:
> - Move FF to connect
>   - will have to put lock around it
>   - this will fix that FF bug
> - Rip out the whole process\* heirarchy
>   - put a cache in front of storage (SortedMap)
>   - immediately execute changes against cache
>   - every n ms
>     - if there are changes:
>       - processdisconnect
>       - flush
> - remove `baseCookie` from `ClientRecord`. It shouldn''t be there.
> - overlap turns
> - add source timestamps
> - implement buffering

Questions:
1. Re: `remove baseCookie from ClientRecord. It shouldn''t be there.`, is that because it should be part of connection state (i.e. ClientState)?
2. What do you mean by `overlap turns`?



','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('SbmxDMeYi5uQ1_mufRnrK','SZ8UieCHMznEsf_Q6EybI',1673295234000,'Yeah I can''t remember what the reason was we were storing `baseCookie`, but it seems pretty clear that is connection-specific state. Client says "hello client 42 here, connecting from state X, wassup".','OeVnr1y5bEM_Yg06sUFtD'),
	 ('1PYRI6xHx81wtlppJZEjJ','SZ8UieCHMznEsf_Q6EybI',1673295301000,'By _overlap turns_ what I mean is that there is no reason to just sit there and do nothing while we are waiting for the persist to happen. We can begin processing the next batch while the IO is outstanding.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('E5hbd_voRBPLqeimEku_M','SZ8UieCHMznEsf_Q6EybI',1681147166000,'🤯','OeVnr1y5bEM_Yg06sUFtD'),
	 ('JhsP8k8litQsIFmdtxeN_','BN8qxUwQ58_bGcy1crLbR',1663600490000,'Here is something strange:

```
▶ npm run perf -- --run "populate 1024x1000 \\(clean, indexes"

> replicache@11.2.1 perf
> npm run build-perf && node perf/runner.js


> replicache@11.2.1 build-perf
> node tool/build.mjs --perf

Running 6 benchmarks on Chromium...
populate 1024x1000 (clean, indexes: 0) x 7.33 MB/s ±43.4% (7 runs sampled)
populate 1024x1000 (clean, indexes: 1) x 43.99 MB/s ±14.5% (19 runs sampled)
populate 1024x1000 (clean, indexes: 2) x 32.55 MB/s ±24.2% (14 runs sampled)
populate 1024x1000 (clean, indexes: 3) x 26.39 MB/s ±25.9% (11 runs sampled)
populate 1024x1000 (clean, indexes: 4) x 21.99 MB/s ±9.8% (9 runs sampled)
populate 1024x1000 (clean, indexes: 5) x 19.53 MB/s ±8.7% (8 runs sampled)
Done!
```

As you can see, no indexes is a ~5x slower than one index. Something is fishy! Adding a `noop()` mutator and calling that before the call to `populate` gives a more predictable (expected) result:

```
▶ npm run perf -- --run "populate 1024x1000 \\(clean, indexes"

> replicache@11.2.1 perf
> npm run build-perf && node perf/runner.js


> replicache@11.2.1 build-perf
> node tool/build.mjs --perf

Running 6 benchmarks on Chromium...
populate 1024x1000 (clean, indexes: 0) x 76.89 MB/s ±5.2% (19 runs sampled)
populate 1024x1000 (clean, indexes: 1) x 44.59 MB/s ±6.2% (19 runs sampled)
populate 1024x1000 (clean, indexes: 2) x 32.34 MB/s ±8.0% (15 runs sampled)
populate 1024x1000 (clean, indexes: 3) x 26.04 MB/s ±13.3% (11 runs sampled)
populate 1024x1000 (clean, indexes: 4) x 22.30 MB/s ±27.7% (9 runs sampled)
populate 1024x1000 (clean, indexes: 5) x 18.57 MB/s ±8.6% (8 runs sampled)
Done!
```','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('6opD2AXpoTFZUW2uBligk','BN8qxUwQ58_bGcy1crLbR',1663680176000,'One more data point. Instead of a `noop` mutator, we can add a `sleep(100)` before measuring. This makes it clear that we are waiting for some initialization... Changing things to `await rep.clientID;` means we wait for everything to be ready before we start the perf benchmark. We should have done this in a bunch of places throughout this tests.','nqYkxAGMnzk7Y5STjZryV'),
	 ('rOoMOJWo3SKJCBmikDWkX','BN8qxUwQ58_bGcy1crLbR',1663680225000,'Next up. Does this mean that there was a perf regression or it was just the above bug being manifested? Will look more later...','nqYkxAGMnzk7Y5STjZryV'),
	 ('vFxkbueAQwYMIECN-A6bX','HxotH2CR2e4AsbnkpR-ZD',1663623425000,'I''m not sure how else we could do it reasonably. Number of keys?

There is a very strong (universal?) pattern of using prefixes to the keys to separate different kinds of data. We don''t enforce or recommend a separator for those keys. But it''s highly likely that keys which share a prefix are going to be similar in size.

Can we exploit that? We don''t need to measure every todo, just a few of them to get an idea how large todos are. Like we could sample 1% of todos or even 0.1%.

I suppose we could even sample a subset of keys independent of prefix on the theory that perf will be dominated by the most common prefixes.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('pN2n8VQiG2BgroLpw9JKU','HxotH2CR2e4AsbnkpR-ZD',1663623649000,'We do tell people that key sizes should be 100B to 10KB: https://doc.replicache.dev/performance#typical-workload. So the mid of that is 1KB. If we want chunks to be 64KB, which I think is what we''re aiming for, then we''re talking about approx 64 keys per chunk. However if the user doesn''t follow our advice that could result in very large chunks.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('yYQ-8ch865F5AI1Pyiq5Q','HxotH2CR2e4AsbnkpR-ZD',1663666102000,'The B+Tree could be based on the number of keys instead.

The LazyStore could also be based on number of chunks instead of the estimated size of a a chunk.

There is always back to square one and use binary 🤷 Maybe worth doing a "spike" for that ','nqYkxAGMnzk7Y5STjZryV'),
	 ('qF54uzgS7u0nAD7sV6Tzw','ObHwvKvMqY5uHhQ6JVhVQ',1663592180000,'I looked at this and a few things stands out.

1. The codesandbox has a bug:

```diff
  mutators: {
    putFeatures: async (tx, updates) => {
      for (let i = 0; i < updates.length; i++) {
-        await tx.put(String(i), updates);
+        await tx.put(String(i), updates[i]);
      }
    }
  }
```

Which means that the array of `25413` items gets inserted `25413` times! Fixing that makes the sandbox behave better and we can look at the perf issues.

2. The click handler does not await the mutator so the numbers being printed has no real significance. The log is also including the download time. Fixing these things gives us `Put time: 773` which is much more inline with our performance metrics.

3. `getSizeOfValue` is a bottle neck here. `getSizeOfValue` is really just an approximation and it is used as a heuristic to determine how to partition the B+Tree as well as to determine how much of the data to cache in memory. One possible short term solution is to compute an average for the N first entries of arrays/objects and use the average of that as the basis for the size of the whole array/object.

4. Once `getSizeOfValue` is changed using averages the big remaining item is `hashOf` which is the native hash function provided by `crypto.subtle`. The good thing is that we are moving away from hashes in an upcoming release.','nqYkxAGMnzk7Y5STjZryV'),
	 ('STl03x0VrR1kUiEvqhqSI','ObHwvKvMqY5uHhQ6JVhVQ',1663592224000,'And here is the forked code sandbox: https://codesandbox.io/s/angry-wright-qugo6h','nqYkxAGMnzk7Y5STjZryV'),
	 ('ZGlQWrM7-87qrPVoxKZnn','ObHwvKvMqY5uHhQ6JVhVQ',1663593927000,'Here is a trace with `getSizeOfValue` being replaced by `1`.

[Profile-20220919T152113.json.zip](https://github.com/rocicorp/replicache/files/9599253/Profile-20220919T152113.json.zip)

','nqYkxAGMnzk7Y5STjZryV'),
	 ('Bu4Jl0uk7DRFiC9qPABrL','ObHwvKvMqY5uHhQ6JVhVQ',1663641173000,'> The codesandbox has a bug:

Whoops.

> Fixing these things gives us Put time: 773 which is much more inline with our performance metrics.

I do not see this on your forked codesandbox. I see numbers closer to 1500.

Chrome:

<img width="622" alt="Screen Shot 2022-09-19 at 4 29 31 PM" src="https://user-images.githubusercontent.com/80388/191154014-126eb547-47c0-4046-b386-f73de5c4a278.png">

Edge:

<img width="564" alt="Screen Shot 2022-09-19 at 4 31 11 PM" src="https://user-images.githubusercontent.com/80388/191154154-29b852a3-2fe4-49e4-989e-04accad00bf5.png">

Firefox:

<img width="646" alt="Screen Shot 2022-09-19 at 4 31 25 PM" src="https://user-images.githubusercontent.com/80388/191154185-651965fd-eec7-4918-872b-32bb300f8e12.png">

Safari:

<img width="524" alt="Screen Shot 2022-09-19 at 4 31 59 PM" src="https://user-images.githubusercontent.com/80388/191154272-6e1b205f-99f0-4eee-b45e-ce08b9d9c3aa.png">

Are you sure you weren''t quoting the number after making the changes to hashing and/or getSizeOfValue?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('s-mveq6M0Nh6Rzr7k_a18','ObHwvKvMqY5uHhQ6JVhVQ',1663641200000,'<img width="248" alt="Screen Shot 2022-09-19 at 4 33 16 PM" src="https://user-images.githubusercontent.com/80388/191154407-99d5bd99-a02d-482b-99e3-b3f2ccc5ada1.png">
','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('F8BZ3A_zUYoaeNWrsxKuX','ObHwvKvMqY5uHhQ6JVhVQ',1663643281000,'Looked at the trace, a few interesting things. See video here: https://drive.google.com/file/d/19V6KupZkLPrv-H41-PoOQhCyJX0KLMWO/view?usp=sharing

0. I still see times 2.5-3x slower than expected by perf test. If you are seeing times like 600ms, perhaps it''s because your computer is faster. What times do you see locally for the populate 1024x1000 test? Is it 30ms like the continuous test sees, or something significantly faster?
1. A huge percent of the time in your new trace is in defensive deep clones for the argument to mutate and put.
2. We actually clone the entire dataset *twice* - once to protect mutate, and then again to protect put!
3. We have to find a way to get these defensive copies off for tom. Maybe the right thing is that in release mode we don''t defensive copy ever. We just rely on typescript and documentation. And in dev mode we do the defensive copies. Didn''t we do something like this for read already?
4. Maybe copy isn''t even the right thing -- maybe in dev mode we should do deepFreeze, not deepClone so that user gets an error when they modify something they are not supposed to in read-only.
5. Hash is also a huge cost here. Can we get the uuid change in sooner?
6. Regarding getSizeOfValue() I still see it contributing meaningful to the trace 4-10% depending on what part you''re looking at. How can that be possible if it was returning a constant value?

That''s all. I think overall these are very exciting results as it means there are very easy low hanging fruit to *massively* improve populate. Let''s make some of these changes and make a customer stoked! I think it would be epic for Tom to be able to use 100MB files in his app.

But I think we also need to understand (0) - why is tom''s test case slower than what we''re testing. Perhaps it''s just because the values are more complex.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('6sAfKd3-t7iH57jm1MRJw','ObHwvKvMqY5uHhQ6JVhVQ',1663664844000,'Some comments...

## getSizeOfValue

The getSizeOfValue is strange. Must have uploaded the wrong trace? Here is a new one: [Profile-20220920T095422.json.zip](https://github.com/rocicorp/replicache-internal/files/9605333/Profile-20220920T095422.json.zip) I think the trace I uploaded was with getSizeOfValue using an average for arrays and objects.

## deepClone

deepClone is unfortunate. In this case we do a double deepClone which is even more unfortunate. Good catch.

This is a longstanding issue. You''ve argued that you want to be able to mutate the arguments passed into mutators and you''ve also argued that you want to mutate the return values from get/scan in a mutator. To allow that we have two options:
1. Deep clone
2. Proxy to do copy on write. A while back I tried using Immer to get a sense for the performance implications and it was slower than deep clone. Maybe worth looking at something more specific than immer?

I still think we should freeze things. Keep them frozen in debug and skip the freeze in release. One benefit of using freeze is that we can quickly check if something is already frozen to skip the deep freeze. There is no way to similarly check if an object was previously cloned.

### More about the double clone

One clone is needed to clone the argument when we rebase. This is so that the data we store in the LazyStore is not mutated. This clone could be removed when the mutator is called manually. The other clone is for when putting data into the LazyStore.

## Speed 

TMWC''s test: I''m getting 611 ms for 105MB => ~200MB/s. I made sure that I disabled all the asserts (src/config isProd=true)

For the perf tests I''m seeing  ~100MB/s with the stubbed out getSizeOfValue

The difference is probably the shape of the objects but I''m not certain yet.


## Hash

I think we can change to UUIDs now. It is not a format change since we never validate hashes
','nqYkxAGMnzk7Y5STjZryV'),
	 ('PEEJb3rdZP0uLwta7gySV','ObHwvKvMqY5uHhQ6JVhVQ',1666297964000,'Closing. We did a bunch of things here and we do not know what tmcw is considering to still be slow.','nqYkxAGMnzk7Y5STjZryV'),
	 ('iWBZDB7EQ4Mabcbrm96i1','PB7lAOkUHhL5Q5Xrgi_DY',1663895555000,'Argh, pinning a DO to an explicit `jurisdiction` [is only available if we get object ids via `newUniqueID`](https://developers.cloudflare.com/workers/runtime-apis/durable-objects/#restricting-objects-to-a-jurisdiction). Of course we [derive the id from the room name using `idFromName`](https://github.com/rocicorp/reflect-server/blob/350dc90a01654629671af8e51a18c8b552b7180a/src/server/auth-do.ts#L146) instead. In order to close this issue we would probably need to:
- stop deriving the DO id from the room name and instead use random ids via `newUniqueID`. if we want to keep passing the room name in connect as seems desirable, this means we should keep a map from room id to DO id, probably in the auth DO. workers could cache an entry forever once they have seen it.
- stop creating rooms implicitly and instead add an explicit room creation interface or have some mechanism by which the ''create this in the EU'' bit is passed in the first connect(s)','yJ5hiysWE-LBcDfT44lR8'),
	 ('J0YKlbFdbF-TYwt_1NxB0','PB7lAOkUHhL5Q5Xrgi_DY',1667624196000,'Closing in favor of rocicorp/reflect-server#160 ','yJ5hiysWE-LBcDfT44lR8'),
	 ('gwNy31iqBNUm3cgF_C5CB','xtl0ioLiBlRejkZNEfCz1',1663352576000,'> decide whether a room can be re-created. my sense is ''no'' but it''s not a strong feeling

There is an almost fundamental rule of the universe in Replicache that every single time we reuse an identifier (outside of user data, where the sync engine knows how to deal with it), it causes a problem.

At this point without even working it through, I have a strong visceral gut reaction that reusing the room IDs will definitely break something, somewhere. Perhaps multiple things.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('hTcjEHk31iyn2w3edBWr3','xtl0ioLiBlRejkZNEfCz1',1663352664000,'So many parts of Replicache are based on the intuitions flowing from an immutable append-only DAG. Every single time we make something that doesn''t follow that pattern we end up regretting it.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('aM9UB15O4o9eQg8RSmLvg','xtl0ioLiBlRejkZNEfCz1',1663352854000,'> if ''no'' then we need to decide the mechanism by which a deleted room is prevented from being recreated, as well as say what should happen when a client tries to connect to a deleted room. this last part is related to the question of whether we need to delete room data from all clients.

I guess following the pattern, we should tombstone the room.

I don''t think we have a concept in the protocol of "the server you are trying to talk to has been deleted / doesn''t exist", but maybe we should add one analogous to `ClientNotFound`. Then the client could use this to delete its state too.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Kp-Zys_WV6Zao1W_D6vdE','xtl0ioLiBlRejkZNEfCz1',1663353191000,'> I guess following the pattern, we should tombstone the room.

Maybe we model this as a map from room name to status stored in the authdo, expecting that we''ll find other uses for this kind of info in the future. 

> I don''t think we have a concept in the protocol of "the server you are trying to talk to has been deleted / doesn''t exist", but maybe we should add one analogous to ClientNotFound. Then the client could use this to delete its state too.

Assuming that noam tells us that this is required. If he doesn''t say so, do we do it anyway? Seems prudent, but perhaps beyond what one might expect, and any time we can not do something it means we can do something else more important...','yJ5hiysWE-LBcDfT44lR8'),
	 ('8ie0v6xWsmbTB8paKBuLn','xtl0ioLiBlRejkZNEfCz1',1663353349000,'> Assuming that noam tells us that this is required. If he doesn''t say so, do we do it anyway? Seems prudent, but perhaps beyond what one might expect, and any time we can not do something it means we can do something else more important...

Agreed on spirit of doing min we can get away with. I think we can''t have the clients re-creating rooms accidentally as it would be very confusing, incorrect, and could even violate spirit of this feature request.

Right now `reflect-server` implicitly creates room on connection so I think something minimally has to be done about this path. Like right this second if you delete a room and a client is connected, I believe the clients will reconnect and then other bad things will happen (#152).

If all that happens is clients get disconnected and can''t reconnect / get errors, I think that''s fine for v1 of this feature (modulo noam saying it''s not fine).','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('UA-bMqG5q3cdyzJRqeber','xtl0ioLiBlRejkZNEfCz1',1663353440000,'In most of the Replicache servers we have converged on having an explicit `createSpace()` path - connection doesn''t implicitly create. This was useful for many reasons. Perhaps we need the same here.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('MUvDe5RakSkQng0UshHcF','xtl0ioLiBlRejkZNEfCz1',1663354120000,'> I think we can''t have the clients re-creating rooms accidentally

Yeah agree we 100% need to prevent room re-creation. I was referring to deletion of deleted room data from clients. If we can skip doing that, that seems like less work. 

As scoped, we are saying that when we get the call to delete a room we transactionally:
- log all users out of that room
- tell the room to delete all its data, and ensure it completes
- remove all connection records for the room
- record that the room is deleted

As you say we''ll have to add a check at room creation time to enforce not re-creating rooms. Will leave it up to whoever works on this if we add the explicit creation step, which seems sensible to me.','yJ5hiysWE-LBcDfT44lR8'),
	 ('r4q4LlBwEUbQFUByyXcE4','xtl0ioLiBlRejkZNEfCz1',1663354471000,'> ensure it completes

I think we could also get away with reporting an error if any of this fails and letting customer re-call?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Zg4ggoraw-hI1jfLAyvQH','xtl0ioLiBlRejkZNEfCz1',1663355213000,'> I think we could also get away with reporting an error if any of this fails and letting customer re-call?

Sure, but it''s specifically called out in the docs that `storage. deleteAll` might not complete in a single call for rooms that store a lot of data, and should be called again and it will pick up deleting where it left off the previous time. If deleteAll doesn''t complete we could have the customer could re-call the api but that leaves a window where the room still exists but is in a totally broken state with some but not all its data deleted, and clients can still connect to it. Seems better to just kill the room transactionally by repeating the call to deleteAll if it doesn''t complete, and if that turns out to be a perf problem in reality then we can deal with it. ','yJ5hiysWE-LBcDfT44lR8'),
	 ('D9ctZjHimxfjvTTh8AiRS','xtl0ioLiBlRejkZNEfCz1',1663361215000,'@phritz there is no other metadata I''m aware of that would need to be cleaned up.

Note the auth do currently only stores information about open connections (and periodically gcs them).  With the idea of it keeping tombstones for deleted rooms, I think we should likely have it store records about all rooms.   ','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('wZpDKUsZY63MN21kwwJ3L','xtl0ioLiBlRejkZNEfCz1',1667422192000,'Most recent feedback from noam: https://www.notion.so/replicache/Monday-Priorities-shared-ce186403a079408abcdbb6aa123c48f8#efec9142bcdd406799d9098ba2df3658','yJ5hiysWE-LBcDfT44lR8'),
	 ('9mkvvkCNz92nLbs-7NDFj','xtl0ioLiBlRejkZNEfCz1',1667437173000,'I took a look at what''s required here and the only thing not already covered is that we need to switch how we derive DO object IDs (the things you pass to the namespace to get a stub). We currently use `roomDO.idFromName(roomName)` and we need to move to `roomDO.newUniqueId()` because only `newUniqueId` supports creating DOs that stay in the EU. So we''ll need to keep a mapping from room name to object ID so that we can take a roomName that is passed into `connect` or whatever and look up its object ID, which is required to get a stub. 

In order to support existing rooms that monday created with `idFromName` we have a choice. Option 1 is have monday call an endpoint that creates `room name => object ID` records for existing rooms on a one time basis. Option 2 is to overload how object ID is derived from a roomName, either by lookup (for new rooms) or via `idFromName` (for old rooms). This involves a helper that can distinguish between those two kinds of rooms. I have done things like (2) in the past because it is easier and I''m pretty sure I regretted it each time. So I''m probably going to do (1). 

---
So in summary what needs to happen here is:
- [ ] in the authDO introduce a `roomRecordMap` which maps from `roomName (string) => RoomRecord` where `RoomRecord` holds the `objectID` and a `status` bit (open, closed, deleted).
- [ ] add a `createRoom(roomName: string)` endpoint that adds an entry to `roomRecordMap` and remove implicit creation from `connect`. Ensure `createRoom` errors if a record for the given `roomName` already exists.
- [ ] add `createRoom` call to reflect client
- [ ]  add a migration endpoint that [enumerates existing DO instances via this api](https://api.cloudflare.com/#durable-objects-namespace-list-objects) (namespace id gotten from [this api](https://api.cloudflare.com/#durable-objects-namespace-list-namespaces)) and then creates a `RoomRecord`s for each, mapping room name to `idFromName`-derived object id. This call will need to take the customer''s CF api token and account id as parameters.

(at this point we could release the breaking change; what comes after could come in a second release that enables data deletion)

- [ ] extend creating a room to support rooms that need to stay in the EU `createRoom(roomName: string, jurisdiction: undefined|''eu'')`, pass that bit into DO instantiation, and keep it in the `RoomRecord`.
-  [ ] add an endpoint to "close" a room by logging everyone out of it and changing its `RoomRecord` status to closed. Have `connect` only accept connections to rooms that are open.
-  [ ] add an endpoint to delete a room. The room must first be closed. When we delete a room we delete all its data and then mark the `RoomRecord` status as deleted. 
-  [ ] probably tweak how the version string in the worker path works, [currently only internal apis have versions](https://github.com/rocicorp/reflect-server/blob/1954e4e842b9b398003390b2c05493c03d9a3c15/src/server/dispatch.ts#L28). I''m inclined to add a version string to the externally accessible paths (eg, `/api/v1/create`) and remove the version string from the internal apis because  we never have different versions of room and auth dos trying to talk to each other. We deploy the room and auth do together, even in RaaS. Or i dunno, maybe just keep it @grgbkr ?
- [ ] integrate with replidraw-do and anything else using reflect-server
- [ ] update docs
- [ ] release
- [ ] get users to switch

---
List of things the customer is going to have to do (updating this list as we go):
- (probably) return the "EU-only" bit as part of UserData in the auth call
- make an explicit `createRoom` call from the client instead of relying on `connect` to create room implicitly
- handle new `connect` error case: the room is closed or deleted
- ack that the auth call covers both room creation and connection 
- (TBD how) run the room record migration once
- to delete a user''s data, call `close` on the room, then `delete`
','yJ5hiysWE-LBcDfT44lR8'),
	 ('8NSviTBMAY9AEuz3M0hhc','xtl0ioLiBlRejkZNEfCz1',1667687254000,'Design sketch for the migration step: https://www.notion.so/replicache/authDO-storage-migration-design-sketch-c2a15c2eb26149bc9dbe0c207fdc0851','yJ5hiysWE-LBcDfT44lR8'),
	 ('GcqBsSQxMY7BajF4VN1vj','xtl0ioLiBlRejkZNEfCz1',1672740123000,'I think this is done right @phritz ?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('8gmOm1DBF5UOIhJASbBMK','xtl0ioLiBlRejkZNEfCz1',1672775365000,'Done in the sense of the code is there and it is available in 0.19. I don''t think anyone is using it yet. I''m fine closing this bug and just treating arising issues as follow ups.','yJ5hiysWE-LBcDfT44lR8');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('VCwiPQ_-mYnLmzy3sSSjc','I0FuCh24hdKAroVi8kvMt',1663289152000,'Random test from @phritz on Android/pixel 4a (circa 2020):

https://user-images.githubusercontent.com/80388/190532866-0f8b053e-4c73-40ba-a743-823c5534d381.mp4

So either customer''s phone or their network conditions.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('91hLeQKJz4BrjuurhOVZY','I0FuCh24hdKAroVi8kvMt',1672740230000,'I think the task here is simply to test on a variety of old phones and makes sure performance looks good. Without a repo of what went wrong in original case, not sure what else we can do for beta (I guess there could conceivably be metrics, but that seems overkill).','OeVnr1y5bEM_Yg06sUFtD'),
	 ('XlQfw7eJDuD6n46WFiyRW','0_xC05llckH00ow44OBG_',1663382641000,'OK got some clarification on this from aaron. This issue is about ensuring reflect client sends mutations individually. It already [does not wait for mutations to accumulate before sending them](https://github.com/rocicorp/reflect/blob/b12a12df9ea4ac511649eeff5eaeed1da42133ef/src/client/reflect.ts#L95), but that doesn''t preclude multiple mutations from having landed by the time we actually crawl the memdag to get the pending mutations. For this issue we want to NOT [bundle all the mutations together into one push request](https://github.com/rocicorp/replicache-internal/blob/4cddcfdb1aeac8fac1b92b0d1fa9ecd6683462bc/src/sync/push.ts#L140) but instead `callPusher` on them individually. Unclear if we should keep using the PushRequest to wrap these mutations. On one hand it''s there and works. On the other hand it''s got unnecessary stuff in it.

More generally all the connection loop stuff that''s in replicache might be doing us a disservice in reflect because reflect is just a different situation (eg, replicache protocol is stateless). As part of this issue maybe take a look and see if we should maybe use a totally different push path in reflect than what''s in replicache. Eg can ws reconnect happen automatically in reflect in a way that doesn''t make any sense in replicache?','yJ5hiysWE-LBcDfT44lR8'),
	 ('KwBhTS7NahfcQE8s9FyvG','EE3cR-hbWnphH5z32cg1f',1663900779000,'Might we get this automatically as part of rocicorp/reflect-server#149 ?','yJ5hiysWE-LBcDfT44lR8'),
	 ('cLpATRSeohlXY0v3Bdz-L','EE3cR-hbWnphH5z32cg1f',1663900993000,'I''m not sure. Have to think how it interacts with the socket interface. I don''t think "for free", but for cheap probably.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('iPfo7OB5ZLNJY90ljlMHu','EE3cR-hbWnphH5z32cg1f',1672740312000,'Related to rocicorp/mono#218 ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('NPSgnMnHuwqb9pxDkci4k','EE3cR-hbWnphH5z32cg1f',1677080646000,'We do report this error to the client but we do not deal with it in any way att the moment','nqYkxAGMnzk7Y5STjZryV'),
	 ('Fu3tkB_CY1MSVbc64NQmK','ktbLINZJqyyfdmawfCpEP',1663310988000,'Fixed by https://github.com/rocicorp/reflect-server/commit/593f9ceec8351761c692c55924dce57676e51bf1','OeVnr1y5bEM_Yg06sUFtD'),
	 ('o_oP55AyOnjnh2rWzZZKI','z9Cap5OQiu1dgn48WaR6j',1677784897000,'Actually not sure about this one it might be more subtle than it looks. Let''s talk at the meeting. Relatedly I think maybe we should get rid of `createReflectServerWithoutAuthDO`.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('8U2bvLVD_AyGBJcyN5l1Y','z9Cap5OQiu1dgn48WaR6j',1678220922000,'Delete `createReflectServerWithoutAuthDO` and related paths also as part of this.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('6VuJxit2BqUSJ1kjKPkN8','xSXg2DoQufRwGe__p5SLs',1663777672000,'For clarification, you have to set `allowUnconfirmedWrites = false` to enable the output gate.

https://github.com/rocicorp/reflect-server/blob/350dc90a01654629671af8e51a18c8b552b7180a/src/server/reflect.ts#L24-L32','jpRvILZ1tibPsQXKvb3cF'),
	 ('K36ZgcbBHKg0ct_vSfiSX','xSXg2DoQufRwGe__p5SLs',1663908360000,'closing in favor of https://github.com/rocicorp/mono/issues/318','yJ5hiysWE-LBcDfT44lR8'),
	 ('Cb0sPMrie8LLs8NCB3LO_','My1g7O7guep2Z-OLIvtv5',1672928968000,'I''ve previously mentioned the size (and performance) of zod being a problem.

After looking at this again, I think we should go with `simple-runtypes`. It seems to have a good trade off between performance and compile size. It is not using a chaining API so tree shaking is good as well.

https://moltar.github.io/typescript-runtime-type-benchmarks/','nqYkxAGMnzk7Y5STjZryV'),
	 ('gjgNWklkaKZxi8HKDxZZP','My1g7O7guep2Z-OLIvtv5',1672939253000,'Is there any way we can just not do anything about this right now? It seems far less important than so many things we have to do that solve more acute problems we or our customers are experiencing. ','yJ5hiysWE-LBcDfT44lR8'),
	 ('3si3NwZjypTwn04yNEP6s','My1g7O7guep2Z-OLIvtv5',1672960143000,'*edit* - I remembered the current endpoints are already validated with superstruct.

It came up because of routing.  I wanted to have validation of all routes happen mechanically, and we use zod everywhere else.

It''s really trivial to switch from one of these to the other. They all have about the same API. And there''s not really a cost to doing it, the cost is in implementing router support, not in annotating the call sites with one schema system or the other.

So I could just keep using superstruct as part of routing, that''s easy, but it''s also easy to choose a different one. I think it makes sense to make this choice as part of the routing work. 

For my part, from the benchmark Erik, there are a bunch that are way faster in "assertion" mode than parsing mode. We don''t need parsing mode. The "strict assertion" mode would work fine for us. maybe we could even enable in production which would be amaze! Should we use one of those instead? Maybe since this a server and we dont'' care about the size as much we should choose the fastest one as a starter. Like I said, since they all have equivalent APIs we could change it easy enough later.

~It came up because of routing, the reason I wanted to work on routing was to add validation.~

~I chose zod because we already have it everywhere else and have experience with it. The size concerns don''t matter because it''s on the server, and I believe perf has increased significantly since the benchmark erik points to.~

~I don''t think there is a way to avoid sorting this out soonish, it''s a piece of low-level infrastructure, not a nice-to-have. We need to report errors to users. If we don''t have a system for this we will end up adding one-off error checking to every entrypoint and unit testing those. The work will be greater.~

~OTOH, I don''t think it''s a big deal to just pick one and go with it. They all have basically the same API.~

','OeVnr1y5bEM_Yg06sUFtD'),
	 ('fk_trmJQeCpItvExnpDIn','My1g7O7guep2Z-OLIvtv5',1677703455000,'We decided not to do this.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('mWLMdQHXrR4V-pTMgZR_t','tURezy_ctECa2TpCSYMaZ',1663278318000,'This should not be possible if client and server are correct, but currently server is *not* correct (output gate) so this does happen. And anyway we should complain loudly if either side is being incorrect and stop doing wrong things.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('6QBT5yM1pesBi67Uudc1m','tURezy_ctECa2TpCSYMaZ',1679304620000,'https://github.com/rocicorp/mono/blob/f68a76fa5e4f84dceda0ebcf25ee7d4f59f4bb77/packages/reflect-server/src/server/connect.ts#L94-L108

https://github.com/rocicorp/mono/blob/a1bc9996470aa52517cff9361f72078f9d08a896/packages/reflect-server/src/server/connect.ts#L110-L120

We also have tests for both of these.

The client logs `InvalidConnectionRequest`s as `error`. https://github.com/rocicorp/mono/blob/a1bc9996470aa52517cff9361f72078f9d08a896/packages/reflect/src/client/reflect.test.ts#L1172 ','nqYkxAGMnzk7Y5STjZryV'),
	 ('8TVjJ7oxZzx7e3mrJBvOR','c99Kay_0wR92shCDEyfKV',1663287321000,'If we update the auth interfaces which I think we should (there is no sense in doing this halfway) then we should get customers to move to the new interface.','yJ5hiysWE-LBcDfT44lR8'),
	 ('pIVvQIjPk2FTvq7LWOmHU','c99Kay_0wR92shCDEyfKV',1671588821000,'Now as convinced we should do this now. Deprioritizing.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('iR497SkYDNRaooDiOELD_','4KU-kSQIAdfrmqt09_STI',1677704703000,'We''re in a monorepo now. thanks @cesara !','OeVnr1y5bEM_Yg06sUFtD'),
	 ('mxCe07UIIebaqH2v7CZy0','eltGpf5xMAK4DDZoVRJQU',1663242236000,'I have temporarily pinned replidraw-do to the correct version but this is wonky.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('w03PfcyJIFvOtQnN4If-3','eltGpf5xMAK4DDZoVRJQU',1672740424000,'I think the task here is just to update to latest wrangler.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Lx-T4CwHClJjzA5xbnSxR','eltGpf5xMAK4DDZoVRJQU',1675936495000,'Thanks @arv ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('qhHS9TF98SPN-J2dXu1iY','yXIQjHlkpzyYtd6i2iATz',1663706572000,'It wasn''t working for me -- I had mistakenly had my websocket URI pointing at a production CF worker.

But we''ve figured out what this is, there are a bug in wrangler2 which, once fixed, I think will resolve our issues:

https://github.com/cloudflare/wrangler2/issues/1767','jpRvILZ1tibPsQXKvb3cF'),
	 ('del0-L6uEeVayAOJr-pJz','yXIQjHlkpzyYtd6i2iATz',1663724214000,'I think there are lots of advantages to having dev be as close to prod as possible. However while I see these wrangler/miniflare bugs I don''t think we should necessarily switch away from local because of them. So removing from beta list for now and we will keep an eye on it. ','yJ5hiysWE-LBcDfT44lR8'),
	 ('qBG1ajDyFMOoY-w4KJ0ul','yXIQjHlkpzyYtd6i2iATz',1672740466000,'Well, we did switch away, so closing this.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('I3NKGVm7pMc1M-bwsaiL8','Jm_xBm09E7h9hhqIE-jRl',1663901014000,'When we close rocicorp/mono#243 we hope the new solution will not have this problem, but will wait to close this issue until we can verify that it doesn''t. cc @ingar ','yJ5hiysWE-LBcDfT44lR8'),
	 ('TK89WW_E10iIYgNh84ALr','Jm_xBm09E7h9hhqIE-jRl',1677605173000,'once this bug is fixed we should remove the empty init from the scaffold,create-reflect, and reflect-todo apps','pgyTvcxh2hjmq2l4WKzK6'),
	 ('yWdecxTkN061DMAdsLi_R','Jm_xBm09E7h9hhqIE-jRl',1677698713000,'Adding comment from @grgbkr from duplicate bug #176:

> My current thinking is to keep fastforward in turn processing (its efficient to do it as a batch when a DO restarts, and has a bunch of clients reconnecting). I think new connections should cause a turn to run (just as mutations and disconnects do). This structure can allow onConnect (https://github.com/rocicorp/mono/issues/175 ) to be implemented in basically the same way onDisconnect is today','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('itVIvTVlaWXOAAhojKApG','Jm_xBm09E7h9hhqIE-jRl',1679687455000,'Fixed in https://github.com/rocicorp/mono/commit/6e5c5eb93c4bb78a84de7136dba45ed9f77c42ce','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('JayhWoII4EJ0PqKYNwMX5','mjumIfM9c03NS78CEFwdj',1663125916000,'You should be able to write code to process a push (and pull) request using the exported types from Replicache.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('-eh5HyU1m_hfm9l9_OcyZ','mjumIfM9c03NS78CEFwdj',1663231371000,'Can you expand on the use case? I''m not understanding why we should expose this?','nqYkxAGMnzk7Y5STjZryV'),
	 ('DxQURz7BGD5NVbsQaADlM','mjumIfM9c03NS78CEFwdj',1663235545000,'We expose it now. The reason we expose it is because it is common to write servers in typescript and it''s useful to not have to rewrite/copy the types from the docs.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('IBEhEz-F25PWM2UGz4AHx','BWCxvrupwSoF2wcIXU3bX',1663231665000,'This is intentional: https://github.com/rocicorp/replicache/releases/tag/v10.0.0#:~:text=%F0%9F%8E%81-,Features,-Introduce%20the%20concept

We intentionally keep `process.env.NODE_ENV` in there to allow people to hit some of our asserts in their **debug** builds.

I have this "task"/thought that we should publish a **release** and a **debug** version instead.','nqYkxAGMnzk7Y5STjZryV'),
	 ('tXxCF4IycRi5oYsdTqQlw','BWCxvrupwSoF2wcIXU3bX',1663233994000,'👍 that''d be really helpful for the "try this out quickly" scenario imo. 

Another pattern I see pretty commonly is to have a ./dist folder with production (and maybe debug) builds in them. 

So the file that a build tool would ingest would be `./replicache.js` or ./src/replicache.js (which could be listed as the entry point in package.json). 

Then for pre-built files:
- `./dist/replicache.js` (unminified) 
- `./dist/replicache.min.js` (minified prod build)
- `./dist/replicache.debug.js` (unminified debug build)

^ Could also include `.mjs` esm versions. ','vxBeM-NB6tEvoQuhMhFJ9'),
	 ('7_kxptv9P4G1uluHAHvuk','BWCxvrupwSoF2wcIXU3bX',1663234416000,'We don''t want to release the unminified build so that would be:

```
./out/replicache.debug.js
./out/replicache.release.js
./out/replicache.debug.mjs
./out/replicache.release.mjs
```

With package.json something like:

```json
"exports": {
    ".": {
      "module": "./out/replicache.release.mjs",
      "require": "./out/replicache.release.js",
      "default": "./out/replicache.release.mjs"
    },
    "debug": {
      "module": "./out/replicache.debug.mjs",
      "require": "./out/replicache.debug.js",
      "default": "./out/replicache.debug.mjs"
    },
    "release": {
      "module": "./out/replicache.release.mjs",
      "require": "./out/replicache.release.js",
      "default": "./out/replicache.release.mjs"
    },
},
```

This also means that we would strip the `process.env.NODE_ENV` completely from all build artifacts.','nqYkxAGMnzk7Y5STjZryV'),
	 ('lCIZ7eFAO8O0mbKFFTzin','BWCxvrupwSoF2wcIXU3bX',1663234744000,'🔥🔥👍👍💥💥💯','vxBeM-NB6tEvoQuhMhFJ9'),
	 ('ZAxWtdZCV6qV9Zspzw0FO','w3ZE0-8JOHV1A-eCT_xZI',1663015345000,'WIP PR here: https://github.com/rocicorp/reflect-server/pull/131

The end result is to implement the not-implemented here: https://github.com/rocicorp/reflect-server/blob/main/src/storage/replicache-transaction.ts#L85

See https://github.com/rocicorp/replicache-express/blob/main/src/backend/replicache-transaction.ts#L73 for a working example in a different repo.

Some things to be careful implementing this:
1. The semantics of scan are that keys come out in a specific order (the order of the JavaScript sort() method). I am not sure if the built-in order of durable object''s list() method are the same. (That''s why the PR sorts, defensively)
2. The scan() method needs to scan over the union of pending changes and the stored data. We have a helper function in Replicache makeScanResult() that helps with this.
3. The scan() method is lazy (it''s an async iterator), so it would be nice if we didn''t read the entire keyspace into memory to implement this, but if the answer to (1) is unfavorable there might be no choice.
4. There are parameters to scan (startAt, endAt, limit) etc which could at least reduce the amount of data we read into memory from DO, but this fledgling PR doesn''t use them.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('J5DKbsrL7arYXMblxn2QG','w3ZE0-8JOHV1A-eCT_xZI',1663015372000,'There is also a valid answer here where we do something inefficient to get scan() working then circle back and do it more efficiently later.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('j7jz4-VRFSgPV-3bXw1EX','AMvmsOeKXsUeuIp6gSQfa',1662365047000,'Now that I spell this out it seems simpler to just have the developer create normal DD31 Replicache instance on the worker and have them provide identical definitions.','nqYkxAGMnzk7Y5STjZryV'),
	 ('eb3eDgGydUUZiPvNpp0JQ','X16YYvzWiSnMXb3I58MI8',1677782679000,'It seems like the right thing is to put *another* layer of cache between? Wheee. Send another nested cache into `ReplicacheTransaction` and flush it in the success case.

@cesara I think you can take this one.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('4sz1mvHNOCzcEEDMMyX8J','X16YYvzWiSnMXb3I58MI8',1677782721000,'This nested `EntryCache` abstraction is the gift that keeps on giving.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('2seqU4vR4AcBE5gziTIGt','JTyziL5bz1bnMLoHqwBPc',1661765771000,'Wont fix.

`allowEmpty` was added later so it needs to be kept optional so we might as well keep things optional everywhere.','nqYkxAGMnzk7Y5STjZryV'),
	 ('G3wCcl41_Kz6SuAE_dm2Z','a04zSxdTm6z7kokm3pw_s',1661535663000,'Low prio, but would be nice.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Mov3E8L-AjMtU0IIzSLt4','V8lho3Cqi5_qJaRtvHjPj',1661535624000,'This already exists: undocumented `enableLicensing` field on `ReplicacheOptions` (https://github.com/rocicorp/replicache-internal/blame/main/src/replicache-options.ts#L227)','OeVnr1y5bEM_Yg06sUFtD'),
	 ('2YzhIrbL9er4VKR7wuk-i','w6C-v7kaSKr1ZOvXqkuGs',1659479616000,'Then when people search "svelte offline" we can buy that keyword and send em right to the corresponding sample! same with "solidjs offline", "react native offline", etc.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('t1NGHIixK3J6tK0m-x1n7','w6C-v7kaSKr1ZOvXqkuGs',1659495704000,'> No express, no nothing.

I guess on second thought I don''t feel so strongly about this. As this server is meant to be more a reusable thingy and less a learning tool it''s OK if it has deps that make it a little more "professional". But we shouldn''t use anything too obscure or fancy as people will want to look at this and understand it.

It definitely shouldn''t use Next, just because Next is so focused on the client-side and client/server integration and what we''re going for here is a plain API server.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('HBFu0pMTuF4Eof-wCUE_6','w6C-v7kaSKr1ZOvXqkuGs',1666298180000,'Is this done? @cesara @aboodman ','nqYkxAGMnzk7Y5STjZryV'),
	 ('efsB5huQwcUApZVHWUYtQ','w6C-v7kaSKr1ZOvXqkuGs',1666315958000,'@arv yes, factored out replicache-express and replicache-nextjs','pgyTvcxh2hjmq2l4WKzK6');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('6-ooiOhLZ4bSVyTAAR2Dn','mGsF2VpVlZpLxNbdhI4FL',1658472387000,'The internal values change somehow manages to break Repliear too. Verified both replicache-todo and repliear work again with the disable PR.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('io1_kI_KdYZvyRszGxFNZ','mGsF2VpVlZpLxNbdhI4FL',1658472404000,'Don''t know if same underlying cause though, please confirm when you fix this @arv .','OeVnr1y5bEM_Yg06sUFtD'),
	 ('9xYiE64w9_eD3tRj0zrkO','mGsF2VpVlZpLxNbdhI4FL',1667404336000,'When I first saw this I thought it made sense and that there was a case I missed related to `makeScanResult` but I don''t see it any more.

I will try to repro this with replicache-todo.','nqYkxAGMnzk7Y5STjZryV'),
	 ('_F0WAyMGKC-VYW0AzVnzE','66pxCrNqC19YHf2LdFOXS',1657167825000,'The Internal api for the database of databases is here https://github.com/rocicorp/replicache-internal/blob/main/src/persist/idb-databases-store.ts

I think this is a good addition and straightforward to add.  ','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('Eq6aupCOx9VJ0PZ78w-ak','66pxCrNqC19YHf2LdFOXS',1657306511000,'The reflect client also uses the `replicache-dbs-v0` database.  Just wanted to confirm that we want to delete reflect''s IDBs also?  I guess it''s technically replicache also.

<img width="588" alt="image" src="https://user-images.githubusercontent.com/85998/178053548-1f1fe62b-79a7-4316-92f1-af5a784b4f47.png">
','jpRvILZ1tibPsQXKvb3cF'),
	 ('NY3XAQU9XGJtDc7hivPTW','66pxCrNqC19YHf2LdFOXS',1657311763000,'Hm, it''s kind of an academic question since I don''t expect them to ever be
used together in reality, but sure, let''s delete everything.

On Fri, Jul 8, 2022 at 8:55 AM Ingar Shu ***@***.***> wrote:

> The reflect client also uses the replicache-dbs-v0 database. Just wanted
> to confirm that we want to delete reflect''s IDBs also? I guess it''s
> technically replicache also.
>
> [image: image]
> <https://user-images.githubusercontent.com/85998/178053548-1f1fe62b-79a7-4316-92f1-af5a784b4f47.png>
>
> —
> Reply to this email directly, view it on GitHub
> <https://github.com/rocicorp/mono/issues/89>,
> or unsubscribe
> <https://github.com/notifications/unsubscribe-auth/AAATUBCAJQMWJHQOZ2OGQD3VTB2RVANCNFSM5234KE6Q>
> .
> You are receiving this because you authored the thread.Message ID:
> ***@***.***>
>
','OeVnr1y5bEM_Yg06sUFtD'),
	 ('17HUAzW-YxCF2Eon_Jtqi','zrpBifgcCO7g1DjYQ1v9A',1659295205000,'Decided this doesn''t need to be a task in itself or something we need to prioritize.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('gPG-_Um1_gGlxO8NoLez8','jC2ZSB7TCGAhDeK_CTNuj',1656407396000,'Please take care of this','nqYkxAGMnzk7Y5STjZryV'),
	 ('hl8l1bydlr8nIB-2sKZmS','bbBCilPYtx_FfumPLQRqG',1656407365000,'Please followup on this','nqYkxAGMnzk7Y5STjZryV'),
	 ('x3xwWNFsSvlwOJS4P28gE','KUuDk9aUk-QlE5TL2KFn_',1655692608000,'I guess another way this could work is by (ab)using `schemaVersion` mechanism:

* When we setup the postgres schema we generate a random `instanceID` and store it in the database persistently. This identifies the "instance" of this particular postgres schema.
* The schema version system in the postgres setup works as today and is separate from this.
* As part of `[id].tsx` we read the instanceID from postgres and embed it in the page.
* When we construct Replicache we set the Replicache `schemaVersion` to `${instanceID}:${replicacheSchemaVersion}` (where $replicacheSchemaVersion is currently zero).

This is a bit confusing because there are two "schemas" floating around:

- The postgres schema, which is not (necessarily) visible to the client
- The replicache schema version, the schema of the data stored in Replicache

The two interact but are not the same thing.

','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('ThxCKHkISYB-zxiOKxb-h','KUuDk9aUk-QlE5TL2KFn_',1655712736000,'`ServerNotFound` -> `ServerStateNotFound`','nqYkxAGMnzk7Y5STjZryV'),
	 ('O4IulhFmnvB_r_8OpJ3cC','KUuDk9aUk-QlE5TL2KFn_',1655755208000,'I thought about that, seemed weird for some reason I can''t quite explain. Will try it.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('W5_PUfA-WrQI7YtWorJ5E','KUuDk9aUk-QlE5TL2KFn_',1655755509000,'> I guess another way this could work is by (ab)using schemaVersion mechanism

This would work if the entire database (all spaces) is deleted. But do we want to handle the case where just one space is deleted? It seems like this mechanism should indeed handle it.

In that case you could put the "instanceID" in the space row instead of globally in the database. But now the question is: why not just the spaceID which we already have, and the problem there is that reloading the page wouldn''t get you a new spaceID (nor would you want it to).

Also I kind of don''t like this mechanism because it creates work for the developer. Now every app that cares about being able to delete server-side spaces has to have this mechanism to communicate the schema/instance to the client. It would be nicer to wrap this up into the Replicache protocol as https://github.com/rocicorp/mono/issues/91 proposes, but that is so much more work.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('iAUeQOR_UudOr9Zvq98rz','KUuDk9aUk-QlE5TL2KFn_',1655860681000,'Actually I do not think `ServerStateNotFound` as described https://github.com/rocicorp/mono/issues/91 works. Here''s why:

- Client A loads replicache-todo space S1
- Client A pushes first mutation which implicitly creates S1 (in replicache-todo)
- Client A pulls cookie 1 for space S1
- Now server state is deleted
- Client B loads replicache-todo space S1 (perhaps the URL was shared)
- Client B pushes first mutation which implicitly re-creates S1
- Now Client A pulls from cookie 1 for space S1
- Server returns nop patch, client A has wrong state.

Basically the ServerStateNotFound error tells a client that a particular server is not known, but because we share the server IDs among clients there is a chance the server can get recreated before a particular client pulls again and finds out it is deleted.

It seems like we have to ensure with these sample apps that use spaces that spaceIDs are not reused.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('-Y6Jbsc3Qbnfg4z5xIbWB','KUuDk9aUk-QlE5TL2KFn_',1655861500000,'OMG I think the solution is waaaaay easier than any of this. Problem is fundamentally that (a) by using an in-memory database we are basically deleting all the spaces, and (b) we implicitly create spaces on push if they don''t exist.

(a) and (b) together mean that old clients that are referring to spaces created before a delete will recreate the space on the server, but find themselves in an incompatible state.

Easiest solution: stop doing (b). It doesn''t reflect what real apps would do anyway -- documents aren''t created implicitly by visiting a URL, they are created by tapping a "create document" button. We can create the space programmatically in https://github.com/rocicorp/replicache-todo/blob/main/pages/index.tsx before redirecting to it. Then we take out the code that implicitly creates in push. Then change push and pull to 500 if referring to a space that doesn''t exist.

The effect will be:

- if you delete a replicache-todo database while a client is running, push and pull will both start failing because they refer to a space that no longer exists
- even if another client visits the `/d/<spaceid>` URL because it was shared, the space won''t be recreated. The only way a space gets created is by visiting `/` and that creates a random new space, so spaces will never be reused.

No code changes in replicache at all.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('mqNYFh2hmCBXzf7ip4wZ2','KUuDk9aUk-QlE5TL2KFn_',1655861871000,'Or from Replicache''s pov, the resolution here is as it was before I opened this bug:

- It''s not valid for a server-side database to go backward in time.
- Deleting a database and reusing its "namespace" is the same as going backward. Don''t do that.
- If you must support deleting database, then make sure that it''s not possible for their namespaces to get reused.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('3IMDBXvSr0aAsAHUFjubp','KUuDk9aUk-QlE5TL2KFn_',1655890996000,'Close as "working as intended" then?','nqYkxAGMnzk7Y5STjZryV'),
	 ('DcadzA6cCtVMpF4CImv4s','KUuDk9aUk-QlE5TL2KFn_',1689319347000,'Now that we''re not using spaces so much and recommending other diff strategies for users, this is coming up again. I think we should fix it.

See also: https://github.com/rocicorp/mono/issues/92 and https://github.com/rocicorp/mono/issues/232','OeVnr1y5bEM_Yg06sUFtD'),
	 ('UsrmBKG6K8BNPizbN4h4E','bI-IPe9PigVIUaxQaTVMD',1655388813000,'One unsatisfying solution is to remove the read lock and only have a write lock. In that case the scan will only show what the tree looked like at startup. But at least it will not dead-lock.','nqYkxAGMnzk7Y5STjZryV'),
	 ('fypHgwtw25oOprHsT623o','bI-IPe9PigVIUaxQaTVMD',1655391622000,'One solution is to keep track of the `rootHash` as we `scan`. If the `rootHash` changes we go back to the root and continue the iteration from the new root.

WIP PR coming...','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('7zECj4JV1ct6eU7Fpp_s-','p6KCIi4r8zu6zV7FWa0cV',1654801111000,'Another option is planetscale: https://planetscale.com/. This is intriguing because they say, publicly, "planetscale doesn''t believe in localhost". They have a forking/deploy model for upgrading the db built right into the product. So you''d just start online in dev mode from the beginning.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('vLfJq-8vpu0wUgLpcBDed','p6KCIi4r8zu6zV7FWa0cV',1656007415000,'This is live!','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Ha28OnvxBLXLw4CGvEghR','mVgbZcfCvMzf0YIqg1Y0m',1654063918000,'Replidraw is kinda a pita to run locally right now tho. Directions aren''t correct. Will attempt to fix.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('dIiP-H0N5pwPlbR6Qw-RZ','mVgbZcfCvMzf0YIqg1Y0m',1654074115000,'I believe the setup instructions for Replidraw are fixed now: https://github.com/rocicorp/replidraw/blob/main/README.md','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Dl6drQSjh4ZJrrIfqHJwa','mVgbZcfCvMzf0YIqg1Y0m',1655110902000,'Fixed','nqYkxAGMnzk7Y5STjZryV'),
	 ('DO4ByWgfdu9fgalVwDBzW','DICKK8bsykenS1luaM5bR',1653898968000,'A few comments in no specific order:

- Don''t you think people use `subscribe` without `scan`? I feel like it is useful to watch a single or a set of keys
- The callback to watch seems to imply a single diff operation.
   - Would it make more sense to have it as an iterator/stream then?
   - This makes it hard to know when to start/end batch updates. Maybe it is better to use an array of diff ops?
- `map` makes the diff computation harder. We would now have to diff the values produced by `map` and keep old values around at each `map` "layer".','nqYkxAGMnzk7Y5STjZryV'),
	 ('MMhr4dJPuh-gfGZUM0Lbn','DICKK8bsykenS1luaM5bR',1654360969000,'> Don''t you think people use subscribe without scan? 

I''m not aware of anyone using for anything except getting a single key or getting a contiguous set. Definitely those two use cases are overwhelmingly the most common from my observation.

We don''t *need* to deprecate subscribe but we might want to if watch() can basically cover it as having both adds API complexity and bundle size (presumably?). Also if subscribe() can be implemented in terms of watch that''s a good reason to move it out of the core.

> The callback to watch seems to imply a single diff operation.

Yeah good point. The callback should receive an array of diffOps so you can apply them all atomically to receivers. I think it wants to be a callback rather than a stream API because almost always people are going to hook this up to something like `useEffect()`. I feel like the async iterator would just create boilerplate.

> map makes the diff computation harder. 

Good point. I can''t think of a clear use case for `map()` so let''s leave it out until we have some.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('mS12kHeYO0ewG3ISWPvvQ','DICKK8bsykenS1luaM5bR',1654361877000,'Some of my own observations:

* Having `watch()` as a method of `ScanResult()` doesn''t make sense after all because `ScanResult` is something that is scoped to a single transaction, whereas watch by definition spans transactions. So this seems to mean that watch should be a method of Replicache not of `ReadTransaction`, more similar to how `subscribe()` is today.
* It''s common to want to monitor a single key for changes but this is less elegant in this API (have to `limit: 1`, and get first item from result).

Putting these two together I''m currently thinking something like:

```ts
rep.watchMany({prefix, startAt, limit})
    .filter(entry => ...)
    .sort((e1, e2) => ...)

rep.watch(id)
```

Open questions:

* Where does the callback go? With the API coming off `scan()` it was elegant to put it as the last method - `tx.scan().filter().sort().watch()`. Now that doesn''t make sense.
  * Should the callback go in the `rep.watch()` method as a formal param or field of the options param? That''s awkward to type with the chain after.
  * Or should we skip the chain and put `filter` and `sort` as optional fields on `WatchOptions`. That''s less useful because you can''t do multiple filters but maybe also less footgunny because you can''t do silly things like have multiple sorts.
* It seems like ideally:
  * callback is last thing you type
  * should be possible to have zero or more filters
  * should be possible to have zero or one sorts
  * should be enforced that sort happens after filter
* The base use case is to receive through the callback a stream of arrays of diff ops. This would be used by e.g., solid, react+mobx (, and maybe svelte? need to investigate). But as a convenience for React and VanillaJS I think it would also be good to have `entries()`, `keys()`, `values()` that return an array of keys and/or values. The array should change identity each time there''s a change, but the entries inside should only change identity when they change. This makes it easy to use with React and `memo()`. There''s an argument here that maybe that should be in replicache-react, but it seems like something more generally useful.
* I feel like it would be useful to have `filter()` and `sort()` on `ReadTransaction.scan()` too. Could be a separate task but we should keep in mind we might go that direction.

@arv any ideas on these questions/points API-wise? I''ll keep thinking about it too.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Vi00nWWD87346kpEzS2TT','DICKK8bsykenS1luaM5bR',1654362556000,'Maybe it''s `rep.watch(details).with(callback)`.

Or maybe `rep.watch(details, [callback])` and if you pass the callback the return type is `void` but if you don''t pass it the return type is the chainable interface.

We can achieve the restriction on having multiple sorts and order of filter vs sort by factoring the return interfaces:

```ts
class Watchable {
  // `with` is a reserved word in js, but vscode doesn''t seem to complain about this usage.
  // https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Lexical_grammar#reserved_word_usage
  // not sure if we should use it.
  with((patch: DiffOp[]) => void): void;
  async keys(): Promise<string[]>;
  async values(): Promise<ReadonlyJSONValue[]>;
  async entries(): Promise<Entry[]>;
}

class Sortable extends Watchable {
  sort((e1: Entry, e2: Entry) => number): Watchable;
}

class Filterable extends Sortable {
  filter((e: Entry) => boolean): Filterable;
}
```','OeVnr1y5bEM_Yg06sUFtD'),
	 ('T7AjRJ_uC2lvgPC-UGalH','DICKK8bsykenS1luaM5bR',1654490460000,'OK thinking about this over the weekend here''s a concrete proposal. I realized that the diff ops have to be in terms of positions, not keys, since there''s sorting involved!

Also I haven''t thought this through at an impl level at all, there could very well be issues. This needs a design doc going into code-level design for sure.

# Overview

```ts
type WatchOptions = ScanOptions;

type Entry = {
  key: string;
  value: ReadonlyJSONValue;
};

type WatchChange = WatchInsert | WatchUpdate | WatchDelete;

type WatchInsert = {
  type: "insert";
  position: number;
  key: string;
  value: ReadonlyJSONValue;
};

type WatchUpdate = {
  type: "update";
  position: number;
  value: ReadonlyJSONValue;
};

type WatchDelete = {
  type: "delete";
  position: number;
};

// Call to cancel an existing watch
type CancelWatch = () => void;

class Replicache {
  ...
  watch(options: WatchOptions): FilterableWatchResult;

  // Just a convenience, really has nothing to do with watch(), can be implemented much more easily.
  watchOne(id: string, (entry: Entry|undefined) => void): CancelWatch;
  ...
}

interface WatchResult {
  // Fires every time one or more watched keys changes. Changes must be processed in order for positions
  // to make sense. All changes for a particular mutation are passed atomically to `changes()`. However,
  // multiple mutations may be reflected in same call to `changes()` (i.e., if one frame had many mutations).
  changes((changes: WatchChange[]) => void): CancelWatch;

  // Fires every time changes would, but passes an array of all current entries matching the watch.
  // The identity of the array does *not* change across calls, nor do the identities of unchanged values.
  // However the identity of changed values does change. This is intended to be used with e.g., React.memo().
  entries((entries: Entry[]) => void): CancelWatch;

  // Same as entries, but only returns the keys.
  keys((keys: string[]) => void): CancelWatch;

  // Same as entries, but only returns the values.
  values((values: ReadonlyJSONValue[]) => void): CancelWatch;
};

interface SortableWatchResult extends WatchResult {
  sort((e1: Entry, e2: Entry) => number): WatchResult;
};

interface FilterableWatchResult extends SortableWatchResult {
  filter((e: Entry) => boolean): FilterableWatchResult;
};
```

# First Result

When user first calls `watch()` their callback gets fired with a diff that is all `WatchInsert` representing the current state. If they call `entries()`, `keys()`, `values()`, their callback fires with an array matching current state.

If there are multiple open watches that need there first result (for example during page load) it is possible to collapse their watched key ranges and do only one iteration over the Replicache keyspace. Unclear whether this is a win, needs a test.

# Incremental Results

As the keyspace changes, Replicache checks changes against open watches. If they match, they are passed through the filter / sort chain incrementally, without re-scanning Replicache.
','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('Y2SdZFOaESXRLj5hBhqTW','DICKK8bsykenS1luaM5bR',1654848969000,'A few things:

- I would like to include the key in the entry as well.
- I assume the position is all about updating an in memory array? I don''t know if it is useful? If the filter changes the output array then the positions change. The only time I think the position can be useful is if there is no filter and no sort.

One option when designing the API is to realize that multiple filters can always be folded into one filter. And we only allow a single sort. Given that, maybe "chaining" isn''t the way to go? Instead we could try an option bag:

```ts
watch(options: {
  prefix?: string,
  filter?: (e: Entry) => boolean,
  sort?: (a: Entry, b: Entry) => number,
  indexName?: string, 
  start?: ...
}): WatchResult;
```


','nqYkxAGMnzk7Y5STjZryV'),
	 ('RDhPHrG6OFx0vafx7nSGp','DICKK8bsykenS1luaM5bR',1655753864000,'Sorry I forgot to reply to this.

> I would like to include the key in the entry as well.

I''m confused. The `Entry` type proposed here does include the key.

> I assume the position is all about updating an in memory array? I don''t know if it is useful? If the filter changes the output array then the positions change. The only time I think the position can be useful is if there is no filter and no sort.

Right, the position represented in the callback would be adjusted. What''s happening is that the output of a `watch()` is a list of key/value pairs sorted by some criteria (the `sort()` criteria). So the incremental updates have to be index-based, not key-based. Alternately you can think of it as outputting a set of splices. But since each change event will have arbitrary number of splices (because each transaction can touch arbitrary items) there doesn''t seem to be any advantage to introducing a real splice concept and instead I just went with simpler delete(pos), insertAt(pos), update(pos).

You could actually get away with just delete and insertAt obvs. Maybe we should do that.

Put another way, the output of watch is a patch, but a patch to a list, not a patch to a dictionary.

> One option when designing the API is to realize that multiple filters can always be folded into one filter.

I thought about this, it just feels less ergonomic? If you feel strongly about it I''m OK limiting to one filter to start.

> And we only allow a single sort. Given that, maybe "chaining" isn''t the way to go? Instead we could try an option bag:

Yeah, this also felt non-ergonomic to me. I guess I don''t feel super strongly here but do have an aesthetic preference for the chained API. I''m OK trying the non-chained API on for size, I don''t think there''s any functional difference.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('5GvyXtw9uhBm4ZNxoCTmh','DICKK8bsykenS1luaM5bR',1655754095000,'Certainly the non-chained API is easier to implement and probably lower code weight?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('pfJVoDPvjFTJi5eZGlHqo','DICKK8bsykenS1luaM5bR',1655800179000,'My initial reaction to this was that it was great. At this point I feel like the semantics (and implementation) is a bit unclear and given that I feel less excited about it.

Can we try to nail down the semantics a bit more and maybe things fall into place after that?','nqYkxAGMnzk7Y5STjZryV'),
	 ('is9QHzI74ych6kQ_xL4Xg','DICKK8bsykenS1luaM5bR',1656726042000,'New new new proposal, taking into account @arv''s online and offline feedback:

https://www.notion.so/replicache/RFP-watch-cf3110a59db446a59848ea40f48b799b
','OeVnr1y5bEM_Yg06sUFtD'),
	 ('mszflX6bQbVpIU0sxFH7r','DICKK8bsykenS1luaM5bR',1658175675000,'m0c from lazerfocus has an interesting use case involving a join ([discord message](
https://discord.com/channels/830183651022471199/830183651022471202/998568464241397910)).  
<img width="962" alt="image" src="https://user-images.githubusercontent.com/19158916/179610038-417b4b8f-a661-4df2-90d5-1316df277194.png">

I''m not sure how you would do a join using watch (it is possible with subscribe).  


','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('5f1fbOkgqD01-xhaYhJ_v','0daDDPiMlexz7joLYgRIN',1663282072000,'I think we should nt do this since we plan to re-merge, and will get it for free with that. See: rocicorp/mono#290 ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('es6_5xbRW6FB_MLsIRNDI','gl0rUV1-DcgaowRvLpoEO',1653672027000,'@aboodman do you have thoughts on what the API should be?  ','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('XKgDuAv7oMvltord5ilLn','gl0rUV1-DcgaowRvLpoEO',1653673282000,'The Replicache API seems reasonable?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('BHGxZv3CELPKXlTNnWkGb','gl0rUV1-DcgaowRvLpoEO',1653677953000,'To be more decisive: the current Replicache API is good with me.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('BwAW1CEWsn_7Tryj48KFU','Cl9nAIiqlhr4IA1QdJz1Y',1652995659000,'[image: image.png]

Some random thoughts

0. I think we want the DO (as in the single in-memory running instance).
The DO is critical to our whole design here -- it''s the key bit. I don''t
think SQLite changes that.
1. This embedded compute *sounds* like something we want, but I bet in
practice it is not. Because we are using the persistent storage more for
backup, not for complex calculations that need to run near to the db.
2. If we succeed in moving persistence more off the critical path then
SQLite becomes more viable!

On Thu, May 19, 2022 at 10:19 AM Greg Baker ***@***.***>
wrote:

> Evaluate if this will meet our goals and provide customer''s with better
> visibility / tooling for their data store (i.e. it is currently very hard
> to see what is in your DO storage).
>
> https://blog.cloudflare.com/introducing-d1/
>
> —
> Reply to this email directly, view it on GitHub
> <https://github.com/rocicorp/mono/issues/250>, or unsubscribe
> <https://github.com/notifications/unsubscribe-auth/AAATUBAGIV7A4RYEE3VDSV3VK2O6HANCNFSM5WNLDFIA>
> .
> You are receiving this because you are subscribed to this thread.Message
> ID: ***@***.***>
>
','OeVnr1y5bEM_Yg06sUFtD'),
	 ('DIZOEezYlLorVTDJ2WBnH','Cl9nAIiqlhr4IA1QdJz1Y',1672740639000,'I do not think that D1 meets our needs for a few reasons:

1. It doesn''t exist yet
2. Because we are still persisting quite frequently, we need storage to be nearby. The design of D1 appears to be that storage might be distant from the DO (it''s shared)
3. The way I think we''d want to use D1 ideally (from a dx perspective) is to have a single DB that all DO''s write to, so you can do selects across it, etc. multitenancy basically. But that would serialize writes across all rooms which we don''t want.

Closing this for now.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('B0gbdZOtkNUXeEXOggC7L','KolhFMo131bi8uFmKPXxP',1653323249000,'We can certainly migrate what replidraw-do currently runs on vercel to cloudflare pages (https://developers.cloudflare.com/pages/migrations/migrating-from-vercel/).

The question is how much of the worker and dos (room and auth) of replidraw-do can/should be migrated to pages.

1. Currently the DOs cannot be deployed using Pages, there just isn''t support.
2. The worker can be deployed using Pages, worker deployment support is called "Functions", and is currently in Beta https://developers.cloudflare.com/pages/platform/functions/

Since we have to publish the DOs, and the worker gets published as part of the same command, I don''t see any advantage to moving the worker to Pages.

cc @aboodman ','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('XW4_nxn2beqK7iOdXDbGO','KolhFMo131bi8uFmKPXxP',1653330752000,'This conflicts with something I was told by a CF employee in their discord. Let me find the reference.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('FmISeBSj9I4s7FJYMpdYX','KolhFMo131bi8uFmKPXxP',1653331335000,'Nevermind, it seems consistent: https://discord.com/channels/595317990191398933/779390076219686943/955606582471819294','OeVnr1y5bEM_Yg06sUFtD'),
	 ('WVBfxvvID-rZUSTZ1I6UY','KolhFMo131bi8uFmKPXxP',1653336098000,'Well it would be nice to figure out how to do preview deploys of Replidraw somehow, including the DO, since this will be a common request from our users. I don''t think it''s critical for the next milestone, however.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('iHHzUAMFrSjMs91elgKoc','KolhFMo131bi8uFmKPXxP',1653336190000,'Sorry for chain-of-comments here, but does it makes sense to move the UI to pages just so we can deploy everything using the same tools and the user only has to deal with one service?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('o7sDGoy7q9Lesm_S7WsW2','KolhFMo131bi8uFmKPXxP',1663282010000,'Whoops duplicate of rocicorp/mono#288 ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('8y7rJk6bubclFNl9gltB7','8mw0qcXR-xQt4XVuJsLUB',1663492333000,'External bug report: https://github.com/rocicorp/replicache/issues/1026','OeVnr1y5bEM_Yg06sUFtD'),
	 ('gfLR3m6_rYb-gZ6WzhKFX','iCzgBp712q79Xv8lMu4J7',1690343278000,'Well it shows up in the docs now, but it''s not described.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('LS1e5ZIiOpVfL_dA-nz2E','vs5FPtr5lLosii1jETfQ4',1677704820000,'Super old.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('MAOHWVUl5xaBvSyI0bqAT','ztJ4x5Pq6dt4AiXOHxgg4',1667310994000,'I think we should make this blocking v12 because with DD31 the type of the request json changed and without "fixing" this TS will not capture errors there.','nqYkxAGMnzk7Y5STjZryV'),
	 ('_WsgVv-szYwAv2Pzt5qJB','ztJ4x5Pq6dt4AiXOHxgg4',1667311053000,'label:DD31 because DD31 changes the type of the json request body','nqYkxAGMnzk7Y5STjZryV'),
	 ('oyKR2SRrb4zqwqGSHE-l2','ztJ4x5Pq6dt4AiXOHxgg4',1670612679000,'See: https://github.com/rocicorp/replicache-internal/pull/331#discussion_r1009743025 for more details on how this relates to DD31 and mutation recovery in particular.','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('JgF5YUBTE8Y7mfOty7LLU','8ptyVwdA5xue16kPC0cUl',1651675885000,'I had this idea that we should be able to do a comparison of two JS strings using the same semantics as if we first encoded those strings as UTF8 and then did a byte-wise comparison.

Basically we could achieve this semantics:

```js
function compareUTF8(a, b) {
  const encoder = new TextEncoder();
  const aBytes = encoder.encode(a);
  const bBytes = encoder.encode(b);
  return compareArrays(a, b);
}
```

without having to allocate a buffer for the whole string. We could do a character by character comparison and when we hit a character that is not the same in UTF16 and UTF8 we then encode that character and compare that etc.','nqYkxAGMnzk7Y5STjZryV'),
	 ('WawKD0EZwDKygGefH9K0r','8ptyVwdA5xue16kPC0cUl',1652942889000,'> I had this idea that we should be able to do a comparison of two JS strings using the same semantics as if we first encoded those strings as UTF8 and then did a byte-wise comparison.

This is a neat solution, "neat" as in "clean" or "tidy". To make sure I understand the implications (and cc @aboodman) I think having key sort order be according to code point means that:
- to compare keys on the server, if it has UTF-16 strings it must to use our comparison function and not whatever is native; and, if it does not have UTF-16 strings, it needs to do a bytewise comparison on the UTF8 encoding of the string (or some other equivalent way to sort by code point).
- locale-aware key sort order is not supported by Replicache. The code point sort order is what you get, even though it''s not what is natural in non-english locales. So if you wanted to for example implement an in-order scannable dictionary in german you need to keep a secondary data structure with the appropriate order, or have a strategy to map from the actual key to a key with the right sort order.
- replicache doesn''t do any kind of canonicalization; if this matters for the customer they need to do it before handing us a string.

Yes?

Second question: can you help me understand the advantage of the proposal above over the seemingly equivalent strategy of defining keys as UTF8 strings? Is it transcoding cost? If so it seems like most keys are generated by the server and will be received by the client as UTF8, so it seems like transcoding overhead from UTF16 to UTF8 might just be for strings that are created on the client. So, relatively small?

Thanks!','yJ5hiysWE-LBcDfT44lR8'),
	 ('eWq-AFI5UlaU-SnCvQ0Jy','8ptyVwdA5xue16kPC0cUl',1652944723000,'> Second question: can you help me understand the advantage of the proposal above over the seemingly equivalent strategy of defining keys as UTF8 strings? 

I talked with aaron a bit about this and he pointed out a couple of things:
1. we don''t currently have a way to directly access the keys in the pull response as a UTF8 string or bytes. When we decode the response we get UTF16 strings, so we''d have to translate them back into UTF8 to do this. Presumably that is too costly. Or we could switch to a decoder that gave us direct access to the bytes while decoding, if such a thing exists.
2. it might be less convenient to browse keys in the web inspector. right now you can read them as strings and that is very useful. if they were displayed as bytes or similar that''s a lot less useful.','yJ5hiysWE-LBcDfT44lR8'),
	 ('e0u_lT80aE36A2rB2msdg','8ptyVwdA5xue16kPC0cUl',1652948544000,'@phritz This all sounds right to me.

Another thing to remember is that the keys that gets passed into put, get, has, scan are all JS strings (utf16).

In the past when we used `Uint8Arrays` as keys we saw a lot of time being spent in `TextEncoder` and `TextDecoder`. Logically it should not be expensive to use these but these are not part of V8 and a lot of optimizations are not done.

Another thing to remember is that V8 (and other engines too) internally use ASCII strings whenever possible and this is the common case and these are very efficient.

I would be willing to do an experiment with using `Uint8Arrays` again but I cannot imagine it being faster.

','nqYkxAGMnzk7Y5STjZryV'),
	 ('Pjd-RN4e5nUW_yKzGfvjG','8ptyVwdA5xue16kPC0cUl',1652987399000,'@arv can we make part of closing this issue out adding an item to HOWTO > Launch to Production (or similar spot in docs) that covers key sort order and what they have to do on the server? 

@aboodman when you get a sec can you ack that won''t support locale-aware key collation? Seems OK to me as I think about keys more as identifiers and less as content anyway.

Re:

> I would be willing to do an experiment with using Uint8Arrays again but I cannot imagine it being faster.

I do not think it is worthwhile having byte arrays as keys for efficiency''s sake. I think it would be worthwhile from a *usability/understandability* point of view. If we had byte string keys it would be super clear how to sort the keys on both client and server, there is no opportunity for using the wrong locale or sorting function, and there''s no "missing feature" of having locale-aware key sorting (because nobody expects that of byte arrays).','yJ5hiysWE-LBcDfT44lR8'),
	 ('llZ0I2XPyHwLyfOGF8z_d','8ptyVwdA5xue16kPC0cUl',1652995262000,'> @aboodman when you get a sec can you ack that won''t support locale-aware key collation? Seems OK to me as I think about keys more as identifiers and less as content anyway.

Yes, I agree that is how we should think of the keys.

> If we had byte string keys it would be super clear how to sort the keys on both client and server, there is no opportunity for using the wrong locale or sorting function, and there''s no "missing feature" of having locale-aware key sorting (because nobody expects that of byte arrays).

I agree but it''s hard to implement with the rest of our system because:

1. The pull response is JSON. JSON doesn''t have a byte array type for the keys. It would have to be some kind of encoded string.
2. Our target audience is JS developers. JS doesn''t have good support for byte arrays.

It basically just very un-ergonomic to work with byte arrays in JavaScript. I think overall the simplicity/understandability is better if we say they are strings and specify the sort to be bytewise of utf-8 encoding.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('dtFnhrfOotoEgpZTYR1oA','8ptyVwdA5xue16kPC0cUl',1666298396000,'Done','nqYkxAGMnzk7Y5STjZryV'),
	 ('gdVtx4hgEPZxp3MLyzA02','cmNezw-IwDiR4s7r6Iv0k',1652363436000,'Done by @aboodman in e70bef9a35343b4e285ce5134c12ba7892a4c620','nqYkxAGMnzk7Y5STjZryV'),
	 ('huUbf2JtORK6-2O_P02Uh','bnsnliVDixMU36BmevLrZ',1653529529000,'Moving internal discussion internal. I think this external bug is a good example of why we should have an internal repo and an external one :). We gain little by airing our dirty laundry.

Anyway: With some space, I don''t want to go overboard with the options for this silly little API.

I agree with Tom that it''s typical in database systems to be able to say whether a foreign key permits nulls or not. If it permits nulls, then obviously there should be no message at all and just skip the row. If it does *not* permit nulls, then I agree with everyone who has said that ideally the transaction should not commit in the first place (i.e., the behavior should be `throw`, not `skip`). I don''t know that there is any real use for the behavior `skip-and-log`, which is what we have now.

However, if we make the default `throw` now that would be a breaking change. So what I would like to propose is:

1. Add an `allowNull` flag to `CreateIndexOptions` which defaults to `false` which changes the behavior to silently allow nulls. This is a non-breaking change so can go out right away.
2. As a separate commit, change the default (`allowNull = false`) behavior to:
  - throw on null index values if `allowNull` is false (this is a breaking change)
  - put the better validation on json paths suggested in https://github.com/rocicorp/replicache/issues/913#issuecomment-1136730132 (also a breaking change)

We can do a point release from trunk after 1 is landed. Nothing else on trunk is a breaking change currently.

Separately, I think we should do:

3. Guard the changes from 2 behind a runtime flag. This would be good because it would mean that we could still do dot releases of 10.x after (2) has landed. We have never used this "always shippable" strategy before on Replicache, but it''s common at Google, and we''ve talked about it being a good idea for Replicache in the past. I will file a separate bug for this however.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('V2WowQvG7QPNhVFTa3ga2','bnsnliVDixMU36BmevLrZ',1653531129000,'Discussion for part 3 here, but can be totally separate from this bug: https://www.notion.so/replicache/Runtime-Flags-1f38820f4d4b4ea18905fb62dc9ecb4e','OeVnr1y5bEM_Yg06sUFtD'),
	 ('o5NK6a2FplGMJYD2Ds8yR','bnsnliVDixMU36BmevLrZ',1653635505000,'@aboodman `allowNull` is too hand-wavey. What does it mean to allow null?

- Does it cover a present value of `null`?
- What about missing missing properties?
- Then there is the case of invalid array indexing.
- Invalid path syntax

All of these were silently ignored before.

Now we are adding a flag that covers one of these cases. Which one is not clear?','nqYkxAGMnzk7Y5STjZryV'),
	 ('TWr-PNAICs5YIfFVJVRI5','bnsnliVDixMU36BmevLrZ',1653638355000,'I think the right approach is:

1. Make path syntax errors early errors in `createIndex`

The current behavior is to abort indexing when an error occurs. It does not revert the keys and we end up with an incomplete index map 😢

I don''t know if we can really throw. indexing happens in `createIndex` as well as mutations and pull. If there is an error indexing we must not abort the mutation or pull.

2. Make sure we do not write incomplete index trees when there is an error

3. Decrease verbosity of logging the error **or make it optional** (using `LogLevel`)
    1. If optional my suggested option name is `errorLogLevel`','nqYkxAGMnzk7Y5STjZryV'),
	 ('Fz3YV69znuDzZ0Rrkv24B','bnsnliVDixMU36BmevLrZ',1653644826000,'Sorry ingar :-/.

I think this is going to be hard to solve when none of us are online at the same time. Big picture I was trying to suggest separating out something simple that addressed user complaint from the "right thing".

Stepping back further nobody is even asking us for the current behavior of treating null/undefined fields as an error. The only reason we log when encountering null/undefined is because we only know how to index string, and I felt it was confusing to silently skip other types. But it''s silly to keep trying to work around such a speculative feature. Let''s just remove it.

I''m now in favor of just deleting the log line in the case the value is null/undefined on trunk and forgetting a `allowNull` or similar field entirely. The rest of this can be separate and might take awhile to asynchronously work through. Ingar could move onto other tasks in the meantime.

===

> Make path syntax errors early errors in createIndex

We agree. I was just trying to do this separate from this review since it''s a breaking change.

> The current behavior is to abort indexing when an error occurs. It does not revert the keys and we end up with an incomplete index map 😢

That sounds like an existing problem not introduced by this PR? Can be addressed separately.

> If there is an error indexing we must not abort the mutation or pull.

I can see both sides of this. We do abort mutations and pulls for other reasons btw that are dev-controlled. So it''s not breaking precedent. And you could say that if the user said allowEmpty=false it should be an exception to write such a value! That all said nobody is asking us to do anything if empty values are present so let''s not drive ourselves crazy. We can just remove this error case until we have more information from users.

> Decrease verbosity of logging the error or make it optional (using LogLevel)

I don''t want to add a bunch of API for such a silly feature that nobody is asking for.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('9qIqZwAzs-Ur3D32cJXC0','bnsnliVDixMU36BmevLrZ',1653644888000,'Basically if we can please do something simple and non-breaking to address user complaint of log spew let''s do that and treat the rest of this separately and potentially lower priority.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('0Wl5u9TFGIQ2BO73Ic-dZ','bnsnliVDixMU36BmevLrZ',1653654350000,'Right now we log using `info` which is the default. If we switch go `debug` then the logging will be off by default.','nqYkxAGMnzk7Y5STjZryV'),
	 ('ELlY-Ouxo7xm23oOvAJAh','bnsnliVDixMU36BmevLrZ',1653678533000,'I''ve gone back and forth about this and I see what you mean, but I think the current solution has some things to like:

1. The first time I (and many) people use `createIndex` they get something about the syntax wrong. If the system doesn''t complain loudly, it''s hard to know whether it''s working, what the problem is, etc. Silent failure for the default is a bad dx. If we change the log level to `debug` people won''t see this output because people don''t typically leave `debug` on.

2. But once people know the system and are seeing this message and don''t want it, they can turn it off manually.

I agree the API around indexes in general is wonky and needs rethink, as well as some near-term better error handling, but I think what was just landed is a good first step. Are you good to ship it? (Think of this as API review).','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('OEY2xGHhQTVSz0hSPT4_k','mtxFCKynw84ZemGw1t4rl',1651268263000,'See https://github.com/rocicorp/licensing/blob/main/api-versioning.md for how to add things to the active ping request.','yJ5hiysWE-LBcDfT44lR8'),
	 ('mXeLH2Jg30ntco5ZTBGCP','mtxFCKynw84ZemGw1t4rl',1651567645000,'`version` was added in cbb2e6ef85dfdfc53686f1783b5d17da0753793d','nqYkxAGMnzk7Y5STjZryV'),
	 ('8xFaz2reDSD6_7Q3_YWV9','qdDqYR6kNrdt3fmGyAuWS',1651388010000,'The new website says five and five is what we’ve said elsewhere. Any reason to not do five?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('OlnXS3BGnsPO8F3MswGVl','qdDqYR6kNrdt3fmGyAuWS',1651388358000,'I dunno bro, the bug says 10. Who can we trust?','yJ5hiysWE-LBcDfT44lR8'),
	 ('UxB2ZB1NUKiikOOnMDzW8','jZMXbzDw01_CmXMZ3vM0z',1650984708000,'I guess we should change the script to output two files (no need to run the perf tests (twice)','nqYkxAGMnzk7Y5STjZryV'),
	 ('0I2_xDxbggh50hk8i1VTU','jZMXbzDw01_CmXMZ3vM0z',1655480167000,'Fixed with 8f06229a3d4ebc90051d60f99732aa41cdeb1f6e and 91da6166062f6d4e00ee71cb76303517d7a37018','nqYkxAGMnzk7Y5STjZryV'),
	 ('fhiltnVN1VirxZxGH6xwf','slf7fb_6GOoSn-oma8F0y',1652362900000,'Seems fine according to https://www.skypack.dev/view/replicache

<img width="322" alt="Screen Shot 2022-05-12 at 15 41 25" src="https://user-images.githubusercontent.com/45845/168088757-6895a836-e5f3-4bc2-815e-5a4c8c04c76f.png">


','nqYkxAGMnzk7Y5STjZryV'),
	 ('3mkRdAaRqbg_Fj9eiIyr0','slf7fb_6GOoSn-oma8F0y',1652362968000,'rocicorp/mono#102 for keywords','nqYkxAGMnzk7Y5STjZryV'),
	 ('ivnzfE5qiX1wqTsOEL2JD','VJrT_lsZaglNI0YzlL8Qa',1650913873000,'I see a data point for https://github.com/rocicorp/replicache-internal/commit/184321fef9c4db86aa94e45fdac68e827f4da983 and that has a failure due to the perf regression

<img width="504" alt="Screen Shot 2022-04-25 at 21 10 03" src="https://user-images.githubusercontent.com/45845/165157525-a8daa2d6-8f22-469b-860e-4c1f575b2b98.png">
<img width="791" alt="Screen Shot 2022-04-25 at 21 10 38" src="https://user-images.githubusercontent.com/45845/165157613-d452f0fb-bc77-4f9b-bb18-fe520ea243e6.png">
 ','nqYkxAGMnzk7Y5STjZryV'),
	 ('AnOGzAZO8DhLv3hDgOVM9','VJrT_lsZaglNI0YzlL8Qa',1650923582000,'We seem to be getting data points now but there is a discontinuity between `558d93c` and `1c6460f`. Guess we are just prepared to say 🤷 to what was going on? That''s fine with me I guess. @arv if you concur feel free to close this one.','yJ5hiysWE-LBcDfT44lR8');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('cxO0ssjjfsqnv0hbzT_Mr','VJrT_lsZaglNI0YzlL8Qa',1650981322000,'I think it is working now... Keeping my eyes on it a little bit longer','nqYkxAGMnzk7Y5STjZryV'),
	 ('H3FRTH1UrSNBscwwCtDKx','VJrT_lsZaglNI0YzlL8Qa',1651133428000,'Closing. Works now','nqYkxAGMnzk7Y5STjZryV'),
	 ('yH1aPJozQqUyiK6F2fkSP','L3U1O8LZ9aHznzFxgKe5u',1648666850000,'Starting on this.','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('eoBjLr-7zWK5XprEAs4KG','L3U1O8LZ9aHznzFxgKe5u',1648668810000,'This is generally speaking to enable *customers* to send their users'' logs to the *customer''s* datadog, correct? We of course can default our sample apps to sending to our datadog, and let existing customers send to our datadog until we shake the bugs out. But longer term the idea is not that all customers'' client logs come to us, correct?','yJ5hiysWE-LBcDfT44lR8'),
	 ('b-omfA5xfVy_vD1y4XheY','L3U1O8LZ9aHznzFxgKe5u',1648670491000,'It’s only to enable customers to send logs to their own DataDog. Nobody’s sending logs to us except us.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Bb7FvG0ZVh9wqkdMSkaDt','L3U1O8LZ9aHznzFxgKe5u',1648671851000,'See https://github.com/rocicorp/replicache/pull/907','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('fzT1Jntom4Iam_Q0qyZzR','L3U1O8LZ9aHznzFxgKe5u',1649098969000,'It looks like replidraw-do needs to be updated to take advantage of this still? (But I assume you will do that after npm/api cleanup).','OeVnr1y5bEM_Yg06sUFtD'),
	 ('O3mWAiJy6M9F9JQGfmPO2','9Xg_d8fLu0FJk_3_WOkBb',1673279505000,'If I remember correctly, the reason for this to be a function is that it needed access to the DO env and that is not available at startup creation of the server... Actually, it was the logger/logSink that needed the Env.

https://github.com/rocicorp/reflect-server/blob/0b1e163f5204e3621874623c21a365526df31d15/src/server/reflect.ts#L17-L18

For consistency the `logLevel` should also be a function. It is very reasonable to have the logLevel be a function of the Env.

CC @grgbkr ','nqYkxAGMnzk7Y5STjZryV'),
	 ('WhFI3PNtMPNhcvDOnBlcQ','9Xg_d8fLu0FJk_3_WOkBb',1673292761000,'OK that actually does make sense. Apologies for the noise.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('RVOAmUPGBA_JJ-UdpNtex','G3Y8HOtttXVHq9lDqCO9R',1673615807000,'I really do not know what to do here. Are you talking about `reflect`, `reflect-server`, `@rociciro/logger` and/or `replicache-do`? 

Remember that reflect/reflect-server cannot use `DataDogLogSink` by default. It needs a datadog client token. Also, it seems plausible that our customers wants to use Sentry or some other logging service.

For reflect-server I think it is fine to always log things to the console, but for the client that does not seem like a good idea to do by default.','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('uoEqgRzESjfc-GFgd-b2g','G3Y8HOtttXVHq9lDqCO9R',1673630524000,'I think @aboodman is talking about @rocicorp/logger, which might have implications that trickle out to its consumers. I think the suggestion is that if you are using logger then consoleLogger is enabled by default. And then if they want to pass an _additional_ logger they can and it gets tee''d. That''s the suggestion as I read it. I think you might provide an answer to aaron''s question "I''m struggling to imagine a case where one would not want console logging enabled", which is "for the client that does not seem like a good idea to do by default."

So I think we should wait to hear from aaron.','yJ5hiysWE-LBcDfT44lR8'),
	 ('tcLbc3l6ByX7Cn4TXR4xf','G3Y8HOtttXVHq9lDqCO9R',1673634702000,'This bug was fixed since it was filed. I wanted to not have to setup the
tee logger manually (as replidraw does) and instead pass in an array of log
sinks. This has been done.

There is a separate much smaller question of whether to assume the user
always wants console logging. I can see Erik’s pov that it’s nice to be
able to disable it (ie for tests).

So this bug can be closed.

On Fri, Jan 13, 2023 at 7:22 AM Phritz ***@***.***> wrote:

> I think @aboodman <https://github.com/aboodman> is talking about
> @rocicorp/logger, which might have implications that trickle out to its
> consumers. I think the suggestion is that if you are using logger then
> consoleLogger is enabled by default. And then if they want to pass an
> *additional* logger they can and it gets tee''d. That''s the suggestion as
> I read it. I think you might provide an answer to aaron''s question "I''m
> struggling to imagine a case where one would not want console logging
> enabled", which is "for the client that does not seem like a good idea to
> do by default."
>
> So I think we should wait to hear from aaron.
>
> —
> Reply to this email directly, view it on GitHub
> <https://github.com/rocicorp/mono/issues/259>,
> or unsubscribe
> <https://github.com/notifications/unsubscribe-auth/AAATUBDKXRELG2NMZ73DNSLWSGFMRANCNFSM5RDTJ52A>
> .
> You are receiving this because you were mentioned.Message ID:
> ***@***.***>
>
-- 
a (phone)
','OeVnr1y5bEM_Yg06sUFtD'),
	 ('XpTsWun8xranCzT0T4vqL','NnQSsduxnjkJa-3xMjLqA',1647373649000,'I can do this one.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('nW-sSySVh5hl-1shGR-iV','hVF5Qjgb4NtorcRApYFf3',1673262939000,'I think this issue is stale.

What does this mean? When the server starts there is no `roomID`. The roomID is created by the  REST endpoint `/createRoom`. The `roomID` is then later used as part of the URL of the web socket. On the client we also include the `roomID` in the LogContext.','nqYkxAGMnzk7Y5STjZryV'),
	 ('e6RRPqD-1E_fRK56djQVB','hVF5Qjgb4NtorcRApYFf3',1673288408000,'I think this means printing the room ID in a log line when the room DOl starts. I''m on mobile so not sure if stale. I suspect the reason we wanted this is so that we can see when room dos are restarting','yJ5hiysWE-LBcDfT44lR8'),
	 ('DhGQKpoQvxMFHn0ricmG_','hVF5Qjgb4NtorcRApYFf3',1673290471000,'I was using loose language when I created the issue. I meant printing the roomID early on when the DO starts.

I think this means moving this branch: https://github.com/rocicorp/reflect-server/blob/main/src/server/room-do.ts#L104 into the one above and then printing out a "initializing room" or something from the lc. I think it should be at info level (counts as "significant state change" to use Fritz''s language.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Ub9AxfRuf2lf2-O18PAno','yEFLQ6xLl-mn_IT5cSg3b',1648671942000,'As a first step adding a optional LogSink ReplicacheOption https://github.com/rocicorp/replicache/pull/907','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('ahH4o2hz0Q6x8tDCa8KPX','yEFLQ6xLl-mn_IT5cSg3b',1649695897000,'Available in @rocicorp/reflect@0.4.0','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('2b-2-Lq6t8Z8KfDuqls2Q','C5xZ7-mdn6HvNY0DdaUpb',1647373610000,'I created a bug for client-side logs (https://github.com/rocicorp/reflect-client/issues/12) and added it to the internal monday priorities list: https://www.notion.so/replicache/Monday-com-Priorities-internal-3fd7351956cd4e1baf1e5617f0ee8498.

I also created bugs for the others, but they aren''t as high priority and don''t need to block other Reflect work.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('DswtkOYDESbYh4ptJAEe1','VTkq5SJThomH_-HUdqHC9',1646772765000,'Thanks @arv ','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('NRyCIrE26hPe6LETAjQAz','_EIQUWiyG3kD9i7bsOoTV',1675935866000,'Waiting on @aboodman to review pr.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('2rNb-zpG4YiadvqeNFoSF','bUPHRFd58OwFTd73qpvqT',1684383883000,'https://www.notion.so/replicache/Fast-er-Forward-62a96385bd0d4931b5db868e172049cd','ieK09sy2C_AIWE8KRkrQR'),
	 ('ah-Sfj2A9m31Jbo-1UIjq','2fVC187meiZ9ta39EpxRj',1663351170000,'related: 
- https://www.notion.so/replicache/Requirements-fb82ffc6c695496aadd59875fa03acfb
- https://www.notion.so/replicache/WIP-Streaming-Replicache-4acd7513121949f5898f7eeeeeaef96f#e2dbf4dee6574e2c81d266bd57d2fc77
- https://www.notion.so/replicache/Reflect-Alpha-a5369ac380d247b98a0170bb1688804d#49be32958b174b458a43e1b2bca39f04
- typical change is a mouse move:
    - 16 byte client ID
    - 8 byte timestamp
    - two 8 byte coordinates
    - 100 bytes','yJ5hiysWE-LBcDfT44lR8'),
	 ('82QYPRCWfbECEK3hTD9om','ITE0vq4xtyRQcJCBsVCCc',1672740936000,'Duplicate of rocicorp/mono#316 ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('WBN0Ct738S537Gu96i0xA','Ssw407hnzRxkq9qxjB4VI',1663280923000,'For beta: We need to setup basic framework for monitoring and alerting so that we can move fast when something comes up.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('3qc8I-QPNrr21aWmynxZw','Ssw407hnzRxkq9qxjB4VI',1673557440000,'To make this a little more concrete I think the scope of this issue is potentially quite broad, seems like it will spin other issues out as we get to them. The ''monitoring and alerting'' umbrella I think could potentially cover at the very least (feel free to edit):
- monitoring
  - have an app-agnostic dashboard graphing key metrics from reflect client and server
  - have at least one sample app running this dashboard
  - have a way for customers to import the dashboard config so they don''t have to build it themselves
  - include environment (prod etc) and reflect/server version in metrics via tags
- log analysis
  - teach datadog to parse out our custom attributes (doID, etc) if it doesn''t already know (via pipline)
  - have a way for customers to import the pipeline 
  - include environment (prod, etc) and reflect version in logged lines and teach datadog to filter by it
- alerting
  - for the sample that has the dashboard, configure data dog to alert on errors (after https://github.com/rocicorp/mono/issues/195)
  - determine what metrics we should have alerts for and implement those
  - have a way for customers to import our alert rules
','yJ5hiysWE-LBcDfT44lR8'),
	 ('5Mg0Mo0UxxkPCz4TOXvA2','aV6nM8f0Qr2Hc8A91q9CT',1663280579000,'CF hard restarts a DO when exceptions goes to top level so we def want to catch.','yJ5hiysWE-LBcDfT44lR8'),
	 ('YVznTGX2cforR5_sqNLmf','aV6nM8f0Qr2Hc8A91q9CT',1672740841000,'I think that probably rocicorp/mono#212 fixes this, but we should try it and confirm.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('pGQ3GE0EUzwA4bxZxKZDt','aV6nM8f0Qr2Hc8A91q9CT',1672972264000,'Kinda bigger picture, we want to ensure that we see when DOs are restarting for expected (code update) and unexpected (uncaught exception, OOM, etc) reasons. Part of this story is ensuring we hear about these events via logs, current logging buffers non-errors for 10s and it''s likely we don''t hear about OOMs and similar conditions that just outright kill the DO. Theory is that CF logpush can help. But the other part of this story is that we need metrics around (re)starting so we are not relying on logging for this eg count of DO starts in a given period as well as some kind of rapid restart or flapping detection (a given roomDO quits and starts in rapid succession). This second aspect is probably part of https://github.com/rocicorp/mono/issues/201 which requires fleshing out. ','yJ5hiysWE-LBcDfT44lR8'),
	 ('jmmH0ZrKIS1SEjnx5wF0Q','aV6nM8f0Qr2Hc8A91q9CT',1675936034000,'@arv this is basically a dupe of rocicorp/mono#212 but I guess there''s a chance logpush doesn''t work out for us (which would be odd).','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('n-4zGZtFS7uW2TwHkIaoj','aV6nM8f0Qr2Hc8A91q9CT',1677704985000,'This has been fixed by #22 ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('GsoU0lXij07J4zYOhyhId','J9HZcO1Q86K3d75M1I7sM',1663280530000,'I think it should actually use same exact code as Replicache. The reconnect/backoff options would work prefect for how often to try to reconnect the socket.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('SWY_AUalPVPIOEKvbDeKW','J9HZcO1Q86K3d75M1I7sM',1675129559000,'superseded by rocicorp/mono#200 ','yJ5hiysWE-LBcDfT44lR8'),
	 ('P4WpPqUFrUeHJcDKyXcm5','VRpbBmU-uZBULvFGhpwru',1649695986000,'Published at [@rocicorp/reflect ](https://www.npmjs.com/package/@rocicorp/reflect) and [@rocicorp/reflect-server](https://www.npmjs.com/package/@rocicorp/reflect-server).','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('yFO6IhvHLm-j8zf5ENGEm','OQPzDXIhsddK9TNefJIIf',1647498293000,'Specifically, we should end up with one `Reflect` class which is the client which has an API which is roughly:

```
Reflect = Replicache
- stateless http protocol
- createIndex and friends (just no need for it yet, let''s wait for more info)
+ stateful socket protocol
```','OeVnr1y5bEM_Yg06sUFtD'),
	 ('bEH4HTH5kgxxVtZ7bxKIW','lmMmnhdd5zR6wjTC5Blph',1646940883000,'# What to replace Zod with?

I used superstruct but it turns out that I misread the benchmarks It is slower than zod

Some quick notes based on the benchmarks at https://moltar.github.io/typescript-runtime-type-benchmarks/

- `ajv` is too large
- `ts-json-validator` depends on `ajv` and is too large
- `suretype` depends on `ajv` and is too large
- `valita` has no runtime deps... Let me try','nqYkxAGMnzk7Y5STjZryV'),
	 ('it84Pj57qfY8X-gjK_dae','lmMmnhdd5zR6wjTC5Blph',1646942690000,'Here is a working valita example:

```ts
import * as v from ''@badrap/valita'';

type JSONValue =
  | string
  | number
  | boolean
  | { [key: string]: JSONValue | undefined }
  | JSONValue[];

const jsonValueSchema: v.Type<JSONValue> = v.lazy(() =>
  v.union(
    v.literal(null),
    v.string(),
    v.boolean(),
    v.number(),
    v.array(jsonValueSchema),
    v.record(v.union(jsonValueSchema, v.undefined())),
  ),
);

const o = { a: ''a'', b: 1, c: true } as unknown;
const o2 = jsonValueSchema.parse(o);
console.log(o2);

// test extra fields
const s2 = v.object({
  a: v.string(),
});
console.log(s2.parse({ a: ''s'', b: ''extra'' }, { mode: ''passthrough'' }));
```

esbuild minimized:

```
  index.js  12.2kb
```','nqYkxAGMnzk7Y5STjZryV'),
	 ('29O9oSCNcg5qfWzW5N0x3','lmMmnhdd5zR6wjTC5Blph',1678376843000,'I know I''ve been going back and forth on this for too many times to count... But I''m reopening this with some new insights.

## Problems:

1. All existing runtime type validators are too slow to validate JSON. Especially large JSON structures that we have seen in the wild (i.e. Placemark)
2. Some validators clone the data at all times (i.e. zod)
3. Some validators have large code size and do not allow dead code elimination.
4. Some validators have bad error messages (i.e. superstruct)

## What I''m suggesting 

Use a validator that allows custom validation and use that for the JSON type. That way we can short circuit the runtime validation with our own that is much faster. We can even completely disable it in release mode.

After another stab at this I''m leaning towards [@badrap/valita](https://github.com/badrap/valita):
1. It has a way to do custom validation using `v.unknown().chain()` so we can use our own json validation function
2. Valita does not clone when doing `parse` (when `strict` or `passthrough` parsing)
3. Relatively small code size [bundlephobia](https://bundlephobia.com/package/@badrap/valita@0.2.0)
4. OK error messages. We can wrap these if we want','nqYkxAGMnzk7Y5STjZryV'),
	 ('EdKu_96gffdl8DZ4VH4h-','oDYo4rX4-Q5R2MApWlVkp',1646733539000,'What does this mean? License pings etc?','nqYkxAGMnzk7Y5STjZryV'),
	 ('goEArLApjjjE2wGxnnfun','oDYo4rX4-Q5R2MApWlVkp',1646759796000,'Yes potentially checks and pings but also anything required on the backend: a new license type, whatever billing view we need on reflect licenses, updating any visualizations to include, etc.','yJ5hiysWE-LBcDfT44lR8');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('4gNe_pTtg82Ax6E_lLrFM','oDYo4rX4-Q5R2MApWlVkp',1646760551000,'Updated description. Sorry for lack of detail.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('PogfnFSBgLUCCYfTbpLfA','oDYo4rX4-Q5R2MApWlVkp',1663280497000,'Needs product/pricing design. Closing for now. Also kinda dupe of rocicorp/mono#23 .','OeVnr1y5bEM_Yg06sUFtD'),
	 ('kM0bH-BduquEFKRK9BBrt','xDVlNUOoYCK1X8H78brz_',1677698531000,'This idea was abandonded: https://rocicorp.slack.com/archives/C013XFG80JC/p1677695514288939','OeVnr1y5bEM_Yg06sUFtD'),
	 ('s3h4hzPNzsuo_M3zE2pYg','UL9p6OuZRrHsVCW4rUGVv',1672741332000,'Note this ideally includes as a dependency the new roci.dev webpage :-/.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('etBnuutYXmhI85HVFKOpu','UL9p6OuZRrHsVCW4rUGVv',1679346200000,'latest review here: https://rocicorp.slack.com/archives/C013XFG80JC/p1679345082508799','OeVnr1y5bEM_Yg06sUFtD'),
	 ('oiI1Fsy76hreLezjoF5E8','UL9p6OuZRrHsVCW4rUGVv',1681146777000,'I''m going to mark this done - now into ongoing maintenance.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('42AI4lqRBGSb56qkw7R_Z','-s-7JuRdK5po4Te_CmiqA',1647909210000,'I think this is complete right @grgbkr ?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('RF8jMFPdQQdN_Uj4_T6pD','-s-7JuRdK5po4Te_CmiqA',1647972008000,'@aboodman There is one follow up that really needs to be done.   We need to garbage collect connections from the AuthDO (right now they will grow unbounded).  

The GC follow up is the only must do, there are also these other potential improvements:
1. re-auth connections every N minutes.
2. use finer grain locking in AuthDO (requires adding userID as param to connect requests)','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('QLew4Dm-m-sicPLSmRqUl','HflGUiig7qx-antN2p-Hb',1646149867000,'Lots of updates here:

1. Noam wasn''t able to add us to cf for security reasons, but he did invite us to datadog. You should have receivied an invite at greg@roci.dev. Once you accept, you need to login and you will have ability to select a different org in datadog here:

<img width="423" alt="Screen Shot 2022-03-01 at 5 40 08 AM" src="https://user-images.githubusercontent.com/80388/156199850-b279d3ba-6b07-4477-97fb-2efc445ed627.png">

2. Noam says it is relatively easy to reproduce this bug. He says is happens ~everytime he draws "intensely".

3. Noam says that when the bug happens the symptom visible to source user is typically this client-side error message: 

<img width="1498" alt="Screen_Shot_2022-03-01_at_16 45 47" src="https://user-images.githubusercontent.com/80388/156202030-2587e3e3-b60d-4c37-9e97-a360a5e1a5a3.png">

4. Noam captured client-side and server-side logs (at info level) from one of these sessions:
[logs.zip](https://github.com/rocicorp/reflect/files/8162834/logs.zip). The zip file also contains a heap profile but I''m not sure if that''s from the same session. Noam not able to reproduce error at debug log level so far.

Thoughts scanning through these logs real quick... it looks like the "client not found" is the immediate cause. It does make sense that if a client wasn''t found on server then symptom would be as Noam describes: mutations would pile up client side, drawing would appear to work, but when you refresh drawing not saved.

I do see the client-not-found error on server too. It appears this happens after two disconnect/reconnect cycles on client. Appears that somehow server state gets confused as to whether client is present.

5. (Not sure if related) Noam says that when he refreshed the session this occurs in he gets this error immediately on refresh:

<img width="1512" alt="Screen_Shot_2022-03-01_at_16 59 08" src="https://user-images.githubusercontent.com/80388/156202528-1f387e04-4e0a-4c8f-8519-d6167c925847.png">','OeVnr1y5bEM_Yg06sUFtD'),
	 ('kfTY15-WG3sxAXnr2BHVJ','HflGUiig7qx-antN2p-Hb',1646150841000,'> It appears this happens after two disconnect/reconnect cycles on client.

An underlying question is: why do we disconnect? I do see the server restarted right before this happened, but there''s no indication why.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('WdBL4myFF2ArSjgJEXPNU','HflGUiig7qx-antN2p-Hb',1646152140000,'> but there''s no indication why.

Two thoughts:

- What does a durable object do when an unhandled exception happens directly inside a request handler? What about outside a request handler? You''d expect such unhandled error to make it to wrangler log, but not surprising it doesn''t make it to datadog.

- We already know of one case where the server fails silently -- large upload. Is there some way that we could have gotten into a situation where we have a large 1MB upload?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('CkVtQ9-Tv_JlGg4xDTaJe','HflGUiig7qx-antN2p-Hb',1646153028000,'> What does a durable object do when an unhandled exception happens directly inside a request handler? What about outside a request handler? You''d expect such unhandled error to make it to wrangler log, but not surprising it doesn''t make it to datadog.

I tested this. The error doesn''t make it to datadog :(. But it also doesn''t restart the server. The exception is caught by CF at top of event loop and logged to wrangler output. So it doesn''t explain the server restarts in noam''s log.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('1IIzc1m2xdZ57ZW0r2pKP','HflGUiig7qx-antN2p-Hb',1646156634000,'Lots of useful debugging info here.  Thanks Noam!

A few updates.

1. I am able to successfully access Monday''s datadog logs.   
2. I have not been able to reproduce the bug myself despite intensely scribbling for 4 mins (now my hand is tired :)).
3. I do see others hitting this "client not found" in the server log.  These clients are then wedged and try to keep pushing over the same web socket connection with the same client id, resulting in this same error over and over again.  I have not yet found the root cause for why the client is not found.  I have identified one change we should make that will prevent clients from becoming wedged when "client not found" occurs.  The connection should be closed by the server, as no messages over that connection will succeed.  Then the client can reconnect, and after reconnection should be unwedged.

I''m continuing to try to find the root cause of "client not found".



 
','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('Dro71RhYXibHOFnWGmqyh','HflGUiig7qx-antN2p-Hb',1646156977000,'If a client receives "client not found" (for some reason) and user keeps scribbling, they will soon hit 1MB upload limit, right?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('bMMdLCjmoR9bDgCrbH0lf','HflGUiig7qx-antN2p-Hb',1646157993000,'> If a client receives "client not found" (for some reason) and user keeps scribbling, they will soon hit 1MB upload limit, right?

I think it would take about 10 minutes of drawing to get to a 1MB push.  (based on going offline and scribbling hard for 3 mins led to 300KB push on reconnect.)','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('Vm8RYXo27VHYZ6P8A8Rx-','HflGUiig7qx-antN2p-Hb',1646161588000,'I found one bug in connection management that results in the "client not found" error.  If a client tries to reconnect, while the previous connection is still open on the server, a race condition occurs and we end up deleting the new connections entry in the client map (when we meant to delete the entry for the previous connection).  I am fixing this race now.  Next why... why is the client trying to reconnect?','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('7hpRQ-M_b2RDwyRPWQLni','WPfQD0ViKhZJWWZIU78gP',1646660742000,'It would also be interesting to know if this limit applies to binary web socket messages too?','nqYkxAGMnzk7Y5STjZryV'),
	 ('HYalIqmY9iUu6I5KtRc9-','lBxK59WH_zVrsTABv5o5z',1645666346000,'Here is a video of the bug. I can reproduce this easily in canvas:

https://drive.google.com/file/d/1Bf3rUjcsoFuOAA1VhOl2Zdq8_xuIorOI/view?usp=sharing','OeVnr1y5bEM_Yg06sUFtD'),
	 ('3PdPhLM26iSuI3FVuiz6V','lBxK59WH_zVrsTABv5o5z',1645666449000,'I think there are two questions here:

1. Why does the server crash? This doesn''t seem like sufficiently many mutations (by a long shot) to exceed the 128MB of memory workers are allotted.

2. I get that if a particular push is going to crash the server, it''s going to happen when we try to recover mutations too. But in that case, how come it doesn''t keep happening forever? 😬 Did we do something smart to only try to recover mutations for a little bit?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('m1NqzmXTkP_6o5r5K2M9q','lBxK59WH_zVrsTABv5o5z',1645668739000,'I created a PR in replidraw-do that replicates this issue I believe: https://github.com/rocicorp/replidraw-do/pull/32.

If you press the "duplicate all" button enough times the server crashes -- I assume for the same reason as canvas.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('R2OQPIzpGCa3hfcYkC85-','lBxK59WH_zVrsTABv5o5z',1645669209000,'Here is a video repo of the stress test PR:

https://user-images.githubusercontent.com/80388/155444911-45d5edc1-9379-4395-a179-4515325cd819.mov

','OeVnr1y5bEM_Yg06sUFtD'),
	 ('kWlsQP9HZr80L1x5npvhC','lBxK59WH_zVrsTABv5o5z',1645722843000,'I did a CPU and memory profile of the worker using the `wrangler --inspect` option (super easy!) and found something very interesting:

1. It definitely doesn''t appear to be CPU bound. The worker isn''t doing anything according to the profile for all those seconds. The processing only takes a matter of a few hundred ms.

2. I don''t see any massive memory allocations either. It reports to only be using like 5MB or something.

If I run the worker in logLevel=debug mode, I do see an OOM crash that occurs. But it typically happens on the 500KB push, not the 1MB one, so not sure if it''s the same reason we crash in logLevel=info mode. In info mode, I don''t get any such message, the worker just reboots. Need to check the cloudflare dashboard and see if better messages there. Or maybe if you run it under inspector and have it pause on exceptions you can catch why it''s rebooting?

Simplifying: the current question is: why does the worker reboot at ~1MB push using the stress test under logLevel=info. Also (and presumably related) why is the poke response from the 1MB push so slow.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('gJtYpVN-NTHIN3Y68pXmm','lBxK59WH_zVrsTABv5o5z',1645749216000,'@grgbkr isolated this down to an apparently undocumented limit on upstream message size in workers. If we send more than 1100000 (~1MB) bytes upstream to a worker, it restarts. In the downstream directly we haven''t discovered the limit yet - up to ~50MB appears to work.

We are asking our contacts at CF to confirm this, but assuming this is a limit issue and it mainly applies in the upstream direction then changing this mutator to a `copyAndPaste(ids)` shape should alleviate the issue.

We have also confirmed that the actual amount of time spent processing this message isn''t an issue -- in our tests only a hundred ms or so is spent processing this large simulated copy/paste on the server.

As for the data loss - this isn''t unexpected given above:
- If the push fails due to size limits, then the client will reconnect and try to push again, and that push will also fail.
- If you reload, the client will again try to send the push and it will fail.

The thing that *is* unexpected is that the client seems to somehow get *past* this large push and carry on before reload. We would expect the client to be permanently stuck trying to send this large push until the page is reloaded, but we don''t see that.

Finally, there is a separate issue of _preparing_ the push to be sent to the server getting progressively slower on the client-side and stalling the UI. Unclear if that is something that needs to be fixed or if `copyAndPaste(ids)` would also fix that.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('LzRZquCwm614QsLIil1WP','lBxK59WH_zVrsTABv5o5z',1645773092000,'Proposal: Enforce a new restriction that individual mutations cannot be bigger than, say, 1MB. Then we automatically break the push into multiple messages as necessary to stay under some per-push configurable limit.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('utVmb2bcYphBnpnjETjQg','lBxK59WH_zVrsTABv5o5z',1646074653000,'> 
> As for the data loss - this isn''t unexpected given above:
> 
> * If the push fails due to size limits, then the client will reconnect and try to push again, and that push will also fail.
> * If you reload, the client will again try to send the push and it will fail.
> 
> The thing that _is_ unexpected is that the client seems to somehow get _past_ this large push and carry on before reload. We would expect the client to be permanently stuck trying to send this large push until the page is reloaded, but we don''t see that.

@aboodman where do you see the client get past the large push?  In my testing with your aa/stress-test branch, the client is behaving as expected.  After the large push fails and the connection is closed, the client reconnects, and on next mutation (you need a mutation to trigger the push) it will try to do the large push again.  One potential improvement is the client could auto retry the push (with backoffs and some cap on retries).

<img width="870" alt="image" src="https://user-images.githubusercontent.com/19158916/156041572-f0744ade-a345-4e35-8fdd-3630eb2222f0.png">

<img width="870" alt="image" src="https://user-images.githubusercontent.com/19158916/156041694-9f0c8bd4-0201-456c-93c1-d2e7a9d55ed5.png">


','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('pH4kApifIN686OWOj9R9y','lBxK59WH_zVrsTABv5o5z',1646077024000,'I''m not sure I saw this on the stress test. I only saw it when drawing with canvas.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('j-IyJiaExGXjNr5bnuNFt','lBxK59WH_zVrsTABv5o5z',1663279494000,'The change to push changes as they occur should fix the issues with a batch > 1MB. We don''t have to catch individual mutations > 1mb for beta.','yJ5hiysWE-LBcDfT44lR8'),
	 ('BBrlMebSG8YHKBYLG_TgV','bjCcZRrgs0o2pf6lCgHUZ',1645473382000,'Actually that approach in https://github.com/rocicorp/reps-do/pull/31 is not right, because we don''t have env.DATADOG_API_KEY available at the time we `createWorker`.

','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('Hvsm4IJxAyGvBtSn-y5KR','uRhaQ6_Q635H4g3WwJvO5',1663279195000,'We should also measure the perf cost of doing this (e.g., in Replidraw) and consider having it enabled even in release.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('gcLvdIPaETEhkzuy9NSAp','uRhaQ6_Q635H4g3WwJvO5',1677705089000,'I think this is actually already done but verify and/or do.','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('RV9kaOZ2fUhWej1_2iiUs','uRhaQ6_Q635H4g3WwJvO5',1678358831000,'Related to #216 ','nqYkxAGMnzk7Y5STjZryV'),
	 ('gLlh1hmPmzVHRB_hyJghc','uRhaQ6_Q635H4g3WwJvO5',1679067974000,'This still doesn''t check the json in release mode. Leaving open.','nqYkxAGMnzk7Y5STjZryV'),
	 ('mOJgPReQndWiE_5lFK7WP','uRhaQ6_Q635H4g3WwJvO5',1679431139000,'We decided that we should check these in release mode for now and have an "escape-hatch" that can be used for customers that want things super fast.','nqYkxAGMnzk7Y5STjZryV'),
	 ('YBH8dM3_sMWwv7n6BgJrw','uRhaQ6_Q635H4g3WwJvO5',1680637349000,'Left to do:
* perf test
* escape hatch','OeVnr1y5bEM_Yg06sUFtD'),
	 ('yQ0Gx9G_5101uVVG1TDFz','qIS_bVLLjzfv0twuNsw0S',1644314355000,'See: https://github.com/rocicorp/replidraw-do/blob/main/README.md#how-to-list-the-rooms-for-your-reps-server','OeVnr1y5bEM_Yg06sUFtD'),
	 ('YyKn7GIbmRtW1MVtLa65e','qIS_bVLLjzfv0twuNsw0S',1672783293000,'https://github.com/rocicorp/reflect-todo/blob/main/doc/server-api.md#get-room-records','yJ5hiysWE-LBcDfT44lR8'),
	 ('tKbMooLISHGPw97ZrlCnY','o67nWa46P-P9kWRsc9aDa',1644304766000,'If this is solved by doing something inside `reps-do` then I could imagine:

* some webpage baked into `reps-do` which is a datagrid
* datagrid backed by Replicache, of course :)
* some mutators baked into the DO like `_reps_editEntry()`','OeVnr1y5bEM_Yg06sUFtD'),
	 ('n_wW7ThRh4CKRM7coiyA3','o67nWa46P-P9kWRsc9aDa',1645829018000,'Per discussion it might be nice/easier if we could just start with a way to view and leave the editing until we absolutely need it.','yJ5hiysWE-LBcDfT44lR8'),
	 ('mSJvrWUq3IBeIbmjZIePh','5vhM9LcujDBpEWXKEO-W_',1645828742000,'Additional notes so we don''t forget: 
- websocket output gate now supposedly works, so we should turn that on as part of this issue. (We should probably have a metric for how much headroom we have in a frame, latency-wise, so we can see when we get close or get behind.)
- I believe we also need to ensure that we are doing our own caching here for cost reasons.
- this batching needs to gracefully accommodate slow up and down websocket connections (maybe this happens automatically if so yay, but it''s an important requirement)','yJ5hiysWE-LBcDfT44lR8'),
	 ('cx789ceDzUun6fkbMahv4','5vhM9LcujDBpEWXKEO-W_',1663908350000,'see also https://github.com/rocicorp/mono/issues/285','yJ5hiysWE-LBcDfT44lR8');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('rq8qU_1jZp2f1YTDl5MC5','5vhM9LcujDBpEWXKEO-W_',1675936152000,'https://github.com/rocicorp/mono/issues/243','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Poa0PGKe98s0AfYdq3bte','1j8tnx0eVLybcbf8W7PkU',1645472581000,'https://www.notion.so/replicache/WebSocket-Authentication-Authorization-and-Security-bf2b1a2e31844efca73c4aa453fdecdb','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('XeJ889LZ-HtAgA3wwSJ4n','qaYILbO6MwgV0O_iESnt0',1663246427000,'Replaced with rocicorp/mono#290 ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('hL8n6NbleQIJn7zBRm39I','rQo1DeZZgBnqWT7wvBXHS',1644861653000,'If the next mutation queued for a client doesn''t match the next expected mutation the server will iloop (it keeps trying to run the next frame because there is a pending mutation, but then find its can''t make progress. next frame same thing happens).

I guess there are a number of tasks here:

1. Perhaps the initial connection should additionally send its lmid in the querystring (https://github.com/rocicorp/reps-do/blob/main/src/server/connect.ts#L92). If the client already exists but the lmid is not as expected, then the connection is invalid and the client can never make progress. We should send an error message (https://github.com/rocicorp/reps-do/blob/main/src/protocol/down.ts#L10) then close the socket.
2. Later, we can interpret the error message from (1) above similarly to rocicorp/mono#179, that we should nuke client state and start over because this client is toast.
3. Because web sockets are ordered, if (1) prevents initial OOO connection from being made, then it should also be impossible for a push message to be received OOO. In the case we do receive one, I think we should drop the socket and make the client reconnect. Then if the client really is wedged (1) will apply.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('jEh_XW4QguXlt1etFHFW8','rQo1DeZZgBnqWT7wvBXHS',1644862147000,'Note it is technically possible for this to happen on production right now since the output gate is not working:

1. Client sends push
2. Server sends poke without waiting for commit
3. Server shuts down uncleanly before commit
4. Client sends next push
5. Server finds that received mutation is from the future','OeVnr1y5bEM_Yg06sUFtD'),
	 ('STTYrWKlDSQFwRoqFQ6rA','rQo1DeZZgBnqWT7wvBXHS',1644868132000,'>  output gate is not working:

Not following closely but "the server correctly implements the protocol" seems so fundamental that we should probably hack our own output gate until it properly works. I can imagine mysterious behavior and wasted time due to assumed confirmed but actually unconfirmed mutations.','yJ5hiysWE-LBcDfT44lR8'),
	 ('dYHkJVxvDXFSdUaxBbK_8','rQo1DeZZgBnqWT7wvBXHS',1644873514000,'I am totally in favor of that but I doubt that we have the time before Feb 23 as this would also require implementing batched mutations.

I think it makes sense in any case to put the protection of step 1 above in place since from server''s pov, it needs to protect itself against badly behaved client.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('6fBZm3OURmnJcGDKYf2M9','rQo1DeZZgBnqWT7wvBXHS',1646772373000,'This does not iloop any more but it does raise an exception which is now covered by https://github.com/rocicorp/replicache/issues/335','nqYkxAGMnzk7Y5STjZryV'),
	 ('u9eDJKXLXF5N-AJ2p9PYA','CCsMsi5XESPApUiyOXfiM',1644348032000,'Why was this closed?','nqYkxAGMnzk7Y5STjZryV'),
	 ('zlkWjUw2dapCsefV5dWmQ','CCsMsi5XESPApUiyOXfiM',1644383901000,'I thought you completed it.  Was just trying to get a handle on what’s left
for v9
On Tue, Feb 8, 2022 at 12:20 PM Erik Arvidsson ***@***.***>
wrote:

> Why was this closed?
>
> —
> Reply to this email directly, view it on GitHub
> <rocicorp/replicache-internal#83>,
> or unsubscribe
> <https://github.com/notifications/unsubscribe-auth/AESFPBDC46V3LZ5NQUFBFZDU2FUIXANCNFSM5MX47VOQ>
> .
> Triage notifications on the go with GitHub Mobile for iOS
> <https://apps.apple.com/app/apple-store/id1477376905?ct=notification-email&mt=8&pt=524675>
> or Android
> <https://play.google.com/store/apps/details?id=com.github.android&referrer=utm_campaign%3Dnotification-email%26utm_medium%3Demail%26utm_source%3Dgithub>.
>
> You are receiving this because you modified the open/close state.Message
> ID: ***@***.***>
>
','Gg4MskWt3M-ttzzlrJ9jn');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('xc2nAFcoQFwm1PemXeua9','CCsMsi5XESPApUiyOXfiM',1644401426000,'The pr is still not done. The issue with pullVersion/pushVersion is not resolved.

I could just remove that from the doc for now?','nqYkxAGMnzk7Y5STjZryV'),
	 ('mpmc6lqnsyCs04HAjg0BU','oj7ViYmOhygtxCuG5Q1HN',1641496200000,'I''m surprised, I would have thought that the oldHeads path would have decremented the count and the newHeads path would increment it, leaving it unchanged.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('SxQ91mPBz7xMZpAoaBYm3','oj7ViYmOhygtxCuG5Q1HN',1641499157000,'Aaron I think you are correct and since we increment first and then decrement this is guaranteed not to recurse a lot and so should be cheap.  But its a coincidence that the code currently increments before decrementing, if they were swapped we would in certain cases do an expensive deep recursion for no reason.

I think its still a good idea to:
1. write a test cases for this
2. add the check to avoid potential future perf regressions','Gg4MskWt3M-ttzzlrJ9jn'),
	 ('dQ93FVokF53UKynE6WCXe','oj7ViYmOhygtxCuG5Q1HN',1644788422000,'Agreed.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('Xinhb6RCfQt5XpMLsP1MD','zHk5PvZbKetkVVQkX1Ynu',1709599717000,'This is out of date.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('4-W_MptWl61vu0JNd07eI','yim3tSni-ncY5B1fEtazj',1637270333000,'OK but let''s be sure that it shows up on some benchmark before complexifying the code.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('rN8lBCI844yxxkCKt5gmi','yim3tSni-ncY5B1fEtazj',1637270347000,'(the allocs in scan could easily be coming from some other random thing)','OeVnr1y5bEM_Yg06sUFtD'),
	 ('lZwNaCYVzswE-2J7UeHLW','YXDDa24OnTfb4ouoaA8aP',1637276364000,'Very ... "interested" ... to see what effect computing a totally different
bundle and putting it in an embedded string would do to bundle size.

Perhaps it would compress well?

On Thu, Nov 18, 2021 at 11:12 AM Erik Arvidsson ***@***.***>
wrote:

> We can/should run the perdag in the worker. That would allow us to use the
> native hash functions (we can precompute the hash of the chunks in the
> persist operation)
>
> According to this SO post
> <https://stackoverflow.com/questions/10343913/how-to-create-a-web-worker-from-a-string>
> you can create a worker from a string but it is not clear what CSP policies
> this runs under.
>
> —
> You are receiving this because you are subscribed to this thread.
> Reply to this email directly, view it on GitHub
> <rocicorp/replicache-internal#49>, or unsubscribe
> <https://github.com/notifications/unsubscribe-auth/AAATUBBYQ3XKYRTAZPAFHE3UMVTZ5ANCNFSM5IKTQW7A>
> .
> Triage notifications on the go with GitHub Mobile for iOS
> <https://apps.apple.com/app/apple-store/id1477376905?ct=notification-email&mt=8&pt=524675>
> or Android
> <https://play.google.com/store/apps/details?id=com.github.android&referrer=utm_campaign%3Dnotification-email%26utm_medium%3Demail%26utm_source%3Dgithub>.
>
>
','OeVnr1y5bEM_Yg06sUFtD'),
	 ('B7ZDoCta1P6v9Mvu4pydq','YXDDa24OnTfb4ouoaA8aP',1637287202000,'This might be relevant too:

https://github.com/mitschabaude/esbuild-plugin-inline-worker

Or at least be an inspiration','nqYkxAGMnzk7Y5STjZryV'),
	 ('L_786NYhb4qF9mXAWL5qT','yIUPmTFehw2TgOSSZNG_Y',1652805278000,'Is the GC perf here regarding the DAG or the JS runtime?','jpRvILZ1tibPsQXKvb3cF');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('I9wW6aTcwicWGmlhzPrYv','J3-tsjZxV8PlJSH44m3w1',1636137063000,'I think it''s more than that -- basically I think that customers should *always* name Replicache instances with a user id. Otherwise implementing diff correctly becomes more difficult. ','OeVnr1y5bEM_Yg06sUFtD'),
	 ('yvxU7U3uG6D-ssYNwOMjO','QuTzFD3KoD-vdbFGPROzl',1635372877000,'But maybe we need to start looking at the big picture. We want to achieve more stable output. Should we run until things settle down?','nqYkxAGMnzk7Y5STjZryV'),
	 ('BRUQjPbcZqfYbmWfjphsx','QuTzFD3KoD-vdbFGPROzl',1635430029000,'We could add a maxRuns component or something.

My guess is that the "variance" is a red herring. I bet that if we print
out the 50/75/90/95 percentiles we will see that it is actually pretty
stable now and that there is just 1 or 2 massive outliers (probably cold
start effects).

I think we should stop printing the variance and instead always print the
50/75/90/95 and then see how it is working. The variance is not that useful.

On Wed, Oct 27, 2021 at 12:14 PM Erik Arvidsson ***@***.***>
wrote:

> But maybe we need to start looking at the big picture. We want to achieve
> more stable output. Should we run until things settle down?
>
> —
> You are receiving this because you are subscribed to this thread.
> Reply to this email directly, view it on GitHub
> <rocicorp/replicache-internal#51>,
> or unsubscribe
> <https://github.com/notifications/unsubscribe-auth/AAATUBEQTRH3TBLLDGBVQ7TUJB2VPANCNFSM5G3M4VQA>
> .
> Triage notifications on the go with GitHub Mobile for iOS
> <https://apps.apple.com/app/apple-store/id1477376905?ct=notification-email&mt=8&pt=524675>
> or Android
> <https://play.google.com/store/apps/details?id=com.github.android&referrer=utm_campaign%3Dnotification-email%26utm_medium%3Demail%26utm_source%3Dgithub>.
>
>
','OeVnr1y5bEM_Yg06sUFtD'),
	 ('n6L1Rtkve7uSm0UkivH8J','UgayxjXiqQZn8_Yt9F5qq',1651317951000,'It is available in chromium browsers too (and Firefox as well) now','nqYkxAGMnzk7Y5STjZryV'),
	 ('1dmTALnPO6CLko-POjkyq','UgayxjXiqQZn8_Yt9F5qq',1652363597000,'I did a perf test for this:

```
json deep clone x 41.67 ops/sec ±6.1% (19 runs sampled)
structured clone x 8.07 ops/sec ±25.3% (7 runs sampled)
```

Closing','nqYkxAGMnzk7Y5STjZryV'),
	 ('5mFbbMSo8wWmuRa8nafVq','HO85ObWb4MZiZkq638vuk',1632901959000,'Strawperson:

* There is an npm script in the Replicache package that generates a *license key* which encodes:
  - a unique account id
  - one or more host names
* The license key is signed by a private key known only to Rocicorp
* The license key and signature are provided as arguments to Replicache at startup
* Each time Replicache is constructed it validates the provided license key signature using Rocicorp''s public key (embedded into Replicache) and also that the current host matches one of the allowed hosts
* If the license and usage is valid, Replicache pings a central server with:
  - The hash of the license key
  - The account ID from the license key
  - The client ID Replicache is instantiated in
  - (maybe?) the index of the host in the list of allowed hosts
 * The server records the ping associated with the account and client - the hash of the license key and ordinal of the host could be used for reporting UI for users later, though I admit it is kind of limited utility without mapping back to actual content of license
 * If the account is paid, or is free and within the free tier usage limits, then the ping returns OK. Otherwise it returns an error, and the client throws and does not work.
 * To upgrade from a free to paid account, customers email us and say hi, and we collect their credit card info over the phone, then mark their account paid in our server-side db so that pings return OK.
 * Once a month some other process (read: somebody at Rocicorp runs a SQL query) uses this data to charge customers','OeVnr1y5bEM_Yg06sUFtD'),
	 ('XJd1MmCWOzFxlIuBAgP1C','HO85ObWb4MZiZkq638vuk',1638165673000,'Note: since this bug was filed the `clientID` in Replicache has become local to a single tab/session. So we need another permanent `profileID` to track unique profiles as opposed to clients.

Maybe it would be wise to report both `profileID` *and* `clientID` so that we could have flexibility on pricing model in the future.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('9mIeFRtJpxC4Ucst3DzZp','HO85ObWb4MZiZkq638vuk',1638233060000,'> we do not want it to be possible to accidentally (or maliciously) use someone else''s account id and charge their credit card ... Therefore, accounts should be somehow tied to domains

I do not think we should have this as a requirement, or at least we should not have this as a requirement right now. This feels like one of those requirements we ultimately did away with in DD like "a snapshot should be useful without first having to hit disk": sure, _ideally_ it''s not possible to accidentally charge someone else''s account, but practically speaking it doesn''t seem worth the effort to implement such a mechanism at this point. We don''t have this problem and adding the mechanism now constrains the way Replicache works in ways potentially annoying to customers (they go to use Replicache on some new thing and it doesn''t work because domain).

I do not think we need to do anything about misuse of keys at the moment, but if we must how about instead of _preventing_ the misuse of keys we instead provide tooling that makes it easy to _detect_ and _recover from_ misuse of keys? There are lots of obvious ways we can do this when the time comes and it seems easier/less annoying than prevention. 

I definitely think we should have some lightweight mechanism that makes accidental misuse of licenses less likely, but I do not think it should be a requirement to _prevent_ it, and I definitely do not think Replicache should stop working if you start using it on an unexpected domain.

(I realize there is risk in proposing to eliminate a requirement without proposing something to go in its place... if this turns into a big discussion I''ll just propose the thing I think we should do instead.)','yJ5hiysWE-LBcDfT44lR8'),
	 ('zcoLu05OBdXaY9LfQBJ74','HO85ObWb4MZiZkq638vuk',1638234304000,'BTW regarding a customer cloning our repo and using whatever key happens to be in there, that''s the kind of accidental misuse that I think we should have a lightweight mechanism to catch, and I think that that lightweight mechanism can be a lot less work to implement and a lot less onerous for customers than what was suggested above.

> I definitely think we should have some lightweight mechanism that makes accidental misuse of licenses less likely, but I do not think it should be a requirement to prevent it, and I definitely do not think Replicache should stop working if you start using it on an unexpected domain.

','yJ5hiysWE-LBcDfT44lR8'),
	 ('saEzrfwDpHK9BFRwmTW51','HO85ObWb4MZiZkq638vuk',1638234507000,'Hm maybe. Make a counter-proposal?','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('gdi8jSGem60svonGp9xK5','HO85ObWb4MZiZkq638vuk',1638234629000,'I guess I agree it''s not a *requirement* right now. But it seems like it will quickly become an issue especially with people accidentally copying the sample license key. I''m not sure how we could detect / understand that if we can''t tell where the usage is coming from.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('nSOkp4v85jVgnlDTIxoYh','HO85ObWb4MZiZkq638vuk',1638234795000,'> I think that that lightweight mechanism can be a lot less work to implement and a lot less onerous for customers than what was suggested above.

Interested to hear what you have in mind!','OeVnr1y5bEM_Yg06sUFtD'),
	 ('7cUhUj4CzUCCh8zB1lh5M','HO85ObWb4MZiZkq638vuk',1638235093000,'> I guess I agree it''s not a requirement right now. But it seems like it will quickly become an issue especially with people accidentally copying the sample license key

I don''t think we have to treat those two separate problems as one problem. I certainly agree that we should do something right now to prevent people from using the sample license key for something other than tire-kicking. However I don''t think we need to prevent customers from using other customers keys right now. The first problem can be solved in a variety of simple ways that don''t require elaborate mechanisms, for example if the sample key is used we can check in the client if the URL contains "replidraw", and if so then they are probably tire-kicking. If not we tell them to get a key and stop working after a while.

I will make a proposal.','yJ5hiysWE-LBcDfT44lR8'),
	 ('ODo7bTzDpVtdFAFFMa-Lc','HO85ObWb4MZiZkq638vuk',1638236035000,'> constrains the way Replicache works in ways potentially annoying to customers (they go to use Replicache on some new thing and it doesn''t work because domain).

Unsurprisingly, I''m sympathetic to this bit.

> if the sample key is used we can check in the client if the URL contains "replidraw"

I think there are other variants of this "sample app problem" -- when people create and share tutorials online on their own blogs, what sample key will they use? Maybe the sample keys should only run for a short time or something, like in minutes, then you have to reload the app?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('5fCxQIjZmZHGUFFhe04Xj','HO85ObWb4MZiZkq638vuk',1638236077000,'Open to ideas like this as long as people don''t accidentally end up using sample keys and people can still create and share tutorials. (as usually there are probably other requirements/desires in my head that we won''t find until we start discussing alternatives).','OeVnr1y5bEM_Yg06sUFtD'),
	 ('gMBQU2wQVHm8WneHklWrp','HO85ObWb4MZiZkq638vuk',1638518671000,'Proposal: https://docs.google.com/document/d/1MxPhS55ie57TdjSPfrq8B5xQCug9hJW1GhpKH1tD9lA/edit?usp=sharing

Sorry/not sorry it''s in a doc, we can make public or highlight essential elements in the issue if it is of interest outside Rocicorp.','yJ5hiysWE-LBcDfT44lR8'),
	 ('-cAax2dpI9iLP8raqJK07','HO85ObWb4MZiZkq638vuk',1690343135000,'We have the "done done" issue, no need for this to exist anymore.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('RmpNgoeNSlaWJQh-wLky6','4jSfGKGJZnmJB7wqIfWZo',1632896558000,'@phritz can you please add targets for the two sync items?','OeVnr1y5bEM_Yg06sUFtD'),
	 ('u-fsGyKrLmZyEanySZi7W','4jSfGKGJZnmJB7wqIfWZo',1636502435000,'> @1gb: 95% Read first 100kb in < 1000ms

A customer points out that this is never going to make sense for users. Never going to want 1gb in Replicache if it has this effect on startup.

I don''t think we actually expect startup to be influenced by cache size, so should we just say:

> @1gb: 95% Read first 100kb in < 100ms

(e.g., we expect startup to be flat past 100MB)','OeVnr1y5bEM_Yg06sUFtD'),
	 ('ruSuJqx4m8Fq6lA02m8AK','4jSfGKGJZnmJB7wqIfWZo',1636502441000,'@phritz ','OeVnr1y5bEM_Yg06sUFtD');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('limh6gOIaQmkzK7YaDVoQ','k1RWJfF-aQu5SoB5E-q56',1634497187000,'We should also document that serialized transactions are required/recommended server-side and justify why.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('o8XX_2k_19AvqtEDpymuA','k1RWJfF-aQu5SoB5E-q56',1690343025000,'This is scattered in a few places like the BYOB guide and the push/pull reference as well as probably the design doc. But it should be consolidated.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('h5Gs8dwEFpZqZmLqi74Aj','LZnu7lfrjDvi8k-3XbZox',1690342957000,'There is now class-level docs for these, but the methods are not documented.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('KywPKwLHo-vXKaEVsRK73','rmuRNbTUJ43LPAV_UbdzJ',1690342899000,'This no longer makes sense to do (and is not often requested by users either).','OeVnr1y5bEM_Yg06sUFtD'),
	 ('H-hvpC2rF_ciWzJt0lT2-','WV-Eq1VhQLCdOhcArGQKC',1632726983000,'Also update https://github.com/rocicorp/replicache-sample-chat','OeVnr1y5bEM_Yg06sUFtD'),
	 ('7d8BfdNOePL7CbleHfR0x','WV-Eq1VhQLCdOhcArGQKC',1690342830000,'This has been done.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('KBFAzUD97ZDDkhc54QE_E','-guR4swz7EX7CeNnEQI3P',1630886399000,'Yup. I was hoping we could only have, `withRead` and `withWrite` (and no `read`, `write` and `release`). I think it is still doable if I refactor transactions to remove some intermediate abstractions.','nqYkxAGMnzk7Y5STjZryV'),
	 ('3TUnJZRY9JNDi99Ob0ug8','7bHPpjhEyp7j726iOKQab',1643321397000,'chai has been released with loupe, you can pick it up after v4.3.5','FPEhfZQwiEMgpk3WnEY72'),
	 ('OCUkAtyZltHI3uFxtGt7X','7bHPpjhEyp7j726iOKQab',1643322268000,'Nice! Thanks for the heads up.','OeVnr1y5bEM_Yg06sUFtD'),
	 ('FnDYuk1RMunMIqY6nHjZl','7bHPpjhEyp7j726iOKQab',1643711641000,'We are using @esm-bundle/chai because we run in a real browser. chaijs/chai is broken so either have to update the @esm-bundle/chai or wait for chaijs/chai to fix their esm version','nqYkxAGMnzk7Y5STjZryV');
INSERT INTO public."comment" (id,"issueID",created,body,"creatorID") VALUES
	 ('lJhpaOAkRZxrNfCyP40nb','058B73_9XTp1ayBO4K1n3',1630833229000,'I realized the other day an easy way to do this is to just read all the keys starting with `c/<hash>`. We want all three of em anyway.

Not sure how much a benefit it is but worth trying!','OeVnr1y5bEM_Yg06sUFtD'),
	 ('0NAw6ozlF3XqG5EnluAcI','058B73_9XTp1ayBO4K1n3',1630863699000,'But you suggested that we store these in the same chunk in the future?','nqYkxAGMnzk7Y5STjZryV'),
	 ('2-Z7zAPW2ZWVrqj-QPSCZ','3O2nk8ljXPknw7Hl_Azyj',1623419851000,'Maybe something to consider, I am storing the access token in localstorage and I just assume it is working. And `getPushAuth/getPullAuth` call my backend to refresh a token. If they are called initially, I would trigger refreshes even in cases where it is not necessary. ','9Fw3n6EQS8mzE126AFJxb'),
	 ('qHOVbXtqdkuor-SmCXZVO','3O2nk8ljXPknw7Hl_Azyj',1646689072000,'Interesting feedback, thanks @KeKs0r. Removing ''fixit'' label pending more user feedback.','OeVnr1y5bEM_Yg06sUFtD');
