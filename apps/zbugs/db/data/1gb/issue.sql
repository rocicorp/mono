INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('HdpMkgbHpK3_OcOIiQOuW','Leaking listeners on AbortSignal',true,1726473766000,1726473756000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Node.js is complaining about 

![image](https://github.com/user-attachments/assets/fabb10ce-e684-4e9a-bb02-e36f77dc0c93)

```
• [ssg] http://127.0.0.1:5173/ resolved to ./(feed)/index.tsx
(node:55167) MaxListenersExceededWarning: Possible EventTarget memory leak detected. 11 abort listeners added to [AbortSignal]. Use events
•setMaxListeners() to increase limit
at [kNewListener] (node:internal/event_target:572:17)
at [kNewListener] (node: internal/abort_controller:241:24)
at EventTarget.addEventListener (node: internal/event_target:685:23) at eval (/Users/n8/github/mono/packages/shared/src/sleep.ts: 25:14)
at new Promise (<anonymous>)
at abortedPromise (/Users/n8/github/mono/packages/shared/src/sleep.ts:19:10)
at throttle (/Users/n8/github/mono/packages/replicache/src/process-scheduler.ts:99:11)
at ProcessScheduler.scheduleInternal (/Users/n8/github/mono/packages/replicache/src/process-scheduler.ts:80:29)
```

It seems like we most likely are not using `{once: true}` for an `addEventListener` somewhere... Or that the version of Node that this is coming from does not support `once`?','public'),
	 ('y8cMnHKAiOfdv7P4gCFXn','Postgres (CVR_DB) sometimes gets wedged with "too many clients already" error',false,1726276413000,1726269542000,'ieK09sy2C_AIWE8KRkrQR',NULL,'Peculiarly, the ChangeDB stub (which is pointed to the same database) works. And so does the Mutagen stub (which is to upstream). But it''s invariably the CVR_DB that gets wedged.

```
worker=replicator pid=82812 component=change-source started replication stream at 0/57453261 (5o7pvfl)
worker=#1 pid=82812 watermark=5o7pvfm started worker
worker=#1 pid=82812 watermark=5o7pvfm worker done
worker=#1 pid=82812 watermark=5o7pvfm Executed statement (16 ms) ["INSERT INTO cdc.\"ChangeLog\" "," ON CONFLICT DO NOTHING"]
worker=#1 pid=82812 watermark=5o7pvfm Executed statement (16 ms) ["INSERT INTO cdc.\"ChangeLog\" "," ON CONFLICT DO NOTHING"]
worker=#1 pid=82812 watermark=5o7pvfm Executed statement (17 ms) ["INSERT INTO cdc.\"ChangeLog\" "," ON CONFLICT DO NOTHING"]
worker=#1 pid=82812 watermark=5o7pvfm Executed statement (18 ms) ["INSERT INTO cdc.\"ChangeLog\" "," ON CONFLICT DO NOTHING"]
worker=replicator pid=82812 watermark=5o7pvfm transaction pool done
worker=#1 pid=82812 watermark=5o7qmqx started worker
worker=#1 pid=82812 watermark=5o7qmqx worker done
worker=#1 pid=82812 watermark=5o7qmqx Executed statement (8 ms) ["INSERT INTO cdc.\"ChangeLog\" "," ON CONFLICT DO NOTHING"]
worker=#1 pid=82812 watermark=5o7qmqx Executed statement (7 ms) ["INSERT INTO cdc.\"ChangeLog\" "," ON CONFLICT DO NOTHING"]
worker=#1 pid=82812 watermark=5o7qmqx Executed statement (11 ms) ["INSERT INTO cdc.\"ChangeLog\" "," ON CONFLICT DO NOTHING"]
worker=#1 pid=82812 watermark=5o7qmqx Executed statement (13 ms) ["INSERT INTO cdc.\"ChangeLog\" "," ON CONFLICT DO NOTHING"]
worker=replicator pid=82812 watermark=5o7qmqx transaction pool done
...
worker=dispatcher pid=82810 all workers ready (4883 ms)
worker=dispatcher pid=82810 Server listening at http://[::1]:3000
worker=dispatcher pid=82810 connecting ith0ponqs4n5aaa2k1 to syncer 5
worker=syncer pid=82818 Initial snapshot at version 62oupsxs
worker=syncer pid=82818 component=Mutagen serviceID=ith0ponqs4n5aaa2k1 mutationID=1 processMutation Process mutation start {"type":"crud","id":1,"clientID":"9s0bjks0s9u2sh4g73","name":"_zero_crud","timestamp":1726339644284,"args":[{"ops":[{"op":"update","entityType":"issue","id":{"id":"4pQVnTwnc7_dup6"},"partialValue":{"id":"4pQVnTwnc7_dup6","status":3,"modified":5039707750405}}]}]}
worker=syncer pid=82818 component=Mutagen serviceID=ith0ponqs4n5aaa2k1 mutationID=2 processMutation Process mutation start {"type":"crud","id":2,"clientID":"9s0bjks0s9u2sh4g73","name":"_zero_crud","timestamp":1726339646439,"args":[{"ops":[{"op":"update","entityType":"issue","id":{"id":"6X80NWiWy4_dup6"},"partialValue":{"id":"6X80NWiWy4_dup6","status":4,"modified":5039707752561}}]}]}
worker=syncer pid=82818 component=Mutagen serviceID=ith0ponqs4n5aaa2k1 mutationID=3 processMutation Process mutation start {"type":"crud","id":3,"clientID":"9s0bjks0s9u2sh4g73","name":"_zero_crud","timestamp":1726339669586,"args":[{"ops":[{"op":"update","entityType":"issue","id":{"id":"4S-SqnpyTT_dup6"},"partialValue":{"id":"4S-SqnpyTT_dup6","status":2,"modified":5039707775708}}]}]}
...
worker=syncer pid=82818 component=view-syncer serviceID=ith0ponqs4n5aaa2k1 {"name":"PostgresError","message":"sorry, too many clients already",
"stack":"PostgresError: sorry, too many clients already\n
    at ErrorResponse (file:///Users/ocean/roci/mono/node_modules/postgres/src/connection.js:788:26)\n
    at handle (file:///Users/ocean/roci/mono/node_modules/postgres/src/connection.js:474:6)\n
    at Socket.data (file:///Users/ocean/roci/mono/node_modules/postgres/src/connection.js:315:9)\n
    at Socket.emit (node:events:513:28)\n    at Socket.emit (node:domain:489:12)\n
    at addChunk (node:internal/streams/readable:324:12)\n
    at readableAddChunk (node:internal/streams/readable:297:9)\n
    at Socket.Readable.push (node:internal/streams/readable:234:10)\n
    at TCP.onStreamRead (node:internal/stream_base_commons:190:23)"}
worker=syncer pid=82818 closed database connections
worker=syncer pid=82818 component=view-syncer serviceID=ith0ponqs4n5aaa2k1 view-syncer stopped
worker=syncer pid=82818 component=view-syncer serviceID=ith0ponqs4n5aaa2k1 clientID=k8372iknd8b3c52dag wsID=nojamv6FjpFWP_LByHK23 cmd=initConnection closing connection with error {"name":"PostgresError","message":"sorry, too many clients already","stack":"PostgresError: sorry, too many clients already\n    at ErrorResponse (file:///Users/ocean/roci/mono/node_modules/postgres/src/connection.js:788:26)\n    at handle (file:///Users/ocean/roci/mono/node_modules/postgres/src/connection.js:474:6)\n    at Socket.data (file:///Users/ocean/roci/mono/node_modules/postgres/src/connection.js:315:9)\n    at Socket.emit (node:events:513:28)\n    at Socket.emit (node:domain:489:12)\n    at addChunk (node:internal/streams/readable:324:12)\n    at readableAddChunk (node:internal/streams/readable:297:9)\n    at Socket.Readable.push (node:internal/streams/readable:234:10)\n    at TCP.onStreamRead (node:internal/stream_base_commons:190:23)"}
worker=syncer pid=82818 connection clientID=k8372iknd8b3c52dag clientGroupID=ith0ponqs4n5aaa2k1 wsID=nojamv6FjpFWP_LByHK23 errorKind=Internal Sending error on WebSocket ["error","Internal","PostgresError: sorry, too many clients already"] {"name":"PostgresError","message":"sorry, too many clients already","stack":"PostgresError: sorry, too many clients already\n    at ErrorResponse (file:///Users/ocean/roci/mono/node_modules/postgres/src/connection.js:788:26)\n    at handle (file:///Users/ocean/roci/mono/node_modules/postgres/src/connection.js:474:6)\n    at Socket.data (file:///Users/ocean/roci/mono/node_modules/postgres/src/connection.js:315:9)\n    at Socket.emit (node:events:513:28)\n    at Socket.emit (node:domain:489:12)\n    at addChunk (node:internal/streams/readable:324:12)\n    at readableAddChunk (node:internal/streams/readable:297:9)\n    at Socket.Readable.push (node:internal/streams/readable:234:10)\n    at TCP.onStreamRead (node:internal/stream_base_commons:190:23)"}
worker=syncer pid=82818 connection clientID=k8372iknd8b3c52dag clientGroupID=ith0ponqs4n5aaa2k1 wsID=nojamv6FjpFWP_LByHK23 close
worker=syncer pid=82818 component=Mutagen serviceID=ith0ponqs4n5aaa2k1 mutationID=2 processMutation Ignoring mutation from 9s0bjks0s9u2sh4g73 with ID 2 as it was already processed. Expected: 4
worker=syncer pid=82818 component=Mutagen serviceID=ith0ponqs4n5aaa2k1 mutationID=2 processMutation Process mutation complete in 20
worker=dispatcher pid=82810 received request 127.0.0.1:3000 /api/canary/v0/get?id=P4zVqN9Grgum31U2uw4hY
worker=syncer pid=82818 component=Mutagen serviceID=ith0ponqs4n5aaa2k1 mutationID=3 processMutation Ignoring mutation from 9s0bjks0s9u2sh4g73 with ID 3 as it was already processed. Expected: 4
worker=syncer pid=82818 component=Mutagen serviceID=ith0ponqs4n5aaa2k1 mutationID=3 processMutation Process mutation complete in 21
worker=syncer pid=82818 component=Mutagen serviceID=ith0ponqs4n5aaa2k1 mutationID=1 processMutation Ignoring mutation from 9s0bjks0s9u2sh4g73 with ID 1 as it was already processed. Expected: 4
worker=syncer pid=82818 component=Mutagen serviceID=ith0ponqs4n5aaa2k1 mutationID=1 processMutation Process mutation complete in 24
```

I also get the error when attempting to inspect the DB sessions locally:

<img width="839" alt="Screenshot 2024-09-13 at 16 08 10" src="https://github.com/user-attachments/assets/e976c5e8-7c65-46c1-a703-9784f2c228f9">

One thing that is interesting and perhaps telling is that both the upstream and replica (running in Docker) appear to get wedged in this state:

<img width="1367" alt="Screenshot 2024-09-13 at 16 17 53" src="https://github.com/user-attachments/assets/3d256889-14a4-456e-94d6-e56b89fb23d0">
','public'),
	 ('AWj8hMrcbwFLPAXUWSZ1m','CVR sometimes getting updated properly on initialization',false,1726196672000,1726194999000,'ieK09sy2C_AIWE8KRkrQR',NULL,'Just got this when clearing application data and reloading.

@cesara has seen this too.

```
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
```
','public'),
	 ('HC7kWsm0qUYvf2BqjfiD_','Replicator dies when attempting to commit its (begin concurrent) transaction while litestream has the lock',false,1726188938000,1726184763000,'ieK09sy2C_AIWE8KRkrQR',NULL,'<img width="1048" alt="Screenshot 2024-09-12 at 16 34 08" src="https://github.com/user-attachments/assets/b95390ff-7acc-4975-b2f3-19ff70dfdff1">

Improvements, not mutually exclusive:
* Handle lock acquisition failures better
* Restart the replicator worker on failure

@cesara ','public'),
	 ('A_5GErKdgkngmbFRyygQp','LSN-order is not suitable for storage of Postgres Replication messages',false,1726276791000,1726184649000,'ieK09sy2C_AIWE8KRkrQR',NULL,'The change-streamer currently uses the LSN of replication messages to sort change messages for replay. However, LSNs for messages of multiple transactions may be interleaved, and while the upstream postgres will partition out transactions while streaming them (in commit order), the LSN numbers themselves will still be interleaved, so if we try to replay them in LSN order, we''ll get interleaved transactions.

<img width="1113" alt="Screenshot 2024-09-12 at 17 58 21" src="https://github.com/user-attachments/assets/c4549ca6-ce93-4a0a-be1d-5540d9ece514">

So we need to either:
1. figure out a different watermark scheme for ordering ChangeLog entries so that they are ordered correctly in contiguous transactions, possibly reordering them on commit, or
2. implement postgres''s partitioning logic when streaming them back.

I''m guessing that (1) will be more straightforward. 

@arv FYI','public'),
	 ('A6JEvOx1Lpj5qQgiy0_R-','Sort and remove duplicates from refs',true,1722450720000,1722450712000,'nqYkxAGMnzk7Y5STjZryV',NULL,'We have duplicates in the `c/<HASH>/m` keys in the kv store. These keys are also not sorted.

Removing the duplicates makes the data slightly smaller and we have less work to do even though we do cache things so we do not end up going to IDB twice.

Sorting could also potentially help with the lookup speed if IDB uses a cursor internally.','public'),
	 ('dSHOxE86nkbGafajdd1ew','UnknownError: Internal error opening backing store for indexedDB.open.',true,1722332514000,1722332514000,'nqYkxAGMnzk7Y5STjZryV',NULL,'https://discord.com/channels/830183651022471199/1267770276972593162

This was a corrupted chrome user profile

But maybe we should expose these kind of errors on the Replicache instance so that the app can tell the user what is going on.

We could also fallback to MemStore in this case.','public'),
	 ('Fv76FHJw6xEfMDZfRd0uv','Presence keys not removed when room is invalidated',true,1719261305000,1719261305000,'nqYkxAGMnzk7Y5STjZryV',NULL,'> another issue I''ve found with the presence key - when I invalidate the room using connections/rooms:invalidate the presence keys of the disconnected users are still there

Seems like we need to go through the REST endpoints and see how they impact the presence keys.','public'),
	 ('_6cIk1pH24n6nmJ5sI6Ar','Buffer tail messages during authHandler and roomStartHandler',true,1718613057000,1718613057000,'nqYkxAGMnzk7Y5STjZryV',NULL,'We do not currently have a way for the `reflect tail` log to receive console log messages during the authHandler and roomStartHandler (I believe)

https://discord.com/channels/830183651022471199/1020392595450507304/1251704780196151317

I think we would need to buffer the `console.log` calls in auth and room start handler to support these to be sent to the tail client.','public'),
	 ('oGP3NonM-zTtvcZ1ibD60','TODO: Add unit tests for statement',true,1716997963000,1716997960000,'nqYkxAGMnzk7Y5STjZryV',NULL,'              TODO: Add unit tests for statement

_Originally posted by @arv in https://github.com/rocicorp/mono/pull/1959#discussion_r1619128486_
            ','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('gW4A4-tVeA0e0YrSsYLqX','zql: Limit needs to pull more data',true,1716811440000,1716811440000,'nqYkxAGMnzk7Y5STjZryV',NULL,'When we have a query with a limit. If a row is removed or changed so that it no longer is in the view we should pull for more data from the source.

#1866 has tests for this case and these tests fail

The problem is that we do not know that we changed the min/max until we are in the commit phase and pullHistoricalData does not work in the commit phase.

','public'),
	 ('y1MUdMqchAamKfMC-6aFU','ZQL: Write test for remove all',true,1715968725000,1715968664000,'nqYkxAGMnzk7Y5STjZryV',NULL,'I should really have written a test for this... TODO

_Originally posted by @arv in https://github.com/rocicorp/mono/pull/1848#issuecomment-2117936187_','public'),
	 ('azP-Z7eh40PAVtIliJ-12','View Syncer b0rked with invalid snapshot identifier',false,1715906651000,1715877820000,'ieK09sy2C_AIWE8KRkrQR',NULL,'<img width="2503" alt="Screenshot 2024-05-16 at 09 41 53" src="https://github.com/rocicorp/mono/assets/132324914/8c161d0a-3dd5-4b59-adb9-cbba6c9a43c1">

I bet this has to do with the TransactionPool caching. I''ll revert that to get things working and study the logs post-mortem.','public'),
	 ('EXpLtXkeB--ideoLqoopz','Zeppliear: Exception because issue missing properties',true,1715699188000,1715676220000,'nqYkxAGMnzk7Y5STjZryV',NULL,'To reproduce scroll down in Zeppliear.

![image](https://github.com/rocicorp/mono/assets/45845/5b80ca74-1600-438b-b40d-18e7f9f35d7c)

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

but the type of `row` is supposed to be `{issue: Issue; labels: string[]};`','public'),
	 ('Lo98nXz8eCj624qfhyfMd','Zeppliear: limit is broken ',false,1716811486000,1715675567000,'nqYkxAGMnzk7Y5STjZryV',NULL,'I was hitting this when testing Zeppliear

In Zeppliear we have a limit of 200 but we end up with a case where we get to `#limitedAddAll` where  the size of the BTree is 201 (changing the limit to 10 hits a case where the data.size is 11):

https://github.com/rocicorp/mono/blob/baff399d78101ac77813803516b06a0c84fa4209/packages/zql/src/ivm/view/tree-view.ts#L142-L144


I did some debugging and the problem seems to be that the we call tree `set` without going through the _limit function_ so the tree size is larger than we expect.

https://github.com/rocicorp/mono/blob/baff399d78101ac77813803516b06a0c84fa4209/packages/zql/src/ivm/source/set-source.ts#L173-L175
','public'),
	 ('GgpFqe8eiV-A3d6_AEMk5','Type Generation for Client API',true,1715671242000,1715671153000,'OeVnr1y5bEM_Yg06sUFtD',NULL,NULL,'public'),
	 ('wXojICEjDrM2vZK7SXBSI','Auth',true,1715671253000,1715671048000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Right now we have a few paragraphs of text and a code block. We need to design and implement both authentication and authorization.','public'),
	 ('Y3WTx0k75v5gebho0Yps0','test',false,1715671278000,1715670370000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'test','public'),
	 ('-Thbh2750u8nQUnsjkl2X','AST `field` should be split into `table` and `column` parts',false,1716564468000,1715198543000,'tDY6IbKdVqbBlRBc3XMwF',NULL,'All `fields` should be tuples of `table`, `column` -- https://github.com/rocicorp/mono/blob/main/packages/zql/src/ast/ast.ts#L19

Right now we encode table + column via a dot in the string.','public'),
	 ('IFSSd3eg8K3Sr9Pli5xiI','Implement sub-queries / move to north star query API',true,1715196481000,1715196474000,'tDY6IbKdVqbBlRBc3XMwF',NULL,'To get us to our aspirational API and remove the awkward:

```
issue
  .join(issueLabel)
  .join(label)
  .groupBy(issue.id)
  .select(issue.*, agg.array(label))
```

to:

```
issue.select(issue.*, nest(''labels'', q => q.queryLabels()))
```

- Requires #1779 for the nice `q.queryLabels` sort of syntax','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('2_l4gdyWVihEYEY3qwL2W','Simplify `join` by using client side schema information',true,1715196272000,1715196272000,'tDY6IbKdVqbBlRBc3XMwF',NULL,'- #1775 

Once we know available foreign keys the `Join` API can go from:

```
issue.join(issueLabel, ''issueLabel'', ''id'', ''issueID'')
```

to:

```
issue.join(issueLabel)
```','public'),
	 ('s7nKQH4tFZB6n0Y-UaVXE','AST and QueryBuilder type divergence',true,1715196166000,1715196166000,'tDY6IbKdVqbBlRBc3XMwF',NULL,'- where conditions definitely have duplicative structures
- order by too
- others?','public'),
	 ('Wh3BGUvDYTfCdTKsUm4aw','typescript types for `join` selectors on the right table are incorrect',true,1716564397000,1715192998000,'tDY6IbKdVqbBlRBc3XMwF',NULL,'If you alias a table that you join, TypeScript tells you that the selector in un-aliased. This is wrong. The selector needs to use the alias name.

```ts
issue.join(label, ''stuff'', ''id'', ''label.id'')
```

''label.id'' should be ''stuff.id''','public'),
	 ('jdZuxcMAcF22nwLZ2-wI6','Need schema information on the client',true,1716564478000,1715192910000,'tDY6IbKdVqbBlRBc3XMwF',NULL,'for:

1. compound primary keys
2. #1700 
3. better ergonomics for foreign key traversal','public'),
	 ('oMXXm_O2ekVt9C8koCkou','Zeppliear: Back button no longer works',true,1715085530000,1715085530000,'nqYkxAGMnzk7Y5STjZryV',NULL,'We used to be able to go back and forth using the browser back and forward buttons','public'),
	 ('gZw3ftjun5SZcLwQN7qre','Zero/Reflect: Consider using websocket ping frames?',false,1715066131000,1714987020000,'nqYkxAGMnzk7Y5STjZryV',NULL,'The websocket protocol has built in ping support.

https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API/Writing_WebSocket_servers#pings_and_pongs_the_heartbeat_of_websockets

https://www.rfc-editor.org/rfc/rfc6455

Do we really need application level support for ping/pong?','public'),
	 ('qjceEA0PK8JwC8ZtIRheS','ZQL: need to ignore incomplete rows',false,1718868285000,1714601979000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Say we have a row cached with columns {a, b, c}. If we select the same row with columns {a, b, c, d}, we shouldn''t return the cached row.','public'),
	 ('02kEEyJGmES9OjXXAkyBA','ZQL - double check operators are all lazy where possible',true,1715649261000,1714484962000,'tDY6IbKdVqbBlRBc3XMwF',NULL,'map & filter & the newest distinct operator are lazy & 0-copy in terms of making new collections. All operators should be lazy if possible.

- can join be more lazy?
- reduce?
- agg & full agg operators?','public'),
	 ('SihBUomWCf-ksK8hBvy6p','[notes] ZQL Join & Sub-query Optimization',true,1714760950000,1714155182000,'tDY6IbKdVqbBlRBc3XMwF',NULL,'- If/when we need to optimize joins, there are some thoughts here in the description: https://github.com/rocicorp/mono/pull/1644

One item not mentioned there is that we can also re-order where `join` occurs in the data flow graph in order to:
1. Compute a join once and share it among all queries
2. Constrain a join to a subset of the table being joined

These same optimizations apply to correlated sub-queries.

An interesting little bit about sub-queries is that if the sub-query is not used in a `where` or `order-by`, we can completely skip evaluating it until the limited view returns it.

E.g.,
```ts
issue.select(user.select..., title, id).limit(10)
```

We can wait to evaluate the `user.select` until we know the final 10 rows being returned.


related docs:
- https://www.notion.so/replicache/No-Memory-IVM-4ff5c1746fc14cf6a4a8b283cfe48e0e
- https://www.notion.so/replicache/Sub-Query-Implementation-Options-50ecab0b78b44c7aa04114d440e5a046','public'),
	 ('MZwjPkwQNzpkrw57gVWcr','RFE: `tx.update()`',false,1709536330000,1679234721000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'The following pattern is very, very common in Replicache apps:

```
mutators: {
  updateThing: async (tx: WriteTransaction, update: Partial<Thing>) {
    const prev = await tx.get(update.id);
    await tx.put(update.id, {...prev, ...update});
  }
}
```

We chould automate by providing a helper:

```
mutators: {
  updateThing: async (tx: WriteTransaction, update: Partial<Thing>) {
    await tx.update(update.id, update);
  }
}
```

Perhaps there could also be some options to create if item not already present.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('AuiUsUv4Q9XFVqfbGlx9U','zql devx issues',true,1714157742000,1714130250000,'tDY6IbKdVqbBlRBc3XMwF',NULL,'1. The developer having to provide `FromSet` is awkward. Can this just be a list of `Entity` types instead? E.g., `EntityQuery<FromSet>` -> `EntityQuery<...Entities>`
2. join changing the nesting level of the results of a query could be problematic. E.g., from `Issue[]` to `{issue: Issue, label: Label}[]` See commit: https://github.com/rocicorp/mono/pull/1640/commits/ac0deea5b64f3a4c50565c1ad11285eabaebf7a7 where the query is changed to join labels and the impact ripples through the app. The commit comment documents a few options.
3. Select changing the return type of a query makes it impossible to re-assign to the same variable after applying select. `q = query; q = q.select(''foo'') // type error`
4. `Return` being on a type param in the EntityQuery makes it hard to take `EntityQuery` as an arg to a function and error prone when defining filters.
   1. Specifically, if someone fumbles `Return` and/or `FromSet` then they will write unqualified `where` when the `where` requires qualification. The root of the problem is that `{a: any}` is wider than `{a: any, b: any}` and we allow unqalified `where` if the `FromSet` only has a single prop.
5. Because `join` changes generics on the query, having a function that updates the query with new `HAVING` filters (and no selects) can produce an incompatible query. Specifically: https://github.com/rocicorp/mono/pull/1640/commits/183979c6cea435e11d4ab9fca977b412c312fa0c#diff-a275f49b14e8e300928bee76064f80e20e5cba32e36975fda20caa65a395075cR383
   1. We can remove most of this problem by introducing `whereExists`
   2. Maybe a non-joined query should be wider than a joined query in a type sense? So the one with joins can be assigned to the one without?','public'),
	 ('GUIB-n9eAZy82_FZ_c2Ww','`where` applied prior to `join` does not set a qualified selector in the `ast`',false,1714429076000,1714066521000,'tDY6IbKdVqbBlRBc3XMwF',NULL,'Zeppliear does:

```ts
query = issue.where(''priority'', ''in'', x);
```

then

```
query.join(''issueLabels'', ...)
```

but the first `where` doesn''t write itself into the AST as a qualified selector. This means that after the `join` we don''t know what to compare against.

We should always use fully qualified selectors under the hood, even if the user does not provide it.','public'),
	 ('HEQKdh59ynsn-lscZfHAr','zql subscribe callback called with wrong arguments',true,1714034881000,1714034881000,'nqYkxAGMnzk7Y5STjZryV',NULL,'If I do 


```js
query.subscribe((...args) => {
  console.log(...args);
})
```

It will log the value but also the internal zql tx version.','public'),
	 ('Ug-22TAa01jHLgsN5C_4B','Zeppliear `count` & `reduce` are incorrect by a constant factor',false,1715193127000,1714004632000,'tDY6IbKdVqbBlRBc3XMwF',NULL,'For some reason the Zeppliear issue count is sometimes 2 and sometimes 3 times larger than expected 🤔
','public'),
	 ('IxiKp7PlkscyxnOIJbBTi','ZQL: Optimization in SetSource?',false,1713202888000,1713187276000,'nqYkxAGMnzk7Y5STjZryV',NULL,'We have the following optimization:

```ts
      onCommitEnqueue: (version: Version) => {
        for (let i = 0; i < this.#pending.length; i++) {
          const [val, mult] = must(this.#pending[i]);
          // small optimization to reduce operations for replace
          if (i + 1 < this.#pending.length) {
            const [nextVal, nextMult] = must(this.#pending[i + 1]);
            if (
              Math.abs(mult) === 1 &&
              mult === -nextMult &&
              comparator(val, nextVal) === 0
            ) {
              // The tree doesn''t allow dupes -- so this is a replace.
              this.#tree = this.#tree.add(nextMult > 0 ? nextVal : val);
              ++i;
              continue;
            }
          }
          if (mult < 0) {
            this.#tree = this.#tree.delete(val);
          } else if (mult > 0) {
            this.#tree = this.#tree.add(val);
          }
        }

        this.#stream.newDifference(version, this.#pending);
        this.#pending = [];
      },
```

However, how does the old value get deleted from the Set?','public'),
	 ('LzMGiIAzW4_sGY1Eq4mL3','rolling back to previous version of reflect causes rooms to break ',true,1712015712000,1712015712000,'pgyTvcxh2hjmq2l4WKzK6',NULL,'if a user installs a version of Reflect past "0.39.202402230127" and rolls back to "0.39.202402230127" their rooms will encounter this issue.


```text
[Unhandled exception in #processNext, {stack=TypeError: Unexpected property deleted
    at parse (index.js:5388:11)
    at validateOrNormalize (index.js:7183:20)
    at listEntries (index.js:7178:10)
    at async listClientRecords (index.js:8004:19)
    at async fastForwardRoom (index.js:8130:29)
    at async processRoom (index.js:9071:23)
    at async processPending (index.js:9217:27)
    at async #processNextInLock (index.js:10781:65)
    at async index.js:10770:11
    at async index.js:9436:16, name=TypeError, message=Unexpected property deleted}]
``` ','public'),
	 ('1YIoRcTwfwaxnGtAg8Luj','Cloudflare returning `null` from DNSRecords.list()',true,1711992914000,1711992914000,'ieK09sy2C_AIWE8KRkrQR',NULL,'We''re seeing an error in our `setCustomHostnames()` method for both publishes and deletes.


https://console.cloud.google.com/monitoring/alerting/incidents/0.nb7hvre49xxg?channelType=slack&project=reflect-mirror-prod


```
TypeError: Cannot read properties of null (reading ''forEach'')
    at setCustomHostnames (file:///workspace/out/index.js:14639:18)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async publishCustomHostname (file:///workspace/out/index.js:14616:20)
    at async NamespacedScriptHandler._doPublish (file:///workspace/out/index.js:15259:22)
    at async NamespacedScriptHandler.publish (file:///workspace/out/index.js:15190:22)
    at async runDeployment (file:///workspace/out/index.js:15487:22)
    at async file:///workspace/out/index.js:15393:5
```

```
TypeError: Cannot read properties of null (reading ''forEach'')
    at setCustomHostnames (file:///workspace/out/index.js:14639:18)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async deleteCustomHostnames (file:///workspace/out/index.js:14621:20)
    at async NamespacedScriptHandler.delete (file:///workspace/out/index.js:15271:7)
    at async runDeployment (file:///workspace/out/index.js:15465:7)
    at async file:///workspace/out/index.js:15393:5
```



In the [code](https://github.com/rocicorp/mono/blob/c6020d36b62935fec5459d06e969d44590d09143/mirror/mirror-server/src/cloudflare/publish-custom-hostnames.ts#L34), the only time the target of "forEach" can be null is when it is called on the return value of DNSRecords.list():

<img width="611" alt="Screenshot 2024-04-01 at 10 22 38" src="https://github.com/rocicorp/mono/assets/132324914/d395c254-2fc0-4d8b-82cb-bd6dbc6f6782">

And indeed the errors always follow a call to `GET /zones/1b044253688b6ddb8e67738539a2b6d0/dns_records`

<img width="1202" alt="Screenshot 2024-04-01 at 10 24 53" src="https://github.com/rocicorp/mono/assets/132324914/8dff045c-e99a-49bc-b0c0-c956c56acef5">

So evidence points to the GET DNSRecords API call returning `null` (presumably instead of an empty list). 

I can''t yet tell whether this is new behavior that we have to adjust to, or a bug that they''ve since rolled back. I will keep an eye on this.','public'),
	 ('Aoa5PXN4vkXX5z_Wb9M5W','frequent error in logs around WebSocket send() after close()',true,1710962933000,1710962932000,'pgyTvcxh2hjmq2l4WKzK6',NULL,'`
[Unhandled exception in handleMessage, {stack=TypeError: Can''t call WebSocket send() after close().     at sendErrorInternal (reflect-server.js:3351:6)     at closeWithErrorInternal (reflect-server.js:3334:3)     at closeWithError (reflect-server.js:3331:3)     at handleMessage (reflect-server.js:5920:5)     at reflect-server.js:6649:15     at reflect-server.js:5368:22     at run (reflect-server.js:1179:18)     at async LoggingLock.withLock (reflect-server.js:5357:20)     at async #handleMessageInner (reflect-server.js:6648:7), name=TypeError, message=Can''t call WebSocket send() after close().}]
`


[datadog url](https://app.datadoghq.com/logs?query=-source%3A%28heroku%20OR%20client%20OR%20cloudflare%29%20status%3Aerror&agg_q=status%2Cservice&agg_q_source=base%2Cbase&cols=host%2Cservice&fromUser=true&index=%2A&messageDisplay=inline&refresh_mode=sliding&sort_m=%2C&sort_t=%2C&storage=hot&stream_sort=desc&top_n=10%2C10&top_o=top%2Ctop&viz=pattern&x_missing=true%2Ctrue&from_ts=1710876290597&to_ts=1710962690597&live=true)','public'),
	 ('WnlHcMY3_QYYPJYKfvLOZ','Simplify IDB handling for Firefox 115',true,1710489884000,1710488861000,'nqYkxAGMnzk7Y5STjZryV',NULL,'https://www.mozilla.org/en-US/firefox/115.0/releasenotes/

> [IndexedDB](https://w3c.github.io/IndexedDB/) is now also supported in [private browsing](https://bugzilla.mozilla.org/show_bug.cgi?id=1639542) without memory limits thanks to encrypted storage on disk. The temporary keys to decrypt the information are held in RAM only and all stored information is purged at the normal end of a private browsing session from disk.

They way Replicache deals with this is that it catches an exception and switches to an in memory store. With Firefox 115 this exception is no longer triggered. This means that we already use IDB in Firefox private browsing but there is room to simplify the code to remove this fallback.','public'),
	 ('d12exOmd6AAzgWiUqCc5i','Watch with empty result not working as intended?',false,1710452110000,1710447788000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Looks like watch has logic for this case, but evidently not working as intended
https://github.com/rocicorp/mono/blob/4aa964038da9aff98ba0d8a43d5032fcadb072ac/packages/replicache/src/subscriptions.ts#L267
cc @arv

_Originally posted by @grgbkr in https://github.com/rocicorp/rails/pull/33#discussion_r1525273482_
            ','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('1M1sj4szpJElpkdR4Wo0F','Rename authHandler and roomStartHandler',true,1708950746000,1708948359000,'nqYkxAGMnzk7Y5STjZryV',NULL,'We should rename these to `onAuth` and `onRoomStart`.

Regarding naming for callbacks we still have two of the ''Handler'' suffix style (`authHandler` and `roomStartHandler`).   What is your thinking on when a callback option should have the ''Handler'' suffix vs the ''on'' prefix?

_Originally posted by @grgbkr in https://github.com/rocicorp/mono/issues/1431#issuecomment-1954824651_
            ','public'),
	 ('cwteAankkXYTAV4nfFi5S','Fix update version in disconnect handler etc',true,1708940240000,1708940235000,'nqYkxAGMnzk7Y5STjZryV',NULL,'I''m finding the version updating a bit hard to follow.  

I believe there are some cases where collectClientIfDeleted may modify the user values but version does not get updated.

From my reading collectClientIfDeleted, does not itself call putVersion, and prior to this patch relied on its caller to call putVersion.  It does now call callCloseHandler which sometimes calls putVersion, but only if the closeHandler actually modified user values.

So in the case that the above disconnectHandler throws an exception (and thus does not putVersion), the closeHandler does not modify user values, and  collectOldUserSpaceClientKeys is run and deletes some user values, I believe we end up with user values modified but version not updated.

_Originally posted by @grgbkr in https://github.com/rocicorp/mono/pull/1418#discussion_r1496149854_
            ','public'),
	 ('V3uumaQ83jP-yQAPkxUyc','Check for client tombstone in connect',true,1709537138000,1708938736000,'nqYkxAGMnzk7Y5STjZryV',NULL,'That reminds me. I don''t think that check is in place.

_Originally posted by @arv in https://github.com/rocicorp/mono/pull/1420#discussion_r1485232520_
            ','public'),
	 ('U5pWKpe7w-EekKWr7e2VZ','Add comment to code',true,1709537152000,1708938663000,'nqYkxAGMnzk7Y5STjZryV',NULL,'We should add something to the code that covers this:


Regarding 

"Another gotcha here is that when we process disconnect we collect the client even if lastMutationId is larger than lastMutationIdAtClose. Intuitively this seems correct but this could use some more thought."

I think this is correct.  I think the initial assumption was that there can be no mutations from the client with a lastMutationID larger than the one sent it sent in the close beacon, but then you discovered that is not quite true, a client can sneak mutations over the websocket as the page is unloading. If it manages to do this its still correct to collect it when it disconnect.

What would be problematic is if the client:
1. manages to execute mutations after the close beacon
2. does not manage to send them over the websocket
3. but does manage to persist them to idb (via persists)

In this case the server may collect the client, but the client group may later push mutations from that client.  I think the client group might get wedged in this case.  Do you think this is possible?

_Originally posted by @grgbkr in https://github.com/rocicorp/mono/issues/1407#issuecomment-1936865370_
            ','public'),
	 ('GInRIPj0w4RGU9O5LZylY','Better message when calling `npx reflect keys` for users with no apps?',false,1709681779000,1708746279000,'ieK09sy2C_AIWE8KRkrQR',NULL,'The error alerts have reported a cli error that we''re displaying when a user with no apps (and thus no teams) calls `npx reflect keys`:

```
Error: You are not an admin of any teams
```

Note that we''ll also display this message for `npx reflect apps list` and anything else that looks up the team.

Not an urgent priority, but perhaps we can tailor the messages better.

@aboodman ?

<img width="1262" alt="Screenshot 2024-02-23 at 19 41 18" src="https://github.com/rocicorp/mono/assets/132324914/093abf3e-916e-4c97-bc6c-2a5097fddd78">
','public'),
	 ('4CuOpzv26yKxhL47hizM_','DNS Validation Error from Cloudflare',false,1708746207000,1708635824000,'ieK09sy2C_AIWE8KRkrQR',NULL,'Need to look into when this happens.

@aboodman 

<img width="1927" alt="Screenshot 2024-02-22 at 13 02 41" src="https://github.com/rocicorp/mono/assets/132324914/f7c1b6da-6432-4dec-92e9-cedb4fe24a2b">
','public'),
	 ('dct7Ai7_EypFcGxEDioPU','User invalidations return `410: Gone` for deleted rooms',false,1708109133000,1707802650000,'ieK09sy2C_AIWE8KRkrQR',NULL,'We''re returning `410: Gone` for requests of the form:

```
https://mapkeep-preproduction-20231217-tristanls.reflect-server.net/api/v1/connections/users/c9be64ea-0802-4a0e-85a9-d16f6cbe6e53_deleteme:invalidate
```

I think the AuthDO is forwarding invalidate requests to rooms that have been deleted.

@grgbkr, is this a situation that is expected?
* If not, what''s the fix?
* If so, perhaps we can treat 410: Gone responses from rooms as OK in the case of invalidations?

I''m happy to make a fix if you''d like, but I''d like to confirm whether this is exposing some kind of bookkeeping bug.

<img width="1495" alt="Screenshot 2024-02-12 at 21 32 06" src="https://github.com/rocicorp/mono/assets/132324914/be15c743-5e33-45c0-b3b3-7b2027d26e68">
','public'),
	 ('XW1MbnGrrr8zn8M3os47u','`410: Gone` responses are counted as warnings',false,1707802349000,1707802201000,'ieK09sy2C_AIWE8KRkrQR',NULL,'When an API room:delete is called on a deleted room, the `reflect-server` returns `410: Gone`.

This is the intended behavior, but the error reporting system is flagging these as warnings (to us) and triggering alerts above a certain threshold. 

Perhaps we should return a different error for `410: Gone`. A delete of a deleted room should probably be a 2xx response? Maybe `204: No content`?

@grgbkr, any opinions?','public'),
	 ('8yjdh-9gaDHKm9acvePLq','Rest invalidate-by-user API does not properly handle user ID''s with colons in them',false,1707963679000,1707801918000,'ieK09sy2C_AIWE8KRkrQR',NULL,'API calls to urls that look like:

```
https://api-apps-emelgxcryq-uc.a.run.app/v1/apps/lq9xwmf6/connections/users/00969251-c431-457a-b581-b3b3a372c1e2:villagermapper:invalidate
```

are resulting in an error at the parsing stage:

```
HttpsError: Invalid resource or command "connections:villagermapper:invalidate" 
```

<img width="1503" alt="Screenshot 2024-02-12 at 21 23 27" src="https://github.com/rocicorp/mono/assets/132324914/eacce58f-cfa0-4171-9cb2-cd376dbebfe5">
','public'),
	 ('-qM7gov1mBTYHPBpDoq2e','Clean up and complete Reflect server client hooks',true,1707388515000,1707388515000,'nqYkxAGMnzk7Y5STjZryV',NULL,'We now have

- `disconnectHandler` which gets called when a client is disconnected (network dropped).
- `closeHandler` which gets called when a client (`clientID`) sends a close beacon or it gets collected when the client has not been seen for 2 weeks.
It seems like we might want to introduce `connectHandler` and `openHandler` for symmetry.

Maybe the API names should be?

- `onClientCreate`
- `onClientConnect`
- `onClientDisconnect`
- `onClientDelete`
','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('JMNwL7DMGjz9qr2Xy9cxn','docs needed: closeHandler',true,1707388353000,1707388352000,'nqYkxAGMnzk7Y5STjZryV',NULL,'#1418 added a closeHandler to reflect server options.

This is called when the client is GC''d on the server.

This needs to be documented.','public'),
	 ('JcbPh_jwRN4xlwv3nzRlN','Check isValidAppName() on `--app` so show the user a better error message',false,1707409113000,1707334887000,'ieK09sy2C_AIWE8KRkrQR',NULL,'I think right now we''re showing them some Firestore error.

<img width="1277" alt="Screenshot 2024-02-07 at 11 38 20" src="https://github.com/rocicorp/mono/assets/132324914/034cfc69-5e0e-446c-ac13-aa6b6ec3bd99">

We can instead check `isValidAppName()` and show them a more informative error instead.','public'),
	 ('DSnV1eRo8fT1ajY-FxwPg','Suppress (new) `reflect dev` errors that should be reported as warnings',false,1706916902000,1706915844000,'ieK09sy2C_AIWE8KRkrQR',NULL,'@grgbkr @arv 

https://console.cloud.google.com/monitoring/alerting/incidents/0.n8v6wwmzyyvn?channelType=slack&project=reflect-mirror-prod&pageState=(%22interval%22:(%22i%22:(%22s%22:%222024-02-02T20:43:20.000Z%22,%22e%22:%222024-02-02T21:58:27.000Z%22)))

<img width="1119" alt="Screenshot 2024-02-02 at 15 16 11" src="https://github.com/rocicorp/mono/assets/132324914/da1e208a-5a28-474c-b111-925358fb6251">
','public'),
	 ('H6cCUgJgky0ctjrOul493','Increase Custom Hostname quota in Cloudflare',false,1707857509000,1706895951000,'ieK09sy2C_AIWE8KRkrQR',NULL,'For tracking: https://console.cloud.google.com/errors/detail/COn4rtn636HXuwE;time=P30D?project=reflect-mirror-prod&utm_source=error-reporting-notification&utm_medium=slack&utm_content=resolved-error

```
_FetchResultError: POST /zones/1b044253688b6ddb8e67738539a2b6d0/custom_hostnames: {"result":{"ssl":null},"success":false,"errors":[{"code":1405,"message":"Quota exceeded. If you''re already a paid SSL for SaaS customer, please contact your Customer Success Manager for additional provisioning. If you''re not yet enrolled, please fill out this form and someone from our sales team will contact you: https://www.cloudflare.com/plans/enterprise/contact/."}],"messages":[]}
  at Quota exceeded. If you''re already a paid SSL for SaaS customer, please contact your Customer Success Manager for additional provisioning. If you''re not yet enrolled, please fill out this form and someone from our sales team will contact you: https://www.cloudflare.com/plans/enterprise/contact/. (cloudflare:1405)
```

<img width="1040" alt="Screenshot 2024-02-02 at 09 43 32" src="https://github.com/rocicorp/mono/assets/132324914/f0919d59-53d7-47a3-a962-5a5bdae45c6a">
','public'),
	 ('nwQPt2rxZRrSXCFAWYABs','reflect.net fails the close beacon',false,1707395741000,1706622320000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Something is going on with the auth headers.

<img width="788" alt="Screenshot 2024-01-29 at 22 36 18" src="https://github.com/rocicorp/mono/assets/45845/c96436cb-62e5-4467-826d-6115b9cd7e43">
','public'),
	 ('I_T-VBVpg6QH9T1DXW3jc','Prolonged Cloudflare Analytics outages can result in orphaning data',false,1706236737000,1705951016000,'ieK09sy2C_AIWE8KRkrQR',NULL,'Our metrics aggregation process received an error: `429: Please wait and consider throttling your request speed` (even with just one request) for what looks like at least 5 minutes:

<img width="1692" alt="Screenshot 2024-01-22 at 11 11 56 AM" src="https://github.com/rocicorp/mono/assets/132324914/c13e8951-ccd3-471d-abde-5d10c1f70e5a">

Our Error Reporting shows that this isn''t the first or only time that it''s happened:

https://console.cloud.google.com/errors/detail/CPTJ2fj3s5D8zwE;time=P30D?project=reflect-mirror-prod&utm_source=error-reporting-notification&utm_medium=slack&utm_content=resolved-error

<img width="708" alt="Screenshot 2024-01-22 at 11 14 54 AM" src="https://github.com/rocicorp/mono/assets/132324914/08788984-5d69-4fb6-907a-ca6c89491a92">

We might get lucky because we run the aggregation process 3 times per hour, at minutes 1, 5, and 30 (for the metrics of the previous hour).

However, this is not a reliable guarantee. We should add reliability to this archiving process so that we will theoretically go back to previous hour windows that were not successfully archived.','public'),
	 ('B3NEChF6-jhIk0elyYbtg','Reduce the latency of cold starts of Cloud Functions',false,1705080101000,1705017847000,'ieK09sy2C_AIWE8KRkrQR',NULL,'The brute-force method of avoiding cold starts is to run our functions with a `minInstances: 1` configuration. However, this results in incurring a non-negligible cost:

![Screenshot 2024-01-11 at 3 18 03 PM](https://github.com/rocicorp/mono/assets/132324914/cc6fc8b6-9d54-4364-b5a2-cc3d29d101bd)

A more cost effective method is to amortize the cost of the cold start by pre-initializing the function while we''re waiting for other things to initialize:

1. For the login page, we can ping `user-ensure` when we show the login form.
2. For cli commands, we can ping the destination function while we''re waiting for firebase authentication (creds from `~/.reflect/config/default.json`

An initial POC indicates that this will work:

![Screenshot 2024-01-11 at 3 44 25 PM](https://github.com/rocicorp/mono/assets/132324914/5f83bef3-d093-4b3a-9a33-aa39b175cf0f)

![Screenshot 2024-01-11 at 3 44 51 PM](https://github.com/rocicorp/mono/assets/132324914/6b60704d-e15b-4324-a24b-927afa6b1128)

It shouldn''t take too much work to make this a little cleaner so that we''re not littering our server logs with 401 requests.

@aboodman @cesara ','public'),
	 ('66yOMgedbN6yAq-su_H8h','Move Firestore writes out of the critical API serving path',false,1705008876000,1705002688000,'ieK09sy2C_AIWE8KRkrQR',NULL,'Every time an app key is used, we update the `lastUsed` timestamp of the key, which is surfaced to the user when they list their keys. This is useful, for example, for knowing when keys are used and which keys are no longer used.

![Screenshot 2024-01-11 at 11 41 49 AM](https://github.com/rocicorp/mono/assets/132324914/f69610ba-95b7-459c-b6c2-e4d400c53dc6)

The initial implementation of key authorization performs this Firestore write while authorizing the request. This is fine for manual developer actions like `reflect publish` and `reflect env set|delete`. However, it is less acceptable for actions that affect the actual app runtime, which as API calls.

For example, if we get simultaneous API calls to create or modify (close, delete) 1000 rooms, the calls will bottleneck/serialize at the step where we perform the transactional update of the app key. (And it will incur 1000 Firestore writes when we really only needed one).

Ideally, the update should be done off of the critical path. In fact, the code was written when a [TODO to address this](https://github.com/rocicorp/mono/blob/f5f8334dc50c3b9cd9060379aa0102eec700ba25/mirror/mirror-server/src/functions/validators/auth.ts#L257):

```ts
        txn.update(appKeyDocRef, {lastUsed: FieldValue.serverTimestamp()});
        return {app};
      },
      // TODO(darick): Add a mechanism for initiating writes (like the `lastUsed` timestamp update)
      // in the background so as not to delay request processing. Then this Transaction can be readOnly.
      // {readOnly: true},
    );
```

Note that simply delaying the write to happen later in the function is not an option, as cloud functions do not support "background" processing; the response is only sent after the returned Promise is resolved, and any unresolved Promises are not guaranteed to complete as the function may be hibernated before hand.

https://firebase.google.com/docs/functions/terminate-functions

So to correctly move logic off of the critical path, the logic must be handed off to another process (such as another function).
','public'),
	 ('5j1c8cgCbdgbXQ9WwZfht','Active serving user dashboard Firestore queries',true,1709537194000,1704937088000,'ieK09sy2C_AIWE8KRkrQR',NULL,'## Motivation

[We need an “active serving users” metric](https://www.notion.so/replicache/Reflect-Roadmap-d5a5dfbbf67f4a3a8077804e8250dc06?p=b86657b1edbc49c28e6b984b79563b58&pm=s)

@aboodman 

## Definitions
* An **active team** for a given period (month, day, hour) is one for which there are active room seconds recorded for that team.
* An **active app** can be similarly defined at the scope of an app instead of a team

## Metrics

Room seconds are stored as the `rs` metric in Firestore ([schema](https://github.com/rocicorp/mono/blob/7e70ce828b59bce7941bcae94ad293fe110f466e/mirror/mirror-schema/src/metrics.ts#L10)). An example of what these look like can be seen in [ledger.test.ts](https://github.com/rocicorp/mono/blob/7e70ce828b59bce7941bcae94ad293fe110f466e/mirror/mirror-server/src/metrics/ledger.test.ts#L364)

## Queries

* For yearly or monthly team totals, query the total metrics docs:

```ts
const query = await getFirestore()
    .collectionGroup(''metrics'')
    .where(''yearMonth'', ''=='', null)
    .where(''appID'', ''=='', null)
    .get();
```

From the results you can count the number of documents (i.e. teams) in which there are non-zero `rs` values for each month.

* For yearly or monthly app totals, similarly query:

```ts
const query = await getFirestore()
    .collectionGroup(''metrics'')
    .where(''yearMonth'', ''=='', null)
    .where(''appID'', ''!='', null)
    .get();
```
 
* For daily or hourly team totals, query the monthly metrics docs for the appropriate `yearMonth` value:

```ts
const query = await getFirestore()
    .collectionGroup(''metrics'')
    .where(''yearMonth'', ''=='', 202401)
    .where(''appID'', ''=='', null)
    .get();
```

This is structured similarly to the total metrics docs, but instead of a year/month breakdown, the docs will have day/hour breakdowns for the associated month.

* For the per-app equivalent, similarly change the condition of the `appID`:
 
```ts
const query = await getFirestore()
    .collectionGroup(''metrics'')
    .where(''yearMonth'', ''=='', 202401)
    .where(''appID'', ''!='', null)
    .get();
```

## Permissions / setup

The service account `metrics-dashboard@reflect-mirror-prod.iam.gserviceaccount.com` has been created with Firestore read-access. [Create a key](https://console.cloud.google.com/iam-admin/serviceaccounts/details/116401512738751249544/keys?project=reflect-mirror-prod) for this service account (JSON), download it, and use that to initialize the Firestore client in the dashboard backend.

Note that this key grants read access to all of Firestore and should be considered sensitive. DO NOT check it in to source control. ','public'),
	 ('yrQYuJmcPtW7IRZrHWQFd','`app-autoDeploy` -> `app-deploy` delayed for almost a minute',false,1704914930000,1704845422000,'ieK09sy2C_AIWE8KRkrQR',NULL,'An error was triggered by a 500 response for an `app-deploy` run:

https://console.cloud.google.com/monitoring/alerting/incidents/0.n7y0k7cssgsw?project=reflect-mirror-prod&pageState=(%22interval%22:(%22i%22:(%22s%22:%222024-01-09T22:52:09.000Z%22,%22e%22:%222024-01-09T23:46:01.464Z%22)))

But as far as I can tell, the function succeeded.

![Screenshot 2024-01-09 at 4 01 28 PM](https://github.com/rocicorp/mono/assets/132324914/0f313bd1-5bf6-4ec3-a065-9ccc57a71a75)

A closer look suggests that the error was because it took 1 minute for the command to succeed, which is [default timeout for v2 functions](https://cloud.google.com/functions/docs/configuring/timeout).

Why it took so long is an interesting question. I will increase timeouts and add logging to see if there''s some kind of transaction contention that''s causing a delay.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('lnAbNqboNtqs8HAciVrWU','Suppress error (as warning) when the cli is run from a non-git directory',true,1703267147000,1703267147000,'ieK09sy2C_AIWE8KRkrQR',NULL,'These aren''t actionable errors on our side.

![Screenshot 2023-12-22 at 9 43 42 AM](https://github.com/rocicorp/mono/assets/132324914/3c426e7e-f657-4e43-8145-b649afbea4f8)
','public'),
	 ('NVKnZm0EqOibdtJUTUzvk','authHandler custom error codes',true,1702663040000,1702663001000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'<img width="993" alt="image" src="https://github.com/rocicorp/mono/assets/19158916/a8b853ed-316d-4f46-9b48-57599a3c3e71">
','public'),
	 ('HE7AguYesQrepmpLZXU90','network checks should not be done in dev mode',false,1705337488000,1702662369000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'We already disable analytics and remote logging we should also disable the network check calls.

<img width="727" alt="image" src="https://github.com/rocicorp/mono/assets/19158916/7c38d5bc-f4fc-4650-9c4a-992c5fb349cc">
','public'),
	 ('K9bUxS6ifgHMMA3zpkldG','reflect .d.ts references DurableObject',true,1709537218000,1702637807000,'nqYkxAGMnzk7Y5STjZryV',NULL,'We are "exporting" something that depends on DurableObject (and related types) in our public API.

DOs are not supposed to be part of the public API.','public'),
	 ('Ecpf1e7XcMNOnqepmUJw1','Suppress errors due to missing template file',false,1701886435000,1701886304000,'ieK09sy2C_AIWE8KRkrQR',NULL,'As reported by @arv in https://rocicorp.slack.com/archives/C05TT7N4Z09/p1701780314529909

![image (16)](https://github.com/rocicorp/mono/assets/132324914/6d58bed2-767a-4c64-9186-4d628c9f3585)
','public'),
	 ('v5A83jMmbjIminWCA9I9U','Intercept Miniflare stdout/stderr',true,1709537245000,1701431420000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Details here?

https://github.com/cloudflare/workers-sdk/releases/tag/miniflare%403.20231030.1','public'),
	 ('03ArnhWRmiKTwsfNAIT0Q','`app-deploy` runs out of memory when all of prod is migrated / republished',false,1701397669000,1701396616000,'ieK09sy2C_AIWE8KRkrQR',NULL,'I just ran a migration that renames an env var resulting in a simultaneous publish of all 57 apps. This resulted in `app-deploy` running out of memory:

![Screenshot 2023-11-30 at 6 06 22 PM](https://github.com/rocicorp/mono/assets/132324914/d98bd8c8-d04a-4895-9d6f-42eeb881aac8)

It looks like the function is running with a default concurrency of 80:

![Screenshot 2023-11-30 at 6 07 59 PM](https://github.com/rocicorp/mono/assets/132324914/26cd7d9b-8789-48e6-aa18-c44388dbefb5)

which is incongruent with to the [documentation](https://cloud.google.com/functions/docs/configuring/concurrency) which says the default is 1, but to be honest 1 is not very efficient.

I''ll set a global default to something lower than 80. ','public'),
	 ('tsadGN3INs_G1mm83D1qb','ENV deployment timeout is too short',false,1701286628000,1701278769000,'ieK09sy2C_AIWE8KRkrQR',NULL,'https://console.cloud.google.com/monitoring/alerting/incidents/0.n55zr14ya686?project=reflect-mirror-prod&pageState=(%22interval%22:(%22i%22:(%22s%22:%222023-11-29T16:21:16.000Z%22,%22e%22:%222023-11-29T17:12:32.311Z%22)))

The deployment timeout fired less than a half second before the deployment was created.

![Screenshot 2023-11-29 at 9 22 34 AM](https://github.com/rocicorp/mono/assets/132324914/5f767ad7-3863-4324-a0e4-0c4496a7ac56)
','public'),
	 ('SyKOFcPJ1msqpjxeG5BPc','Error saving dev vars for `reflect env --dev`',false,1701207411000,1701203642000,'ieK09sy2C_AIWE8KRkrQR',NULL,'https://console.cloud.google.com/monitoring/alerting/incidents/0.n54qfjgfwu8a?project=reflect-mirror-prod&pageState=(%22interval%22:(%22i%22:(%22s%22:%222023-11-28T19:41:18.000Z%22,%22e%22:%222023-11-28T20:31:53.189Z%22)))

```
Error: ENOENT: no such file or directory, open ''/home/jared/work/2023/garden/.reflect/dev-vars.env''
    at Object.openSync (node:fs:601:3)
    at writeFileSync (node:fs:2249:35)
    at saveDevVars (file:///home/jared/work/2023/garden/node_modules/@rocicorp/reflect/bin/cli.js:139087:3)
    at setDevVars (file:///home/jared/work/2023/garden/node_modules/@rocicorp/reflect/bin/cli.js:139038:3)
    at setVarsHandler (file:///home/jared/work/2023/garden/node_modules/@rocicorp/reflect/bin/cli.js:140345:5)
    at Object.handler (file:///home/jared/work/2023/garden/node_modules/@rocicorp/reflect/bin/cli.js:139251:15)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
```

![Screenshot 2023-11-28 at 12 32 43 PM](https://github.com/rocicorp/mono/assets/132324914/578b61f5-0bff-45d2-afa8-904b76d1e2b2)
','public'),
	 ('qzRb5YFzYjzZv2oLjmhTm','`metrics-backup` needs more memory',false,1701202072000,1701196942000,'ieK09sy2C_AIWE8KRkrQR',NULL,'We''re getting enough usage such that a week''s worth of connection data exceeds 256MB.  🎉 

https://console.cloud.google.com/monitoring/alerting/incidents/0.n54di51hyww0?project=reflect-mirror-prod&pageState=(%22interval%22:(%22d%22:%22P1D%22,%22i%22:(%22s%22:%222023-11-28T10:15:12.000Z%22,%22e%22:%222023-11-28T11:16:11.000Z%22)))

![Screenshot 2023-11-28 at 10 40 59 AM](https://github.com/rocicorp/mono/assets/132324914/67b4cf53-23bf-40ba-ae09-adda932f66a9)
','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('B5e7I6LwBvqpAnpR887fw','Syntax errors from manual edits of `reflect.config.json`',false,1701216974000,1700504881000,'ieK09sy2C_AIWE8KRkrQR',NULL,'Syntax errors from users manually editing `reflect.config.json` are current classified as errors. We should bucket them as warnings.

https://console.cloud.google.com/errors/detail/CJXXuJPQ_IPskwE;time=P30D?project=reflect-mirror-prod

![Screenshot 2023-11-20 at 10 23 51 AM](https://github.com/rocicorp/mono/assets/132324914/bbc05234-f165-4a52-a8cc-4ce918069562)

I think the slightly subtle / tricky thing is downgrading Syntax errors from reading the `reflect.config.json` file without necessarily suppressing  other errors that should be surfaced.','public'),
	 ('-khfmO1A7fxYo98dEUSmh','Firestore library mismatch',false,1700158696000,1700157449000,'ieK09sy2C_AIWE8KRkrQR',NULL,'One of the pain points of the nodejs Firestore libraries is that they use `instanceof` for processing FieldValue transforms, and this can break when referencing multiple versions of the library in `node_modules` of different packages in a repo.

https://console.cloud.google.com/monitoring/alerting/incidents/0.n4nzsab1hu6a?project=reflect-mirror-prod&pageState=(%22interval%22:(%22i%22:(%22s%22:%222023-11-16T16:46:12.000Z%22,%22e%22:%222023-11-16T17:54:37.922Z%22)))

![Screenshot 2023-11-16 at 9 55 49 AM](https://github.com/rocicorp/mono/assets/132324914/d6f30187-b24d-43bc-85ab-9ab667a4ebe7)
','public'),
	 ('r8EsfSIxlJ3NKaHamaZSi','Error Reporting is lumping Cloudflare API errors together',false,1700005868000,1699987036000,'ieK09sy2C_AIWE8KRkrQR',NULL,'We didn''t get an alert for #1228 because the Error Reporter is lumping the error in with the errors that we were getting during the Cloudflare outage (which I had set to "Acknowledged").

![Screenshot 2023-11-14 at 10 33 58 AM](https://github.com/rocicorp/mono/assets/132324914/907ae928-5936-4ba4-a0f7-f3a4a8ecfdc3)

I will see if I can figure out how to make the Error Reporter distinguish these errors. It''s probably thinking that they have the same stack trace, i.e.

```
 _FetchResultError: GET /zones/1b044253688b6ddb8e67738539a2b6d0/dns_records: {"success":false,"errors":[{"code":10000,"message":"Internal authentication error: internal server error"}]}
    at cfFetch (file:///workspace/out/index.js:13701:11)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5) 
```

vs

```
 _FetchResultError: PUT /accounts/085f6d8eb08e5b23debfb08b21bda1eb/workers/dispatch/namespaces/prod/scripts/likeable-lackadaisical-store-lox8pviw: {"result":null,"success":false,"errors":[{"code":10021,"message":"Uncaught ReferenceError: window is not defined\n  at index.js:169:23\n  at index.js:41:70\n  at index.js:42:7 in node_modules/leaflet/dist/leaflet-src.js\n  at index.js:11:50 in __require\n  at index.js:9701:30\n"}],"messages":[]}
    at cfFetch (file:///workspace/out/index.js:13720:11)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5) 
```','public'),
	 ('GY-1qG2U10bTurze8FSk7','Some Cloudflare publish errors are not caught by esbuild before publishing',false,1700005867000,1699986772000,'ieK09sy2C_AIWE8KRkrQR',NULL,'One of our users was able to successfully upload a script that ends up with an error when uploading to Cloudflare:

```
Uncaught ReferenceError: window is not defined
  at index.js:169:23
  at index.js:41:70
  at index.js:42:7 in node_modules/leaflet/dist/leaflet-src.js
  at index.js:11:50 in __require
  at index.js:9701:30
```

![Screenshot 2023-11-14 at 10 31 52 AM](https://github.com/rocicorp/mono/assets/132324914/15f1a842-5390-4e80-a672-4b52c45021eb)

I wonder if `esbuild` is accepting references to `window` whereas Cloudflare is not. Would it be possible for `reflect publish` to reject this before upload?','public'),
	 ('dRUQj1xRyZWMw-n2fv3aC','Poor error message when userID passed to constructor doesn''t match userID returned by authHandler',false,1699741367000,1699740352000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'See: https://discord.com/channels/830183651022471199/1173010960408137829/1173010965634228315','public'),
	 ('tVxbMnV6abTPXYyJk9ar5','`reflect init` doesn''t work on a previous `reflect create`d project',false,1709537228000,1699409957000,'ieK09sy2C_AIWE8KRkrQR',NULL,'This may be a weird scenario but we get these errors in production a lot so I don''t think it''s a one-off:

![Screenshot 2023-10-28 at 9 57 56 AM](https://github.com/rocicorp/mono/assets/132324914/9dd56ed4-7c37-4771-a8db-7f3722d25ee8)

The way it happened to me just now:
* run `npx reflect create foo`. This sets everything up to point to `src/reflect/index.ts`
* run `npx reflect publish` to create the server-side app
* delete the app with `npx reflect delete`
* also delete `reflect.config.json`
* now try to revive the app with `npx reflect init`

Unlike `create`, `init` sets up `reflect.config.json` to point to `reflect/index.ts`. I think this may be why we see so many of those `dev` / `ENOENT` errors.

I know `reflect init` is supposed to be in line with the instructions in https://hello.reflect.net/add-to-existing, but it''s a bit unfortunate that it''s not compatible with the project created by `reflect create`.

Not an emergency, but improving this might reduce some bounce factor.

Maybe have `init` check for the file? Or be even fancier and replace the file path if it''s in `src/reflect/index.ts`?

@cesara @aboodman ','public'),
	 ('qW9PBbt-NguUD6tl6BL2j','Move secrets out of the App document',false,1699422390000,1699234885000,'ieK09sy2C_AIWE8KRkrQR',NULL,'Currently, app secrets (implemented for #679 and #1150) are stored in encrypted form in the App doc. This makes it convenient for triggering a new deployment when secrets change. However, there are a couple of reasons this is non-ideal:

* We currently limit the number of variables per env to 50 * 5kb secrets, which is ~256kb of data. Each Firestore document stores a maximum of 1MB, so this schema limits the number of environments we can support. And it''s better not to load that much data for the App every time we perform an operation on it anyway.
* The App doc is accessible to anyone on the App''s "team", which means we technically give them unlimited power to generate plaintext / ciphertext pairs. Although the generation includes a random initialization vector that they can''t control, it may provide some advantage in brute-forcing the global encryption key. It would be better not to expose the ciphertexts either.

Fix:

* Move app secrets into an app subcollection `envs`, with the default one living in `apps/<appID>/envs/(default)`
* Disallow external read access to the `envs` subcollection.
* In the App doc (or in the future, whatever doc tracks a deployment instance), store the `secretsUpdateTime` in the `DeploymentSpec` and use that to determine when a new deployment in necessary (instead of the current `hashesOfSecrets` field).','public'),
	 ('9sXSBObAdZzi6f0hGWqky','Error Reporter buckets disparate errors together',false,1698865533000,1698865011000,'ieK09sy2C_AIWE8KRkrQR',NULL,'Error reports like https://console.cloud.google.com/errors/detail/CMeqyPmjg-zPBQ;time=P30D?project=reflect-mirror-prod&utm_source=error-reporting-notification&utm_medium=slack&utm_content=resolved-error

group many disparate errors together:

![Screenshot 2023-11-01 at 11 55 00 AM](https://github.com/rocicorp/mono/assets/132324914/fd91fbda-6dd3-470d-a39d-324bc8968902)

This is likely because they all have similar stack traces, being thrown by the `error-report` handler.

We can likely improve this by having the thrown errors inherit the original stack trace from the error reported by the cli.','public'),
	 ('QC3Eso1j24URE4SWY1H5N','tail returns a 500 if the app is not deployed',false,1698859700000,1698798878000,'ieK09sy2C_AIWE8KRkrQR',NULL,'https://rocicorp.slack.com/archives/C05TT7N4Z09/p1698795167619959

https://console.cloud.google.com/errors/detail/CPzZ--WJrr63Zg;time=P30D?project=reflect-mirror-prod

![Screenshot 2023-10-31 at 4 35 53 PM](https://github.com/rocicorp/mono/assets/132324914/535ed400-ae71-4aa5-a48c-34b999888125)

Short-term fix is to return a 400.

Long-term, perhaps we should not auto-create the app for `tail`, and instead prompt the user to `publish` first?','public'),
	 ('ISJdIl3mCFbsjixC-Cgwz','reflect tail to the wrong room results in a TypeError',false,1699480526000,1698709252000,'ieK09sy2C_AIWE8KRkrQR',NULL,'@arv I think you fixed this in one place but perhaps there is another protocol mismatch (in the AuthDO)?

![Screenshot 2023-10-30 at 4 39 04 PM](https://github.com/rocicorp/mono/assets/132324914/50b797bd-b5bf-4411-8707-022845f02d91)
','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('Us_A9kc4ldfHuChlbKeU6','app-publish function needs more Memory',false,1701202102000,1698518825000,'ieK09sy2C_AIWE8KRkrQR',NULL,'https://console.cloud.google.com/errors/detail/CKnV1_TJwMrDQw?project=reflect-mirror-prod&supportedpurview=project

![Screenshot 2023-10-28 at 11 46 36 AM](https://github.com/rocicorp/mono/assets/132324914/6d36409d-c367-4637-ab20-83c831962d6b)
','public'),
	 ('PJ9Bn1-zoLEuMR6OkwP70','Errors from Firestore-triggered functions are not making it to alerts',false,1698518550000,1698513286000,'ieK09sy2C_AIWE8KRkrQR',NULL,'When I was debugging https://github.com/rocicorp/mono/issues/1159 I generated a lot of errors in prod, but they didn''t trigger alerts.

It seems that even if we throw an error from a Firestore-triggered function, the containing "request" ends with a 200:

![Screenshot 2023-10-28 at 10 09 56 AM](https://github.com/rocicorp/mono/assets/132324914/0dd0a596-05b5-4d14-9ea1-f6c57180d574)

And are indeed classified as `2xx` responses in monitoring:

![Screenshot 2023-10-28 at 10 12 23 AM](https://github.com/rocicorp/mono/assets/132324914/8cbed0d1-e735-4f19-8408-c97b9d94c775)

Firestore functions (and [event-driven functions](https://cloud.google.com/functions/docs/writing/write-event-driven-functions) in general) probably have some slightly different semantics in terms of errors (particularly when it comes to retries). I need to investigate this so that these errors don''t go unnoticed.
','public'),
	 ('vK2pxdp1DXeMYUz6GGjag','SSL handshake error when attempting to verify liveness of a worker',false,1698471768000,1698456662000,'ieK09sy2C_AIWE8KRkrQR',NULL,'I''m seeing this in sandbox. Not sure why.

```
GET https://104.18.14.180:443/ error TypeError: fetch failed
    at Object.fetch (node:internal/deps/undici/undici:11372:11)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async waitForLiveness (file:///workspace/out/index.js:14536:19)
    at async NamespacedScriptHandler.publish (file:///workspace/out/index.js:14427:22)
    at async runDeployment (file:///workspace/out/index.js:14742:22)
    at async file:///workspace/out/index.js:14657:5 {
  cause: [Error: C027AA16023E0000:error:0A000410:SSL routines:ssl3_read_bytes:sslv3 alert handshake failure:../deps/openssl/openssl/ssl/record/rec_layer_s3.c:1586:SSL alert number 40
  ] {
    library: ''SSL routines'',
    reason: ''sslv3 alert handshake failure'',
    code: ''ERR_SSL_SSLV3_ALERT_HANDSHAKE_FAILURE''
  }
}
```

![Screenshot 2023-10-27 at 6 29 35 PM](https://github.com/rocicorp/mono/assets/132324914/b89778ba-9293-40e6-87a4-2cc39511d39c)
','public'),
	 ('2ALrC8ttbAySW1GLcaGXA','Demote `publish` compilation errors to WARNING severity',false,1698429366000,1698425243000,'ieK09sy2C_AIWE8KRkrQR',NULL,'Compilation errors are generally user error and not something actionable on our part.

https://console.cloud.google.com/monitoring/alerting/incidents/0.n3vqnhz9wgeo?project=reflect-mirror-prod

![Screenshot 2023-10-27 at 9 45 46 AM](https://github.com/rocicorp/mono/assets/132324914/f32edbfe-7cc0-4c85-9cff-cfb2c0c1e5a6)
','public'),
	 ('ttN93oYNQN0K-9xl_3SiO','Support Environment Variables for Mirror Customers',false,1699068689000,1698424695000,'ieK09sy2C_AIWE8KRkrQR',NULL,'Feature is self explanatory.

Design: https://www.notion.so/replicache/Reflect-Server-Vars-21909a4672f340c0a2ef20cd4c503d50','public'),
	 ('h2kEk3PAywhtIlcGRhsHC','Certain `reflect-cli` commands fail when run with Node versions <18',false,1698280434000,1698273745000,'ieK09sy2C_AIWE8KRkrQR',NULL,'In particular our calls to `fetch()` for npm dist tags and Google analytics will fail with a `ReferenceError`.

https://rocicorp.slack.com/archives/C05TT7N4Z09/p1698272539648889

![Screenshot 2023-10-25 at 3 27 19 PM](https://github.com/rocicorp/mono/assets/132324914/5879d149-e58c-484e-b51f-37a8d7b2644a)
','public'),
	 ('d7OdJV18QzZwnX7k-3v23','Avoid reporting FirebaseError errors from the cli',false,1698190096000,1698111374000,'ieK09sy2C_AIWE8KRkrQR',NULL,'`FirebaseError` errors come from the mirror-server and are already counted (and classified). They shouldn''t be reported by the cli as that is redundant (and we lose the warn vs error classification logic).

https://console.cloud.google.com/monitoring/alerting/incidents/0.n3rhwm8g0m4i?project=reflect-mirror-prod

![Screenshot 2023-10-23 at 6 19 29 PM](https://github.com/rocicorp/mono/assets/132324914/e9d514f5-8c03-4bc9-8318-2da9f0617c88)

@cesara this should be an easy one?','public'),
	 ('hXnRe6iXs5MI_7Kt0BUWV','Exclude or raise threshold for alerts from roci team member accounts',false,1698190096000,1698013368000,'ieK09sy2C_AIWE8KRkrQR',NULL,'Alerts from our own accounts are usually due to us choosing the wrong stack or something else that need not alert the team (as one of us already knows about it).

Example:

https://console.cloud.google.com/monitoring/alerting/incidents/0.n3nyfzyvytcc?project=reflect-mirror-prod

![Screenshot 2023-10-22 at 3 20 32 PM](https://github.com/rocicorp/mono/assets/132324914/4873cc5a-bf09-4e0b-8f98-7b36385c1935)
','public'),
	 ('YGqUmKGXrsmLuCKr6HPMp','Avoid alerting when `npm add / install` fail in `reflect create / init`',false,1698198485000,1698013085000,'ieK09sy2C_AIWE8KRkrQR',NULL,'We get a regular cadence of alerts that are non-actionable, such as `npm` action failures. We should treat these differently and avoid alerting on therm, or at least raise the threshold for alerts.

@cesara 

Examples: 

https://console.cloud.google.com/monitoring/alerting/incidents/0.n3nt2pemzawz?project=reflect-mirror-prod

https://console.cloud.google.com/monitoring/alerting/incidents/0.n3nu0ecvz18n?project=reflect-mirror-prod

![Screenshot 2023-10-22 at 3 14 35 PM](https://github.com/rocicorp/mono/assets/132324914/25aa0a7e-c3cd-4321-8ad8-ce07496b969a)
','public'),
	 ('y0Ns_753Y19-y5yz6d4zH','Catch (and avoid reporting) ENOENT errors from `reflect dev`',false,1698430980000,1697817833000,'ieK09sy2C_AIWE8KRkrQR',NULL,'These are generally user error and not worth grabbing our attention to investigate each time. Thank you @arv!

https://console.cloud.google.com/monitoring/alerting/incidents/0.n3mifj58m8e8?project=reflect-mirror-prod

![Screenshot 2023-10-20 at 12 00 20 PM](https://github.com/rocicorp/mono/assets/132324914/f23539a9-b0fe-43a5-a898-40ea45dd348a)

https://console.cloud.google.com/monitoring/alerting/incidents/0.n3mdczsszhmt?project=reflect-mirror-prod

![Screenshot 2023-10-20 at 11 11 24 AM](https://github.com/rocicorp/mono/assets/132324914/75360008-917c-4cc5-9c7d-7f984806da47)
','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('a7fP1sZUkwWCBki6j7WRe','Reflect CLI should check node version',true,1709537245000,1697727821000,'nqYkxAGMnzk7Y5STjZryV',NULL,'The CLI should check the node version and maybe even the npm version since we use npm in some of these cases.

Wrangler has a "wrapper" that does these checks before invoking node again with a new child process.','public'),
	 ('pS6uV3ez7FMpWIl6PQSKT','Cloudflare sometimes returns 504: Gateway Timeout on a Custom Hostname GET',false,1697656427000,1697647879000,'ieK09sy2C_AIWE8KRkrQR',NULL,'This resulted in an error when @arv was trying to publish. It appears to be transient, as his subsequent publish succeedeed.

Maybe Cloudflare would eventually recover with exponential backoff.

https://console.cloud.google.com/monitoring/alerting/incidents/0.n3jr4772yv4d?project=reflect-mirror-prod

```
Error: GET /zones/1b044253688b6ddb8e67738539a2b6d0/custom_hostnames/997fa415-c18c-4a80-acff-a76d522c8417: 504: Gateway Time-out: SyntaxError: Unexpected token < in JSON at position 0
    at cfFetch (file:///workspace/out/index.js:13417:11)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async createCustomHostname (file:///workspace/out/index.js:13720:66)
    at async Promise.allSettled (index 0)
    at async setCustomHostnames (file:///workspace/out/index.js:13676:19)
    at async publishCustomHostname (file:///workspace/out/index.js:13630:20)
    at async NamespacedScriptHandler._doPublish (file:///workspace/out/index.js:14291:22)
    at async NamespacedScriptHandler.publish (file:///workspace/out/index.js:14222:22)
    at async runDeployment (file:///workspace/out/index.js:14454:22)
    at async file:///workspace/out/index.js:14369:5
```

![Screenshot 2023-10-18 at 12 47 24 PM](https://github.com/rocicorp/mono/assets/132324914/9ff02b4f-3e27-4d9f-8738-1c008494d8ea)
','public'),
	 ('YVZr9JZJcaGHoKizfAmaP','`reflect delete --all` could use a checkbox list',false,1698190076000,1697645365000,'nqYkxAGMnzk7Y5STjZryV',NULL,'I think a better UI might be to select all the apps to delete first instead of doing them one by one. Something along:

```
[ ] app-1
[x] app-2 
[ ] app-3
(Delete selected)
```','public'),
	 ('THQZZNKQcXBKWFHL_cis7','Use JSON5 for reflect config files',true,1709537264000,1697566191000,'nqYkxAGMnzk7Y5STjZryV',NULL,'So that we can have comments in these config files','public'),
	 ('YPP0y4PmxYWaq-DRFLqI6','Use code splitting for reflect-cli',false,1697190426000,1697108907000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Reflect cli is very large and it is slow to start.

If we use code splitting we can skip loading things that are not needed.','public'),
	 ('AUuDgcjnoIuEq-aT1fSaW','Analytics report consistently fails on `reflect create`',false,1697000917000,1696999738000,'ieK09sy2C_AIWE8KRkrQR',NULL,'Error reports have shown that the analytics report call often fails on the `reflect create` command:

https://console.cloud.google.com/monitoring/alerting/incidents/0.n385u4w9xu68?project=reflect-mirror-prod
https://console.cloud.google.com/monitoring/alerting/incidents/0.n38tisluhgjk?project=reflect-mirror-prod
https://console.cloud.google.com/monitoring/alerting/incidents/0.n39fudf8ny0w?project=reflect-mirror-prod

and it can actually be consistently reproduced locally:

```
examples $ node ~/roci/mono/mirror/reflect-cli/out/index.mjs --stack=sandbox create analytics-test
Installing @rocicorp/reflect

You''re all set! 🎉

Run Reflect dev server and UI:

cd analytics-test && npm run watch

TypeError: fetch failed
    at Object.fetch (node:internal/deps/undici/undici:11457:11)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async sendAnalyticsEvent (file:///Users/ocean/roci/mono/mirror/reflect-cli/out/index.mjs:304:3)
    at async Promise.all (index 0)
    at async Object.handler (file:///Users/ocean/roci/mono/mirror/reflect-cli/out/index.mjs:501:9) {
  cause: ConnectTimeoutError: Connect Timeout Error
      at onConnectTimeout (/Users/ocean/roci/mono/node_modules/miniflare/node_modules/undici/lib/core/connect.js:184:24)
      at /Users/ocean/roci/mono/node_modules/miniflare/node_modules/undici/lib/core/connect.js:131:46
      at Immediate._onImmediate (/Users/ocean/roci/mono/node_modules/miniflare/node_modules/undici/lib/core/connect.js:172:9)
      at process.processImmediate (node:internal/timers:476:21) {
    code: ''UND_ERR_CONNECT_TIMEOUT''
  }
}
 examples $ 
```
One thing that''s puzzling is that the error is coming from `node_modules/miniflare/node_modules/...`. I''m not sure why `fetch` is using that library.

@cesara @arv ','public'),
	 ('XJmKS2yoUSpBSk9JCCmK-','Monday reports large number of connection timeout errors',false,1696949095000,1696875620000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'See: https://discord.com/channels/830183651022471199/1158706195436159026/1158706201614372954

@grgbkr and I have identified one reason this error gets thrown spuriously, which is that when Reflect is `close()`''d, the timer never gets canceled. There is evidence this is happening often in the logs:

https://discord.com/channels/830183651022471199/1158706195436159026/1160169594158321674','public'),
	 ('1kcQ0SFauB8RyMVhHYpoR','Monday reports that BroadcastChannel sometimes not found on Safari >= 15.4?',false,1709537063000,1696280013000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'I feel like this must be people spoofing their user agent or something? But in any case, we should bring back the polyfil. This would also help people use Reflect on servers.','public'),
	 ('kGjZqdn3tW-YsT3rKyfEM','Ensure reflect-cli / schema compatibility with self-deprecation',false,1695945546000,1695660588000,'ieK09sy2C_AIWE8KRkrQR',NULL,'The fact that the `reflect-cli` reads directly from Firestore is a double-edged sword.
* On the one hand, it obviates the need to create and maintain a Cloud Function API for reading data from the store
* On the other hand, it essentially makes the Firestore schema the API for which versioning must be managed instead.

Last week I had forgotten about this and mistakenly removed the required `teamSubdomains` field from the App doc, as the server code was updated to no longer expect it. However, the `reflect-cli` expected it, and the `reflect delete` functionality consequently broke.

There is a yet-to-be-implemented [scheme for handling cli / schema versioning / compatibility](https://www.notion.so/replicache/Mirror-4d1c7f00d95c410299ceecd9311e1560#3ef1f998cefc4c7d97c9c5a8a3e158b8) in which we publish min-supported, max-deprecated, and current versions of the cli, and the cli knows to check these versions before operating. It can prompt the user to upgrade if its own version is deprecated, and refuse to proceed if unsupported. This will allow us to eventually clean up old schemas.

The document proposes maintaining these versions in Firestore, but if the versions do not need to be stack-specific, they could also be dist-tags in npm. `@latest` is already there by default, and we could add `@cli-supported` and `@cli-deprecated` tags.

@aboodman @arv @grgbkr for any additional thoughts / preferences','public'),
	 ('bI-IPe9PigVIUaxQaTVMD','put during scan?',false,1677091453000,1655388699000,'nqYkxAGMnzk7Y5STjZryV',NULL,'If we have a for await loop for a `scan` and we then try to mutate the map in the loop body.

```ts
mutators: {
  async test(tx) {
    for await (const key of tx.scan().keys()) {
      await tx.put(''e'', 4);
    }
  },
},
```

Today this dead-locks. We have an `RWLock` for the `BTreeWrite` and we create a read lock for `scan` and a write lock for `put`.

What should the expected behavior be?

The optimal behavior is that the scan is live and new entries _after_ the current entry show up and deleted entries _after_ the current entry are skipped.




','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('aV8cHC0fy75Qa0VMwrLTN','Change return type of mutators to be a Promise',true,1677091106000,1655110856000,'nqYkxAGMnzk7Y5STjZryV',NULL,'It is a foot gun to return a non promise in a Mutator. We should change the return type from `MaybePromise` to `Promise`.','public'),
	 ('jJd3-dXu2nheL40kaBR3x','replicache cjs package broken',false,1695373060000,1695301277000,'nqYkxAGMnzk7Y5STjZryV',NULL,'We compile replicache to both ESM and CJS. However, with v13 some dependencies are kept as external (this was to prevent duplicated code in reflect) so the cjs file has lines like:

```js
require(''@rocicorp/logger'')
```

However, `@rocicorp/logger` (and the other `@rocicorp/*` deps) don''t publish CJS modules. This leads to an cjs module trying to require an esm module, which isn''t supported by nodejs. The error that looks like:

```
temp/funnele/app  main ✗                                                                                                        7m ⚑
▶ SKIP_ENV_VALIDATION=1 npm run build


> app@0.1.0 build
> next build

 ✓ Creating an optimized production build
 ✓ Compiled successfully
 ✓ Linting and checking validity of types
   Collecting page data ...Error [ERR_REQUIRE_ESM]: require() of ES Module /Users/arv/src/temp/funnele/app/node_modules/@rocicorp/logger/out/logger.js from /Users/arv/src/temp/funnele/app/node_modules/replicache/out/replicache.cjs not supported.
Instead change the require of logger.js in /Users/arv/src/temp/funnele/app/node_modules/replicache/out/replicache.cjs to a dynamic import() which is available in all CommonJS modules.
    at mod.require (/Users/arv/src/temp/funnele/app/node_modules/next/dist/server/require-hook.js:64:28)
    at Object.<anonymous> (/Users/arv/src/temp/funnele/app/node_modules/replicache/out/replicache.cjs:1:2550)
    at mod.require (/Users/arv/src/temp/funnele/app/node_modules/next/dist/server/require-hook.js:64:28)
    at 5326 (/Users/arv/src/temp/funnele/app/.next/server/pages/replicache-example.js:1:2335)
    at t (/Users/arv/src/temp/funnele/app/.next/server/webpack-runtime.js:1:128)
    at /Users/arv/src/temp/funnele/app/.next/server/pages/replicache-example.js:1:1209
    at t.a (/Users/arv/src/temp/funnele/app/.next/server/webpack-runtime.js:1:879)
    at 5919 (/Users/arv/src/temp/funnele/app/.next/server/pages/replicache-example.js:1:1132)
    at t (/Users/arv/src/temp/funnele/app/.next/server/webpack-runtime.js:1:128)
    at /Users/arv/src/temp/funnele/app/.next/server/pages/replicache-example.js:1:479
    at t.a (/Users/arv/src/temp/funnele/app/.next/server/webpack-runtime.js:1:879)
    at 45015 (/Users/arv/src/temp/funnele/app/.next/server/pages/replicache-example.js:1:85)
    at t (/Users/arv/src/temp/funnele/app/.next/server/webpack-runtime.js:1:128)
    at r (/Users/arv/src/temp/funnele/app/.next/server/pages/replicache-example.js:1:2580)
    at /Users/arv/src/temp/funnele/app/.next/server/pages/replicache-example.js:1:2615
    at t.X (/Users/arv/src/temp/funnele/app/.next/server/webpack-runtime.js:1:2116)
    at /Users/arv/src/temp/funnele/app/.next/server/pages/replicache-example.js:1:2593
    at Object.<anonymous> (/Users/arv/src/temp/funnele/app/.next/server/pages/replicache-example.js:1:2643)
    at mod.require (/Users/arv/src/temp/funnele/app/node_modules/next/dist/server/require-hook.js:64:28)
    at requirePage (/Users/arv/src/temp/funnele/app/node_modules/next/dist/server/require.js:109:84)
    at /Users/arv/src/temp/funnele/app/node_modules/next/dist/server/load-components.js:59:84
    at async loadComponentsImpl (/Users/arv/src/temp/funnele/app/node_modules/next/dist/server/load-components.js:59:26)
    at async /Users/arv/src/temp/funnele/app/node_modules/next/dist/build/utils.js:1045:32
    at async Span.traceAsyncFn (/Users/arv/src/temp/funnele/app/node_modules/next/dist/trace/trace.js:105:20) {
  code: ''ERR_REQUIRE_ESM''
}

> Build error occurred
Error: Failed to collect page data for /replicache-example
    at /Users/arv/src/temp/funnele/app/node_modules/next/dist/build/utils.js:1195:15
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5) {
  type: ''Error''
}
   Collecting page data .%

```','public'),
	 ('X3l7WaurF55GePP0sLJmh','Reduce risk of AuthDO revalidate causing overloaded errors',true,1709537289000,1695146703000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'Revalidate makes a fetch to each RoomDO its needs to revalidate connections for.  It take a write lock on the authLock for each request.  If the RoomDO is slow to start up or respond it could cause the AuthDO to get overloaded errors, as all connection requests will be blocked by this write lock on the authLock.  Monday WorkCanvas''s AuthDO has had overload errors that may be due to this.

This risk can be reduced by:
1. send a fetch outside the lock to the RoomDO to wake it up, only after its awake take the lock and make the critical fetch that needs to be locked
2. add a timeout to the fetch inside the lock so it can''t hold it for more than say 300ms (if we''ve already woke the RoomDO it should be very fast).

More context https://rocicorp.slack.com/archives/C013XFG80JC/p1695091818933899

cc @d-llama @aboodman ','public'),
	 ('x_yzltL4WLH7N2KwJ4_Cv','RFE: A way to subscribe to only keys efficiently',false,1709537043000,1694111023000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'A common pattern in Replicache/Reflect apps is that there is some container (a canvas, a list of todos, etc) and then there are items.

In React, we want to reduce renders. If the container element subscribes to all the elements and passes them down to child components, then any change in a child will cause the container to re-render, which will also cause all the _other_ children to re-render. This can be mitigated somewhat with `React.memo` on the children elements, but this pattern will always result in at least an extraneous re-render of the container for every child change.

Alternately, we could have the container subscribe to only the keys (`useSubscribe(r, async tx => await tx.scan().keys())`). When a child changes, the query will re-run at the container, but it will compute the same keys so the container won''t re-render. We still do extraneous work, but less.

If there were a way to subscribe to keys and have Replicache only re-run the query when a key is added/removed, then the second pattern could be implemented perfectly efficiently in React. Replicache would only re-run the query when a child element is added or removed, and the container would only re-render when that happened.','public'),
	 ('OMiIbH3YrOa2RT9B4HI2c','Cloudflare domains quota exceeded',false,1697255776000,1693510992000,'ieK09sy2C_AIWE8KRkrQR',NULL,'Good news: deployment errors are surfaced to the cli:

<img width="1224" alt="Screenshot 2023-08-31 at 12 39 13 PM" src="https://github.com/rocicorp/mono/assets/132324914/e168a921-ebff-4e07-9987-776a17c69be1">

Bad news: The error from Cloudflare indicates that our domain quota has been exceeded:

```
cfFetch: URL: https://api.cloudflare.com/client/v4/accounts/085f6d8eb08e5b23debfb08b21bda1eb/workers/scripts/inconclusive-entertaining-cornet-llzkgp8i/domains/records response: {
  "result": null,
  "success": false,
  "errors": [
    {
      "code": 100122,
      "message": "workers.api.error.origin_quota_exceeded"
    }
  ],
  "messages": []
}


ParseError: A request to the Cloudflare API (/accounts/085f6d8eb08e5b23debfb08b21bda1eb/workers/scripts/inconclusive-entertaining-cornet-llzkgp8i/domains/records) failed.
    at throwFetchError (file:///workspace/out/index.js:13017:17)
    at cfFetch (file:///workspace/out/index.js:13007:3)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async publishCustomDomains (file:///workspace/out/index.js:13420:3)
    at async Promise.all (index 0)
    at async publish2 (file:///workspace/out/index.js:13537:3)
    at async runDeployment (file:///workspace/out/index.js:13759:5)
    at async file:///workspace/out/index.js:13707:5
```

<img width="1483" alt="Screenshot 2023-08-31 at 12 40 44 PM" src="https://github.com/rocicorp/mono/assets/132324914/728f1e44-d5bc-473a-8c80-afe1d97e7f93">


@arv in case you think we want to handle errors more gracefully than a ParseError (although it''s probably fine).

@aboodman for plan of action w.r.t. Cloudflare. Is there a way to increase our quota?

In the meantime, I''ll start working on cleaning up the staging apps from our prod Cloudflare account. ','public'),
	 ('PxFcGtMEWEjB09ecJKtPx','Build server modules from canonical npm package instead of mono repo source',false,1695036728000,1692313609000,'ieK09sy2C_AIWE8KRkrQR',NULL,'Right now the `mirror-cli upload` command resolves the server module from `@rocicorp/reflect` and uploads it as the version in `packages/reflect/package.json`, and unless I''m mistaken, this comes from the current source files in the `mono` repo and not actually from the official `@rocicorp/reflect` package in npm. (This is one of the things @aboodman was wondering about). This means that the server module can have unpublished changes that were committed to the repo without a version bump.

Is there a way to have the `mirror-cli` package reference the canonical `@rocicorp/reflect` npm (while still being in the `mono` repo)? I imagine that having multiple instances of a package with the same version can wreak some havoc, but maybe there is some npm-fu that can do this.

Fwiw, I found a similar question on stack overflow but no answer ...

https://stackoverflow.com/questions/76899569/how-to-use-published-version-of-a-package-in-an-npm-workspace-mono-repo

@arv  ','public'),
	 ('a-NYYxMddnoE7PWL7IzG2','Private release instructions in README are not correct',false,1709537314000,1692008625000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'There is no `rocicorp-replicache` branch.','public'),
	 ('rdusQ_QfKBY14U2RAr04X','Reconnect on `online` event (Replicache)',true,1709537120000,1691542142000,'OeVnr1y5bEM_Yg06sUFtD',NULL,NULL,'public'),
	 ('IGUfO7KoLZNoNBQ4ODWbJ','Add a public push method, similar to the existing pull one',false,1709537095000,1691542080000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Thinking `push({now: boolean?}?)`.

If `now` is true, then bypass exponential backoff and everything else and literally push right now. For consistency add same to `pull()`.

See also #807

Recent user request: https://discord.com/channels/830183651022471199/1176237877764571176/1176243172758790224','public'),
	 ('tYJm3oeADlqGLTfNGw3a0','Make pull override exponential backoff and immediately pull',false,1709537100000,1691542041000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'I am not sure whether this needs to go through the connection loop at all, or whether it can simply bypass it.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('H1CChLY9exEX7igHpZMaX','reflect reduce valita validation in prod',true,1709537340000,1691465582000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'Reflect client and server do a lot of valita validation (client validates messages received, server validates messages received, values written my mutators, values read by mutators, meta data read from storage).

Much of this is overly defensive, given how expensive we know this validation to be.  

We should probably disable most of the validation in production builds and deployments.  

cc @aboodman @arv ','public'),
	 ('_4bceppFYIYK_8dy_Fhgq','Use client-side Firestore SDK in reflect-cli',false,1691396851000,1691004218000,'ieK09sy2C_AIWE8KRkrQR',NULL,'We have to use the client-side SDK (i.e. `firebase/firestore`) in the `reflect-cli` in order to act on behalf of the logged in user and honor all of the client-side security rules (i.e. not allow them to read other users'' data). The current use of the `@google-cloud/firestore` library doesn''t do this, and it only happens to work via the default credentials provided by our local gcloud logins (and thus won''t work for our customers).

Unfortunately, this means that we won''t be able to share as much code between the server (Firebase Functions) and client (cli) because the two SDKs are pretty different. And we may need to use the emulator to test stuff on the client side.

Investigating this.

@arv @cesara ','public'),
	 ('waY6st2l7-J3GW0mawypv','Evenflow: dramatically increasing capacity of Reflect',true,1690852014000,1690852003000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We should implement Evenflow (https://www.notion.so/replicache/Evenflow-7f968a66d88d42e1bafdf1ed195b8d63) so that Reflect can support many more users per room :).','public'),
	 ('Yfoz9WZxoQFBshtfKs-0g','Setup reflect.net sandbox and cleanup env naming',true,1709537366000,1690584817000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'Current state
===========
**Vercel setup**

- staging.reflect.net is simply the same as reflect.net, both deploy from branch main with Production env vars pointing them at reflect-server.net.  Production deployment deploys reflect-net-east.
- all other branches are deployed with Preview env vars point them at staging.reflect-server.net.  Preview deployment deploys reflect-net-east env.staging (i.e. reflect-net-east-staging).

**CF setup**
- reflect-net-east is served on custom domain reflect-server.net.  It is deployed when Vercel does a Production deploy.  It is used by reflect.net and staging.reflect.net.
- reflect-net-east env.staging (i.e. reflect-net-east-staging) is on custom domain staging.reflect-server.net.  It is deployed when Vercel does a Preview deploy.  It is used by Vercel previews (i.e. the preview links on github code reviews).

Target state
=========
**Vercel setup**
- reflect.net is deployed from branch main with Production env vars pointing at reflect-server.net.  Production deployment deploys reflect-net-east.
- sandbox.reflect.net is deployed from branch sandbox (which is a mirror of main) with its [own env vars ](https://vercel.com/docs/concepts/deployments/git#multiple-preview-phases) that point it at sandbox.reflect-server.net.   When sandbox.reflect.net is deployed it deploys reflect-net-east env.sandbox (i.e. reflect-net-east-sandbox).
- all other branches are deployed with Preview env vars point them at preview.reflect-server.net.  Preview deployment deploys reflect-net-east env.preview (i.e. reflect-net-east-preview).
- For sandbox setup to work, we need to ensure @rocicorp/mono''s `main` branch is mirror''d to a new `sandbox` branch (in the same repo).  This github action looks promising for doing that: https://github.com/google/mirror-branch-action
- staging.reflect.net should go away

**CF setup**
- reflect-net-east is served on custom domain reflect-server.net.  It is deployed when Vercel does a Production deploy.  It is used by reflect.net.
- reflect-net-east env.sandbox (i.e. reflect-net-east-sandbox) is on custom domain sandbox.reflect-server.net.  It is deployed when Vercel does a deploy of the sandbox branch.  It is used by sandbox.reflect.net.
- reflect-net-east env.preview (i.e. reflect-net-east-preview) is on custom domain preview.reflect-server.net.   It is deployed when Vercel does a Preview deploy.  It is used by Vercel previews (i.e. the preview links on github code reviews).
- reflect-net-eash env.staging should go away

cc @d-llama @aboodman 

','public'),
	 ('0PGwUHDk4LqigVDlDaMJd','Odd/inconsistent behavior when duplicate cookie received',false,1710163848000,1690493550000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Reproduction:

1. Follow BYOB instructions: https://doc.replicache.dev/byob/client-view
2. At local mutations step:
  a. Create local mutation
  b. Change data being returned by pull handler, but leave cookie unchanged at `42`
  c. Observe that changes from pull response are shown temporarily, then reverted

![CleanShot 2024-03-03 at 23 41 38](https://github.com/rocicorp/mono/assets/80388/af22cd89-65ce-4687-8719-9c08994780ba)

What''s happening here I believe is that the pull handler code is honoring the new pull response, but the *persist* handler is not (because cookie unchanged).

The two paths should be the same. Both should ignore an unchanged cookie value. We should also print out an info log when this occurs.','public'),
	 ('zWTHSpDpcGeNMapE40Id6','Consider not acquiring the room lock when handling pings',true,1709537365000,1690396255000,'ieK09sy2C_AIWE8KRkrQR',NULL,'This may prevent disconnects when the RoomDO is busy with queued up lock-holding logic, but otherwise up and running.

#497 ','public'),
	 ('zaQ1rxulr8tkM_dbmpq6H','doc: byob: break up push (and perhaps pull?) into smaller steps',false,1709536134000,1690343549000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'The db setup is currently broken into two steps: the skeleton of `db.ts` and the implementation of `initDB()`.

This is just a nice simple thing that makes it feel easier to move through the guide, because you do it bite by bite. We should do same thing with push. I could imagine a few steps:

1. Read request body and iterate mutations
2. processMutation
3. handle errors?','public'),
	 ('reOMvq5qspEnL3yYyrswb','reflect dev needs watch mode',false,1697008646000,1690285725000,'nqYkxAGMnzk7Y5STjZryV',NULL,'I''m punting on watch mode at the moment.

esbuild has a watch mode but it is not clear how that works with returning the result as a string (instead of writing an output file).

We could get the dependency graph from esbuild using the `metafile` build option and set up our own file watchers.','public'),
	 ('v2ed6dz-Na0p9ZmloZBir','Refactor connection revalidation to use an alarm rather than a cron.  ',true,1709537365000,1690222687000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'Refactor connection revalidation to use an [alarm](https://developers.cloudflare.com/workers/runtime-apis/durable-objects/#how-to-use-the-alarm-handler-method) rather than a cron.  

A few benefits:
1. simplifies project setup (don''t need to create cron, don''t need cron handling in worker forward of cron from worker to AuthDO)
2. we can run the alarm only when there are connections to revalidate, currently cron runs even if there are no connections to revalidate','public'),
	 ('v53ak17lkoR6ve1KktAy6','reflect publish/upload: Use esbuild conditions',false,1697008929000,1689773791000,'nqYkxAGMnzk7Y5STjZryV',NULL,'When compiling with esbuild we should declare the conditions so that npm packages work better

```
conditions: ["workerd", "worker", "browser"],
```','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('u-lfNFiLXJRj2_E5XmoLk','Publishing reflect is error prone',true,1709537364000,1689705548000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'To ensure correct package contents, before publishing reflect one needs to run `turbo run build --force` from the root directory of the mono repo.

If one forgets to do this, what is packed is whatever is in the `/out` dir.  If the `--force` flag is not used, then the version variable in the result package is likely to be wrong because turbo build will use cached build output of reflect-shared where the [version](https://github.com/rocicorp/mono/blob/main/packages/reflect-shared/src/version.ts#L4) variable is defined (if its available).  

To make this more robust we should:

1.  Move the build that generates “version” into “packages/reflect” (though we may run in to some circular dep issues before packages/reflect, packages/reflect-client and packages/reflect-server).
2. Add a prepack script to packages/reflect (and probably other published packages in mono), that ensures that build is run from root, and tests are run, before the pack happens.
3. Add check-ambient context to reflect-server (currently only on reflect-client)
4. Document release process and check into mono/packages/reflect
5. Explore replacing rollup with api-extractor

cc @arv ','public'),
	 ('jevoTlhabxVRkXuDbPVjK','PullResponse claims to support `null` cookies responses, but doesn''t',false,1710026568000,1689230868000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Reproduction:

1. Follow BYOB instructions: https://doc.replicache.dev/byob/client-view
  * In Client View step, return cookie `null` not `42`.
2. At local mutations step:
  a. Create local mutation
  b. Observe other data besides local data disappear

![CleanShot 2024-03-03 at 23 32 15](https://github.com/rocicorp/mono/assets/80388/8e0a8ea1-605a-4c77-b8f1-30c26a11254b)
','public'),
	 ('nX6Ty8TT2ggpam2E1pqad','In development mode, `ClientStateNotFound` should delete all local state and refresh',true,1689277411000,1689228715000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Currently, when the server returns `ClientStateNotFound`, Replicache "disables" the client group. This prevents pushes and pulls, writes an error to the console, and from the user''s perspective the app is then wedged.

This made sense when designed because the Replicache protocol doesn''t support deleting client records, so this situation is not supposed to happen.

However, it does happen all the time during development, when deleting server databases is common. And unlike in Replicache 12 just refreshing doesn''t solve it, so this is extremely confusing and frustrating.

I think this blocks releasing Replicache 13, because the behavior is so much worse than 12.

I think that in development mode (when NODE_ENV === "development"), when Replicache receives `ClientStateNotFound` from the server, it should by default:

1. Delete all local state
2. Refresh app
3. Print an error to the console saying what happened

I think this is very similar to other error paths we have already so it should be easy to reuse.

In production, the current behavior should remain as-is.','public'),
	 ('eLdyfZP3th1jJmzZxS_R-','Reflect (and Replicache?) doesn''t run on Safari 15.3 due to reliance on BroadcastChannel',false,1709537380000,1689207055000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'See: https://discord.com/channels/830183651022471199/1055564993883549787/1091435550403219576

Is there any other reason we can''t support older Safari? If not we should at least turn this into a no-op (disabling cross-tab sync on older Safari) but perhaps also bring back the local-storage based workaround.','public'),
	 ('RWM0Ij3ALI6mTA5l7At0A','Create and store a custom token to authenticate the reflect-cli ',false,1689276172000,1689187465000,'ieK09sy2C_AIWE8KRkrQR',NULL,'In order to (1) authenticate correctly (e.g. handling token expiration and refresh) and (2) leverage the `firebase` client sdk, the reflect-cli needs to initialize the internal firebase user with `signInWith{Xyz}()` method of the firebase auth library.

The way to do this is to convert the github-based authentication retrieved in the login page to a "[custom token](https://firebase.google.com/docs/auth/admin/create-custom-tokens)". This must be done by privileged, server-side code, and the `user-ensure` function is a logical place to do so.','public'),
	 ('Is9thv8wlN9pyAK3J6KAf','What to do about REFLECT_AUTH_API_KEY in reflect apps',false,1702714212000,1689061278000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Right now we need to set the CF secret REFLECT_AUTH_API_KEY. It doesn''t matter what this is to kick the tires... But we need this to allow invoking the REST APIs.

We need to figure out how we want to expose this to reflect apps.','public'),
	 ('dIUIl0AyAS099CO8M-a1o','Make socketOrigin nullable for testing',false,1709536961000,1688755561000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'https://discord.com/channels/830183651022471199/1020392595450507304/1126942229542482160

This seems like a reasonable easy idea.','public'),
	 ('GcG6tP5TSs9Sg6YaQElnO','Fix withAdminAuthorization',true,1688632750000,1688632750000,'nqYkxAGMnzk7Y5STjZryV',NULL,'withAdminAuthorization is currently just a hack. Figure out how to deal with this the right way in Firebase.','public'),
	 ('ZhUktojn1c_KNQY7PFqTX','license check request failing fo react native',false,1687814591000,1687545835000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'eh_rob reports on discord https://discord.com/channels/830183651022471199/1121148590723711116/1121148590723711116



Hi,
I''m having an issue with the replicache license status ping from a react-native application. Here''s the response I receive:
```
Error sending license active ping: Error: Got 400 fetching https://replicache-license.herokuapp.com/api/1.0/license/active: [
  {
    "code": "invalid_type",
    "expected": "string",
    "received": "undefined",
    "path": [
      "licenseKey"
    ],
    "message": "Required"
  },
  {
    "code": "invalid_type",
    "expected": "string",
    "received": "undefined",
    "path": [
      "profileID"
    ],
    "message": "Required"
  }
]
```

I have verified that the license key payload is being sent in the request. I can see it in my network request logs, and it matches the body sent from a desktop web browser.
Request body format from logs:
```
{"licenseKey":"xxx123mylicensestring"}
```
and my headers look ok:
```
Accept: application/json
Content-Type: application/json
```

Furthermore, I have no issues with the same key from a desktop web browser react app. I''ve compared the request payload and it''s identical. 

Are there maybe some additional headers the license server is looking for that the mobile application isn''t sending?

I recognize that react-native isn''t "officially" supported, but I''ve had success with the TEST_LICENSE_KEY using this helpful package: https://github.com/Braden1996/react-native-replicache

Thanks in advance for any help!','public'),
	 ('vGmjjI13s36VXwdf36Dzx','wrangler publish from vercel deploy frequently failing',false,1687793082000,1687497489000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'<img width="816" alt="image" src="https://github.com/rocicorp/mono/assets/19158916/8b04e173-e1ae-49ca-8a7d-da36c125ab04">


It succeed when I publish from the command line from my machine.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('87zPmfDfX5GQVTh4NS0VX','Text exiting ',false,1709537394000,1687496980000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Replicache/Reflect are going to need a recommended solution for text editing.

Perhaps this is just yjs. However it seems worth understanding alternatives.

Does OT make more sense since we are server authoritative? https://twitter.com/justinfagnani/status/1664183367586451458?s=46&t=oIKhjpiwUquk0cgppqu9Ng

codeMirror has a design that seems very compatible with us (immutability, transactions, etc): https://codemirror.net/docs/guide/


https://twitter.com/ekzhang1/status/1672043743115968516?s=46&t=oIKhjpiwUquk0cgppqu9Ng','public'),
	 ('o06_1qH-I4kJFL_6cI8SQ','reflect 0.27.1 logs its version as 0.0.0',false,1687524570000,1687477622000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'The version is logged incorrectly by both the client and server.

<img width="1484" alt="image" src="https://github.com/rocicorp/mono/assets/19158916/8990acc0-3ada-45c2-af92-b8d560ddf4c9">
','public'),
	 ('EdU6LLE7VatzZZXmMFnna','replicache 13.0.0-beta.1 ChunkNotFoundError when calling tx.get on an already deleted key',false,1689241106000,1687203625000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'Customer report of reproducible ChunkNotFoundError in replicache 13.0.0-beta.1.
https://discord.com/channels/830183651022471199/1119036709007540257/1119036709007540257

Videos with details of repro:
https://drive.google.com/file/d/1B8i3DeaF6rWOp7aGZIrLdJEfnYhcNyAJ/view?usp=drive_link
https://drive.google.com/file/d/1X8p_LlEu4JvRkqE1618NlrCewhlDhZBO/view?usp=drive_link

Stack trace:

```
Uncaught (in promise) ChunkNotFoundError: Chunk not found 5a3cf77d13ac41a1ab2e764dda4bf06e000000002056
    at mustGetChunk (webpack-internal:///(app-client)/./node_modules/replicache/out/replicache.js:987:9)
    at async BTreeWrite.getNode (webpack-internal:///(app-client)/./node_modules/replicache/out/replicache.js:2267:19)
    at async findLeaf (webpack-internal:///(app-client)/./node_modules/replicache/out/replicache.js:1803:16)
    at async BTreeWrite.get (webpack-internal:///(app-client)/./node_modules/replicache/out/replicache.js:2288:18)
    at async getInternal (webpack-internal:///(app-client)/./node_modules/@rocicorp/rails/out/index.js:92:17)
    at async getImpl (webpack-internal:///(app-client)/./node_modules/@rocicorp/rails/out/index.js:49:12)
    at async getBlockOrCache (webpack-internal:///(app-client)/./src/replicache/schema/repo-space/block/mutators/re-hash.ts:50:23)
    at async eval (webpack-internal:///(app-client)/./src/replicache/schema/repo-space/block/mutators/re-hash.ts:59:13)
    at async Promise.all (index 0)
    at async eval (webpack-internal:///(app-client)/./src/replicache/schema/repo-space/block/mutators/re-hash.ts:58:9)
    at async Promise.all (index 0)
    at async rehashBlockPaths (webpack-internal:///(app-client)/./src/replicache/schema/repo-space/block/mutators/re-hash.ts:57:5)
    at async deleteBlockAndNestedBlocks (webpack-internal:///(app-client)/./src/replicache/schema/repo-space/block/mutators/crud.ts:331:9)
    at async eval (webpack-internal:///(app-client)/./src/replicache/schema/repo-space/block/mutators/crud.ts:340:13)
    at async Promise.all (index 0)
    at async deleteBlockAndNestedBlocks (webpack-internal:///(app-client)/./src/replicache/schema/repo-space/block/mutators/crud.ts:338:5)
    at async deleteBlock (webpack-internal:///(app-client)/./src/replicache/schema/repo-space/block/mutators/crud.ts:259:5)
    at async deleteBlocks (webpack-internal:///(app-client)/./src/replicache/schema/repo-space/block/mutators/crud.ts:300:9)
    at async eval (webpack-internal:///(app-client)/./node_modules/replicache/out/replicache.js:8890:24)
    at async using (webpack-internal:///(app-client)/./node_modules/replicache/out/replicache.js:3189:12)
    at async Object.eval [as deleteBlocks] (webpack-internal:///(app-client)/./node_modules/replicache/out/replicache.js:8853:29)
    at async onDelete (webpack-internal:///(app-client)/./src/app/(dash)/[team-slug]/(repo)/[repo-slug]/content/components/sidebar-tabs/explorer/context-menu-actions.tsx:104:13)
```

cc @arv 

The users description of the mutator that reproduces this first deletes a key and then tries to read that key in the same mutator transaction.','public'),
	 ('pZ11EgiQRZFsu-WEK6Det','Reverse scan',false,1709536236000,1687162547000,'nqYkxAGMnzk7Y5STjZryV',NULL,'The APIs for BTrees generally includes a reverse scan. It should not be too hard to implement.

Strawman API. ScanOptions gets a `reverse` flag.

Internally it might make sense to separate into a reverseScan. I haven''t thought too hard about it yet.','public'),
	 ('wDYB9lsmTFlpp8a-3Rhqk','RoomDO overloaded by single user scribling',false,1709537409000,1686854010000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'Monday reports a reproducible means of causing all clients connected to a room to disconnect.

<img width="991" alt="image" src="https://github.com/rocicorp/mono/assets/19158916/5b578906-5e75-4488-8f66-cf3d5856107c">
https://drive.google.com/file/d/1IhKSMNw0P1kUAy3i8MlgYvn9BG6iOhwy/view

Looking at the logs, I see that the RoomDO is overloaded.  
<img width="1675" alt="image" src="https://github.com/rocicorp/mono/assets/19158916/2073eafd-2840-4fc1-a342-c7c647302854">

It is concerning that a single user drawing rapidly can overload a DO. 

Note I can reproduce with drawing but not with just moving the cursor.  I''ve asked Noam to share the code for the mutators involved in drawing.  

cc @aboodman @d-llama ','public'),
	 ('GVkdnxtXN2bBzlaZsLG_v','Throw uncaught exceptions in addition to logging at log level error ',true,1686850051000,1686850024000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'In replicache/reflect we tend to catch errors in background processes and if they are not an expected error case, log them at log level error.  

This results in these unexpected error cases not showing up in customer error reporting systems like Sentry.

One example is errors in subscribe bodies.  We should audit and fix everywhere we do this.

Background: 
https://twitter.com/aboodman/status/1668590555880890368

<img width="960" alt="image" src="https://github.com/rocicorp/mono/assets/19158916/6e1f2e7b-f240-4624-baf9-f82f7a2071d5">


cc @aboodman @arv ','public'),
	 ('w23SLgSbmbNHaYm3j_L-C','No need to depende on tslib',false,1686840666000,1686824620000,'nqYkxAGMnzk7Y5STjZryV',NULL,'I hope

https://github.com/rocicorp/mono/blob/41874645c1e3fcc7843ba4dea2e3028331b875e0/mirror/mirror-server/package.json#L30','public'),
	 ('XrL0gir52YcWLkKodQHBg','After disconnecting due to Reflect.close call, we still get ConnectTimeout',false,1709537461000,1686605241000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'This also results in the error log `disconnect() called while disconnected`.

<img width="1432" alt="image" src="https://github.com/rocicorp/mono/assets/19158916/eb44d671-d3ec-4501-9b88-88364c709b7e">
','public'),
	 ('AHrj7iDzz_Vo8YRsq8uz4','Figure out what to do with reflect-server/cli bin',true,1709537486000,1686577181000,'nqYkxAGMnzk7Y5STjZryV',NULL,'`@rocicorp/reflect-server` has a binary in it. What do we want to do with that?','public'),
	 ('f4qnSWqFPW-vSm0KhfD3i','remove version from reflect-server',false,1697008956000,1686564031000,'nqYkxAGMnzk7Y5STjZryV',NULL,'https://github.com/rocicorp/mono/blob/df4a4952757f3692d913fc26e941c49db9846746/packages/reflect-server/src/util/version.ts#L1-L3','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('88r3PFTk8Ztr5C8Opry1P','Map "util" to "node:util" in reflect-server package',false,1686841267000,1686087231000,'ieK09sy2C_AIWE8KRkrQR',NULL,'              I think we can map "util" to "node:util" and it will work with cf workers... Maybe file an issue to follow up on this in the future.

_Originally posted by @arv in https://github.com/rocicorp/mono/pull/568#discussion_r1219561850_
            ','public'),
	 ('8yCQ65_2H0D80hgfMlZHc','Add timestamps to authentication and invalidation protocols',true,1709537485000,1685638992000,'ieK09sy2C_AIWE8KRkrQR',NULL,'Currently, the AuthDO relies on locking and request ordering to avoid one direction of authenticate-invalidate race condition (detailed [here](https://www.notion.so/replicache/Invalidating-Auth-732e9f9abb6a4806b5461c87dfde580f?pvs=4#fc93bf38e9f44bc48aa7d40dd9a678a1)). This can lead to unbounded contention since the `authHandler` can do arbitrary I/O, including calling out to external systems.

The way this race is generally handled in the industry is with timestamps. Each auth token encodes a timestamp, and an invalidation sets a watermark for the earliest valid auth token. (This is how you get logged out of Google properties when you change your Google password). For example, the [decoded contents of a Firebase auth token](https://firebase.google.com/docs/reference/admin/node/firebase-admin.auth.decodedidtoken) includes an auth_time. 

In reflect, authentication is opaque to us and we rely on the customer''s `authHandler` and invalidation request to dictate validity of authentications. As such, we can improve our guarantees without relying on locking and request ordering by adding timestamps to both the response of the `authHandler` and the payload of invalidation requests.

Note that this does involve a protocol change to the customer API, so it should be done in a backwards-compatible manner.

@grgbkr @aboodman  ','public'),
	 ('wcchWo_CFGlyYnr8yB2u1','Miscellaneous logging cleanups',true,1685127716000,1685127702000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'I tried to do some, but they didn''t work. See:

https://github.com/rocicorp/mono/pull/551#issuecomment-1564803873

Need to redo this correctly. Also while we''re at it, remove all the "id" suffixes from log attributes:

https://rocicorp.slack.com/archives/C013XFG80JC/p1685123287214549','public'),
	 ('CNaWciBKu4nMCavPO4Ldo','Unify reflect npm packages',false,1686240532000,1685004872000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Today we have two npm packages:

| Name | NPM Package name | Code Location |
|--------|--------|--------|
| Reflect Client | `@rocicorp/reflect` | packages/reflect |
| Reflect Server | `@rocicorp/reflect-server` | packages/reflect-server | 

The idea is to have a single npm package called `@rocicorp/reflect`. This package will export 2 modules called `@rocicorp/reflect/client` and `@rocicorp/reflect/server`


| Name | NPM Package name and subpath |
|--------|--------|
| Reflect Client | `@rocicorp/reflect/client` |
| Reflect Server | `@rocicorp/reflect/server` |

To make this easier to maintain I suggest we move the current `packages/reflect` source to `packages/reflect-client`.

We treat `packages/reflect-client` and `packages/reflect-server` as internal packages.

Then we create a new `packages/reflect` which imports from the two internal packages.','public'),
	 ('kwDP0tjSPolsKWaaVS4eR','Cleanup Reflect connection loop',true,1684868791000,1684868777000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'The code is hard to follow currently and it''s very important that this be clean and understandable.

some thoughts:

https://www.notion.so/replicache/Offline-error-in-homepage-c3b040663a6a43febc81f998dc569d23?pvs=4#d5dd0bd1a3434ab4ab20cd92d4bb94f9','public'),
	 ('YGeybqYWwm2-fg0bFkUin','First-class pattern/solution for presence',false,1709771730000,1684747154000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'There should be a way to implement presence that works easily and well.

Goals:
- associate state with clients/users that are connected
- automatically delete this state when clients disconnect
- doesn''t get confused by mutation recovery
- integrates naturally with persisted state
- don''t bother persisting this state locally
- don''t bother resending changes related to this state when reconnecting from offline

Background docs:
https://www.notion.so/replicache/Presence-problem-71423904be8b4bc39e030084fdb5f890
','public'),
	 ('sCJlnWxTWTQ7eH7lAgJiZ','Create `options` every time DO is instantiated.',true,1684744265000,1684664790000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Caching across instantiations creates problems. See: https://rocicorp.slack.com/archives/C013XFG80JC/p1684615253955599','public'),
	 ('Uupa3zHa0DwmPYHxqCUk6','Update embedded wrangler to v3',true,1684653023000,1684653023000,'OeVnr1y5bEM_Yg06sUFtD',NULL,NULL,'public'),
	 ('HqZr5_QRlbydvzDlXVSxD','dx: monday requests online state change to indicate the error / reason for disconnect',true,1684481018000,1684481009000,'OeVnr1y5bEM_Yg06sUFtD',NULL,NULL,'public'),
	 ('7SOdJwXzzf4JBDHBd_aBc','dx: either start out ''online'' or else add a first-class ''connecting'' state',true,1685448047000,1684479871000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We hit this with reflect.net and Monday just reported it to me.

Currently Reflect''s `online` state starts out `false` which is pedantically true: we don''t yet know whether we''re online so from Reflect''s POV we''re offline.

But from a ux pov this is pretty weird looking. It looks as if something is broken -- you load the app and then see an indicator that it''s offline.

The easy thing would be to just default the other way -- (a) Reflect starts out online. A more technically correct and flexible thing would be to (b) add an ''unknown'' state which is used only at startup, until the first connect attempt completes.

Note that #498 would also solve this in the sense that user could just wait until `onFirstSync` but (a) this makes it harder to implement the online status UI for our users because they have to string together two APIs, and (b) it unnecessarily delays being able to tell the user the online status (because have to wait for actual pokes to be downloaded and played).

I think we should do (b) if it''s not too hard but (a) is also a reasonable solution.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('goP7mmTOY5qoIHaUNC8V4','clients that dont send messages get closed as idle when they shouldnt',true,1684744361000,1684271174000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'I misunderstood how ping works.  I thought the client sent a ping every 5 seconds.  Actually the client only sends a ping if its been 5 seconds since it sent or received a message.  

Due to this misunderstanding this change https://github.com/rocicorp/mono/commit/22ac5ffd742525586ce2ab5edefc7af43dda86c5 is causing clients that are only receiving messages but not sending any to be disconnected after 10 seconds.  ','public'),
	 ('CAWmB8ZfnsGYd2xF51NGc','processPending/processRoom/processFrame naming is poor',true,1684744380000,1684041928000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'I think at one point these made more sense, but the code and names no longer match.

1. `processFrame` does not process one frame
2. What is the distinction be `processPending` (for the roomDO) and `processRoom

Also their is a lot of redundancy in the testing of processPending/processRoom/processFrame.

processPending''s responsibilities are

1. identifying idle connections and closing them
2. selecting the next chunk of mutations to process
3. calling processRoom
4. sending pokes

processRoom''s responsibilities are:

1. creating fast-forward pokes for any clients that are behind
2. calling processFrame

processFrame''s responsibilities are:

1. processing mutations
2. running disconnectHandler
3. creating poke messages for the patches from above mutations and disconnectHandler


We should consider refactoring and renaming things here, possibly something like:

1. processPending is renamed to `processTurn`
2. processTurn calls in turn: `closeIdleClients`, (possibly in the future `processConnects` if we add support for onConnect handlers), `fastForwardStaleClients`, `processMutations`, `processDisconnects` and `sendPokes`.
3. processRoom and processFrame go away.

This refactor is easy except for the tests... its going to be a lot of work to rework the tests.
','public'),
	 ('RoWgbnz3DPcuui1NG_5cG','RFE: We probably need a "connecting..." state too',true,1685448060000,1683939602000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'While doing the puzzle demo, I found myself wanting to distinguish to the user between whether we were connecting or disconnected.','public'),
	 ('bvRr_D9RX0FFIXzgG3B4T','Replicache CTA out of date',false,1684744418000,1683858614000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'The Replicache CTA say:

<img width="1082" alt="CleanShot 2023-05-11 at 16 27 51@2x" src="https://github.com/rocicorp/mono/assets/80388/d77c0b8f-ad26-402f-82a1-689afc06527f">

However:

a) This is no longer the easiest way to get started. Our docs now start with an interactive tutorial: https://doc.replicache.dev/, which is the easiest way. 

b) In conflict with the copy, the docs (https://doc.replicache.dev/) no longer have more details on setting up the starter app.

I think the CTA should change to something like:

_You can learn the basics of Replicache in fifteen minutes using our interactive tutorial._

_**Start Learning**_','public'),
	 ('Q5avePCIp2219ZVlJ52Jj','RFE: `onFirstSync`',true,1683837800000,1683837790000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Users often want to either delay displaying state until after the first sync, or show a progress bar to let the user know that an update is happening.

Two examples:

* In the ALIVE demo, we disconnect when switching away from the tab and reconnect when switching back. When switching back, we display cached data (either persisted, or in memory, doesn''t matter) and then a second later first sync happens and the puzzle jumps to a different state -- typically *very* different because the bots have been running around moving things. It would be better ux to just delay display until we get that first sync.

* In Notion, and many other collaborative apps, it''s common to display the cached state to allow fast loads, but then hvae a spinner in the corner to let the user know that a background refresh is happening.

For both of these we need some kind of API to let the client know it''s gotten up to date with the server.

Of course the server is constantly changing, so how do we define "first sync". What I''m thinking is that this event would fire after the client receives the fast-forward pokes. That is, after it catches up to the state that the server was at at the moment it connected.','public'),
	 ('p4FPenwGPjcMXlbU9Y70i','Monday metrics have a very large number of pings that never connect.',true,1684744480000,1683764067000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We finally got our metrics deployed to Monday and it is currently saying that something like 12% of users do not connect:

https://app.datadoghq.com/dashboard/vm5-ce6-p67/reflect-client-metrics?fullscreen_end_ts=1683764009111&fullscreen_paused=false&fullscreen_section=overview&fullscreen_start_ts=1683760409111&fullscreen_widget=8840315099850438&tpl_var_host%5B0%5D=app.workcanvas.com&tpl_var_service%5B0%5D=canvas-metrics-service&from_ts=1683754105220&to_ts=1683757705220&live=true

![CleanShot 2023-05-10 at 14 14 01@2x](https://github.com/rocicorp/mono/assets/80388/917a9bf9-98aa-4390-9b01-f05583f9ddac)

I dont think this can possibly be true, but we need to understand what''s going on here.','public'),
	 ('PG_082z3sXSzgke7wIADG','Add a auth_time server metric.',true,1685039985000,1683748191000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Sadly this will be out very first server-side metric, so it will require some infrastructure to be built/ported from the client.','public'),
	 ('Hu_H_-IzMPB5qZXo8SvN7','Fine-grained write auth',false,1709537612000,1683342179000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Currently, Reflect supports authentication (who is the currently logged in user) and coarse-grained authorization (can they access this room), but not fine grained authorization (can they access this field? can they modify this field?).

When @grgbkr originally designed auth, there was a feature for the auth handler to return additional "context" which could be exposed to mutators and used for write-time authorization decisions.

Our customers frequently ask for the ability to have "admins" for certain rooms, who are the only ones allowed to edit. This feature could be used for that. The auth handler could return an additional `isAdmin` flag which the mutator used 

For example, one thing you could do is return a bit which says whether the user is an admin. This bit could then be used by mutators to decide how to function.

This idea is sketched here:

https://www.notion.so/replicache/Reflect-Authentication-Authorization-and-Security-bf2b1a2e31844efca73c4aa453fdecdb?pvs=4#caf5a6970f6345b99170dbb6f7ec0a9e

As a starting point, even exposing the `userID` to the `WriteTransaction` would be useful, as it would enable features like editing your own content but not other users''.

We would have to decide whether to add yet another field to Replicache''s `WriteTransaction` that really only makes sense on the server, or to extend a new `ReflectWriteTransaction` that adds this field.','public'),
	 ('hsr9sWG_TMiESLo9gCCxr','Monday: dramatic increase in "accepting connection to send error"',false,1683579930000,1683227016000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We have a sharp increase in this error starting in reflect-server@0.23.0-beta.0.

See analysis here:

https://replicache.notion.site/replicache/Monday-response-bda8cdf4514b44869487a02424090e18

and original report/context here:

https://discord.com/channels/830183651022471199/1055564082087989299/1103256991075422289','public'),
	 ('D4sNwXulmL2z6gSqgzc7d','Monday error: "Durable Object exceeded its memory limit"',false,1683574984000,1683226901000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'This appeared in reflect-server@0.23.0-beta.0. It is happening in authdo. Unfortunately there''s no stack so it''s hard to know where exactly it''s coming from but the error message gives some hints.

Details here:

https://replicache.notion.site/Monday-response-bda8cdf4514b44869487a02424090e18

Original report and context here:

https://discord.com/channels/830183651022471199/1055564082087989299/1103256991075422289

','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('KkLC0bkzlaK4DnE24cLCe','Disconnect due to switch tabs is not reported to `onOnlineChange`',false,1683203232000,1682921147000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'When you switch to a different tab for 10s, we disconnect properly and reconnect on switch back, but we don''t report this to `onOnlineChange`.

It''s important that this work because it''s a common pattern to destroy client state in `onDisconect` and recreate it in `onOnlineChange`.

All reasons for disconnect should be reported to `onOnlineChange` with the one exception that we should retry reconnect one time before reporting the change to the app.','public'),
	 ('tgTx3g8nLTtBXZO1Z3C0S','Reflect enters a tight reconnection loop when going offline',false,1683100909000,1682920776000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'When you are in a reflect app and go offline (ie by killing the server), the client enters a tight loop trying to reconnect without waiting in between attempts.

See https://www.notion.so/replicache/Offline-error-in-homepage-c3b040663a6a43febc81f998dc569d23 for details.','public'),
	 ('SMD8Txp3I8v6aKgaqOBhK','Announcement burndown',false,1684869621000,1681849319000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'https://www.notion.so/replicache/Announce-Burndown-98c1e453429145f7af7913cfb3993a61','public'),
	 ('-f91Mdf0XQuX8XPPi9swV','Remove allowUnconfirmedWrites option',true,1709537612000,1681411313000,'nqYkxAGMnzk7Y5STjZryV',NULL,'We should not expose this in the API. It is not safe?','public'),
	 ('lLDCNkFWQKf-CR_-FmH8H','Hook up metrics to alive demo and "how it works" demos',false,1681819178000,1681243253000,'OeVnr1y5bEM_Yg06sUFtD',NULL,NULL,'public'),
	 ('L5S1gvDaQKHQz0xaXxWAG','Don''t load reflect instances for "how it works" demos until they are scrolled into frame',false,1681172302000,1680919652000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'I am pretty sure that they are delaying startup of "alive" demo.

You can confirm this improved startup by looking at the console output:

<img width="545" alt="CleanShot 2023-04-07 at 16 06 54@2x" src="https://user-images.githubusercontent.com/80388/230698612-224a78b4-1468-4b5b-bc7c-4ec21b1eb79b.png">
','public'),
	 ('lxNic7aS6Z8ip9Jdc24Gr','Do something about "offline" marketing',false,1681849256000,1680638826000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Currently if you go offline and draw a bunch it will be slow to reconect due to #384. We either need to soften the marketing (coming soon) of this feature or else change the impl in the demo sufficiently so that it works.','public'),
	 ('56TpUjT6eLFKe2Of8w5_t','authHandler should allow null to mean not authenticated',false,1681811411000,1680568588000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Right now the signature is that it throws which is a little non-idiomatic (throwing should be for exceptional circumstances).','public'),
	 ('pkY5VRD91YWXX0AYIT8Qt','API nit: warn on potential deadlock',true,1684744576000,1680567962000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'It''s an easy footgun to open a transaction nested inside another transaction. When this happens neither will proceed and we deadlock. Here''s an example of this happening to a user:

https://discord.com/channels/830183651022471199/1092315354606350386/1092561231753261187

It would be nice to detect this and warn the user.

I do not think we can do the obvious thing, which is to detect user opening a transaction while another is running because it is actually totally legitimate to do that. The problem is not:

- open tx1
- open tx2

It''s that in addition to above, tx1 waiting to close for tx2 to close. In that case, we deadlock. But I do not think that is possible for us to detect because we have no way to know *why* tx1 isn''t closing.

Instead, what I think we could do is: *if* the user opens a transaction *and* there is already running, then start a timer for say 5 seconds. If the lock hasn''t released on 5s, then tell the user there was a deadlock and that they may be executing nested transactions.

---

We may also want to improve the documentation of `mutators` and `query` to make it clear that these are exclusive of each other.','public'),
	 ('qyhnHQo5MX_O15GeAOoEY','User report in Replicache 12.0.1: replicache hangs with particular mutator',false,1680568042000,1680548661000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'This original report is: https://discord.com/channels/830183651022471199/1091153888704475226/1091153888704475226

We have established a private Discord channel here: https://discord.com/channels/830183651022471199/1092315354606350386

Sleanshot is not a paying customer but has the potential to be. User has put up a repro of the bug at https://app-dev.sleanshot.com, along with instructions:

<img width="1097" alt="CleanShot 2023-04-03 at 09 03 00@2x" src="https://user-images.githubusercontent.com/80388/229602603-373e84e3-e821-446f-bc1d-0296ada57775.png">

You can''t access this URL yet until customer adds you to the account. I have investigated this using the minified build briefly and the hang is waiting on RwLock in LazyStore for some reason.

User is currently trying to facilitate further debugging.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('O_X2gNFEjvX4_m8Sh0jA0','Questions about connection loop',false,1681480897000,1680336011000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'<s>First, it seems like the ping timeout logic is not correct.

https://github.com/rocicorp/mono/blob/main/packages/reflect/src/client/reflect.ts#L883:

```
// When connected we wait for whatever happens first out of:
// - After PING_INTERVAL_MS we send a ping
// - We get disconnected
// - We get a message
// - The tab becomes hidden (with a delay)
```

ISSUE 1: This seems like it will result in the ping continually getting pushed back 2s on each new message, which might result in us not sending pings for arbitrarily long when the socket is busy. I think I have seen us disconnect due to ping timeout under busy conditions frequently.</s>

This one was incorrect -- I forgot that it''s the client that measures ping timeouts, not the server.

===

Stepping back one level, I''m not sure why we wait for a message in the first place? We [await `#nextMessageResolver`](https://github.com/rocicorp/mono/blob/main/packages/reflect/src/client/reflect.ts#L898).

In the case it fulfills, we do nothing and the loop repeats (only pushing back the ping). In the case it rejects, we jump to the catch block: https://github.com/rocicorp/mono/blob/main/packages/reflect/src/client/reflect.ts#L922.

There are two cases where `#nextMessageResolver` rejects:
1. handling an invalid server->client message: https://github.com/rocicorp/mono/blob/main/packages/reflect/src/client/reflect.ts#L449. Note that in this case we do *not* disconnect -- the socket stays connected.
2. handling an error message from server: https://github.com/rocicorp/mono/blob/main/packages/reflect/src/client/reflect.ts#L531. In this case we do disconnect.

So that means in the catch block, in the case where `#nextMessageResolver` rejected:

- The error count gets incremented either way: https://github.com/rocicorp/mono/blob/main/packages/reflect/src/client/reflect.ts#L946
- We set ourselves offline and sleep either way: https://github.com/rocicorp/mono/blob/main/packages/reflect/src/client/reflect.ts#L955, even though we did not necessarily disconnect

ISSUE 2: This seems inconsistent to me. If we are telling the app that the connection is offline, we should actually be offline. Also we should not be receiving messages, but I think in this case we continue to.

===

While a ping is running the connection loop is not waiting on `nextMessageResolver`. But messages do continue to be processed during a ping.

ISSUE 3: So this means that in the special case of an an invalid message being received during a ping, it will *not* enter the catch block and have the error handling behavior.

<s>ISSUE 4: Actually we don''t listen to visibilityWatcher during ping either. So if it happens while a ping is outstanding we will just miss it and stay connected: https://github.com/rocicorp/mono/blob/main/packages/reflect/src/client/reflect.ts#L898</s> -- this one was also incorrect.

===

Summing up it seems like:

- We could get rid of `#nextMessageResolver` completely. I''m not sure what purpose it has, and I think it is causing many other issues in here. <s>This would immediately fix ISSUE 1.</s> It would also fix ISSUE 2 and ISSUE 3 in that an invalid server message received would not ever trigger the catch block, which is consistent with not closing the connection.
<s>- I think that handling visibility change and disconnecting should happen outside the connection loop. That way it will happen even when a ping is outstanding. This would fix ISSUE 4.</s>','public'),
	 ('wFOe5fd5CIwU46Pggrsoy','Remove `options.metricsIntervalMs`',false,1680341645000,1680312752000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'It was added by mistake.','public'),
	 ('WCJgLUfx56evqh-hajX5j','Missing commit in refresh',false,1680598128000,1680255857000,'nqYkxAGMnzk7Y5STjZryV',NULL,'This is not the reason for ChunkNotFound but it is wrong.

https://github.com/rocicorp/mono/blob/3263520a6de4700ece9500ae28e3507c058c4811/packages/replicache/src/persist/refresh.ts#L235','public'),
	 ('2RTqHBMvaOHkcq5Ldv_0q','Log/metric Reflect client version',false,1680341662000,1679819306000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'It would be super useful when debugging to know for sure what version a particular client is on. We should log out at info level the version of Reflect at startup. We should also likely include this information in the tags for the two metrics.','public'),
	 ('imzYzehAVGxYIWIt0QzTo','Connectivity: canary request',false,1684606603000,1679818157000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'The error handling in the browsers on socket requests is not as good as normal requests. This can make it hard to understand why sockets are failing. For example, we recently saw socket connections failing due to ssl errors, but nothing was reported to log. When we do an https request to same host, then it fails with a clear ssl error.

For this reason I think we should do a "canary" https request in parallel with the socket connection and include whether it failed or succeeded in the logs and in the metrics.

For the metrics, it seems like the best thing would be to include it as a "tag" in the last_connect_error metric. That way we can easily compute how many of a particular error type have a canary request that is succeeding/failing.','public'),
	 ('vc6NI-cMUz74BXqgBTqLb','Mutators thrown out when running with --local --persist',false,1680164279000,1679610623000,'_4MJeHV4T-qZb52fyNwQ9',NULL,'On 771395cf68379850e1b3ec03ce81cf0436799506 (but notably not on the the latest npm versions), when running `npx wrangler --local --persist`, mutators are successfully pushed to the server, but the server never runs them (infinitely prints `running 0 of 1 pending mutations`.

- [x] Repro in reflect-todo
- [x] Verify on latest HEAD of mono','public'),
	 ('Qrjv44_PJ1zw2-Ly9kGuH','ChunkNotFound',false,1681258385000,1679607456000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Both Monday.com and Substack have reported this.

Here is a video showing this for substack:

https://share.cleanshot.com/3rQsRdk2

Here is the Discord thread: https://discord.com/channels/830183651022471199/1055564993883549787/1088529642463449148

From the video we see this:

<img width="641" alt="image" src="https://user-images.githubusercontent.com/45845/227367816-bcb771b2-16a2-4bc4-be3a-bdfe8a2c22d4.png">

There is one call to `visitCommit` in refresh: 

https://github.com/rocicorp/mono/blob/c882ec2da6719927e564f7249187e1c7bd716682/packages/replicache/src/persist/refresh.ts#L116-L121','public'),
	 ('vUZmerDq0nELFiwd8bHnP','fastForward sending deletes on sync from null',false,1679593702000,1679552888000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Our fast-forward impl in Reflect sends deletes on sync from null.  These deletes are not needed since a client syncing from null has no state.  The impact is that in long-lived rooms the number of deletes just builds up and up forever :(.

See: https://rocicorp.slack.com/archives/C013XFG80JC/p1679552209080409','public'),
	 ('Z4cdvgUqkwEMoHFYxCh24','API nit: Remove `allowUnconfirmedWrites` and related code. It''s no longer needed.',false,1683333565000,1679466100000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'@grgbkr is there any reason not to do this right away?','public'),
	 ('snYqI7sUWdElbDgtcgl34','valita when deployed on nextjs results in runtime error',false,1680044558000,1679428252000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'This line of code
https://github.com/badrap/valita/blob/a74795390ef45c34178aa82ff2f38211d6c906dd/src/index.ts#L1360
```
class NeverType extends Type<never> {
```
ends up being compiled to:
```
, F = new class extends (null) {
```
Which results in the error:
```
main-e47071de22fcf081.js:1 TypeError: Super constructor null of anonymous class is not a constructor
    at new <anonymous> (124-07691b1e95823a08.js:1:143075)
    at 4958 (124-07691b1e95823a08.js:1:143037)
    at r (webpack-24780b5468e42e63.js:1:148)
    at 3551 ([id]-e3b149a8aa989bc3.js:1:302)
    at r (webpack-24780b5468e42e63.js:1:148)
    at [id]-e3b149a8aa989bc3.js:1:156
    at main-e47071de22fcf081.js:1:30999
J @ main-e47071de22fcf081.js:1
12:43:52.820 
```','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('Xmx8XDTXi9lXAyI-ShBNw','Setup DNS for reflect.net',false,1680637390000,1679348452000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'reflect.net is currently under my namecheap account, as are roci.dev, replicache.dev, and friends.

Let''s setup reflect.net but not touch replicache.dev and roci.dev right now.

We should eventually transfer all of them into cloudflare, but I''m not sure how long that will take.','public'),
	 ('QE2eFvZrV3g9bN9hUzalE','Duplicate code in reflect npm module',true,1709537611000,1679303742000,'nqYkxAGMnzk7Y5STjZryV',NULL,'I need to verify this but...

reflect depends on replicache which has bundled all of its source. replicache bundles `@rocicorp/logger` (and many other things, including `shared`). reflect also includes `@rocicorp/logger` so we end up with two copies of the code in the bundled reflect.

- [ ] Add reflect to the bundle size dashboard
- [x] Verify that we are duplicating the code
- [ ] Do not duplicate the code. I''m not sure what the best way is but reflect should not depend on the bundled version of replicache. ','public'),
	 ('SlRUkfYdFGClrDSOh_4e6','Reflect announcement blog post',false,1681849183000,1679236811000,'OeVnr1y5bEM_Yg06sUFtD',NULL,NULL,'public'),
	 ('94ByP81RTZCxqhxKzLDFO','invalidateAll etc requires 2 roundtrips',false,1680568644000,1678698103000,'nqYkxAGMnzk7Y5STjZryV',NULL,'When we use the HTTP endpoint to invalidateAll (also cron I believe) the clients get disconnected.... Aaron outlined the flow in discord:

1. `new Reflect()` on client
2. Client calls `auth()` to get a token
3. Server calls `authHandler` to verify token
4. `authHandler` returns the user ID
5. `<connection success>`
6. Your server calls `invalidate*()`
7. server closes relevant connections
8. Client immediately tries to reconnect, using old tokens
9. Server calls `authHandler`
10. `authHandler` throws
11. Server returns auth error to client
12. Client receives auth error and calls `auth()` to get a fresh token
13. Client reconnects, sending new token
14. Server calls `authHandler` to verify token
15. `authHandler` returns `userID`
16. `<connection success>`

In step 7 when the server closes the relevant connection we can send a close code. If that close code is `AuthInvalidated` we call `auth` locally before trying to reconnect. Then the flow would be:

1. `new Reflect()` on client
2. Client calls `auth()` to get a token
3. Server calls `authHandler` to verify token
4. `authHandler` returns the user ID
5. `<connection success>`
6. Your server calls `invalidate*()`
7. server closes relevant connections with `AuthInvalidated` close code.
8. Client receives auth error and calls `auth()` to get a fresh token
9. Client reconnects, sending new token
10. Server calls `authHandler` to verify token
11. `authHandler` returns `userID`
12. `<connection success>`

Reducing the algorithm by 4 steps and removing the extra connection attempt.

The only question is if it is expected that the auth tokens returned in step 2 are generally revoked when a room auth is invalidated. If they are generally not revoked (cron) then there is no need to call `auth` on the client after the disconnect. 
','public'),
	 ('eYGW6TomsSBHX1TpqwMne','ReadTransaction::isEmpty not reactive',false,1678735159000,1678667440000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'```ts
rep.subscribe(async tx => tx.isEmpty())
```

doesn''t re-fire when the emptyness of Replicache changes.','public'),
	 ('6EhERUKvk2Az2oQwoSAt9','Do we want to ensure that the userID returned by `authHandler` matches what was passed into `Reflect` ?',false,1683402496000,1678579070000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Right now there''s no such checking:

https://github.com/rocicorp/mono/blob/main/packages/reflect-server/src/server/auth-do.ts#L408','public'),
	 ('zc-cGtvCOmXYIHUJrKSqN','Recover better from client being ahead of server during connect',false,1679359407000,1678418742000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'a8befac65daeb85e14df4b7ce28c67404d01136a cleaned up some error handling on the server, but it removed a helpful error message:

https://github.com/rocicorp/mono/commit/a8befac65daeb85e14df4b7ce28c67404d01136a#diff-acf49911961d801fe90465dd29429bab86cc16c07016381405263ddbc7ae2cffR88

This situation commonly happens during development and this error helped users know what to do. Now it happens and is confusing again:

https://discord.com/channels/830183651022471199/1055564993883549787/1083495573761560637

The server has errors defined for these two situations: https://github.com/rocicorp/mono/blob/main/packages/reflect-protocol/src/error.ts#L21 but I don''t think they are being used. Can we use them and then on the client if received we should:

- print warning
- dump local state
- reconnect','public'),
	 ('FXgRjIRgQbxf3BvxuWL9M','Fix Replicache public APIs containing DD31',false,1678894915000,1678308945000,'nqYkxAGMnzk7Y5STjZryV',NULL,'There are some public APIs that are named DD31. These were never meant to be exposed.

For example:

https://trunk.doc.replicache.dev/api/#pokedd31','public'),
	 ('859py59MYOCv4BPBfdL_y','Performance of reconnecting with significant offline mutations is terrible',true,1709537611000,1678295159000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'When reconnecting the CPU is pegged by rebasing mutations.  This is because, the pusher logic pushes up mutations individually and then they come down in a series of pokes.  Resulting in something like
1000 pending, poke contains 50, rebasing 950
950 pending, poke contains 50, rebasing 900
900 pending, poke contains 50, rebasing 850
... and so on

This is improved somewhat by the 60fps buffering and playback logic, as the mutations from multiple of the reflect pokes above will often get merged into a single replicache poke.  

This is related to: https://github.com/rocicorp/mono/issues/378','public'),
	 ('mJaf2zc-fsrE0k_C_bgh0','RFE: overload subscribe callback with a function',false,1693817777000,1678268312000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Subscribe currently have the type:

```ts
subscribe<R extends ReadonlyJSONValue | undefined>(
  body: (tx: ReadTransaction) => Promise<R>,
  options: SubscribeOptions<R>,
): () => void;
```

with `SubscribeOptions` defined as:

```ts
export interface SubscribeOptions<R extends ReadonlyJSONValue | undefined> {
  /**
   * Called when the return value of the body function changes.
   */
  onData: (result: R) => void;

  /**
   * If present, called when an error occurs.
   */
  onError?: ((error: unknown) => void) | undefined;

  /**
   * If present, called when the subscription is removed/done.
   */
  onDone?: (() => void) | undefined;
}
```

For convenience we should overload the second parameter to allow passing the `onData` function only:

```ts
subscribe<R extends ReadonlyJSONValue | undefined>(
  body: (tx: ReadTransaction) => Promise<R>,
  options: SubscribeOptions<R>,
): () => void;
subscribe<R extends ReadonlyJSONValue | undefined>(
  body: (tx: ReadTransaction) => Promise<R>,
  onData: (result: R) => void
): () => void;
```

The semantics would be that if the argument is a function then that is used as the `onData` function and `onDone` and `onError` would be `undefined`.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('kFxkx4JRIedt24BpowFC3','RFE: Add a default value to `ReadTransaction::get()`',true,1680568771000,1678236542000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'It''s pretty annoying having to type:

```ts
const count = (await tx.get("count")) ?? 0;
```

How about we can say:

```ts
const count = await tx.get("count", 0);
```','public'),
	 ('YT4h9n3H_ZNrOuiHYlmoA','Offline mutations should replay atomically',true,1678138925000,1678138908000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Right now when you go offline, then come back online, collaborators see your work replay on a poke-by-poke basis 😬. This will look pretty funny if there is a lot of offline work.

I think we need something in the protocol to indicate that mutations being sent were created offline. We''d then either (a) buffer the offline mutations server-side until we have them all and replay in one turn, or (b) process them as normal but tell the receivers to pause motion until replay complete.

(a) has the downside that we will eventually exceed server memory limits. (b) looks ugly but should in many cases actually happen pretty fast and may be the easier tradeoff.

Other options are to store the offline mutations somewhere server-side or maybe to use the socket itself as a queue and just process the mutations from the reconnecting client incrementally as part of the turn processing so that they are never in memory?','public'),
	 ('vSSKL90GWgRgqFRVAbk6F','Reconnect on `online` event (Reflect)',true,1691542322000,1677949309000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'I forgot how frustrating waiting 60s to reconnect is.

I think the max time to wait for reconnect when you have network should be 5s. Even that is going to feel slow and frustrating when you know you have connection, so there should also be a fast path that uses the browser online change event.','public'),
	 ('KsvZeY61p1OfeMapHBrsL','Expose `experimentalKVStore` in Reflect',false,1680313153000,1677942217000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'In Replicache 12.1.0 we added support to plug in a different kvstore:

https://blog.replicache.dev/blog/replicache-12-1-0

Users have been using this in Replicache to add SQLite support for React Native!

https://discord.com/channels/830183651022471199/1072640266684612748/1076897871900717106

Let''s expose `experimentalKVStore` in the Reflect constructor like it is in Replicache so users can use Reflect in RN too!','public'),
	 ('qR--IkCBAIebGeeZi8FGx','Let''s undo the undefined change',false,1686081641000,1677894564000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'See https://discord.com/channels/830183651022471199/1078726432559210536/1081168561071984681 for background.

The idea is to re-allow `undefined` as a value for `JSONObject`. We will not filter it out anywhere explicitly, just let `JSON.stringify()` do it. We have to handle `undefined` in all internal code when dealing with `JSONValue`, as will customers.','public'),
	 ('q7joMFz9UkevLyWkubjC2','Stop exporting all of `replicache`',false,1694088344000,1677842289000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'In reflect''s `mod.ts` we do:

```ts
export * from ''replicache'';
```

This was a lazy convenience. We should check what we need and only export that.','public'),
	 ('9LHQncTcD5cVGrQMf7jAS','Audit for places we are mutating values returned from EntryCache.  Consider making values in EntryCache immutable.',true,1677842339000,1677789451000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'              Is it safe to mutate the clientRecord. Can we add an issue to review the usage of mutable cache entries?

_Originally posted by @arv in https://github.com/rocicorp/mono/pull/355#discussion_r1122948847_
            ','public'),
	 ('fQ5N6benWwiTYuaktIhXi','Deploy Replicache docs from mono',false,1690363763000,1677767920000,'nqYkxAGMnzk7Y5STjZryV',NULL,'doc.replicache.dev is deployed by vercel from the doc branch of replicache-internal','public'),
	 ('ZeDD9LTgg6J2cxLhfU1nM','deleteAllReplicacheData broke in latest minor version',false,1677765782000,1677764777000,'nqYkxAGMnzk7Y5STjZryV',NULL,'https://doc.replicache.dev/api/#deleteallreplicachedata

This didn''t use to take an argument. We need to make this optional again.','public'),
	 ('a0P8t3TjA4959txnuLzty','Q: Why no jurisdiction for AuthDO?',false,1677789788000,1677762264000,'nqYkxAGMnzk7Y5STjZryV',NULL,'When we create the roomDO we pass in the jurisdiction. Is there a reason why we do not do the same for the authDO?','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('iJXY0shOGBxog0PBYWe_T','Mirror - The beginning of the Reflect saas',false,1700011707000,1677701162000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'In order for people to really try Reflect usefully and have a great experience we need to offer it as a saas.

Fritz designed https://www.notion.so/replicache/Reps-v-Smol-Design-Sketch-818bb4a93bd245e48cc0e3b344a2f473 but there is an even more minimal version in progress here: https://www.notion.so/replicache/Reconsidering-SaaS-3aa7ce8bb2e947aeb17bf4220a4be186.

Even as a microsaas, this is a significant amount of work and has several subcomponents:

- Implementing a new service, `mirror`, which manages the deployments of user apps on our service
  - Setting up customer/app storage somewhere on server (either reusing license server or doing something new in CF)
  - Implementing `publish` endpoint
  - Implementing `dev` endpoint for quick development
  - Implementing `tail` endpoint to get logs
  - Tracking usage while running and implementing `usage` endpoint
- Implementing a set of CLI commands in the `reflect` package to invoke the service:
  - `init` to create a new app on the server
  - `publish` that builds the app and sends to server
  - `tail` that streams the logs to console
  - `dev`
  - `usage`','public'),
	 ('nmXzY4Jjr9jGDde0PxxKE','Get Monday and Subset on newest version to validate',false,1683661893000,1677700990000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'For Monday this is somewhat a duplicate of #351 but more generally before we do an MVP release these two customers should be using the code and happy.','public'),
	 ('B6Jpm-sbLu8QN8s2qZ-ky','Allow access to env for all options',false,1679490008000,1677700802000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Factoring this out of #173.

Right now many of the fields of `createReflectServer` is a callback that takes an `Env` parameter. This is required so that, ie, `getLogLevel` can depend on the env.

However, (a) this is just annoying to type for every field, and (b) we''ve gotten user reports that other methods also want access to the env:

<img width="909" alt="Screen Shot 2023-03-01 at 9 50 04 AM" src="https://user-images.githubusercontent.com/80388/222250307-d72ac052-ada8-4f16-8ac7-3759302c7480.png">

I think CF did a bad thing with their env api design. It should be globally available to all user code somehow. So something like:

```ts
export function createReflectServer({env}: {env: Env}) {
  return {
    mutators,
    authHandler,
    logLevel,
    logSink,
    ...
  }
};
```

This will be a bit tricky to marry to CF''s API because we don''t find out about the env until `fetch()` in the case of the worker. But we can delay calling this function until then.

Another thing to check into is whether in CF, the env can change without restarting the context. I bet that it cannot. But if it can then we need to make the env field in the constructor a getter so it can return latest value (or maybe a function to make it clear it''s dynamic).','public'),
	 ('Va0Urkmmr0hvVdA1KlEBV','Fix Monday connectivity',false,1709537625000,1677699474000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'There are a number of connectivity issues we are tracking on Monday''s shared bug tracker:

https://github.com/rocicorp/shared-monday/issues/2
https://github.com/rocicorp/shared-monday/issues/3
https://github.com/rocicorp/shared-monday/issues/5
https://github.com/rocicorp/shared-monday/issues/6

As a requirement for beta, we must squash these bugs by any means necessary. Step 1 is to add metrics ( #186 ) but if that confirms the issue we will still need to debug.','public'),
	 ('M8mfjAnsA7XC4SkPHC1U1','"How it Works" demos',false,1681819114000,1677699233000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'On the marketing page there are some slightly interactive demos that explain the product that require eng resources to implement (or maybe @alexhking can do once the product is a tiny bit further along 👹).

Left todo here:

- [x] hook up "reset" buttons
- [x] Delay loading Reflect instances until demo scrolled into frame (#466)
- [ ] make second demo "whizzier" (pending design from @alexhking)
- [x] please make button on second demo into a slider -- all the way left is -xdeg/ms, all the way right is +xdeg/ms, when you let go it snaps back, the two sides are disconnected.
- [ ] I''m seeing fairly flakey behavior from these particularly on iphone, like sometimes I tap and nothing happens :-/, or sometimes the latency seems far longer than expected from slider. @cesara can you take a QA pass over especially on mobile? Not saying it''s your fault could be Reflect 😬.','public'),
	 ('do7taZKLKim7p5sYVjGyS','ALIVE demo',false,1682371389000,1677699109000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Current worklog: https://www.notion.so/replicache/v13-worklog-c3b4b0645e3c43f19926ee4253c1182f?pvs=4','public'),
	 ('M7AYjBEI0o2ou09qiMklf','close should wait for persist',true,1709580454000,1677671715000,'nqYkxAGMnzk7Y5STjZryV',NULL,'If `close` is explicitly called, then it would make sense to make sure that we have persisted all in memory changes to the durable store before resolving the returned promise.','public'),
	 ('57nIwlDl07Mr-chY_7mTH','Figure out how to change esbuild target',true,1684744728000,1677669813000,'nqYkxAGMnzk7Y5STjZryV',NULL,'CloudFlare workers use the v8 version that is in latest stable chrome:

https://developers.cloudflare.com/workers/runtime-apis/web-standards#javascript-standards

But looking at the output from:

```sh
wrangler publish --dry-run outdir=test-out
```

I can see that the esbuild target is something pretty old. We should figure out how to change this to get maximum speed™️','public'),
	 ('z3aUW9wgfF6fcsEfiy-Ut','Add reflect bundle size to the dashboard',true,1684744744000,1677616903000,'nqYkxAGMnzk7Y5STjZryV',NULL,'And remove the non esm replicache bundle sizes','public'),
	 ('ooi7QtL1QIEfDAg7fJ2Ll','Perf test is failing on refresh tests',false,1677617489000,1677601463000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Started failing here

https://github.com/rocicorp/replicache-internal/actions/runs/3652291573/jobs/6170509878

Looks like it is due to different version of Chrome but I need to investigate more','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('N_zoG-OQkTLODLx233FoP','What do we need to do to support schema changes as elegantly as Replicache did?',true,1677873342000,1677554572000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Don''t think required for beta','public'),
	 ('vQ_s0b4lFLLy1kXejIR1p','Reduce write cost temporarily for beta',false,1677706276000,1677178938000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We need to be competitive with liveblocks'' pricing and the writes every four frames really kill us: https://docs.google.com/spreadsheets/u/1/d/1d6xCMg6c9_oKso-124gFkfuKsY1aJXEdRqo_MX8yzk4/edit#gid=2131158829.

So we will need to (for GA) implement the crazy recovery protocol that @grgbkr designed. ITMT, we discussed and we think it''s fine to just be a little unsafe and persist much less frequently. It will be the case that during crashes the client can get ahead of the server. In that case the client should just discard (or perhaps rebase, breaking causal consistency).

We will circle back and make this rock solid before GA.','public'),
	 ('of-QWTwQHW3zCpsMLGNmC','Tweak eslint no-floating-promises to disallow void P',true,1677148364000,1677148364000,'nqYkxAGMnzk7Y5STjZryV',NULL,'https://typescript-eslint.io/rules/no-floating-promises/

              > @grgbkr Maybe we should disallow `void promise` and force people to use `.then`/`.catch` in all cases?

I''d be in support of this, void often leads to uncaught promise errors.

_Originally posted by @grgbkr in https://github.com/rocicorp/mono/issues/22#issuecomment-1440563431_
            ','public'),
	 ('gtWogKNQPlYZgWawayx1V','Move all bugs from source repos into this repo',false,1677096274000,1677036559000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Move bugs from `replicache-internal`, `reflect`, and `reflect-server` into this repo.

We can use the `gh` cli for this: https://jloh.co/posts/bulk-migrate-issues-github-cli/.

* For each repo, add a label like `replicache`, `reflect`, or `reflect-server` indicating the area.
* If the bug was P1 at source, add a P1 label at dest
* Create a *Playable Beta* milestone in `mono` like the one in current `reflect-server` and tag all the relevant bugs (this one probably makes sense to do by hand).','public'),
	 ('txL1WKD6dr9TjTsDuUu8D','Promise rejections in `_processNext` silently swallowed.',false,1677754392000,1677035813000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'It appears that any unhandled rejections in `_processNext` are silently swallowed.

@jesseditson and I discovered this in the process of debugging rocicorp/mono#166. `DurableStorage#put` was throwing and this bubbled up to the `setInterval` call here: https://github.com/rocicorp/reflect-server/blob/c342e4eba5edba55b45eac5966052b464de44cfa/src/server/room-do.ts#L385. The error was not printed to the local console from `dev-worker`.

I verified that if I just change that setInterval to `return Promise.reject("bonk")` , that error *also* doesn''t show up in the `dev-worker` console.

This all was happening on `reflect-server@0.21.1` which uses an old wrangler. It is possible that the newest wrangler fixes this bug, that needs to be verified.

It is also possible that these errors would have been reported to logpush, that''s not clear.

Finally, I tried registering the global handler `unhandledrejection` and that did get fired. But the `reason` field was `undefined` :(.','public'),
	 ('QuEOZYMU6SWNxI6IVXMAK','Better support for large values',true,1677790411000,1677024009000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'@jesseditson hit the case in paint fight where keys > 128KB not supported by DO.

Generally we don''t recommend large values right now because they don''t work nicely with our key-wise diff. But in Jesse''s case this made sense to do (this value changed rarely and was the flattened view of the image).

At some point we will want better large value support. I think this will also be useful, e.g., for yjs support.

I''m very tempted to implement this by using a rolling hash, ala prolly tree. I don''t think we need the entire tree just the leaf nodes. We can encode a large value as an array of content-sliced chunks, then write the chunks as keys in replicache. Sync takes care of the rest?','public'),
	 ('ltLoJ9hPd_fHUh4SsnySd','@rocicorp/reflect build should use platform neutral',false,1678185815000,1677002707000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'This will preserve `process.env.NODE_ENV` in the output so that it can be determined by the customers build step.  This will give them replicache debug asserts and debug behavior like deep freezing values during development.

Currently all builds are done with plaform ''browser'', and with minification on, resulting in process.env.NODE_ENV expressions being automatically defined to "production".

See https://esbuild.github.io/api/#platform','public'),
	 ('4o--59eZVIs9CbHfxg-GE','Turbo repo code Caching on GH Actions for ty',true,1677165207000,1676892695000,'nqYkxAGMnzk7Y5STjZryV',NULL,'https://turbo.build/repo/docs/ci/github-actions','public'),
	 ('CpF_xfi0giKHoUucp8a6E','Rename replicache-internal to replicache',false,1677154408000,1676884951000,'nqYkxAGMnzk7Y5STjZryV',NULL,'No need to have the distinction. Everything in mono is internal','public'),
	 ('-aHMbMcRa-52TSNraxxyB','Move @rocicorp/licensing into this repo',true,1677093343000,1676884917000,'nqYkxAGMnzk7Y5STjZryV',NULL,'`@rocicorp/licensing` is a private npm package and we need to pass in the NPM_TOKEN to the GitHub Actions runners. Moving this into the mono repo would simplify that.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('j7YYm4ELeHkMM-t3H5NsV','consoleLogSink on server should JSON.stringify log arguments ',false,1678194107000,1676567808000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'I see it in the datadog-log-sink, but I think we need to add something like it to the consoleLogSink for the server as well, because cloudflare''s console log does a bad job of stringifying, giving results like:
```
INF RoomDO doID=219e9351928ed1d8df08e76d617d3c503ec46df3c106a34be75c074b0149fc87 blah Object {
  bar: baz,
  foo: Array(5)
}
```

and 
```
DBG RoomDO doID=219e9351928ed1d8df08e76d617d3c503ec46df3c106a34be75c074b0149fc87 roomID=iOAgoD requestID=mfbwvnbkvxe clientIP=71.228.154.248 wsid=JkNCB40MYzD66H4ABQ5bw client=1598b74f-5af8-4aa6-a061-aad2b8c1e50a wsid=JkNCB40MYzD66H4ABQ5bw msg=f8eesmg3opd processing room clientIDs Array(1) [ Array(2) ]  pendingMutations Array(1) [ Array(2) ]
```

It would be better to do it in the sinks though instead of in all the log lines.','public'),
	 ('UC7o_pd6dFhaIb8qoDGtK','Datadog cleanup',false,1677698295000,1676285022000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'The current situation with datadog integration is confusing for users:

* reflect-server exports an implementation of DatadogLogSink, but reflect does not -- application must provide. This inconsistency is odd, but also:
* the two implementations are different. the server one is our own from scratch, the client one wraps datadog''s client library.
* reflect exposes an abstract Metrics interface apps can implement, and we have an impl in datadog-util. But this impl doesn''t stand alone, because it needs to talk to an endpoint that has the api key. This means that it is not really possible for the datadog impl of metrics to really be pluggable (at least not without a supporting server).

We should:

* Pull our custom DataDogLogSink into `datadog-util` so it can be used on both client and server.
* Add an option on both `Reflect` constructor and `createReflectServer()` like: `{datadog: {apiKey: string, enableLogs: true, enableMetrics: true}}`. Use this option to default the log sink.
* Change `logSinks` (plural) back to `logSink` (singular) and change the semantics: if `logSink` is present it overrides everything else and is the sole sink. Otherwise, the log sinks are `consoleLogSink` and `DatadogLogSink` (if `datadog` option provided).
* Enable metrics if `enableMetrics: true`. No need for caller to pass Metrics impl.

This is required to get a metrics release out to Monday.','public'),
	 ('TBUpMPVad3qM1Cy8nJRbc','Validate migration code of rooms and cleanup replidraw-do',false,1677666988000,1676111018000,'nqYkxAGMnzk7Y5STjZryV',NULL,'              Sounds good. @arv can you turn this PR into that task:

- Pick one of the rooms that is exhibiting this error
- Migrate it: https://github.com/rocicorp/reflect-todo/blob/main/doc/server-api.md#migrate-room (secret is in https://docs.google.com/document/d/1aGHaB0L15SY67wkXQMsST80uHh4-IooTUVzKcUlzjdk/edit)
- Confirm error goes away
- If it does, nuke replidraw-do storage. replidraw is using the script name `replidraw` (per: "https://github.com/rocicorp/replidraw-do/blob/main/wrangler.toml#L1"). Go to cloudflare console > workers > find this worker > manage > delete.
- Rerun sample, confirm error stops happening.

Then this PR should not be necessary. Thanks.

_Originally posted by @aboodman in https://github.com/rocicorp/reflect-server/issues/341#issuecomment-1426358530_
            ','public'),
	 ('Ti6CuXTnxrY_W_f1iev_r','Get rid of `getBaseCookie` hack',true,1677695778000,1676020041000,'nqYkxAGMnzk7Y5STjZryV',NULL,'By exposing this in Replicache

Once we have a mono repo we do not have to use a public API?','public'),
	 ('eT2QXxyPgBpJ2mvKe9OVx','Implement `pull` over the web socket',false,1677752055000,1676019971000,'nqYkxAGMnzk7Y5STjZryV',NULL,'We use pull for mutation recovery. At the moment this is done using an HTTP POST. It would be more consistent to just piggy back on the existing web socket connection.','public'),
	 ('MsrceOpfLM_rpGQw44AfK','Worker Runtime API refresh',true,1684746343000,1675965305000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Needs design, WIP. See: https://www.notion.so/replicache/Worker-Runtime-API-Refresh-1bb2b238b6ed4c4eb97d3adb4301b466','public'),
	 ('Ry3LRznUiySX-I4N1j3N6','Add `onRoomStart` server API',false,1684355253000,1675964410000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'For writing initial data, or migrating.','public'),
	 ('yQ24dH-V3rH-JjZwiSW1i','Add `connectHandler` server API',true,1684744885000,1675964370000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We currently have a `disconnectHandler` [in the server API](https://github.com/rocicorp/reflect-server/blob/c342e4eba5edba55b45eac5966052b464de44cfa/src/server/reflect.ts#L17).

This is most commonly used to clean up per-client state that is created on the client-side.

It would be useful to have an analogous `connectHandler` API. This could be used to populate per-client state that is inconvenient to create client-side (ie because there should be only one of something, or it should happen only once, etc).

This `connectHandler` would be the dual to `disconnectHandler`. Like `disconnectHandler`, it would receive a `WriteTransaction` as an argument, and would run in the `RoomDO`.

We should guarantee to run `connectHandler` before any mutation from that client. Exceptions from the connect handler should prevent the connection from being accepted (and result in an error back to the client). Exceptions should be logged at error level. But they should not take down the server completely.','public'),
	 ('NtisAKbNZloKdZCHzXArH','fastForward only runs on first mutation',false,1677698738000,1675936360000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'It should be running immediately on connect. This causes all the samples and user apps to have a pointless `noop` mutation at startup.','public'),
	 ('Z_tpv354erVHGRnAf0gJP','Runtime API cleanup',false,1677094848000,1675936264000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'See Notion: https://www.notion.so/replicache/Worker-Runtime-API-Refresh-1bb2b238b6ed4c4eb97d3adb4301b466','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('xAyzOlNdHfgU_sYkdnn3x','Flush storage only periodically (say, every 5s)',true,1683332951000,1675935314000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'# Problem

60fps write cost kills us. We have a solution to this that we have spec''d but it''s a lot of work.

# Proposal

Flush only every 5 seconds. There is a chance that if a DO crashes, a client can end up ahead of the server. In that case, we must detect the situation and delete optimistic data.

We can detect this situation by changing the notion of `version` that we store in the DO state. Right now we just store a single `version` field. The problem with that is that when a DO crashes and restarts, it might reuse versions that have already been vended (but not stored).

A simple solution is to store one version per running instance of the DO. We would then communicate to the client on connection which instance it is talking to. The client would save this information in the snapshot along with the version.

On reconnect, the client sends the instance it was last talking to _and_ the version. The server can then compare to the last known version from that instance and determine if the client is ahead. If it is, it closes connection with a special error code that causes the client to nuke its optimistic state.

# Warning

There is a minuscule case that the client goes offline at exactly the moment a DO crash occurs *and* the user works offline for a significant period of time. In this situation all the user''s offline work would be lost.

I am fine with this situation for beta, but for GA we should decide to either abandon strict causal consistency in this situation or else do the fancy solution.','public'),
	 ('YgP3SDin9RCwKKPW3ZYpO','have a test that ensures that if connect throws we hear about it as an error in the client log',false,1677094849000,1675853163000,'nqYkxAGMnzk7Y5STjZryV',NULL,NULL,'public'),
	 ('vkpq6VNfOq-R1mUng83op','Disconnect on hide',false,1677094849000,1675846732000,'nqYkxAGMnzk7Y5STjZryV',NULL,NULL,'public'),
	 ('T-R-nI4O6wERQ4AWJZNXT','onOnlineChange should fire only when reconnect fails, not for passing socket drops',false,1677094850000,1675846722000,'nqYkxAGMnzk7Y5STjZryV',NULL,NULL,'public'),
	 ('7m5jBEv2Sn9XycVaM1q4Y','Figure out how to deal with schemaVersion mismatch',false,1677094850000,1675761614000,'nqYkxAGMnzk7Y5STjZryV',NULL,'The server side implementation of the mutators needs to know about the schema version so that they can deal with old schemas.','public'),
	 ('I6hEvwp3lwm2b4b5jKcIq','Add protocol version on connect',false,1677094851000,1675761541000,'nqYkxAGMnzk7Y5STjZryV',NULL,'### Problem

It is possible that the client and server might be on different versions.

If there are non backwards compatible changes then the client cannot communicate with the server.

### Proposed Solution

- The client sends its protocol version when it tries to connect. 
- If the server is not able to support that version it closes the connection with a `ProtocolMismatch` error kind.
- When the client sees a `ProtocolMismatch` it calls a callback with an error object that can be used to detect this case.
  - Default error handler is to reload?

### Open Question

- Does this overlap with `ReplicacheFormatVersion`?
  - It does not because the Replicache format version is only used to determine how the persistent storage is structured in IDB/memory
- How does this overlap with `PullVersion` and `PushVersion` used in Replicache?
  - A change in either of these would lead to a change in the `ProtocolVersion` change because `PullVersion` and `PushVersion` are used in `Poke` and `Push`
    - Actually `PushVersion` is used when we do a `PushRequest` in Replicache. It is not used in Reflect which seems to be a potential issue.
- How does this overlap with `SchemaVersion`?
  - This should not impact the `ProtocolVersion`...
    - I think we need to consider how `SchemaVersion` can be dealt with in the server side mutations


','public'),
	 ('xyodnAzeq9SWhWNH1-fIS','Time out a connect attempt out after say 10s',false,1677094852000,1675419563000,'nqYkxAGMnzk7Y5STjZryV',NULL,NULL,'public'),
	 ('3TUoja2DXvYmP8eVqn0cm','Backoff so the client is not just reconnecting in a tight loop forever',false,1677094852000,1675419556000,'nqYkxAGMnzk7Y5STjZryV',NULL,NULL,'public'),
	 ('Tckft3fqCqXBy9sTzI0F_','Add metrics to client to track connectivity',false,1679818208000,1675121257000,'yJ5hiysWE-LBcDfT44lR8',NULL,'There is a WIP PR here: https://github.com/rocicorp/mono/pull/334

Fritz''s notes here:

Background:
- seemed like a good idea to use datadog for metrics because we use it for logging. i think that''s still right.
- datadog''s metrics model assumes that metrics are collected/aggregated by something like [statsd](https://github.com/statsd/statsd) (their specific version is dogstatsd), which submits aggregates to datadog itself. statsd can run on node but it seemed like biting off more than we wanted to chew to just get metrics up and running to try to get statsd running on CF in a DO so i did not go that route, or at least deferred it until the client and dashboard pieces were up and running. i also didn''t want to call into existence a separate process running it somewhere else like heroku. so for expedience decided to just submit metrics directly to datadog.
- submitting via the http api comes with constraints:
  - see https://docs.datadoghq.com/developers/dogstatsd/data_aggregation/#why-aggregate-metrics and https://docs.datadoghq.com/metrics/types/?tab=count#submission-types-and-datadog-in-app-types but basically we are constrained to using just count, rate, or distribution points when submitting via http api. and distribution points are the thing that is easiest to aggregate on the CF end, so that''s what we use under the hood for our metrics: distribution points. 
  - the http api does not support cors because as mentioned datadog assumes something else is doing the aggregation and datadog does not want to receive a zillion dataponits individually from browsers or anywhere for that matter. so since we cannot submit points directly from the client to datadog we proxy them through a [worker endpoint](https://github.com/rocicorp/reflect-server/blob/04cc5ebcd2e239303a00fdc2a614201775e9bf50/src/server/worker.ts#L251). it literally just takes the body and sends it to datadog. 
  - in particular the proxy endpoint does not aggregate metrics. every submission from a client is sent directly to datadog. **this doesn''t scale** so can''t be used as is for customers with lots of clients. but is good enough for monday. this issue https://github.com/rocicorp/reflect-server/issues/293 captures the fact that it doesn''t scale. what needs to happen is that the endpoint, possibly a DO though not necessarily, should buffer changes from clients and report them periodically to datadog instead of one at a time. doing this would also enable us to have a richer set of metrics on the client eg a Set of clientids in a particular state that gets buffered and translated into a count distribution point at datadog submission time. in doing this we''d need to be cognizant of the client reporting interval and the dashboard rollup window which must match in order for the dashboards to be accurate/sensible (more on this below).
  - our metrics proxy endpoint is not rate limited or authenticated
  - the capability is there but we do not currently pass tags like the app that is submitting the metrics or the environment (prod vs dev) so metrics from all sources are currently aggregated together in the dashboard
  - replidraw-do has metrics hooked up
- there is a script for spamming datadog with metrics so you can see the dashboards work: https://github.com/rocicorp/datadog-util/blob/main/tool/report-metrics.ts. otherwise there is too little traffic to see much of anything meaningful. 
- https://www.notion.so/replicache/metrics-0b848461ffce4312bdb645d65adb7da2 sketches the metrics that we started implementing. we have the time to connect (including never) and the reason the last connect failed metrics in place. 

OK so briefly how do they work? The thing we want is a count of clients that have a particular state in a moment of time, or over a period of time. For example, we want to say across all clients how long have they taken to connect, or how many are (un)connected at this moment, or how many clients are logged out because of auth. Notice that these most important questions ask for a count of *clients* that have a property at a given instant or over a period of time. They do not ask for a count of *events* like ''a client got logged out for auth'' or ''a connect attempt took 2s''. If we counted events like this then the counts would be skewed by clients that are reconnect looping or similar. We want to know how many *clients* in the *population* have a property. 

The way we do this is by sampling in the client and aggregating in the dashboard. We have a reporting period in the client that is currently 2m. Once every 2m each client submits exactly one datapoint per metric. So if we look at a 2m span we will see very close to if not exactly one data point per client. We can count the number of clients with a given property over that span and it also gives us the total number of clients reporting which is just how many data points we see for a given metric in that span. In the dashboard we do a count rollup over 2m to get these numbers. If the client is reporting at a different interval, or the dashboard is not rolling up over a span that matches the reporting interval, the numbers are not going to make sense. 

Also I guess note that metrics are good for counting things in a snapshot of time, or instantaneously. They are not good for stateful computations like what''s the history of this client. We''d need to keep the data in a form separately queryable to answer those questions. 

In order to create a dashboard:
1. you have to enable ''advanced percentages'' for the distribution metric in question in data dog. It''s under Metrics > Summary, search for the metric and enable advanced percentiles which enable threshold queries.
2. stack graphs are typically what we want. configure a dashboard with timeseries type and then add the metrics in question eg `count(v: v<=600000):time_to_connect_ms{*}.as_count().rollup(120)` is the count of clients with time to connect <= 600s over a 120s window. the little bar slider icon on the right of the metric field in the edit view will enable you to edit the formula directly wihtout having to use the pulldowns and whatnot. 
3. create the stack. list the metrics in the order that you want them (a first, b second, etc). sometimes you want to do math on metrics like to subtract the count of clients connecting in 20s from 10s so you can stack just those in the range 10s-20s on top of the count of those connecting in <=10s. to do this you can toggle display of the metric in question of by clicking its letter eg ''a''. we need the metric but we don''t want it displayed. instead, add a formula to the bottom with the right order. eg you can say something like:
  ```
  (not selected for display)  a: count(v: v<=20000):time_to_connect_ms{*}.as_count().rollup(120)
  (not selected for display)  b: count(v: v<=10000):time_to_connect_ms{*}.as_count().rollup(120)
  ...
  (selected for display):       formula: a-b
  (selected for display):       formula: b
  ```
  This will show the count of clients connecting in 10s with count of connecting between 10s and 20s on top of it. Note that datadog will sometimes "clean up" the formula and re-name the metric labels which is annoying but it usually does it in a way that doesn''t mess the graph up. It must have some kind of canonical form for storage that they get put into.
  
  4. Add tags to the metrics contructor eg `[''env:prod'', ''service:reflectserver'']` and they you can use them in the space aggregator in the metric formula eg `count(v: v<=20000):time_to_connect_ms{env:prod,service:reflectserver}.as_count().rollup(120)`

You can see how metrics are integrated here: https://github.com/rocicorp/replidraw-do/blob/d3d3f1711b2475e614f13640bfd2a498599bd74c/src/pages/d/%5Bid%5D.tsx#L30-L36 and then you simply pass the metrics to the reflect constructor: https://github.com/rocicorp/replidraw-do/blob/d3d3f1711b2475e614f13640bfd2a498599bd74c/src/pages/d/%5Bid%5D.tsx#L49. When you want metrics in reflect server itself we''d just do the same thing.

Dashboard for our two metrics are found here: https://app.datadoghq.com/dashboard/vm5-ce6-p67/reflect-client-metrics. On the left are the "real" dashboards that roll up over 2m (120s). On the right are the same but with 10s rollupts for use with the report-metrics metric-spamming script above.

For a customer to use these dashboards they need to:
- instantiate and pass metrics like replidraw-do does, ideally using tags for their service and environment
- once they start receiving metrics they need to configure advanced percentiles for our metrics: time_to_connect_ms, last_connect_error_{auth_invalidated, client_not_found, invalid_connection_request, invalid_message, ping_timeout, room_closed, room_not_found, unauthorized, unexpected_base_cookie, unexpected_last_mutation_id}. 
- import our dashboard. we can export it to json from the dashboard view and they can import the json.

@arv note that if you add a timeout as a connect error reason to cover connect() actually timing out, you''ll need:
1. add it to the report-metrics script and run it so we start getting the metric
2. configure advanced percentiles for the metric in datadog
3. add the metric to the last connect error dashboards, but the real one and the test/report-metrics one.

for some reason my browser screenshotting is not working so here''s a full screen shot of the two graphs (report-metrics spammed versions).
<img width="2560" alt="screengraphs" src="https://user-images.githubusercontent.com/157153/215613858-f5ae4d23-f93b-4918-ae3c-7092d6effecd.png">
','public'),
	 ('0H56vEPUUuZSGUl-RzKYv','This comment seems outdated.',false,1677094853000,1674601715000,'nqYkxAGMnzk7Y5STjZryV',NULL,'              This comment seems outdated.

_Originally posted by @phritz in https://github.com/rocicorp/reflect-server/pull/288#discussion_r1086022587_
            ','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('SNR_aE9fn7b5Wzpn0-Qmb','RFE: Make clientID sync',false,1709536406000,1674548085000,'nqYkxAGMnzk7Y5STjZryV',NULL,'`clientID` is currently created inside `initClient` which is async but there is nothing async about creating the actual client ID. We could create it synchronously and pass it in.

We do however use `await rep.clientID` in the tests as a signal that the persistent storage is ready. We could expose a `ready` promise or we could just tell people to do `await rep.query(() => true)` which internally waits for the ready promise but it is a bit hackyy. (Not say what you mean)','public'),
	 ('OGe1tnwdeE_3IYZKNqE-i','Handle reauth on the client',false,1677094853000,1674468411000,'nqYkxAGMnzk7Y5STjZryV',NULL,'## Replicache

In Replicache we already set the precedence for how to do this. Here is how it works in **Replicache**:

- The API has a way to provide a function called `getAuth` with the type `() => MaybePromise<string | null | undefined>`.
- Whenever the server returns a `401` we call the `getAuth` function. The function can do HTTP requests, put up dialogs, etc.
- If the function returns a string. `replicachecInstance.auth` is assigned this new auth value and...
- Replicache retries the previously failed push/pull with the new auth.
- This retrying happens `MAX_REAUTH_TRIES` times.

## Reflect Client

In reflect we will do pretty much the same.
- reflect-server will send a close with an error kind
- When we get a `ErrorKind.AuthInvalidated` or `ErrorKind.Unauthorized` we enter into a state where _needsReauth_ is `true`
  - When _neesReauth_ is `true` we will "call" the `options.authToken` getter again. This can be a string or a function that returns a string. It can be async as well.

The API is:

```ts
authToken: MabePromise<string> | (() => MaybePromise<string>);
```','public'),
	 ('p7wxY5YNUecj_HG39MVYl','proxy metrics needs improvement',true,1684745997000,1674192863000,'yJ5hiysWE-LBcDfT44lR8',NULL,'Added in https://github.com/rocicorp/reflect-server/pull/292. 

It doesn''t scale because it sends a request to datadog for every request it receives (which is 1/reporting period=2min/client, so for 100k clients and a 2 minute reporting period that''s almost 1000 requests to datadog per second. It should buffer and report periodically in batches. But this should work for the time being (eg 1000 clients / 2 minutes = 8 requests/sec). 

There are other ways it needs improvements, see code comments (eg, auth).','public'),
	 ('A8IJYzC_gzC6ejrn1RFKv','Allow mocking IDB',false,1677091391000,1674056885000,'nqYkxAGMnzk7Y5STjZryV',NULL,'We added experimental `kvStore` to allow mocking out IDB. But since we adde that we started using IDB in a few places.

Consider replacing `new IDBStore` with `new KVStore` everywhere where KVStore is something that can get passed into `ReplicacheOptions`.','public'),
	 ('wBXewIj2c81V7yXL_eWa2','Get connectivity failure rate from replidraw-do',false,1677696998000,1673894767000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Monday has a connectivity failure rate of 2.5% according to our own logs (https://github.com/rocicorp/shared-monday/issues/2#issuecomment-1374336766) and from Noam''s.

We can''t currently compare to replidraw-do because the client side logging the analysis depends on isn''t present. We need to add the client-side logging to replidraw-do and then compare.

If the connection failure on replidraw-do is also high then we can debug more rapidly by iterating on our own app.','public'),
	 ('4d-w78fngkUNR2nd284MI','Improve Datadog Logger',false,1684892165000,1673617527000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Datadog has a concept of [attributes](https://docs.datadoghq.com/logs/log_configuration/attributes_naming_convention/#default-standard-attribute-list), These are exposed in the UI in a special way.

The DD browser API allows you to pass these attributes using the `messageContext` parameter.

```ts
log(message, messageContext, status)
```

For the DD HTTP REST API the `messageContext` is the json object sent as the payload.

If `messageContext` is set to `{network: {client: {ip: ''1.2.3.4''}}}` then the Client IP shows up in their UI as shown in this screenshot.

<img width="763" alt="dd-ip" src="https://user-images.githubusercontent.com/45845/212326598-707db242-b2fe-47f0-948d-60b5d259e960.png">

`LogContext` has a different concept of _context_. Its context is added to the `message`.

Instead of the current way, I''m proposing we change `LogContext` `addContext` to add to a context object and pass this to DataDog as a `messageContext`. For example:

```ts
const lc = new LogContext();
lc.info(''a'');
// sends
// {message: ''a'', status: ''info''}
const lc2 = lc.addContext(''b'', ''c'');
lc2.info(''d''); 
// {message: ''d'', status: ''info'', b: ''c''}
```

DD also allows the attribute to contain `.` and then it builds the object structure as needed. For example, the following two context objects are equivalent:

```js
const a = {''network.client.ip'': ''1.2.3.4''};
const b = {network: {client: {ip'': ''1.2.3.4''}}};
```','public'),
	 ('CaK0J5eeX10pPH0LmvND6','Assert JSONObject in WriteTransaction put',true,1677705585000,1673469405000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Replicache asserts a valid JSON value is passed into `WriteTransaction` `put` in debug mode.

https://github.com/rocicorp/replicache-internal/blob/ec0066f6ce2907307ab9196bbcd3838c3bb7fc1f/src/transactions.ts#L247

This calls [deepFreeze](https://github.com/rocicorp/replicache-internal/blob/ec0066f6ce2907307ab9196bbcd3838c3bb7fc1f/src/json.ts#L269) which calls [deepFreezeInternal](https://github.com/rocicorp/replicache-internal/blob/ec0066f6ce2907307ab9196bbcd3838c3bb7fc1f/src/json.ts#L316) which throws for non JSON values.

However, the server side implementations do not do any kind of runtime assertions.

We need to add a debug/release mode and enable assertions in debug mode.','public'),
	 ('h_P5TxjqbvTCfqnduTxlm','Consider doing the WS auth as a message instead',true,1677697408000,1673447552000,'nqYkxAGMnzk7Y5STjZryV',NULL,'We currently (ab)use the `Sec-WebSocket-Protocol` to do authentication. We should look into doing auth as a websocket message instead.

I have a spidey sense that this header might cause issues with proxies.','public'),
	 ('CmTCA2KniOlv8idFsiTbP','Too much work being done in durable-storage.ts',false,1678113601000,1673430794000,'nqYkxAGMnzk7Y5STjZryV',NULL,'In `src/storage/durable-storage.ts`

```ts
  put<T extends JSONValue>(key: string, value: T): Promise<void> {
    return putEntry(this._durable, key, value, {
      ...baseOptions,
      allowUnconfirmed: this._allowUnconfirmed,
    });
  }
```

This has a few small issues but this is such a low level operation that these should be fixed:

1. We create a new object every time this is called.
2. We use object spread which is far from free. https://tc39.es/ecma262/#sec-copydataproperties

','public'),
	 ('WDpWkLYwqh5-ivAe6JUB6','getNode is slow?',false,1684746615000,1673258564000,'nqYkxAGMnzk7Y5STjZryV',NULL,'https://rocicorp.slack.com/archives/C013XFG80JC/p1673055269485049

getNode takes a total of 131ms in @jesseditson demo. It is not clear based on the Slack thread if this due to being called too often or if the cloning of the data is hidden in there.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('U9L_QRq8r0p27ZcMYlQ7H','clean up the zillion of ERROR logs on the client (and server)',false,1677698463000,1673229920000,'yJ5hiysWE-LBcDfT44lR8',NULL,'There are lots of errors reported from the client (less so, server) that are not actually things that should get an engineer to look at them. We should clean these errors up (fix the actual problem or convert to info or whatever) so that we have a clear signal when something goes truly wrong on the client, and not just the network is being flaky. You can see a ton of these in monday''s logs eg
```
name=reflect-37603806-IyjKuXhw1Y3yDFMB4_e6a8AiBALlOLZn, bgIntervalProcess=ClientGC, 
intervalID=64, Error running., TransactionInactiveError: Failed to execute ''get'' on 
''IDBObjectStore'': The transaction is inactive or finished.
```','public'),
	 ('D1hkYdF4W_e2afBO8Sq36','Ensure "Connected" log line prints out a req key that connects it to "Connecting..."',false,1677094854000,1673054992000,'OeVnr1y5bEM_Yg06sUFtD',NULL,NULL,'public'),
	 ('PYfC_yMiXwWgXzO2V0ncZ','understand how we tear down/recover when a new instance of a DO becomes canonical',true,1684746121000,1672973395000,'yJ5hiysWE-LBcDfT44lR8',NULL,'A new instance of a DO can become canonical while the old instance is still extant. The old instance doesn''t know that it is no longer canonical until it touches storage or makes an http request: https://discord.com/channels/595317990191398933/773219443911819284/1042891744225796176. I think this means that clients can/will remain connected to an old instance until something happens that requires touching storage or making an http request. So users could be connected to a room that is no longer functional until someone tries to perform a mutation at which point the request to storage should throw with something like "Cannot perform I/O on behalf of a different Durable Object". What should happen in this case is that everyone gets disconnected. Presumably they will reconnect to the new canonical room. We should ensure that this actually happens! It seems possible the exception might be caught by us, and unless CF deallocates the DO, the connect users could stay connected. ','public'),
	 ('dYjfk3Efw8tZFr_fc7K9y','ChunkNotFoundError in DD31 build ',false,1681242589000,1672950710000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'Reported by Subset when using a build of reflect that has a version of Replicache with dd31.

![image](https://user-images.githubusercontent.com/19158916/210873594-69eb7156-b9da-4c2e-afcc-facb2f51a047.png)

Jesse Ditson and Aaron say they have also encountered this.  

','public'),
	 ('NFSnRiWXM86tpWC35kIut','consider whether auth is leading to increased connection problems',false,1677094855000,1672939473000,'yJ5hiysWE-LBcDfT44lR8',NULL,'aaron suggested:

> could Sec-WebSocket-Protocol be screwing this? neither figma/notion/linear use this header and i recall it was somewhat nonstandard usage.

this should probably wait until we can measure whether a change has any effect (ie, after rocicorp/reflect-server#254 )
','public'),
	 ('0K2fsQ-qp2Q1Uk5oltpM9','we should ensure all the reflect server entrypoints are versioned',false,1677094855000,1672894830000,'yJ5hiysWE-LBcDfT44lR8',NULL,'Most entrypoints are versioned but createRoom and connect are not: https://github.com/rocicorp/reflect-server/blob/b01288be9d0ab8a5a7b1f6d25a13568133545000/src/server/dispatch.ts#L48 

This means that old clients can successfully connect to a new, incompatible version of the server (and also new clients can connect to old, incompatible versions of the server). Subset might have tried to do this by downgrading reflect but keeping reflect-server at 0.19/20 with who knows what resulting undesirable behavior. ','public'),
	 ('cdWsuUKZ5sgu9TahRetSK','revisit connection logic ',false,1679346828000,1672882869000,'yJ5hiysWE-LBcDfT44lR8',NULL,'This issue replaces rocicorp/mono#226 which was more of a placeholder.

Background: we didn''t put the care and thought into reflect connection/reconnection logic that we did into replicache client connection logic. We have seen a large number of connection-related issues with monday and would like to put the connection logic on firm footing going forward. Some of the relevant monday issues:
- https://github.com/rocicorp/mono/issues/276
- https://github.com/rocicorp/reflect-server/issues/230
- https://github.com/rocicorp/reflect-server/issues/233
- https://github.com/rocicorp/reflect-server/issues/234
- https://github.com/rocicorp/reflect-server/issues/236
- https://github.com/rocicorp/mono/issues/233

I think to summarize what''s involved in closing this issue, which should probably include a design sketch, is overhauling how connection logic works, inclusive of:

- [x] **Reauth**:
  - [x] Ensure that there is a signal to the app to re-auth the user on auth failure. This should likely already be solved by https://github.com/rocicorp/mono/issues/230 but if not should be part of this.
  - [x] rocicorp/mono#188
- [x] rocicorp/mono#185
- [x] rocicorp/mono#184
- [x] rocicorp/mono#181
- [x] rocicorp/mono#180
  - related to https://github.com/rocicorp/reflect-server/issues/230 for DO duration reasons we should probably disconnect on "hide" and reconnect on "show" (but stop short of disconnecting users from idle rooms, which requires more thought/discussion)
- [ ] consider having a canary http request that is issued when connect fails to determine if the user is offline and include that bit in the logs/metrics
- [x] rocicorp/mono#179
- [ ] take a look at the out of order poke problem which we do not have an explanation for. is there anything we can add or track that could help shed light on it? https://github.com/rocicorp/mono/issues/233
- ensure the logs include the following information:
  - [ ] when a connection is closed log the time the connection was open and how many messages were received
  - [x] connection closed log line should include the reason the connection closed (auth failure, blur, connect timeout, ping/pong failure, etc)
  - [ ] when it successfully connects, include the length of time it took to connect in the log line
- [ ] I think we want metrics that tell us connection-related information. This requires metrics to be added (https://github.com/rocicorp/reflect-server/issues/254) but also some design to figure out how to express what we are interested in. A proposal will follow. Kind of intuitively we want:
  - from the client:
    - information about whether a client is connecting or having trouble connecting. If the client is online and it can''t connect we want to know how often it is happening and why (connect timeout, auth failure, connection quickly reset for some reason). 
    - [ ] how long it takes to connect
    - [ ] how frequently it is getting disconnect for an unexpected reason (server abruptly disconnects, ping/pong failure, etc), exclusive of blur and clean shutdown, and also connections that are closed without receiving a message
    - [ ] connection uptime 
- [ ] in the above, we need to factor in that bc of dd31 going offline for long periods of time is expected 
- Something something ondisconnect? 

I think this issue does NOT necessarily include adding an http fallback. I think this is getting our current connection logic into a place we want. We can evaluate from here whether it is worth prioritizing a fallback.','public'),
	 ('YIfRqAwXdGv8V_ZB99s_5','add client and server metrics',false,1677094856000,1672881415000,'yJ5hiysWE-LBcDfT44lR8',NULL,'just a placeholder for now, will fill in with details later. in particular we need connection-related metrics which i will flesh out a proposal for. 

ensure it includes:
- DO restarts','public'),
	 ('bu5Gu8BI5Izq-DNSjvZVG','consider if there is a better way to wait for the client id to be added to the log context',false,1677094857000,1672875851000,'yJ5hiysWE-LBcDfT44lR8',NULL,'see https://github.com/rocicorp/reflect/pull/52#pullrequestreview-1236557590','public'),
	 ('eQvf5NTwA7IrQ5Lm2Raps','ensure the logger is stringifying objects',false,1677094857000,1672874779000,'yJ5hiysWE-LBcDfT44lR8',NULL,'see https://github.com/rocicorp/reflect/pull/52#discussion_r1061943225 and i think separate discussion on slack with arv. possibly related is https://github.com/rocicorp/reflect-server/issues/91. 

this issue should probably also include logging the request at the point of the original comment','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('w5UIbcmzHvudFnxpX3lk0','ensure request id is properly passed through from hop to hop',false,1677094858000,1672872956000,'yJ5hiysWE-LBcDfT44lR8',NULL,'We use a request id to tie log lines together across hops (eg client to worker to DO). We need to take a pass and ensure that the request id is being faithfully copied hop to hop, and initiated in the right places. This issue spans client and server. In particular I think it should include:

- [x] a poke should generate a request id and include it, and then whatever happens in the client as a result of it should carry that request id
- [x] worker, authdo and roomdo should all look for the http header carrying the request id and populate a log context with it that is used by the code that handles the request. the logcontext can be populated and passed to the handler in router middleware, see https://github.com/rocicorp/reflect-server/pull/238 and https://github.com/rocicorp/reflect-server/issues/250 
- [x] push and pull and anything else on the client that initiates a call to the server should include request id, see https://github.com/rocicorp/reflect/pull/51
','public'),
	 ('9-3eWCzqz3iLkO1e4YdZP','finish sorting out routing',false,1677094859000,1672871889000,'yJ5hiysWE-LBcDfT44lR8',NULL,'@aboodman has started replacing our use of itty router with a custom thing. He or someone else should take that over the finish line, meaning at least:
- get https://github.com/rocicorp/reflect-server/pull/238 in _and manually test it with our existing sample apps_
- replace dispatch with the new routing and delete dispatch.ts 
- establish the pattern for CORS
','public'),
	 ('c5nwL8-1RIOtD-7nhZ8K9','Implement whatever cost-reduction mechanisms we think are necessary',false,1677094859000,1672783416000,'yJ5hiysWE-LBcDfT44lR8',NULL,'I have lost track but our cost spreadsheets typically make assumptions that the client is disconnecting aggressively and similar. There is probably work to do to implement what we need.','public'),
	 ('_fpBn8QYuLRZ9OBSXWf15','Add `mutationID` to `WriteTransaction`',false,1678763107000,1672741807000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Useful as a random seed and for other things.','public'),
	 ('VAL6IFc07pvHccWmY00pg','v0 docs',false,1709537669000,1672741489000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'This needs some thought. The docs for Reflect can naturally be much much lighter than Replicache, but we still need a few really nice docs to get users going.','public'),
	 ('kEj282IgquWxEIwD1U_p1','Revisit `allowConcurrency`',false,1677094860000,1672726343000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We have `allowConcurrency` set to true in our use of Durable Objects. This was done originally because the game loop serializes at a higher level, but:

(a) we are rebuilding the room DO in a different way and those old assumptions probably don''t apply
(b) the same `allowConcurrency: true` setting is being used in AuthDO where it *definitely* doesn''t apply, necessitating things like `_authLock` and `_roomRecordLock` which is adding significant complexity.

I think we should at the very least set `allowConcurrency` to `false` in the auth DO, and get rid of the manual locking there. But perhaps we should also remove from the room DO.','public'),
	 ('znyGxse2L5-lR5-NUrixH','Clients stay connected too long',false,1684746193000,1671673179000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We see examples of clients in the logs that do nothing but ping for hours (like 12 hours!) straight. This costs money by keeping durable objects alive for no reason.

Probably a common cause of this is tabs being in the background and forgotten about. We could disconnect the socket on the tab hide event and reconnect on show.

It''s also worth implementing an "idle" feature: if a client doesn''t send any mutations for awhile, or receive any pokes, it disconnects itself. A potential problem with this is that if the tab is in the foreground, and this idle disconnect happens, then a collaborator could join and start doing things and user would not notice.','public'),
	 ('kYQF1eM1NXqzSISKYjoU5','Monday: Users seem to take a long time to connect',false,1683763877000,1671672014000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'From @noamackerman (https://discord.com/channels/@me/1040553788911661156/1055081386714857543):

@aa it seems we have degradation in number of failed connections (this is from the last 3 days):

<img width="1449" alt="1" src="https://user-images.githubusercontent.com/80388/209033985-183b7ece-6cad-44fd-8343-4f8349199be7.png">

In terms of absolute numbers, - 54 unique (and logged-in)  users that failed to connect is a lot for 3 days 
I''m also getting tickets from users who say Canvas does not load for them - and looking at the logs it seems like they can''t connect
looking at the increase in the number of these tickets and from generally looking at our metrics it seems something is off - is it possible that one of the last versions we deployed had a change that could cause it?
Anyway, I understand you want to wait to get more info before adding http fallback, but for me this is something we need to fix asap. Since we are time sensitive, I feel comfortable saying we need to add the fallback (or other solution you may find more suitable)
This is the room id of the last user that said he can''t connect - 0qXSAxeauYhR6owats8Dx0fekcv3VLmZ. 
I think that looking at data dog we miss some key data points. In DD you are looking at who connected eventually . In the screenshot below we are looking at a 10 seconds time window. It shows us the number of clients who moved from offline to online within 10 seconds or less. We can see that ~10% didn''t connect within 10 seconds or less 

<img width="1474" alt="2" src="https://user-images.githubusercontent.com/80388/209034023-24d2ea39-1c98-4452-8fa9-bfac5edb0d7c.png">

Here we are looking at 60, 120 and 360 seconds time windows 

<img width="1470" alt="3" src="https://user-images.githubusercontent.com/80388/209034056-6708c45c-4a1d-45b6-8057-d43db583d11a.png">
<img width="1453" alt="4" src="https://user-images.githubusercontent.com/80388/209034053-06f26fcd-9dfc-4f61-bfae-fb47c9006013.png">
<img width="1486" alt="5" src="https://user-images.githubusercontent.com/80388/209034049-d38ad9bc-8999-42ff-8fc9-fb5631df359d.png">

So the last one comes close to what we see at data dog, but this is a 360 seconds time window. So this is a critical metric to look at. We should be at a higher number within a few seconds','public'),
	 ('Nx3NmxamoB7soTtyMrcfE','Cloudflare has logpush for workers now!',false,1684746248000,1671671206000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Woo, this means we get all unhandled exceptions in the log and also probably runtime issues like OOM! We should set this up for replidraw and document how to do it for customers:

https://developers.cloudflare.com/workers/platform/logpush/','public'),
	 ('VujlSbbYch9U_P1ZyauT8','Monday: User report: cannot connect: room 0qXSAxeauYhR6owats8Dx0fekcv3VLmZ',false,1683763973000,1671666464000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'From datadog, this room has been accessed by two different IPs. The first IP is from Comcast Denver and it accessed this room and one other:

<img width="1568" alt="Screen Shot 2022-12-21 at 12 48 16 PM" src="https://user-images.githubusercontent.com/80388/209017532-743f4896-845e-4d08-9ea3-01c06cb368e3.png">

Neither room ever shows up in server logs. And we log the URL of the connection request extremely early in startup, so it is near impossible that if our code starts the room ID would not show up servers-side.

I am still suspicious that this isn''t just proxies and other networking hijinks though. I do see something else happening on the server around that same time, but I can''t be sure it''s from same client:

<img width="1169" alt="Screen Shot 2022-12-21 at 1 40 37 PM" src="https://user-images.githubusercontent.com/80388/209023813-baba15b4-be14-4a1d-a422-5c2e5d8d1f48.png">

It''s the "account" room starting up. Unfortunately, I don''t have the client-side logs for the account room so I can''t know if this is from the same device/user.

If it is from same user it would be interesting because it would mean some messages from that user *are* getting to server after all and something else is going on.

# Summary

So summarizing, for this particular user, all we can say with confidence is that they tried to connect but never did successfully.

There''s a possibility that they did actually talk to the server and then something else went wrong.

# Actions for this user:

* @noamackerman: Do you have the account ID of the user who reported the bug? Is it `14530460`? If it is the user that would be intersting.

# Actions to help debug things like this in the future:

Monday:

1. Can you please setup client-side datadog logging for the account room too?
2. It appears that you are setting the same disconnect handler for the account room as you are the canvas room, which is causing noise:

<img width="636" alt="Screen Shot 2022-12-21 at 1 29 06 PM" src="https://user-images.githubusercontent.com/80388/209022707-53dddd8e-54b2-4ffb-892a-828a03c0cbd9.png">

Note that the roomID is `account/`. If you do mean to run a disconnect handler here, you should guard against this state not existing yet when disconnect happens because it is possible for disconnect to happen before any message is processed and the state hasn''t been created.

Rocicorp:

* We need to add the client IP to the server-side log context so we can for sure know if a specific device *ever* talks to our server: https://github.com/rocicorp/reflect-server/issues/229
* We should document how to do logpush so Cloudflare can do and we can be certain we''re getting all errors: https://github.com/rocicorp/mono/issues/212','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('RK7GrOAKhl-bnUNbnWBRd','We should send the client IP in the server logs too.',false,1677094860000,1671662636000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'This would enable us to join with client logs across rooms.','public'),
	 ('jP6qsc8NaVub5KHv0GUS9','Finally finish undo',true,1677703277000,1671590382000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We never shipped [undo](https://github.com/rocicorp/undo) because I (aaron) screwed up the design. It doesn''t cover one of the cases from the famous Figma article:

https://www.notion.so/Redoing-undo-c0183b91d12c4272a3482e3233cc2890','public'),
	 ('UFJjZiGCSP8QdFMe_0t39','Audit all entrypoints and ensure they have beautiful clear error messages',true,1680636120000,1671590071000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Entry points include:
- methods or properties users call
- places we parse data from network or storage

See same bug in Replicache: https://github.com/rocicorp/mono/issues/49','public'),
	 ('xDzOur0W1r62K-h5A9wYX','Enable using watch()/subscribe() to maintain computed data server-side',true,1684746367000,1671589975000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'People often want to maintain some in-memory state in the DO -- lookup tables or  computed values -- that they will need inside mutators. Without this they have to do expensive scans to recompute this data constantly, or else carefully maintain it as a side-effect of performing mutations.

What many people ask about and feels natural to them is an interface that feels more similar to the `Reflect` interface from the client, where they can use `watch()` and `suscribe()` via the API they already know.

This needs some thought because I don''t think we want people actually programmatically invoking mutators via the server, but perhaps we could subset the interface somehow.

Other common requests in this area:

- Is there some way an external process can invoke some change on a room?
- onDisconnect (if we do allow users to directly call mutators, then the onDisconnect API could be the same on client and server)
- Is there some way to do some work async (ie call some other system) then mutate after?','public'),
	 ('o3zV2bl5vZlygIoOV-ToO','Client-side room creation',false,1678186223000,1671589049000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We should bring back the simple declarative client-side room creation.','public'),
	 ('4q0T060LnThGKHr_XhvYt','Finish experimentalWatch()',false,1677704576000,1671588980000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'See https://github.com/rocicorp/mono/issues/76','public'),
	 ('Fccuabzq0hVES_GDqKxEt','Add whether we are on server to `WriteTransaction`',false,1678763108000,1671588935000,'OeVnr1y5bEM_Yg06sUFtD',NULL,NULL,'public'),
	 ('2n0GzU5ohobtwhs6BHAiY','Hide indexes feature',false,1677781527000,1671588728000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'I don''t think we want this in Reflect initially. We are limiting data to 25MB, so indexes not really needed and it''s not really finished.','public'),
	 ('0zTrvA-6aVO8eNdHBoW7G','Evaluate if we should return a ClientStateNotFoundResponse when a pull attempt is made on a room that is not open',false,1678220708000,1671231873000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'        What does the client do when it discovers that a room it is trying to push to is not open? Re-try forever?

_Originally posted by @phritz in https://github.com/rocicorp/reflect-server/pull/211#discussion_r1050951501_
      ','public'),
	 ('9YKMkEd9DAe2ZI1wV8MXb','IDB errors in Safari',true,1677093100000,1671222363000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'From Monday logs:

<img width="1409" alt="Screen Shot 2022-12-16 at 10 25 41 AM" src="https://user-images.githubusercontent.com/80388/208183302-84a9a701-cc78-4eec-becb-a361ce843bc0.png">

clientID: ae56e50d

It doesn''t appear to stop the product from running, but is unexpected.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('HBtkfiTiA7jZ9CSovC7br','Update reflect-server to use @rocicorp/eslint-config',false,1677094861000,1671210573000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'        no else after return... `@rocicorp/eslint-config` would have caught this

_Originally posted by @arv in https://github.com/rocicorp/reflect-server/pull/215#discussion_r1049352674_
      ','public'),
	 ('kN4pldCOlinyctlR_RjwJ','Monday: Some users seem to disconnect/reconnect frequently',false,1683763969000,1671091991000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Sometimes in the logs we see people reconnecting a few times per minute. We should add a metric/logging to track the distribution of connection up-time.','public'),
	 ('D206pQI7AMjje3NeFWqDe','revisit reconnect logic',false,1677094862000,1670960790000,'yJ5hiysWE-LBcDfT44lR8',NULL,'we probably need some backoff, currently it just loops like crazy','public'),
	 ('BXhtvPNgBB250VJrX9Kwo','PullResponseOKSDD should use JSON for cookie',false,1677091391000,1670931122000,'nqYkxAGMnzk7Y5STjZryV',NULL,'And not the `Cookie` type.','public'),
	 ('gom9m84NHghSTKc_Qf9rt','Implement assertCookie',false,1677091392000,1670931059000,'nqYkxAGMnzk7Y5STjZryV',NULL,'The assertion method we are currently using is assertJSONValue which is not quite correct any more.','public'),
	 ('JpaKRaTCbaBever3R1Ucc','Index redefinition test cleanup/commented code',false,1677670789000,1670920420000,'nqYkxAGMnzk7Y5STjZryV',NULL,'        Should this part be commented out?  Isn''t this where the index is redefined and what this test is testing?

_Originally posted by @grgbkr in https://github.com/rocicorp/replicache-internal/pull/458#discussion_r1046153911_
      ','public'),
	 ('sFlYUgbMsvKLkDgWkpjPb','Change how we expose internal APIs to Reflect client?',true,1677090675000,1670920249000,'nqYkxAGMnzk7Y5STjZryV',NULL,'        I felt like I didn''t want to include these names in the compiled bundle.

Does reflect use these?

_Originally posted by @arv in https://github.com/rocicorp/replicache-internal/pull/466#discussion_r1043833649_
      ','public'),
	 ('LmtZrao_3o2dE0DYKoXWB','Document how cookies are compared',true,1677090676000,1670920108000,'nqYkxAGMnzk7Y5STjZryV',NULL,'        Should we document how they are compared here as well?

_Originally posted by @grgbkr in https://github.com/rocicorp/replicache-internal/pull/484#discussion_r1046117384_
      ','public'),
	 ('0SYi2rS6-eptSx9Jli9MG','First-class support for partially syncing',true,1677093101000,1670797262000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'This will be needed for Vercel to adopt Reflect. They are already using partial sync on their busier projects.

Imagine a deployment of comments on vercel that has hundreds or thousands of comments organized into threads. Threads can be "resolved" in which case they disappear from the default view.

Really what Vercel wants is a way to sync just the unresolved threads at first. Then if the user chooses to see resolved threads, then they want to get the rest of the threads, but perhaps paginated by last-modified or created.','public'),
	 ('t8TfRQn4UePo91vNUTSAl','Ship experimentalKVStore',true,1677090676000,1670640210000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'And remove the `withRead`, `withWrite` methods. I don''t think we need them -- they can be implemented by Replicache in terms of just `read` and `write`.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('66t0PwJqknQA_0wfSHwix','Introduce a Cookie type',false,1677091392000,1670586565000,'nqYkxAGMnzk7Y5STjZryV',NULL,'```
string | number | {order: string | number, ....}
```','public'),
	 ('0MqbIfE1OsaggQS9QCZSg','@rocicorp/reflect-server{-debug} package (same with client)',true,1677093102000,1670566681000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'This would be very useful for Monday and other paying customers. I don''t want to make it publicly visible though for IP/legal reasons.

The package will have literally the exact same configuration as current one, just that the build will be unminified. Is the right thing to do a workspace with a shared common package with all the code in it, and then two top-level entrypoints?','public'),
	 ('mDWurE1JZULLC5izuPPy2','we need instructions for how to build your own thing in reflect-todo',false,1677094862000,1670557584000,'yJ5hiysWE-LBcDfT44lR8',NULL,'replidraw has https://github.com/rocicorp/replidraw-do#building-your-own-thing but reflect-todo has nothing...','public'),
	 ('TSwpUWX58yW4--9Q1HVmf','we need a re-auth signal to the client',false,1677094863000,1670556184000,'yJ5hiysWE-LBcDfT44lR8',NULL,'we have this TODO: https://github.com/rocicorp/reflect/blob/main/src/client/reflect.ts#L313 but since the status code is opaque to the client we need to rely on an error message piped down from the server. if we get structured errors out of https://github.com/rocicorp/reflect-server/issues/206 then to close this issue it''s merely a matter of:

- ensuring we have a reauth hook that is triggered when we receive the relevant error from the server
- updating documentation to cover this','public'),
	 ('cexhW1awUBKv-mrwl75H1','upintegrate error message piping into main, and make the errors structured',false,1677094805000,1670556017000,'yJ5hiysWE-LBcDfT44lR8',NULL,'As part of https://github.com/rocicorp/reflect-server/issues/232 we created a branch of 0.18.0 for monday that pipes connect error messages down over the websocket so the client can see them, see this pr: https://github.com/rocicorp/reflect-server/pull/205

We should make main have this behavior as well. Note that we''ll have to dovetail with similar one-off code for room not found errors that is already on main: https://github.com/rocicorp/reflect-server/blob/76d1e37dedc202fec525dccb64b52e22f0bfce42/src/server/auth-do.ts#L253

Right now the error messages are unstructured, meaning that the messages are a brittle signal for the client to take action on. For example, if the client might need to re-auth the user. we should make them structured and treat them like an api.','public'),
	 ('G5xQd15DHqZMJI-6RpEOX','have better protection against devs accidentally re-using a room',false,1709537709000,1670519427000,'yJ5hiysWE-LBcDfT44lR8',NULL,'we''re adding a simple check to 0.19.0 during connection, but we should do better. eg from https://rocicorp.slack.com/archives/C013XFG80JC/p1670461281534869?thread_ts=1670433034.094169&cid=C013XFG80JC

> having the room choose and persist a random value when it is created and return it to a client the first time the client connects. the client passes this in future connections and if it ever does not match what the server has then the server errors the client out.','public'),
	 ('PzmGfX0MN6GFIRq9XcrHC','Monday: Out-of-order poke',false,1709537714000,1670496437000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Noam had a room in a browser that would not start -- it never rendered any data in the app.

The room ID was: Y3n0Tx4DVQHtahaXrgdE8jKH9vn

When we looked at the console together, we saw:

- multiple socket connections for same room being opened, but none seem to be getting closed
- error messages like "WebSocket is already in CLOSING or CLOSED state" when moving mouse

When I find this room on the server I see that it experiencing "out of order poke" frequently.

This error doesn''t seem possible given the way connection works. The client sends its current version as an argument to oonnect.','public'),
	 ('UCN2XOVUVt6_-_TR7gy2v','Monday: Large number of users seem to never connect',false,1683763941000,1670456298000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Noam reports that it appears a large number of users (up to 7%) never connect successfully.

Noam, can you please provide the following information to get us started:

- How is this metric calculated exactly? I know that you are using the [`onOnlineChange`](https://github.com/rocicorp/reflect/blob/main/src/client/reflect.ts#L55) event, but how are change in that value translated into this statistic?
- How often do you receive user reports of this problem? I know you''ve received one, but is that it?
- Have you been able to capture a debug log from either the client or server when this problem occurs?
- Have you ever been able to reproduce it?
- Do you happen to have user agent strings for the affected users?','public'),
	 ('uecSgZG32oJ3kDkz2FiVS','eliminate log spam: Your worker called response.clone(), but did not read the body of both clones',true,1677093103000,1670429266000,'yJ5hiysWE-LBcDfT44lR8',NULL,'This log line appears any time we clone a request but don''t use the original. Since [dispatch always clones the request](https://github.com/rocicorp/reflect-server/blob/52c462517c9b5a44d46cd01d3760b59c3d714b46/src/server/dispatch.ts#L189) no matter what the handler is doing with the body, we will get this message on several paths including createRoom in the roomDO (but also auth-related calls). 

To eliminate this log spam we should read the body exactly once and pass its contents through to handlers in case they need it. Handlers should get the body from the parameter instead of through the request. ','public'),
	 ('ztk0bpSn6AslFHQ0vPusW','eliminate log spam: An event handler returned a promise that will be ignored',true,1677093104000,1670429100000,'yJ5hiysWE-LBcDfT44lR8',NULL,'One of our DOs causes CF to complain:
```
An event handler returned a promise that will be ignored. Event handlers 
should not have a return value and should not be async functions.
```
We should eliminate this log spam and potentially fix the problem. I verified this exists in 0.18.0, so it predates the GDPR changes. 

To repro `npx wrangler dev` and connect to it with a dev server. The worker output will include this line.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('Oi3tH-OXx2Xk1JttpznoR','Update `experimentalPendingMutations` to include client id for pending mutations (or filter to the current client id?).',false,1677091393000,1670356600000,'nqYkxAGMnzk7Y5STjZryV',NULL,NULL,'public'),
	 ('QrofEPWhnJ6HON5bhFoS1','Update `"allow redefinition of indexes"` test when persist exists. Without persist we cannot check that this works.',false,1677091393000,1670355931000,'nqYkxAGMnzk7Y5STjZryV',NULL,NULL,'public'),
	 ('6s2JjE0lB2jf1bKIR4mX6','Update concepts/perf with latest perf numbers',false,1677091394000,1670270830000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'I know that v12 has improved significantly on bulk write performance, so I believe that this is now out of date at least on that dimension. We should update it, and then refer to it from the release notes.

It''s possible that we have improved significantly enough on other metrics that they should be updated as well.
','public'),
	 ('SvrP3QsvXKQLKTm7EQ20X','Update persist benchmark to reflect perf diff in persisting snapshots vs mutations',true,1677090677000,1669996398000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'        > @arv while their wasn''t much perf difference on my machine, there is some on the benchmark runner:
> 
> https://rocicorp.github.io/replicache-internal/perf-v2/
> 
> particularly for createIndex and persist.
> 
> I''m baffled as to why createIndex would be slower, I didn''t think flipping the DD31 flag really changed anything there.
> 
> I''m looking a bit at why persist is slower, but it is a different algorithm.

For persist, since we set up all the data to be persisted in the benchmark with mutations, in the new algorithm this is done by rebasing the mutations onto the perdag (as opposed to gathering chunks and persisting chunks in the old algorithm).  this is necessary for correctness.  Its more realistic that the bulk of the data to persist would come from a snapshot commit, with smaller amounts of data coming from local mutations.  We should update the benchmark to reflect the different perf of persisting snapshots vs mutations.

_Originally posted by @grgbkr in https://github.com/rocicorp/replicache-internal/issues/428#issuecomment-1335453904_
      ','public'),
	 ('bRj2O2xT-_O_tF50pC1So','Remove `reasonServer` from public API',false,1677091394000,1669995285000,'nqYkxAGMnzk7Y5STjZryV',NULL,'I think we can change the public API.

_Originally posted by @arv in https://github.com/rocicorp/replicache-internal/pull/424#discussion_r1038263987_
      ','public'),
	 ('4zSeBfb_JgIkGNOdXvZQs','RFE: Make persistence optional (memory mode)',false,1709536435000,1669921382000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'I think we may want to make persistence optional for a number of reasons:

1. Some customers don''t need it and it makes things more complicated for them. Examples:
 * Vercel doesn''t currently use persistence, but the presence of persistent data means they need code to delete idbs on logout for privacy reasons (#432).
 * Placemark eventually wants to use persistence to support larger drawings but for right now, they are blocked on that for performance reasons. So persistence just creates problems for them (ie rocicorp/mono#46)
3. IDB doesn''t work in Firefox incognito (#433), so we need a memory path eventually at least for that reason.

With Reflect the core of the featureset might very well move to multiplayer with persistence a valuable and optional addition rather than something that should be default-on. From the perspective of somebody who bought Replicache for multiplayer being able to just `persistence: true` seems absurdly magical.','public'),
	 ('xmEjX4ev_l1sVcqZRLNOR','Replicache doesn''t work in Firefox/Private Browsing Mode',false,1677091395000,1669921182000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'This is because IDB doesn''t work there: https://bugzilla.mozilla.org/show_bug.cgi?id=781982.

In order to function in Firefox/PBM, we would need some memory-only fallback for Replicache.','public'),
	 ('X-TwNXBDwTeQB0Mav31bU','RFE: enumerateCaches()',false,1709537728000,1669918206000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We have the awkwardly named `deleteAllReplicacheData()` :). But there are a few examples where we want to find all the Replicache instances for some other reason:

1. Vercel runs Replicache in the context of some host app. Their IDB data is intermixed with any Replicache data from host. They want an easy way to find and delete just their IDB instances (e.g., on logout). Right now they can do that by parsing our registry idb but it relies on private Replicache knowledge and is awkward.

2. An inspector tool for viewing Replicache data would ideally want some way to find all the Replicache data (and its format version, etc) so that it could provide a viewer for them.','public'),
	 ('uUS71yCwTy-mMs2Gs1Kp7','RFE: Support rolling back format upgrades',false,1677091395000,1669859640000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Thinking about this more, I do think it''s a bug that the product basically fails if you rollback. There''s no way to know ahead of time that you can''t do this.

So we should do *something*. Right now, Placemark says that when you do this, there is a "hash mismatch" error. Perhaps this is rocicorp/mono#47?

Big picture it seems like we need to define what should happen when you rollback. I don''t think it''s practical to say that we support mutation recovery. I think our options are:

1. don''t try to recover newer formats (ignore them)
2. delete newer formats (assumption: we are rolling back because something bad happened and there''s a chance we can''t read that data anyway)
3. delete all persistent replicache state','public'),
	 ('lsleH54u3UHhe1yORfDs0','maybe send auth errors down the websocket?',false,1677094805000,1669160057000,'yJ5hiysWE-LBcDfT44lR8',NULL,'We send ''room doesn''t exist'' errors down the websocket when a user tries to connect to a non-existent room: https://github.com/rocicorp/reflect-server/blob/0c80ed4da3a1acbf4b9fa7cc0e1a4157d5ffaba5/src/server/auth-do.ts#L277. If we don''t do this then the client has no way to know what happened. We should _consider_ sending auth errors down this way too. Not sure clear cut tho because if someone is unauthorized I dunno if we should be enabling them to allocate websockets in our server.

','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('ezbhPEJI1y-hMhjqjB4Yr','Make "hashes" less strict',false,1677091396000,1669152917000,'nqYkxAGMnzk7Y5STjZryV',NULL,'We saw issues when people moved from 11.3b and 11.2 because the shape of the hash changed and the regexp we use in 11.2 does not match the hashes in 11.3.

We now have the same issue between 12 and 11.

One way forward would be to do a 11.2.1 where we relax the regexp.

Also, for the future we should just make the hash be of type string of a minimum length of X?','public'),
	 ('FhcBxO3pSA8N_22_ekl-f','feature request: Clone room API',true,1677093058000,1669113771000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Monday requests a server-side API to clone a room. By clone what they mean is the data gets cloned, but not the clients or version. I think this would also be useful for offline scenarios (clone a room to work offline w/o conflicts).

https://discord.com/channels/830183651022471199/830183651022471202/1044194617094066207','public'),
	 ('UQcxWkedGQT8ZMwmBMCCv','manually assert  ReplicaheOptions types',false,1709536564000,1669067459000,'pgyTvcxh2hjmq2l4WKzK6',NULL,'problem: 

Currently we only have typescript validate the option parameter types but this can allow Rplicache to persist potentially invalid options. (e.g. name)

solution: 

Assert the ReplicacheOptions types prior to initializing Replicache.

','public'),
	 ('FoZDjD9_skSMC392IvUBH','audit error messages to ensure they are useful for identifying the cause/location of the problem',false,1709536584000,1669056139000,'yJ5hiysWE-LBcDfT44lR8',NULL,'Context: https://rocicorp.slack.com/archives/C013XFG80JC/p1669055894830389?thread_ts=1669053514.900719&cid=C013XFG80JC

> we have to prioritize doing an audit of all the entrypoints to replicache and making sure they have good error messages. If we''re going to ship compiled code, we have to make sure we always explain what went wrong because the stack is useless.','public'),
	 ('Ew5-8LTOhhub9yJ6SK9kR','Overlapped pokes flaky',false,1709536597000,1669025950000,'nqYkxAGMnzk7Y5STjZryV',NULL,'I see this often on Firefox (on the bots only) but it also happens in Chromium.

This is for DD31

https://github.com/rocicorp/replicache-internal/actions/runs/3496201514/jobs/5885915939#step:8:83

```
 ❌ overlapped pokes not supported (failed on Firefox and Chromium)
      Error: Overlapping syncs
        at maybeEndPull/< (src/sync/pull.ts:581:12)
        at async*withWrite (src/dag/lazy-store.ts:167:19)
        at async*maybeEndPull (src/sync/pull.ts:548:21)
        at _maybeEndPull (src/replicache.ts:939:50)
        at async*poke (src/replicache.ts:1233:15)
        at async* (src/replicache-poke.test.ts:123:17)
```','public'),
	 ('ZPt81ToAqtVOCfUGdOxje','Document spaces better',false,1677091397000,1668897563000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Now that we have decided on spaces being a more fundamental part of Replicache they should be documented that way. There needs to be a page for spaces, perhaps under "Understand Replicache".

I think the key things to communicate are:

- Spaces are not currently "part of" the Replicache protocol (the stuff represented in push/pull). They are "outside" Replicache in that sense.
- But in practice ~every application of Replicache uses this concept.
- Key constraints on spaces

I also think it''s worth reviewing rest of doc and making sure that spaces are discussed at appropriate times. ','public'),
	 ('sOF6posuPS3lQ3-Y3WTYy','placeholder: maintability',false,1691541923000,1668740935000,'yJ5hiysWE-LBcDfT44lR8',NULL,'Recent experience with reflect-server has made me want to keep a list of things we should probably consider/implement before reps beta, so I''m adding this to the beta milestone.

Needed for reflect-server/reps maintainability:
- alerting on logged errors 
- plotting and alerting on a few critical metrics: restarts, latency, HTTP error rate, ws error rate
- capability to centrally sift logs by customer, roomID, not sure what else (not sure what we currently have, worth a review)
- ~uniform routing~
- a strategy for managing auth and room storage schema, i don''t want anything complicated but we have to know how to make changes

@grgbkr feel free to add, I''m treating this as a living list.','public'),
	 ('EpQ6oAOuDW6QCX5QtHBbk','bad reused name',false,1677091397000,1668464279000,'nqYkxAGMnzk7Y5STjZryV',NULL,'        bad reused name

_Originally posted by @grgbkr in https://github.com/rocicorp/replicache-internal/pull/385#discussion_r1021684616_
      ','public'),
	 ('gigTXIcN8cXbUDQTEPn2r','Would be good to disable these for the ''experiment KV Store'' test as well.',false,1677091398000,1668464253000,'nqYkxAGMnzk7Y5STjZryV',NULL,'        Would be good to disable these for the ''experiment KV Store'' test as well.

_Originally posted by @grgbkr in https://github.com/rocicorp/replicache-internal/pull/385#discussion_r1021685960_
      ','public'),
	 ('AN6TTWBD46q_E1tCS4LyC','JSONValue Optional Properties?',false,1677091398000,1668422175000,'nqYkxAGMnzk7Y5STjZryV',NULL,'rocicorp/replicache-internal#364 

## Background:

- JSON does not allow undefined.
- IDB allows undefined
- We have historically allowed our JSON objects to have properties with an undefined value by using TS optional properties.
- We have filtered out undefined properties in some code paths
  - before writing to a store
  - but not when manually calling mutators

## Goals

- No cloning or freezing in release mode

### Nice to haves

- Get static (compile time) errors when passing in an undefined property.
- Runtime errors in debug mode when passing in an undefined property.
- No difference in behavior between debug and release mode.


## Alternative A

- Allow optional property values in JSON objects but do no filtering
- When pull happens these properties will be missing so reading them will get undefined again
- Footgun with object spread operator because it might replace existing properties with undefined values:

```ts
type Data = {
  id: string,
  name: string,
  description?: string,
};

type UpdateData = {
  id: string,
  name?: string,
  description?: string,
};


async function update(tx, updateData: UpdateData) {
  const oldData: Data | undefined = await tx.get(updateData.id);
  assert(oldData);
  const newData: Data = { ...oldData, ...updateData };
  await tx.put(newData);
}
```

If `updateData` has a property with value of undefined the above will override the existing property.



## Alternative B

- Do not allow optional properties in JSON objects
- One problem is that TypeScript is not very strict about this and fails to flag  incorrect usage. We can enable `exactOptionalPropertyTypes` and we can ask our customers to do the same. But, it is a pretty invasive change. Without `exactOptionalPropertyTypes` you can pass an object with and explicit `undefined` value into replicache and [TS will not complain](https://www.typescriptlang.org/play?useUnknownInCatchVariables=true&exactOptionalPropertyTypes=false&ts=4.8.4#code/KYDwDg9gTgLgBDAnmYcBSBlA8gOQGoCGANgK6oC8AUHHAD5wB2JRR1dcAzjFAJYMDmbegCMIEIsAIMhjEgFthwKDICCUKAUQAeTLkKlgAPhm6cWYQCtgAYxgBuSpVCRYCZKlPmrtuOTgAFAlgeYi0AbwBtAGtgRAAuTm4+fgBdBNN9MgBfQwcncGh4JBR0bHxiMgAmXxkmFhkuXgEZUXFJaRp6JgUlVXVNHTLM4ErjTtLcLxsYSrznQrcSz0tp6r9ImPjEptT0oYqRrLywtmLUADExXzgwuDYaYSCExuSAGnu4R4AvAH5npIE7xoWUcNAAZiQGLYeBAGHAwFAINZgBwOABGAAUYLECUuEAAlHtJisfCcaDQoMAYCQoHDsRAHMDQXAIVCYDC4QikSiOJUsTi4HjCRMzCSZjcPpTqbSWWJGXAQWwucjUZiwo8oAkAORorWvT4EL4JSEAE2AYL4wBNWXx8uVPL56qecC1lT1BqNcFN5st1ttlEVlDJiwuV3Wd3JBs12zeH2+-x27G9FoYVqBCuZrOhsPhiJV6P5EFxYmFy288GDFKpNLpcrYivBkOznLzDsLxYJRNF5eqlbgUprsoZ9eZ9tVGKd0Z17vjXoYZpTVptdtbqMdGu1bv1s+TvuXAcoQA).
- Detect this in debug mode and throw an error.
  - It is very likely that customers will only test replicache in release mode.
','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('NRrzGEnN_0Ya4PX8QKRqO','Release blocked: HEAD doesn''t run on any sample apps',false,1677091399000,1668314041000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'When you pack and install HEAD on any of our sample apps, it fails with below error:

<img width="1552" alt="Screen Shot 2022-11-12 at 6 28 01 PM" src="https://user-images.githubusercontent.com/80388/201505758-29c124ba-c887-4ebd-9406-3dc08a7557ef.png">

With `--debug` build, the unminified error is:

<img width="1552" alt="Screen Shot 2022-11-12 at 6 28 44 PM" src="https://user-images.githubusercontent.com/80388/201505770-c5b2bc69-fd5f-4a1f-bea0-97dfb36250c3.png">

For the record, crypto is defined:

<img width="1286" alt="Screen Shot 2022-11-12 at 6 32 30 PM" src="https://user-images.githubusercontent.com/80388/201505863-117e3d0c-3123-4747-b7fa-d3f449a7e085.png">

I bisected this to 217116d77f291ce166207919758a9e400f29a3d4. It''s not clear exactly how this commit introduced this.

One weird thing I see in the code that is probably unrelated:

<img width="1037" alt="Screen Shot 2022-11-12 at 6 33 12 PM" src="https://user-images.githubusercontent.com/80388/201505883-c398ab1d-b1d8-4c44-86bd-282d17da9cf8.png">

If `crypto` is undefined, we use `uuidNoNative` which ... proceeds to call `crypto`.','public'),
	 ('R3o_bo6z1J9azyjTK7cHf','src/replicache.test.ts is flaky on Firefox',false,1677091400000,1668083173000,'nqYkxAGMnzk7Y5STjZryV',NULL,'src/replicache.test.ts:

 ❌ pull (failed on Firefox)
      AssertionError: expected 3 to equal 2
      + expected - actual
      
      -3
      +2
      
      at r (node_modules/@esm-bundle/chai/node_modules/.pnpm/chai@4.3.4/node_modules/chai/chai.js:9[57](https://github.com/rocicorp/replicache-internal/actions/runs/3425553200/jobs/5706460873#step:8:58)4:12)
      at n.exports<[3]</t.exports/i.prototype.assert (node_modules/@esm-bundle/chai/node_modules/.pnpm/chai@4.3.4/node_modules/chai/chai.js:250:12)
      at p (node_modules/@esm-bundle/chai/node_modules/.pnpm/chai@4.3.4/node_modules/chai/chai.js:1409:11)
      at c (node_modules/@esm-bundle/chai/node_modules/.pnpm/chai@4.3.4/node_modules/chai/chai.js:7884:24)
      at src/replicache.test.ts:745:25','public'),
	 ('W37d7VfBqh58X2Mdq71qw','src/replicache-subscribe.test.ts flaky on Firefox',false,1677091401000,1668082894000,'nqYkxAGMnzk7Y5STjZryV',NULL,'src/replicache-subscribe.test.ts:

 ❌ subscription coalescing (failed on Firefox)
      AssertionError: expected 1 to equal 0
      + expected - actual
      
      -1
      +0
      
      at r (node_modules/@esm-bundle/chai/node_modules/.pnpm/chai@4.3.4/node_modules/chai/chai.js:95[74](https://github.com/rocicorp/replicache-internal/actions/runs/3436369895/jobs/5729816322#step:8:75):12)
      at n.exports<[3]</t.exports/i.prototype.assert (node_modules/@esm-bundle/chai/node_modules/.pnpm/chai@4.3.4/node_modules/chai/chai.js:250:12)
      at p (node_modules/@esm-bundle/chai/node_modules/.pnpm/chai@4.3.4/node_modules/chai/chai.js:1409:11)
      at c (node_modules/@esm-bundle/chai/node_modules/.pnpm/chai@4.3.4/node_modules/chai/chai.js:[78](https://github.com/rocicorp/replicache-internal/actions/runs/3436369895/jobs/5729816322#step:8:79)84:24)
      at src/replicache-subscribe.test.ts:857:35','public'),
	 ('4wBDlh9b774qfGD3pWe6d','docs: Add something about offline usage when using out of bounds blobs',true,1677090538000,1667987251000,'nqYkxAGMnzk7Y5STjZryV',NULL,'If we are offline when we try to upload or download a blob we get errors. For the upload case there needs to be code to try again.','public'),
	 ('yFQwxM7cOFO5508Fmjl1w','Fine-grained write auth',false,1683342026000,1667864174000,'yJ5hiysWE-LBcDfT44lR8',NULL,'https://www.notion.so/replicache/Reflect-Authentication-Authorization-and-Security-bf2b1a2e31844efca73c4aa453fdecdb#7993b8db69ea4cefa544cb22519f4af1','public'),
	 ('0MrPG9OT6P3cWTVT7hCL9','Add a new `user` option to constructor, make `name` optional',true,1677090539000,1667677795000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We have to explain everywhere that you should put the userID in `name` and that that is why it''s required.

It would be more user friendly to have a `user` option that is required for this purpose and then make `name` optional (I expect it would be rarely used, only in cases where you have more than one Replicache space per user).','public'),
	 ('Ch_t1N6EnWEtPeiEpOPgR','we should make it easier to debug failing tests',false,1677094806000,1667619962000,'yJ5hiysWE-LBcDfT44lR8',NULL,'The LogSink accumulates logging messages in reflect-server. Pretty much every debug, info, or error message I''ve needed to debug failing tests when factoring createRoom out of connect were in the log. However tests often use something like the [TestLogSink](https://github.com/rocicorp/reflect-server/blob/1954e4e842b9b398003390b2c05493c03d9a3c15/src/util/test-utils.ts#L102) which accumulates the messages, but does not output them. This is great when running passing tests, but when a test fails we want to be able to see the messages; a strong hint at the problem is almost always there. We should have an _easy_ way to dump those log messages when a test fails. Right now the only strategy is to either add manual `console.log` calls to the code, which doesn''t benefit from the log messages, or to find the assertion that failed and add something like `testLogSink.messages.map((m) => console.error(m))`, which often requires refactoring the surrounding code to ensure the log sink is actually available. 

We should have a mechanism that makes it easy to output log messages from a test that fails. I''m happy if they spew for all failing tests. But even better if we can do it selectively. Adding a line of code would be fine. 

Separately, it would be nice to be able to run a single test eg from the command line. 

','public'),
	 ('wZ_Uj_yveXV11BiLu8wVB','I am not sure why, but this type shows up in the generated .d.ts file, but not `PushRequestDD31`.',false,1677091402000,1667554360000,'nqYkxAGMnzk7Y5STjZryV',NULL,'        I am not sure why, but this type shows up in the generated .d.ts file, but not `PushRequestDD31`.

As far as I can tell, the type isn''t referenced by anything in the .d.ts file except the export list, so I guess it''s not doing a huge amount of damage. But it does seem like it would be documented which is unexpected and show up in intellisense and so on. Can we make it go away somehow?

_Originally posted by @aboodman in https://github.com/rocicorp/replicache-internal/pull/351#discussion_r1013796480_
      ','public'),
	 ('ciXzk53Xm-XiKsrpdr0yw','Validate writes are correct JSON not just reads',false,1677094812000,1667502691000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'From @noamackerman:

Received below stack trace, it crashed dev server. Error *not* reproducible.

```
RoomDO doID=910a47d52e70b6710b1d16119d54dda02031f31017821e37f1d06b1b0e4d0cda roomID=e191cy_DXMfTQQUU1hgddkPUm23wycj6 req=fpcgquiztm6 skipping mutation because error {"clientID":"8064160e-b095-451e-9443-7adfba4c7c45","id":5814,"name":"changeElements","args":{"info":[["textBlock-6z75h8jbFlF72kGEo1UhY",{"hidden":true}]]},"timestamp":1667501929902.5} Ge: At path: value -- Expected the value to satisfy a union of `union | array | record`, but received: [object Object]
    at nt (/Users/noamac/workcanvas/workcanvas/dist/index.mjs:162:13)
    at Pt (/Users/noamac/workcanvas/workcanvas/dist/index.mjs:139:11)
    at at (/Users/noamac/workcanvas/workcanvas/dist/index.mjs:632:29)
    at ri (/Users/noamac/workcanvas/workcanvas/dist/index.mjs:1670:12)
    at q.get (/Users/noamac/workcanvas/workcanvas/dist/index.mjs:654:13)
    at q.get (/Users/noamac/workcanvas/workcanvas/dist/index.mjs:654:13)
    at q.get (/Users/noamac/workcanvas/workcanvas/dist/index.mjs:654:13)
    at St.get (/Users/noamac/workcanvas/workcanvas/dist/index.mjs:1383:13)
    at getElement (/Users/noamac/workcanvas/workcanvas/dist/index.mjs:6809:14)
    at changeElements (/Users/noamac/workcanvas/workcanvas/dist/index.mjs:6933:21) {
  value: {
    type: ''textBlock'',
    id: ''6z75h8jbFlF72kGEo1UhY'',
    x: -6350.206713017092,
    y: -2008.6537282241807,
    fill: ''#000000'',
    fontSize: 36,
    width: 300,
    height: 43,
    cursorPosition: 1,
    attachedConnectors: {},
    textPosition: { x: 0, y: 0 },
    align: ''center'',
    zIndexLastChangeTime: 1667501917077,
    fontProps: 0,
    frameId: ''frame-2c48kqxnQQhJzxmKLamVf'',
    lastModifiedTimestamp: 1667501921904,
    text: undefined,
    textColor: ''#15BBB4ff''
  },
  key: ''value'',
  type: ''union'',
  refinement: undefined,
  path: [ ''value'' ],
  branch: [
    { deleted: false, version: 1993, value: [Object] },
    {
      type: ''textBlock'',
      id: ''6z75h8jbFlF72kGEo1UhY'',
      x: -6350.206713017092,
      y: -2008.6537282241807,
      fill: ''#000000'',
      fontSize: 36,
      width: 300,
      height: 43,
      cursorPosition: 1,
      attachedConnectors: {},
      textPosition: [Object],
      align: ''center'',
      zIndexLastChangeTime: 1667501917077,
      fontProps: 0,
      frameId: ''frame-2c48kqxnQQhJzxmKLamVf'',
      lastModifiedTimestamp: 1667501921904,
      text: undefined,
      textColor: ''#15BBB4ff''
    }
  ],
  failures: [Function (anonymous)]
```

Some questions:

1. What does this stack correspond to? Can we use the trick that @arv just did in Replicache to demangle the stack and see where this is coming from?
2. Why isn''t it persistent? From looking at stack I''m guessing this is happening when *reading* data from Replicache. Only way I would expect it to be not persistent is if:
  a. The value getting read was out of a pending transaction
  b. The value getting read isn''t read in every run of the app
3. Why isn''t the error better: it looks like what''s happening here are there are some undefined values in the JSON. The error should say what field exactly was undefined if that''s the issue.','public'),
	 ('RssVf1DE2spPv8zN2vg5R','change subscription onError param type to unknown',false,1677091402000,1667487765000,'nqYkxAGMnzk7Y5STjZryV',NULL,'To be consistent with TS:

Change:

```ts
export interface SubscribeOptions<R extends ReadonlyJSONValue | undefined, E> {
  /**
   * If present, called when an error occurs.
   */
  onError?: (error: E) => void;
}
```

to

```ts
export interface SubscribeOptions<R extends ReadonlyJSONValue | undefined> {
  /**
   * If present, called when an error occurs.
   */
  onError?: (error: unknown) => void;
}
```','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('Ohdwwu22-vX_ImbuTrFv3','add eslint rule for unnecessary async',false,1677091403000,1666975101000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'        remove async... maybe add eslint rule?

_Originally posted by @arv in https://github.com/rocicorp/replicache-internal/pull/326#discussion_r1001599759_
      ','public'),
	 ('YcbGG99oyIW41HwRc09v5','Provide private source builds of Reflect for easier debugging',false,1709536620000,1666296229000,'nqYkxAGMnzk7Y5STjZryV',NULL,'The following comment shows how to do this:

https://github.com/rocicorp/replicache/issues/985#issuecomment-1110708975','public'),
	 ('mrh64by3B9b6MRHbzkLQP','Add DevTools custom formatter',true,1677090672000,1666168138000,'nqYkxAGMnzk7Y5STjZryV',NULL,'https://www.mattzeunert.com/2016/02/19/custom-chrome-devtools-object-formatters.html

We could add a custom formatter for Store. Optimally we would want this for chunks but we need the store to follow those hashes so we can see the commits.

This would be enabled in tests only.','public'),
	 ('f9HpKYDyeknjCeeqiB8Op','Get rid of IndexChangeMeta all together.',true,1677090540000,1666118695000,'nqYkxAGMnzk7Y5STjZryV',NULL,NULL,'public'),
	 ('8tyDj9FUJWQ5qd2JEP3KS','Can we support large numbers of mutations per-frame?',true,1677090541000,1665587844000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'A fairly frequent request for things like graphics editors is to be able to send many mutations per frame e.g., for multiselect and drag. However this currently causes severe performance problems locally:

https://discord.com/channels/830183651022471199/830183651022471202/1029741664799043595

I''m not sure what exactly is behind the perf problems or whether we can/want to support lots per frame, but it''s certainly something that comes up and people try to do.','public'),
	 ('vuUo7EbodLwWIfEiDk24K','Allow mutators to have multiple arguments',true,1686065391000,1665349885000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Right now mutators have one optional argument. We want to change it to a varargs.

We started of with a single argument and changing it to allow multiple arguments is a format version change.

With DD31 we are doing a format change so lets try to get it in there.','public'),
	 ('kWW8COhKn1V7KvaHCOY1w','Get rid of indexes param to newWriteSnapshot',false,1677091403000,1665043671000,'nqYkxAGMnzk7Y5STjZryV',NULL,'With DD31 indexes can only be created in the genesis.','public'),
	 ('1uhZj7HqOSXjxmdcFpmTQ','bug: empty prefix for scan() scans all entries, not just user prefixed entries',false,1677094806000,1665015474000,'jpRvILZ1tibPsQXKvb3cF',NULL,'Logic is wrong here, this should always use the user value prefix when scanning the DO''s store.  As is, it returns all  entries, including internal ones.

https://github.com/rocicorp/reflect-server/blob/ca9f6316e0548d22a9b59b49ffe494d645ebdcb2/src/storage/replicache-transaction.ts#L99

Something like:
```ts
userValueKey(prefix || "")
```','public'),
	 ('1YV1D50AA5UoI5F0uK09J','Implement B+Tree multi put and use that for patch',true,1677090543000,1664966218000,'nqYkxAGMnzk7Y5STjZryV',NULL,'I seem to recall that doing a multi put is more efficient than inserting one entry at a time.

If the patch does not contain a `clear` operation we can sort the patch and do a multi put.

Also, if the patch contains a `clear` we can prune the patch operations before the clear.','public'),
	 ('QY0za0ktqSbHaA_JzvYZm','POST_DD31_CLEANUP: No more need to support concurrent requests in `ConnectionLoop`',true,1677090543000,1664900962000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Ref: https://github.com/rocicorp/replicache-internal/blob/345df2b3594352dcd6cab64b58956711473892ee/src/connection-loop-delegates.ts#L11','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('tgbUKd8vRsIZIvVOEIyh8','POST_DD31_CLEANUP: Question about line in connection-loop.test.ts',false,1677091404000,1664899985000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Ref: https://github.com/rocicorp/replicache-internal/blob/17fbc0618b02610994875f45e63c7fb5d3d543ec/src/connection-loop.test.ts#L59

I''m confused as to why if `invokeResult !== true` then the logged line is prefixed with `f`. I assume `f` means `false`, but it seems like `invokeResult` could also be true in this case?','public'),
	 ('iTnEytlglorlzOde51wrS','Unexpected performance regression in Reflect 0.9 on Monday Canvas',false,1677091404000,1664839588000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Monday has been unable to update reflect (client) because when they move the mouse rapidly, rendering janks, sometimes for several seconds.','public'),
	 ('MhUcjqKALpjUQ9qkuhuT0','Indexes affect performance worse than desirable',true,1677090544000,1664553725000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Populate and persist scale almost exactly linearly with indexes. With 1 index it takes twice as long. With 2 it takes thrice.

I don''t think this is acceptable. I expect answer is (#212) but making this bug more problem-oriented in case other solutions are possible.','public'),
	 ('Ze0cE2b6VsjLyc0ETGjfQ','Add benchmarks for up to 100MB',true,1677090545000,1664553538000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'- persist @ 100MB
- populate @ 100MB
- writeSubRead 64MB -> 100MB

anything with indexes?','public'),
	 ('jhDamM9QgeCgxNsfIKeH0','pull mutate options flaky on Firefox',false,1677091405000,1664438479000,'nqYkxAGMnzk7Y5STjZryV',NULL,'The bots often fail the "pull mutate options" test on Firefox

https://github.com/rocicorp/replicache-internal/actions/runs/3138671081/jobs/5098260368#step:8:62

```
src/replicache.test.ts:

 ❌ pull mutate options (failed on Firefox)
      AssertionError: expected [ Array(21) ] to deeply equal [ Array(21) ]
      + expected - actual
      
       [
         1000
      -  1040
      +  1030
         1060
         1090
         1120
         1150
      
      at r (node_modules/@esm-bundle/chai/node_modules/.pnpm/chai@4.3.4/node_modules/chai/chai.js:9574:12)
      at n.exports<[3]</t.exports/i.prototype.assert (node_modules/@esm-bundle/chai/node_modules/.pnpm/chai@4.3.4/node_modules/chai/chai.js:250:12)
      at l (node_modules/@esm-bundle/chai/node_modules/.pnpm/chai@4.3.4/node_modules/chai/chai.js:1467:9)
      at c (node_modules/@esm-bundle/chai/node_modules/.pnpm/chai@4.3.4/node_modules/chai/chai.js:7884:24)
      at p (node_modules/@esm-bundle/chai/node_modules/.pnpm/chai@4.3.4/node_modules/chai/chai.js:1406:11)
      at c (node_modules/@esm-bundle/chai/node_modules/.pnpm/chai@4.3.4/node_modules/chai/chai.js:7884:24)
      at src/replicache.test.ts:21[64](https://github.com/rocicorp/replicache-internal/actions/runs/3146198834/jobs/5114375312#step:8:65):22
```','public'),
	 ('LtEm7bIDbdzYH4TD6c9sK','One-mutation-per-push caused performance regression in canvas (monday) :(',false,1677094812000,1664358118000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'More information here: https://www.notion.so/replicache/2022-09-27-95be9489ca484e3d8ec9a7c127e10171

I rolled out the change: d20fdc3b36dae36f8e0af84e80e0d81d7038f316

I don''t know why this happens, the profile is super weird. We don''t see rebases for long spans of time. Then a huge number at once. It seems like we aren''t getting pokes for long periods.

I can''t reproduce this on replidraw. I can easily reproduce on https://app-dev.workcanvas.com/.

Profile attached. More information when less tired :).

[Profile-live.json.zip](https://github.com/rocicorp/reflect-server/files/9663620/Profile-live.json.zip)
','public'),
	 ('N983dAQkVWxJwlWi8nb_N','We can get rid of the whole complicated updateClients (which was needed due to async hashing and indexeddb)..',false,1677091437000,1664205709000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Callers can use setClients (and they need to put their own chunks).

Fine to do as a follow up as this is already quite a big pr.

_Originally posted by @grgbkr in https://github.com/rocicorp/replicache-internal/pull/275#discussion_r978787914_
      ','public'),
	 ('y0kKlvySjGINkg-qxjRHT','(dis)confirm that the auth do doesn''t end up proxying all ws messages ',true,1677093106000,1663900342000,'yJ5hiysWE-LBcDfT44lR8',NULL,'The worker does, but the auth do in our mental model shouldn''t. However the upgrade is issued from the auth do not the worker so i suppose it is possible. I can only think of bad things that happen if they do this but who knows. 

See this thread: https://rocicorp.slack.com/archives/C013XFG80JC/p1663122781349039','public'),
	 ('Q53sfE7H1E6Je7ZvYJ3x7','re-estimate cost of running reflect',false,1677094807000,1663899945000,'yJ5hiysWE-LBcDfT44lR8',NULL,'A lot has changed, let''s see where we stand. 
- [old, rough notes on the cost estimation](https://www.notion.so/replicache/Estimating-reps-capacity-3061e9e059734f9cb4ce27cfd502219c#776078509f7d437c90afb5b115186fb9)
- [old cost estimation spreadsheet](https://docs.google.com/spreadsheets/d/18Y7C0Wi5MaMfYtpEYXYEh6C3Mu2xqoFc0bvaaW0srGg/edit#gid=1522528439)

It should probably factor in our perf envelope (#63). ','public'),
	 ('MAAYQvduKVZK6RYRd38Ly','De-experimentalify watch',true,1677091221000,1663807148000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Supersedes rocicorp/mono#150.

I think we might someday want to do the other fancy stuff like filters, sorts, and joins so maybe we should redesign the API taking that into account? Or maybe we should just yolo and ship what we have.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('UVfTqd4_ev9vol4smUp9d','Expose experimentalWatch() in reflect',false,1677094813000,1663806840000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'I think this is purely a matter of putting a `experimentalWatch()` on `Reflect`.

De-experimentalifying is a different matter: https://github.com/rocicorp/mono/issues/76','public'),
	 ('SZ8UieCHMznEsf_Q6EybI','make FPS safe and correct',false,1681147166000,1663741115000,'yJ5hiysWE-LBcDfT44lR8',NULL,'## Background
- we are not doing [streaming reflect](https://www.notion.so/replicache/WIP-Streaming-Replicache-4acd7513121949f5898f7eeeeeaef96f) in the near term
- however reflect as currently built assumes the output gate is disabled, as it would be for streaming reflect
- unfortunately, without a complete streaming implementation, replicache is not correct unless the output gate is enabled
- monday has enabled the output gate, and [output gate enabled will become the default](https://github.com/rocicorp/reflect-server/issues/154)
- we therefore have a reflect that operates under the assumption that the output gate is disabled, which not longer holds. we need to make the adjustments that enable MP to work well when the output gate is enabled
- the primary effect of enabling the output gate is that a write to transactional storage takes ~30ms and prevents outgoing messages from being sent until it is complete
- @ingar has demonstrated that ws message sending throughput from DO to client is not materially affected by this additional latency. latency obviously increases, but throughput doesn''t change much. 
- i have demonstrated to myself that the maximum send rate of out outbound 4kb ws messages from a DO to ~50 clients is in the neighborhood of 8000/second. and that''s if the DO does nothing but send messages. It would be prudent to limit ourselves to say ~2000 outgoing msgs/sec.

## Current model
The idea in the current reflect server code is to send a poke to each client on every frame. Sixty times per second (so every ~16ms) the server creates a frame update. It applies all the mutations it has to in-memory data structures, computes the pokes, writes the data structures to transactional storage (with output gate disabled), and sends pokes to all connected clients. Mutations arriving during and between runs are queued for processing in the next run, and all available mutations are consumed during a run. The client replays pokes in order as soon as it receives them.

Aside from the not-correctness of revealing mutations before they have been confirmed, here are some other things that can happen that might not be desirable (or maybe we don''t care, dunno):
- reflect server doesn''t use information about the temporal spacing of mutations in the originating client. Two mutations that happened frames apart but are delivered to the reflect server at the same time get lumped into the same frame. (Similarly, two mutations that happened in the same frame could appear in different frames if delivery of the second is delayed). Not saying this is a problem we need to fix, just saying this is going to happen.
- similar thing on the receiving client end: it just plays pokes as they are received. Pokes that were sent representing separate frames that arrive close together get replayed in the same frame, and if the second of two consecutive frames'' pokes is delayed then there is a gap in client replay. 

When we enable the output gate we guarantee that we''ll end up making this kind of thing worse: we''ll group changes that span several frames into the same frame maybe on the server and receiving client, reducing apparent FPS on the receiving client. 

The fundamental problem is trying to do 30ms of work (flushing writes) in 16ms. If there''s a mechanism that causes the loop to iterate only every 30ms, eg backpressure or waiting for the last write to flush, then two frames of incoming mutations pile up while the write completes and end up grouped together in the next frame. If there''s no such mechanism then the loop just gets farther and farther behind (it continues to pile up 30ms writes followed by msg sends every 16ms, meaning clients get further and further behind). And either way, pokes only flow out every 30-45ms, depending on how it works, meaning tons of skipped frames. 

## What to do
It seems clear that we need to resurrect something like the old [game loop idea](https://www.notion.so/replicache/Replicache-Game-Loop-37d8d320ea6540ef8c1907dee26e232b) but simpler, where every N > 30 ms we process several frames of mutations into a sequence of updates to be played back sequentially. Aaron has said that if the loop turns every 500ms that would be a bummer bc of the latency hit but still probably ok. Luckily it seems like we could do much better. If the loop turns every ~4 frames (=4*16ms=64ms) it seems like the ~30ms write should have plenty of time to complete. 

Some additional thoughts:
- i don''t see any reason to queue up mutations and process them in a batch, it seems like we could process mutations as they arrive
- for efficiency''s sake (writes to storage including cache are expensive) we probably want to process mutations in memory and flush to storage at the end of every turn. This however potentially limits size of the data we can work with bc DOs are limited to 128MB including the cache (which I guess we can disable). 
- we do not have an explicit signal that writes have flushed. without one, I''m not sure how we could tell whether the server is getting behind. YOLO?
- i don''t think we should try to re-use the current loop implementation except for the in-memory datastore part. there''s a lot of complexity that seems unnecessary at this point
- there needs to be some mechanism for the receiving client to jump forward and apply mutations in batch if it has a bunch of frames but not enough time to replay them. 
- we have in the past been enamored of the idea that we could use mutation timestamps from the sending client to have perfect replay. the mechanism by which the sending client, server, and receiving client clocks are aligned is not clear and seems complicated, my guess is we could start with maybe server receive timestamp (or better, frame number) and see if it works
- let''s start low tech 
- we should have a way to tinker with the settings and see what it looks like under various assumptions. @arv i think had an amazing simulator for replidraw IIRC that automated a ton of clients all moving stuff around at the same time, something like that would be killer.
- note rocicorp/reflect-server#154 should probably come first? ','public'),
	 ('BN8qxUwQ58_bGcy1crLbR','Perf `populate 1024x1000 (clean, indexes: 1)` regressed',false,1677091438000,1663600288000,'nqYkxAGMnzk7Y5STjZryV',NULL,'https://rocicorp.github.io/replicache-internal/perf-v2/

The `populate 1024x1000 (clean, indexes: 1)` regressed in 603bda224dfbab6731e8e8d41563cdd6b77d3426

Last good commit: 5b02db6a31bf59dc8b793966f345b092ad516a91','public'),
	 ('HxotH2CR2e4AsbnkpR-ZD','Do not depend on getSizeOfValue in B+Tree',false,1677091439000,1663594246000,'nqYkxAGMnzk7Y5STjZryV',NULL,'We use an approximate size of a value to figure out how to partitition the B+Tree. The thinking was that we wanted chunks that are close to a certain size to get efficient read/write to disk. Our abstraction is very far away from the actual disk IO and having to compute the approximate size is a bottle neck in https://github.com/rocicorp/mono/issues/79.

We need to figure out a better way to decode where to partition the B+Tree nodes.','public'),
	 ('zVe6i8D895rFEjlRZ4LtW','Change LazyStore to not depend on getSizeOfValue',false,1677091447000,1663594083000,'nqYkxAGMnzk7Y5STjZryV',NULL,'`getSizeOfValue` shows up as a bottle neck in https://github.com/rocicorp/mono/issues/79

For LazyStore we use it as a heuristic for the amount of data to store in memory.

For LazyStore we can probably get away with say N chunks instead.

We would need to do some instrumentation to figure out what N should be.','public'),
	 ('ObHwvKvMqY5uHhQ6JVhVQ','Customer report: unexpectedly poor import performance',false,1677091439000,1663349873000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Discord thread: https://discord.com/channels/830183651022471199/830183651022471202/1020358780610953356

Summary:

* Tom (placemark) reports that import of a 20MB blocks main thread for 2-3s
* We expect 20MB to import in roughly 600ms from our perf tests
* The trace shows even commit taking 600ms alone, and put() is dominated by measuring the size of nodes (though note that everything is slower with trace running, typically ~2x).

Tom created a reduced test case here: https://codesandbox.io/s/gracious-sunset-ricd80?file=/src/App.js.

[trace.json.zip](https://github.com/rocicorp/replicache/files/9586083/trace.json.zip)
','public'),
	 ('PB7lAOkUHhL5Q5Xrgi_DY','data for EU users stays in EU (GDPR)',false,1677094807000,1663293806000,'yJ5hiysWE-LBcDfT44lR8',NULL,'background:
- [monday request](https://www.notion.so/replicache/Monday-Priorities-shared-ce186403a079408abcdbb6aa123c48f8#9461759c182c46e6a6af5a0d3af15fb6)
- [durable objects support for keeping data in the EU](https://blog.cloudflare.com/supporting-jurisdictional-restrictions-for-durable-objects/)

I think we need at least the following questions answered to figure out what to do here:
- determine what governs whether a DO is pinned to the EU: whether the first client to connect is associated with a customer user in the EU? how do they tell us that? 
- if a DO is not pinned to the EU is it ok for an EU user to join its room and presumably create data that is not stored in the EU?
- decide whether metadata (eg authdo) has to be stored in EU? seems like EU just starts sucking in the world if so.
- confirm we don''t have to support migrating a room, eg if the user who owns the room moves to the EU or becomes a citizen or whatever
- confirm that monday''s lawyers view on all this lines up with ours


','public'),
	 ('xtl0ioLiBlRejkZNEfCz1','delete a user''s data upon request',false,1677094808000,1663292986000,'yJ5hiysWE-LBcDfT44lR8',NULL,'background: 
- [feature request from monday](https://www.notion.so/replicache/Monday-Priorities-shared-ce186403a079408abcdbb6aa123c48f8#4591cf6710c9427fbfa04b825dc99738) (be sure to read the comments)
- [this conversation](https://rocicorp.slack.com/archives/G013XFG80JC/p1663122658077229) is related

Distilling it down:
- for each end user account the customer has, the customer keeps the list of rooms that user owns
- when an end user deletes their account with the customer they can request that their data be deleted too. ''their data'' means:
   - all rooms owned by that user and whatever is in them
   - all metadata about the user''s rooms
   - (unclear -- confirming this) room data on any client that is or attempts to connect to the room (?)
   - any of this user''s data in _other_ rooms has PII removed. **This would not be an easy thing for us to do** and it seems complicated to provide the interfaces that would enable the customer to tell us how to do it. Luckily monday already stores anonymized data in CF so this is a nop for us. Seems like customers that are real businesses are all going to need a solution here, so maybe we should be telling them to store anonymized data in CF? @aboodman ?
- it is NOT required (and in fact undesirable) to delete any data the user contributed to rooms they do not own

Seems like to close this issue we should do something like the following. Note that I am new to reflect so this might not be complete or even accurate.
- reflect gains a [new http interface](https://github.com/rocicorp/reflect-server/blob/255606bb461c418fe7a8de036a4df1dcb94a114b/src/server/dispatch.ts#L26) called `deleteRoom` that takes the roomID. It logs everyone out of the room (authInvalidateForRoom) and then calls into an interface on the roomdo that deletes all its data (`storage.deleteAll` -- read the docs for deleteAll, it might need to be called repeatedly until it succeeds). Once the roomdo has deleted all the data then the authdo should probably remove all the connection records for the room from its storage so `authRevalidateConnections` doesn''t go re-create the object. 
   - @grgbkr is there any other metadata aside from the list of connections that needs to be cleaned up? any cache? other list of rooms? etc
- decide whether a room can be re-created. my sense is ''no'' but it''s not a strong feeling -- to me a room name is a unique identifier and we are asking for problems re-using them.
   - if ''no'' then we need to decide the mechanism by which a deleted room is prevented from being recreated, as well as say what should happen when a client tries to connect to a deleted room. this last part is related to the question of whether we need to delete room data from all clients.
   - if ''yes'' then we need to say how we distinguish between a client who has data from the old instance of the room and one who has data from the new room. maybe the cookie just works for this, dunno. also the customer has to be completely sure they have scrubbed all metadata for the room (eg, perms) bc they definitely do not want old metadata to apply to a new room.
- let monday know when the api is available so they can start using it. they need to be sure that they clean up all metadata for the room on their end.

','public'),
	 ('I0FuCh24hdKAroVi8kvMt','Test on older/crappy/Android phones',true,1677093059000,1663288597000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'See: https://discord.com/channels/830183651022471199/830183651022471202/1020113164437831750

We should test this at some point.

@phritz @arv you two use Android right? Can you test https://replidraw-do.vercel.app/ quick and see what you see?','public'),
	 ('0_xC05llckH00ow44OBG_','send mutations from the client as they happen',false,1677094808000,1663285297000,'yJ5hiysWE-LBcDfT44lR8',NULL,'There doesn''t seem to be an apparent advantage to batching them when sending over the web socket and by _not_ batching them we decrease the chances that a batch will run afoul of CF''s 1MB message limit. We are still open to an individual mutation being more than 1MB, that is out of scope for this issue. 

This is a high priority feature request from monday.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('EE3cR-hbWnphH5z32cg1f','Implement `ClientNotFound` behavior in Reflect',true,1677093060000,1663278756000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Replicache has a `ClientNotFound` message in the protocol that causes the client to auto-reload. This is super useful during development when it is common to delete server storage. Without this, it is common for developers to delete server-side state, then their open tabs start throwing errors in a confusing way.

To implement in Reflect we need to:

1. implement the server part of the protocol
2. make sure reflect has the version of reflect that has this code and wire it up','public'),
	 ('ktbLINZJqyyfdmawfCpEP','Add peerDependency on tested version of wrangler to reflect and/or reflect-server',false,1677094813000,1663278429000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'This was @grgbkr''s idea. We can prevent bad dx where user build a new app based on reflect and gets a version of wrangler we have not tested by putting a `peerDependency` in reflect-server and/or reflect that expresses the version we will work with.','public'),
	 ('z9Cap5OQiu1dgn48WaR6j','authHandler parameter to createReflectServer should be optional',false,1680045031000,1663278154000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'It''s common when tire-kicking to not need auth and this just creates friction.','public'),
	 ('xSXg2DoQufRwGe__p5SLs','Default output gate ON',false,1677094814000,1663246567000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Currently the output gate is default OFF and you have to set `allowUnconfirmedWrites` to `true` in the options to enable it. Let''s default the other way.','public'),
	 ('My1g7O7guep2Z-OLIvtv5','Switch to zod',false,1677703455000,1663246500000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'reflect-server uses superstruct, but everywhere else in repliland, we use zod. We should standardize on zod I think.','public'),
	 ('tURezy_ctECa2TpCSYMaZ','Audit fastForward',false,1679304621000,1663246342000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'I believe that fastForward has a bug where if the client passes a cookie or lmid from the future, the server does not correctly reject the connection. Instead, the server just kind of  lets the client connect and waits for the server state to catch up with the cookie the client provided.','public'),
	 ('9tXxeY4Z69n4Xj2aLCZqE','Clean up packing and deployment of replidraw-do',false,1677704917000,1663246209000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'* pure React, not Next.js
* Move to CF pages, not Vercel
* Continuous deployment','public'),
	 ('c99Kay_0wR92shCDEyfKV','`room` -> `space` in API',false,1677094814000,1663246108000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We''ve settled on the terminology `space` elsewhere.','public'),
	 ('4KU-kSQIAdfrmqt09_STI','Merge `reflect` repo back into `replicache`',false,1677704703000,1663246077000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'API strawman:

The main difference between reflect and replicache at this point is just the socket interface vs http. So have two different option bags on `ReplicacheOptions`, one called `http` and the other `ws`.','public'),
	 ('eltGpf5xMAK4DDZoVRJQU','Not compatible with wrangler 2.1.4',false,1677094815000,1663242199000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'When we upgrade replidraw-do to wrangler 2.1.4, we see this error on client connection:

```
GET /connect?clientID=9565b490-0bfd-400c-a460-da0668ebdc01&roomID=I2c1wt&baseCookie=&ts=50297.700000000186&lmid=0 101 Switching Protocols (6.22ms)
[mf:err] Unhandled Promise Rejection: TypeError: WebSocket already closed
    at WebSocket.[kClose] (/Users/aa/work/replidraw-do/node_modules/@miniflare/web-sockets/src/websocket.ts:191:30)
    at WebSocket.close (/Users/aa/work/replidraw-do/node_modules/@miniflare/web-sockets/src/websocket.ts:180:10)
    at Cr (/Users/aa/work/replidraw-do/node_modules/@rocicorp/reflect-server/out/reflect-server.js:1:23411)
    at /Users/aa/work/replidraw-do/node_modules/@rocicorp/reflect-server/out/reflect-server.js:1:27343
    at Je (/Users/aa/work/replidraw-do/node_modules/@rocicorp/reflect-server/out/reflect-server.js:1:10197)
RoomDO doID=a0124dccb0c0054e06a5de46ffd10204bb6cdf5ebfd0e09bf1d06b1b0e4d0cda roomID=I2c1wt req=si39y4fhjik client=e51d7b9e-2259-45f6-8133-64cebe600cf0 parsed request {
  clientID: ''e51d7b9e-2259-45f6-8133-64cebe600cf0'',
  userData: ''redacted'',
  baseCookie: null,
  timestamp: 55365,
  lmid: 0
}
```

This appears to actually be an issue with (wait for it) miniflare!

I''m starting to think Miniflare is not a great idea and we should mock out the dependencies for testing instead. Unfortunately, non-miniflare mode is also not working: https://github.com/rocicorp/mono/issues/292 :(','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('yXIQjHlkpzyYtd6i2iATz','Switch to `wrangler dev` (and away from `wrangler dev --local) for developing apps on reflect',false,1677094815000,1663242170000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We have had a lot of problems with `wrangler dev --local` ("miniflare mode"). We should switch  to non-miniflare mode because it runs on the real production system and will have less compatibility problems.

However currently non-miniflare mode is not working for most of our team and we need to figure out why.

Notably @ingar claims it works for him. But not greg and I.','public'),
	 ('Jm_xBm09E7h9hhqIE-jRl','Initial "fast-forward" poke not sent until first mutation',false,1679687456000,1663241947000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'I never noticed this because the sample app has tons of mutations due to mouse moves. But in a slower moving app, like a todo list, the initial data doesn''t show up until the first mutation after the client connects.

This is happening because the logic around when to send the fastForward poke is not correct. It is currently done in `processRoom` but `processRoom` only happens as long as there are pending mutations.','public'),
	 ('p_5ReMwLkMMCjiu3C_NqE','`tx` should be renamed `dbWrite`',false,1677091448000,1663180225000,'nqYkxAGMnzk7Y5STjZryV',NULL,'`tx` should be renamed `dbWrite`

_Originally posted by @grgbkr in https://github.com/rocicorp/replicache-internal/pull/252#discussion_r970931405_','public'),
	 ('nGNZs10-Hn3bJYj3-hIrR','should this be ClientID instead of string',false,1677091449000,1663180197000,'nqYkxAGMnzk7Y5STjZryV',NULL,'should this be ClientID instead of string

_Originally posted by @grgbkr in https://github.com/rocicorp/replicache-internal/pull/229#discussion_r971014765_','public'),
	 ('mjumIfM9c03NS78CEFwdj','Problems using exported `PushRequest`',false,1677091440000,1663125890000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'1. `Mutation` isn''t exported so you can''t use it for example for a parameter to some function that processes a push.
2. The `args` in `Mutation` is `InternalValue` not `JSONValue`.','public'),
	 ('BWCxvrupwSoF2wcIXU3bX','Strip (or guard against) process.env.NODE_ENV==="production" in `out/replicache.mjs` output on npm',true,1677090546000,1663087499000,'vxBeM-NB6tEvoQuhMhFJ9',NULL,'`import { Replicache, TEST_LICENSE_KEY } from ''https://unpkg.com/replicache@11.2.1/out/replicache.mjs'';`
On my demo I tried importing RC like this, but couldn''t initially because of this code:

`var et=process.env.NODE_ENV==="production"`
https://unpkg.com/browse/replicache@11.2.0/out/replicache.mjs

Once I polyfilled it, it worked great. 

I''d suggest guarding against that somehow so it can just be imported as an es6 module.

Keep up the great work!




','public'),
	 ('w3ZE0-8JOHV1A-eCT_xZI','Implement `scan()` in mutators',false,1677094816000,1662803581000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'I started on this, but never finished. It''s in my local repo.','public'),
	 ('AMvmsOeKXsUeuIp6gSQfa','RFE: Readonly view/mirror of an existing Replicache instance',false,1677091450000,1662364919000,'nqYkxAGMnzk7Y5STjZryV',NULL,'One scenario that keeps coming up is to do full text indexing of data in a worker.

Some people move the Replicache instance to the worker but that has some overhead marshalling everything to the web worker.

Another option is to have 2 instances of Replicache. One on the main thread and one on the web worker. That has the problem that both of the instances have to `pull` to be synchronized. With DD31 the need to pull to sync goes away.

But even with DD31, one needs to provide the same mutators and indexes to get the instance to share the sync branch. It also needs to periodically refresh. 

Another option might be to provide a readonly view/mirror on top of an existing Replicache IDB. This would periodically run refresh or get triggered to refresh from other clients on the same sync branch.','public'),
	 ('X16YYvzWiSnMXb3I58MI8','Fix incorrect flushing of write''s made by mutation that throws an error',false,1677866008000,1661879835000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'We should not flush changes made by a mutator that threw an error.  This could result in a mutator being executed non-transactionally (some of its write apply but not all).  

We still want to putClientRecord with new lastMutationID and putVersion, which will avoid continually trying to reprocess the erroring mutation.

Current code:

```
    const tx = new ReplicacheTransaction(cache, clientID, version);
    try {
      const mutator = mutators.get(mutation.name);
      if (!mutator) {
        lc.info?.("skipping unknown mutator", JSON.stringify(mutation));
      } else {
        await mutator(tx, mutation.args);
      }
    } catch (e) {
      lc.info?.("skipping mutation because error", JSON.stringify(mutation), e);
    }

    record.lastMutationID = expectedMutationID;
    await putClientRecord(clientID, record, cache);
    await putVersion(version, cache);
    await cache.flush();
```','public'),
	 ('U0I3lwUctFFGluv0Rg_PK','Update the version field in PushRequest',false,1677091450000,1661863457000,'nqYkxAGMnzk7Y5STjZryV',NULL,NULL,'public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('gS-csrfUcMG1mc1dvxcaE','Add `branchID` and `clientID` to `Mutation`',false,1677091451000,1661854203000,'nqYkxAGMnzk7Y5STjZryV',NULL,NULL,'public'),
	 ('JTyziL5bz1bnMLoHqwBPc','Normalize index definitions',false,1677091451000,1661763299000,'nqYkxAGMnzk7Y5STjZryV',NULL,'`IndexDefinition` has optional properties but to make the code paths simpler we should normalize these on entry.','public'),
	 ('a04zSxdTm6z7kokm3pw_s','License server should be a subdomain of replicache.dev',true,1677090547000,1661535577000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Putting it on herokuapp looks kind of bad:

https://rocicorp.slack.com/archives/C02CT58AZ5H/p1661533754005729?thread_ts=1661532069.699159&cid=C02CT58AZ5H','public'),
	 ('V8lho3Cqi5_qJaRtvHjPj','Need a way to not send license pings direct from client for important customers',false,1677091441000,1661534432000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'See: https://rocicorp.slack.com/archives/C02CT58AZ5H/p1661532069699159

@arv is it possible to have the compiled code check for environment variables? Perhaps we could have a DISABLE_LICENSE undocumented env var.

Or maybe there could similar be an undocumented client API?','public'),
	 ('-hS3eXkArDXIEBbRliAbq','An error in persist prevents future persists from being scheduled',false,1677091441000,1661304014000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'In replicache.ts we have:

```
  private _schedulePersist(): void {
    if (this._persistIsScheduled) {
      return;
    }
    this._persistIsScheduled = true;
    void (async () => {
      await requestIdle(PERSIST_TIMEOUT);
      await this._persist();
      this._persistIsScheduled = false;
    })();
  }
```

If `this._persist()` throws an error `this._persistIsScheduled` is not reset to false, and no future persists will be scheduled.  `this._persistIsScheduled ` should be reset to false in a finally.

Something like:
```
  private _schedulePersist(): void {
    if (this._persistIsScheduled) {
      return;
    }
    this._persistIsScheduled = true;
    void (async () => {
      await requestIdle(PERSIST_TIMEOUT);
      await this._persist();
    })().finally(() => (this._persistIsScheduled = false));
  }

```

We should add a test for this.

','public'),
	 ('w6C-v7kaSKr1ZOvXqkuGs','De-Nextjs replicache-todo',false,1677091442000,1659479589000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Public bug: https://github.com/rocicorp/replicache/issues/1021

Next.js is not doing that much for us and adds mental overhead to the quickstart. Let''s get rid of it.

Let''s factor a `replicache-node` out of `replicache-nextjs`. I think we should go for pure node. No express, no nothing. I think this package would be server-only, no client (no client bits needed, because without Next.js the complexity of instantiating Replicache client goes away).

This will become the main quickstart. 

`replicache-nextjs` will remain as a thin wrapper around it `replicache-node` -- the main purpose is to support the nextjs integration and partnership.

We will also build https://github.com/rocicorp/replicache/issues/1016, https://github.com/rocicorp/replicache/issues/1015, https://github.com/rocicorp/replicache/issues/1014, and https://github.com/rocicorp/replicache/issues/1013 all depending on `replicache-node`. Woo!','public'),
	 ('HLlD662YuR4MaSKF6pmD2','Indexes should probably be non-covering',true,1677090548000,1659458101000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'I''m generally concerned that we implemented our indexes "covering" (that is each index contains a copy of all the primary data). This was probably a bad idea -- it just generates a ton more data to write. If we were making use of multiple threads for writing then ... maybe, but we''re not.

We should perf test making them non-covering, but I''m betting it will dramatically outperform on write, and only cost a little on read.','public'),
	 ('FlogsI7pWqh5Jhf-nx57-','First-class incremental and window sync',false,1677091443000,1659241408000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'It is possible to use Replicache cookies and other features to incrementally sync a large client view in chunks and/or to sync only a subset of available data. The Repliear demo shows how to do this and we have some docs on that.

We should consider adding features to Replicache to make this first-class.','public'),
	 ('mGsF2VpVlZpLxNbdhI4FL','`makeScanResult()` helper returns impl relying on `InternalValue`',false,1677091443000,1657658482000,'jpRvILZ1tibPsQXKvb3cF',NULL,'The `makeScanResult()` helper for implementing custom backends returns a [`ScanResultImpl`](https://github.com/rocicorp/replicache-internal/blob/273bee0bdc9d0fd3744b872211a0b36372174cc6/src/scan-iterator.ts#L352), which now uses `InternalValue`.  This causes an error: `Error: Internal value expected` when the `.values()` method is called (eg. in the replicache-todo backend).

','public'),
	 ('66pxCrNqC19YHf2LdFOXS','RFE: A `deleteEverything()` API that deletes all Replicache IDBs',false,1677091444000,1657166229000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We get requests like this quite frequently: https://discord.com/channels/830183651022471199/830183651022471202/994362299764707357

Replicache maintains a `replicache_dbs_v0` database that lists all of our databases. There is presumably internal API somewhere that knows how to read this database-of-databases.

<img width="471" alt="Screen Shot 2022-07-06 at 5 54 36 PM" src="https://user-images.githubusercontent.com/80388/177687216-d28c4b3e-698c-41b7-97f2-124342f1a8ac.png">

We should add a global function called `async deleteAllReplicacheData()` that drops each one of the recorded databases, then drops the database-of-databases too.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('zrpBifgcCO7g1DjYQ1v9A','Rails "done done"',false,1677091445000,1657087314000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'To steal a phrase from @phritz:

- [ ] Update Replidraw to use
- [ ] Update ReplidrawDO to use
- [ ] Update Repliear to use
- [ ] Rename to something else... replicache-crud?','public'),
	 ('jC2ZSB7TCGAhDeK_CTNuj','Maybe call this test `crossClientGetMutationID` or something to make more clear why it only runs under DD31.',false,1677091452000,1656407388000,'nqYkxAGMnzk7Y5STjZryV',NULL,'A comment would also help.

_Originally posted by @aboodman in https://github.com/rocicorp/replicache-internal/pull/177#discussion_r906419411_','public'),
	 ('bbBCilPYtx_FfumPLQRqG','Why not adjust these tests to have a dag.Read. Even if most of the commit types / tests don''t need it, some do.',false,1677091453000,1656407353000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Would be easier to read the test if you didn''t have to understand why it''s OK to pass null here.

_Originally posted by @aboodman in https://github.com/rocicorp/replicache-internal/pull/177#discussion_r906418791_','public'),
	 ('0aANjS4hkfgYM6jyk0bnz','DD 3.1',false,1709536712000,1655804122000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Tracking bug for https://www.notion.so/replicache/DD-3-1-e42489fc2e6b4340a01c7fa0de353a30

### Infrastructure

The code will live on the `main` branch.

We will introduce a global flag called DD31 which will be `false` by default.

There are some public APIs that will need changes. During the transition period we will duplicate them and name them with DD31 in their names. We will however not export these from `src/mod.ts` so they will not show up in the npm package.

For testing we can run the tests twice, once with DD31 true and once with false.

- [x] Have esbuild strip this code
- [x] Setup the test runner to run twice.
- [ ] ~Run perf tests on both?~ 

### perdag

TBD

Verify that we have these three heads and no other heads

- [x] `mainBranches`
- [x] `clients`

### memdag

memdag has one new head:
- [x] `refresh`

### Clients

- [x] Add `tempRefreshHash?: Hash;`, including correctly managing refs on the ClientMap chunk for these hashes
- [x] Add `mainBranchID: BranchID;`
- [x] simplify updateClients once we go back to synchronous hashing (or uuids), today it uses a test and set approach to avoid async hashing in idb transaction

### Branches
- [x] Create Branch helpers analogous to Client helpers.   getBranch, getBranches, setBranch, setBranches.  
- [x] Ensure that the chunk that contains the BranchMap has refs to each Branchs''  headHash.

### Commits

- [x] Add `clientID` to `LocalMeta`
- [x] Change `SnapshotMeta` to use a map of last mutation IDS: `+ readonly lastMutationIDs: Map<ClientID, number>;`
- [ ] rocicorp/mono#64
- [x] Implement `getLastMutationIDForClient(clientID)` that walks the commit chain as needed. (64bfc27dbabedff31a6b8e977360850b5d720088) end up just calling this getMutationID(clientID)
- [x] Implement getNextMutationID on top of getLastMutationIDForClient(clientID) (64bfc27dbabedff31a6b8e977360850b5d720088)
- [x] rocicorp/mono#39


### Startup

- [x] Bootstrap from `main` branch with identical mutator names and indexes if one exist.
- [x] Otherwise, if there are existing `main` branches,  chose `main` branch with newest cookie and fork from it''s base snapshot (fixing up indexes as necessary and clearing last mutation ids).     
- [x] If no main branches exist, simply create a new `main` branch with an empty base snapshot.

### Persist

- [x] Implement `gatherLocalCommitsGreaterThan` (4c5dec18a2c5c36b01561f00dfafa01a5a19dcb1)
- [x] Implement `getLastMutationIDForClient(clientID)` that walks the commit chain as needed. (64bfc27dbabedff31a6b8e977360850b5d720088)
- [x] Implement/refactor `rebase`. Our code is not structured like that at the moment. We have `Replicache.p._maybeEndPull` and `sync maybeEndPull` which deals with this logic. (65557c14fc4ada6b2c3f2209b050998f601c36be)
- [x] Implement persist algorithm https://www.notion.so/replicache/DD-3-1-e42489fc2e6b4340a01c7fa0de353a30#92b4b58d07d14e9296ba8b975e312130 (ecbe9f1b8ff0f13c78a28578acb4e81ee8652ff6)

### Hashing and temp hashes
- [x] replace perdag async hashing with sync hashing (either wasm or uuid)
- [x] possibly replace memdag temp hashing with uuids, in which case need to add a new way to identify memdag only chunks for copying to perdag as part of persist

### Refresh

- [x] Implement [algorithm](https://www.notion.so/replicache/DD-3-1-e42489fc2e6b4340a01c7fa0de353a30#ecdb40894ff442c2ba7623600b63e3f1)
- [x] Figure out interval to run on, probably want to run after running persist to increase chance of base snapshot in perdag main branch >= then memdag main branch (59ab943174bdd9a94e58faba8a3c85a4e1cf4be7)

### Push

Updates the protocol slightly

- [x] rocicorp/mono#144
- [x] rocicorp/mono#145

### Pull

Pull now has to handle rebasing interleave mutations from multiple clients.
- [x] `gatherPendingMutations`
- [x] `getBaseSnapshot`
- [x] Update `pullVersion`
- [x] Include `branchID` in request
- [x] Add `isNewBranch` and set to true if previous snapshot has last mutation ids
- [x] Introduce new response field: lastMutationIDChanges = all last mutation ids from clients in branchID that changed between PullRequest.cookie and PullResponseOK.cookie
- [x] Construct new base snapshot lastMutationIDs from previous base snapshots lastMutationIDs and responses lastMutationIDChanges.
- [x] Remove `clientID` from `PullRequestDD31`
- [x] Check that pull response''s cookie >= than cookie sent in pull request and ensure error is logged otherwise (68b5673114c247d5463d45886e2c6b16f5a4e746)
- [x] Check that pull response''s cookie > than current snapshot cookie and discard pull otherwise (this can happenw hen refresh happens during pull) (68b5673114c247d5463d45886e2c6b16f5a4e746)


### Heartbeats

- [x] No need to update on persist

### Collecting Client-Side Client State

- [x] Implement collection of branches from BranchMap (063834e12fd84b6cfb714994399728324667862d
- [x] Ensure idb is not gc''d until all client groups are gc''d 
 
### Collecting Server-Side Sync State And Handling Missing Server-Side Sync State
- [x] On ClientStateNotFound server response disable client group and log error

### Indexes

- [x] Add `indexes?: Record<string, IndexDef>` to ReplicacheOptions.
- [x] Remove `Replicache.createIndex` and `Replicache.dropIndex`.
- [x] Correctly setup indexes as part of startup
- [ ] Eliminate IndexChangeMeta commits

### Mutation recovery

- [x] No longer recover from other clients on the same main branch in the same database
- [x] Update to recover branch at a time instead of client at a time.  We recover all of the branch''s clients'' interleaved mutations in a single push.  Client.mutationId and Client.lastServerAckdMutationID become Branch.mutationIDs and Branch.lastServerAckdMutationIDs.
- [x] Keep code for recovering client by client for now in order to recover from pre DD3.1 Replicache format (can remove in future).
- [x] Test that we can recover from pre DD3.1 Replicache format dbs 
- [x] Recover from other branches in same idb database, and from branches in other idb databases with the same Replicache name (and still supported Replicache format verison).

### User-Level Versioning and onNewVersion
See: https://www.notion.so/DD-3-1-e42489fc2e6b4340a01c7fa0de353a30?d=c966ca11d3574237a04e9832a0898039#1b0ff4c4034846febda91af009dfd84a
and
https://www.notion.so/replicache/Version-incompatibilities-528f68f02f9c43f2904cdfef1e35e9c3
- [x] Expose event for informing user when a new branch for the same replicache name has been created so they can aggressively reload (by default we should just reload the page).
- [ ] Document on user versioning (previous attempt https://github.com/rocicorp/replicache-internal/pull/12)

### experimentalPendingMutations
- [x] rocicorp/mono#38

### LazyStore

- [x] See if we need to rework GC.  There is some risk that refresh will thrash the LazyStore chunk cache with how GC currently work (but possibly not, as running diff as part of refresh may establish enough refs in the cache as to avoid GCing things that are actively in use)  (4378df2f13176e9065e10021fbd110c97411e25d, dff9c8d8e0c222b3051e7080333f87f3b376211b)

### Update sample apps
- [ ] todo
- [ ] replidraw
  - [ ] Make sure we support v0 and v1 of pull/push and manually test mutation recovery
- [ ] repliear
- [ ] others?

### Offline limits testing
 - [ ] what is first sync perf after being offline for multiple weeks
   - [ ] Time slice rebase
 - [ ] is there anything we can do to avoid rebasing all the offline mutations during first sync while back online
 - [x] one known issue is refresh and persist need basesnapshot cookie, and currently have to walk history to get it


## Reflect 
See https://www.notion.so/replicache/DD-3-1-e42489fc2e6b4340a01c7fa0de353a30#9629bd3b9f604aa18dd0072442eaded2
','public'),
	 ('KUuDk9aUk-QlE5TL2KFn_','When server-side data is deleted things go wrong fast',true,1689319383000,1655690311000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'# Problem

We already have a special [ClientStateNotFound](https://github.com/rocicorp/replicache-internal/blob/main/src/puller.ts#L31) error when the server-side database exists but the requesting client''s state has been deleted. This causes Replicache to refresh, assigning a new client.

But what happens if the entire *database* is deleted? This comes up a lot during development, when e.g., a dev might delete entire server database storage.

We don''t have any concept of a "database" in the protocol directly, but in practice applications typically do. For example, all our sample apps use the concept of a *space* which is in the ID. 

What happens when a space gets deleted on the server side? This is not well defined by our protocol, and it practice,  e.g., in replicache-todo, it''s complex:

* For any pulls that happen before the space is re-created, it''s basically a no-op (the lmid defaults to zero and server returns empty patch)
* The next push from any client recreates the space with cookie:0
* All existing clients still have their old data, but don''t get updates initially because they are at cookie X but the server reset to cookie zero. They also can''t send new data because their lmid reset to zero, but they are at lmid > 0.
* New clients from an existing cache fork from existing clients. This means that they inherit that old client''s cookie! So they can''t get updates either! (for awhile, see below)
* But, the new client is at lmid:0, so they start sending pushes which are accepted. These pushes are accepted by the server, but not acknowledged (local cache is at cookie: X>0, server is at 0, so the pull is empty). Thus the local mutations are rolled back. For awhile.
* After enough pushes, the server''s cookie reaches X again and ''catches up'' with the client. After this, pushes from this cache begin being accepted again and the situation resolves.

Note that `ClientStateNotFound` doesn''t solve this issue. If the server returns that, it will cause existing clients to refresh, which just creates a new client forked from the old one.

# Proposal

Introduce a new `ServerNotFound` error which causes the client to delete the entire cache and reload. 

This can be done as a backward-compatible extension of the protocol, albeit a little ugly:

1. Define a `onServerNotFound` callback similar to the existing `onClientStateNotFound` callback. Default this to:
  * delete local cache (entire idb)
  * reload 
2. Extend the current pull response to also allow a new `ServerNotFoundResponse` with `{error: "ServerNotFound"}`. Wire this to the new `onServerNotFound` callback.
3. Currently the response to the push endpoint is ignored. Add an optional `application/json` response. If the response is JSON, parse it. Allow both `ClientStateNotFound` and `ServerNotFound` errors for the push handler and wire them to the appropriate callbacks.
','public'),
	 ('1Yh8Tv8C5s5i_qEfPVOu_','Recovering from a completely missing database',false,1677091446000,1654815577000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Right now if you delete the backend database that Replicache is talking to completely, the client does something ... weird.

For example on the replicache-todo app, if the server is deleted while the client is running:

- push will correctly see the client as new and skip the mutations because they don''t start at correct mutation ID
- pull will either return an empty patch (correctly) or if the server returns ClientNotFound then the client will reload

But the interesting part happens on reload. There''s now a new client ID.

- push will start processing mutations. The new client is correctly starting at clientID zero. A new space will be allocated.
- pull will not succeed initially (return no patch) because request cookie is higher than server cookie. But once enough mutations get sent it will start working. This means that the client will be permanently out of sync with the server as it has state from before the server rebooted that never gets taken away.

I think we need to have an explicit error code that means "the database you are trying to talk to doesn''t exist anymore, give up all hope, delete everything".

But probably also we should wait until the new offline-first stuff is designed as it might affect the way client IDs are allocated (or might not).','public'),
	 ('p6KCIi4r8zu6zV7FWa0cV','zero-dependency starter app',false,1677091447000,1654778840000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'I removed the docker/supabase goop from the starter app, because it was buggy and confusing. But now it''s even harder to install because (a) there is a bigger chance of screwing up the postgres install and (b) you need a pusher account.

We need to do something about this. We need it to be one step to get started.

I looked into knex and was really hopeful. I got it working for postgres. But knex is not really a SQL abstraction, so many things break between it and SQLite. Example: SQLite doesn''t support boolean or date columns. It just seems really brittle.

Maybe the better thing is what @cesara suggested -- use pgmem. I discarded this in favor of knex because I was thinking it would be hard to test offline with an in-memory database (because can''t kill it and restart it). But I forgot -- you could kill it by pausing the process with ctrl+z!.

So I''m thinking the right path is maybe pgmem + ... something ... for poke. Maybe the something is server-sent events? I''m a little worried about doing our own websocket thing because I want to keep the complexity down in this special path.','public'),
	 ('uAksUxt6JD6JYgCPFvUo_','doc: Document ClientNotFound error',false,1677091478000,1654713850000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'It’s not currently on the PullResponse docs.

Also double-check that we’re doing right thing for push? ','public'),
	 ('mVgbZcfCvMzf0YIqg1Y0m','"Internal value expected" in replidraw',false,1677091478000,1654063892000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Running replidraw against current trunk, I see this error when I open the second tab for a URL:

<img width="508" alt="Screen Shot 2022-05-31 at 8 11 24 PM" src="https://user-images.githubusercontent.com/80388/171339164-cd686ce7-42ac-4dc6-a69e-54cbef7fd699.png">

','public'),
	 ('DICKK8bsykenS1luaM5bR','Ship watch()',false,1677091484000,1653699425000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'# Background

https://www.notion.so/replicache/half-idea-for-productionizing-watch-cf3110a59db446a59848ea40f48b799b

# Problem

`watch` was an interesting experiment (https://doc.replicache.dev/api/classes/Replicache#experimentalwatch) and has produced large value in several of our customers'' projects, plus repliear. However, it also introduces undesirable complexity. Every app that uses it needs to implement the same pattern of applying the watch output to maintain a view. In repliear: https://github.com/rocicorp/repliear/blob/main/frontend/app.tsx#L313. In placemark: https://gist.github.com/tmcw/7d38380e0e2dee41ff6df18942742f65. Finally, `watch()` duplicates a subset of `scan()` but not all of it. It''s non-DRY.

# Proposal

What if we extend `ScanResult` with a `watch()` method? This way we immediately solve the duplicate interface problem. `ScanResult.watch()` then returns a stream of diff operations matching the params immediately passed to scan.

We can eliminate the view-maintenance code in React apps by a `useScan()` helper to `replicache-react` which calls `watch()` and does the view-maintenance internally. The `useScan()` hook would re-fire whenever its contents change in some way, but the identity of the values within the array would stay unchanged to facilitate use with `memo()`.

It may also be useful to add a `collect()` helper to ScanResult which does this view maintenance if other frameworks want it (needs investigation).

# Search Helpers

The above proposal only helps when the UI is directly displaying a scan, not filtering it or manipulating it in any way. In Repliear, for example, this wouldn''t completely solve our problem because of the complex filters.

However, by adding several more helper methods to `ScanResult` we can replace all the view management code in Repliear:

* `ScanResult::filter(f: ([k: string, v: ReadonlyJSONValue]) => boolean) => ScanResult`
* `ScanResult::sort(f: ([k1: string, v1: ReadonlyJSONValue], [k2: string, v2: ReadonlyJSONValue]) => number)`
* (maybe, not sure if there''s a need) `ScanResult::map(f: (k: string, v: ReadonlyJSONValue) => ReadonlyJSONValue) => ScanResult`

These method can be chained together so that in total you can say things like:

```ts
const watch = rep.scan({prefix: "foo", limit: 40, startAt: "fp"})
    .filter((k, v) => v.bar > v.baz)
    .sort(([v1],[v2]) => v1.size - v2.size)
    .map((,v) => v)
    .watch(diffOp => doSomething) // or toArray to just get results once!
```

The neat thing is that each of filter, sort, and map can be implemented incrementally so that the cost for updates after the first result is very low.

But the *really* cool thing is that you can then do in React:

```ts
const data = useWatch(scanResult)
```

... and the hook will re-fire whenever the data changes. The result will be an array, with only the values changed inside that changed. 

# Performance

A naive implementation of this would re-scan for each watch whenever the underlying data changes. But a better impl can re-use a single scan to update all watches.

# Impact

I believe that we could probably deprecate subscribe and replace it with this API. I am not familiar with anyone using subscribe for anything beyond what this API does, and this is much more efficient and easier way to do it. This would also enable frameworks like Solid which want to get access to the underlying values as they change so they can feed them into their own dependency tracking system.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('0daDDPiMlexz7joLYgRIN','Expose pendingMutations API to reflect once available in replicache',false,1677094809000,1653421121000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'Replicache issue tracking this work: https://github.com/rocicorp/replicache/issues/490','public'),
	 ('gl0rUV1-DcgaowRvLpoEO','onOnlineChange api',false,1677094809000,1653420938000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'Monday.com feature request.

Way to be informed when connection status changes.  Replicache has an onOnlineChange api for this purpose https://doc.replicache.dev/api/classes/Replicache#ononlinechange, need to adapt to socket connection impl of reflect.

This api would be added to the client Reflect class: https://github.com/rocicorp/reflect/blob/main/src/client/reflect.ts.   The implementation will also likely be in that file based on the existing  `private _state: ConnectionState`.   ','public'),
	 ('Cl9nAIiqlhr4IA1QdJz1Y','Can we replace DO stoarge with D1 (SQL db)',false,1677094810000,1652991575000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'Evaluate if this will meet our goals and provide customer''s with better visibility / tooling for their data store (i.e. it is currently very hard to see what is in your DO storage).

https://blog.cloudflare.com/introducing-d1/','public'),
	 ('KolhFMo131bi8uFmKPXxP','Potentially move replidraw-do to Cloudflare Pages instead of vercel',false,1677094810000,1652991426000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'Simplifying the sample to a single cloud provider.  Need to make sure Cloudflare Pages meets our needs.

https://developers.cloudflare.com/pages','public'),
	 ('bIWKSeko2WHqv9swUiII4','Skip computing diffs when not needed',false,1677091484000,1652901090000,'nqYkxAGMnzk7Y5STjZryV',NULL,'We know whether there are index scans or not and if there is no matching index scan we do not need to generate the "fake" diff.

Maybe we can find other optimizations here and skip other diff computations.','public'),
	 ('1jd8fZpOsSj40eUiePVUC','Cannot upgrade replidraw-do to reflect 0.5.0',false,1677094816000,1652794343000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'When I try, and put two windows side-by-side, the source window does not respond to drag -- though the destination window shows the drag happening.','public'),
	 ('G7LZnogDhN6C7K3fcdpYk','Upgrade to wrangler 2',false,1677094817000,1652793175000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Cloudflare released a new wrangler version, v2, which is at `wrangler` not `@cloudflare/wrangler`. I can''t easily get it to work with this code, when I try to run `npx wrangler dev` the application can''t connect to the socket on localhost. However connecting to the published version seems to work.
','public'),
	 ('8mw0qcXR-xQt4XVuJsLUB','show clear error message at startup if Replicache is being run in a non-secure context',false,1677091483000,1652461662000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'Replicache depends on crypto.subtle for hashing.  crypto.subtle is only available in secure contexts.  

If Replicache is run in a non-secure context (e.g. an http page), the following unhelpful error occurs

"Uncaught (in promise) TypeError: Cannot read properties of undefined (reading ''digest'')"
![image](https://user-images.githubusercontent.com/19158916/168333795-2b74f261-634f-4d0d-bf5c-eaeea7aeb94f.png)

Replicache should fail fast and with a helpful error message.','public'),
	 ('_9qutFdi5zQ81jObeaLYb','Add keywords to package.json',true,1677090553000,1652362945000,'nqYkxAGMnzk7Y5STjZryV',NULL,'https://www.skypack.dev/view/replicache shows missing keywords... We should add them to get better npm ranking/seo?','public'),
	 ('s6kgd_3FLqz583vhh1PUp','Do we need to prevent persist during poke?',false,1677091485000,1652120617000,'nqYkxAGMnzk7Y5STjZryV',NULL,'This does not use the lock on poke yet. Filing an issue to review the situation there.

_Originally posted by @arv in https://github.com/rocicorp/replicache-internal/issues/95#issuecomment-1121430364_','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('iCzgBp712q79Xv8lMu4J7','Docs: timestamp is not in the API docs',true,1690343278000,1652090575000,'nqYkxAGMnzk7Y5STjZryV',NULL,'We added a `timestamp` to `Mutation` which is part of `PushRequest`. However, `timestamp` is not showing up anywhere in the docs.','public'),
	 ('vs5FPtr5lLosii1jETfQ4','missing docs',false,1677704820000,1651860216000,'yJ5hiysWE-LBcDfT44lR8',NULL,'In rough order of priority:
- [ ] what replicache is / not good for
- [ ] maybe? replicache benefits for product owners
- [ ] persistence and startup
- [ ] integration guide update/overhaul
- [ ] auth (and mult-user https://github.com/rocicorp/replicache-internal/issues/66)
- [ ] we need a better discussion of diffs in https://trunk.doc.replicache.dev/server-pull, doesn''t have to be the full diff strategies doc
- [ ] paging data https://github.com/rocicorp/mono/issues/24
- [ ] shared mutators
- [ ] server requirements https://github.com/rocicorp/mono/issues/99
- [ ] isolation level https://github.com/rocicorp/mono/issues/96 (maybe goes into server requirements)
- [ ] versioning: https://github.com/rocicorp/replicache-internal/issues/83
- [ ] logging
- [ ] error handling https://github.com/rocicorp/mono/issues/100 
- [ ] garbage collecting old data https://github.com/rocicorp/mono/issues/97
- [ ] diff strategies: https://github.com/rocicorp/mono/issues/101

Other notes/feedback we have received:
- should probably have a top-level conflict resolution thing so it''s easy to find
- maybe should have a ''replicache mental model'' bit which crisply restates important elements of how replicache works for people already familiar with it (*without* explaining it). eg ''operations are memory fast''.','public'),
	 ('YRaA8okCX62BXlwP6d8OO','Update to the latest beta of replicache.',false,1677094811000,1651774383000,'nqYkxAGMnzk7Y5STjZryV',NULL,'we should be able to update to the latest beta of replicache.

_Originally posted by @grgbkr in https://github.com/rocicorp/reflect-server/pull/120#discussion_r866132680_','public'),
	 ('ztJ4x5Pq6dt4AiXOHxgg4','RFE: Make pusher/puller interface lower level',false,1677091483000,1651739499000,'nqYkxAGMnzk7Y5STjZryV',NULL,'https://github.com/rocicorp/replicache/issues/575','public'),
	 ('4TlAl4BdJNAqtR6rjI-_q','useSuspense in replicache-react',true,1677091063000,1651738861000,'nqYkxAGMnzk7Y5STjZryV',NULL,'https://github.com/rocicorp/replicache/issues/878

https://github.com/rocicorp/replicache-react/issues/19','public'),
	 ('KqfzmQv1M1IHv4MlxHQJ3','Test performance benefit of removing copies in read transactions',false,1677091482000,1651738795000,'nqYkxAGMnzk7Y5STjZryV',NULL,'https://github.com/rocicorp/replicache/issues/885','public'),
	 ('2nZ6_q6TAe1h4pQadnjzG','Doc: Add "Diff Strategies" section to recipes',false,1709536758000,1651738735000,'nqYkxAGMnzk7Y5STjZryV',NULL,'https://github.com/rocicorp/replicache/issues/567','public'),
	 ('MS_ADA0IqX5cz12roUDB0','I think this is missing handling for indexes that are in basisIndexes but missing from this.indexes.',false,1677091481000,1651693670000,'nqYkxAGMnzk7Y5STjZryV',NULL,'rocicorp/mono#106

I think this is missing handling for indexes that are in basisIndexes but missing from this.indexes.

Originally posted by @grgbkr in https://github.com/rocicorp/replicache/pull/974#discussion_r854478544','public'),
	 ('8ptyVwdA5xue16kPC0cUl','B+Tree key ordering',false,1677091486000,1651661645000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Right now we use `<` of `string` keys for B+Tree ordering.

For indexes our keys are `\u0000${secondary}\u0000${primary}` where `secondary` and `primary` are `string`s and `secondary` may not contain `\u0000`.

ECMAScript defines **IsLessThan** as a comparison if the code units (16 bits). https://tc39.es/ecma262/multipage/abstract-operations.html#sec-islessthan

The problem with this is that on backends we might not want to use 16 bit strings.','public'),
	 ('MkEeAADAlOWv2sKgvQcoB','Need to export some of Replicache''s interfaces',false,1677094811000,1651610358000,'nqYkxAGMnzk7Y5STjZryV',NULL,'`@rocicorp/reflect-server` bundles a copy of `replicache` but it does not export things from `replicache`. For example `WriteTransaction` is not exported and `WriteTransaction` references concrete classes (such as `AsyncIterableIteratorToArrayWrapper`). This means that the types are not compatible.

1. `@rocicorp/reflect-server` should export select types from replicache (not everything 😢)
2. Remove concrete classes because they are not compatible.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('cmNezw-IwDiR4s7r6Iv0k','Cleanup: Get rid of VERSION  file',false,1677091486000,1651604298000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Let''s use the version in package.json instead of a dedicated file.','public'),
	 ('bnsnliVDixMU36BmevLrZ','Add `allowNull` to `CreateIndexOptions`',false,1677091487000,1651389331000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'See: https://github.com/rocicorp/replicache/issues/913.','public'),
	 ('mtxFCKynw84ZemGw1t4rl','include rep version in license active ping',true,1677091063000,1651268133000,'yJ5hiysWE-LBcDfT44lR8',NULL,'Depends on https://github.com/rocicorp/replicache/issues/845','public'),
	 ('qdDqYR6kNrdt3fmGyAuWS','time the TEST_LICENSE_KEY out after 5m, triggering kill switch',false,1677091488000,1651267852000,'yJ5hiysWE-LBcDfT44lR8',NULL,NULL,'public'),
	 ('KyQlqsKdfuk_CC6IeO4mT','we should move perf tests to a different ec2 instance type',true,1677091064000,1651183672000,'yJ5hiysWE-LBcDfT44lR8',NULL,'We''re currently using t2.xlarge which has burstable CPU, meaning that it might (or might not!) get full cpu access when doing CPU intensive operations. We should probably move to something that has fixed cpu allocation. (I''m not sure how to get unvirtualized cpu in ec2, but if that''s an option then great, but if not, an m or c type might be best). Worth checking what cpu options we can specify.
','public'),
	 ('Wwpj_ocWQadyE_Fa0moV9','Recursion / infinite loop bug in "delete"',true,1677091065000,1651049044000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Internal bug for https://github.com/rocicorp/replicache/issues/985

```ts
// lazy-store.ts

  delete(cacheEntry: CacheEntry): void {
    const {hash} = cacheEntry.chunk;
    this._size -= cacheEntry.size;
    this._cacheEntries.delete(hash);
    cacheEntry.chunk.meta.forEach(refHash => {
      const oldCount = this._refCounts.get(refHash);
      assertNotUndefined(oldCount);
      assert(oldCount > 0);
      const newCount = oldCount - 1;
      if (newCount === 0) {
        this._refCounts.delete(refHash);
        const refCacheEntry = this._cacheEntries.get(refHash);
        if (refCacheEntry) {
          this.delete(refCacheEntry); // XXX here is the iloop! 
        }
      } else {
        this._refCounts.set(refHash, newCount);
      }
    });
  }
```

It looks like this can only happen if a Chunk references itself but that should not happen.

Let''s add some asserts in here as well as in when we create the meta array.

I''ll give Tom a special build with these asserts

@grgbkr FYI','public'),
	 ('jZMXbzDw01_CmXMZ3vM0z','Split perf-v2 bot into 2',false,1677091488000,1650981842000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Split the current perf benchmarks into two (on GH actions)

- One without the p95 runs which warns/errors on regressions
- One with the p95 runs which does not warns/errors on regressions
- The dash board will display both like today. It can just merge the JSON or run it the function over both json files.','public'),
	 ('slf7fb_6GOoSn-oma8F0y','Fix package.json license field',false,1677091489000,1650794875000,'nqYkxAGMnzk7Y5STjZryV',NULL,'This is not valid. There is a validation of these done by some tools.

We should do this:

```
{
  "license" : "SEE LICENSE IN <filename>"
}
```

@aboodman @grgbkr @phritz

_Originally posted by @arv in https://github.com/rocicorp/replicache-internal/pull/1#discussion_r857101588_','public'),
	 ('gckqvrz5Xcx9AzWO7nYwy','test issue',false,1677091489000,1650665082000,'OeVnr1y5bEM_Yg06sUFtD',NULL,NULL,'public'),
	 ('VJrT_lsZaglNI0YzlL8Qa','perf test output seems busted for the last ~week',false,1677091490000,1650598645000,'yJ5hiysWE-LBcDfT44lR8',NULL,'So I got the bundle and perf runners running in replicache-internal. You can see the bundle size check [run successfully](https://github.com/rocicorp/replicache-internal/actions/runs/2205455219) and see the data point for the recent commit `1c6460f` [in the graph](https://rocicorp.github.io/replicache-internal/bundle-sizes/). 

However for the perf test the [run ran successfully and detected a "regression"](https://github.com/rocicorp/replicache-internal/actions/runs/2205455218) and I got an email about it. But there was no data point for `1c6460f` appended to the [graph](https://rocicorp.github.io/replicache-internal/perf-v2/). The last data point is for `558d93c` which was a week ago. I thought this was a problem with replicache-internal''s setup, but it looks like [replicache''s perf graph has a similar problem](https://rocicorp.github.io/replicache/perf-v2/) -- the last commit there is `59e8869`. 

I doubt that it is coincidence that the next commit is https://github.com/rocicorp/replicache/commit/495d9b7f48e6ba67ff2cfdcecd7777477a23dcf1 which makes changes to the perf test. We don''t see any more perf test data points after that commit. 

I wonder if something changed that now when a regression is detected a perf data point is not appended because the run appears to have "failed"? Like maybe the exit code changed or something? ','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('MVNshVElWxV4obDWwg_E4','I think this is missing handling for indexes that are in basisIndexes but missing from this.indexes.',false,1677091481000,1650484834000,'nqYkxAGMnzk7Y5STjZryV',NULL,'I think this is missing handling for indexes that are in basisIndexes but missing from this.indexes.

_Originally posted by @grgbkr in https://github.com/rocicorp/replicache/pull/974#discussion_r854478544_','public'),
	 ('oZa3voWumsi0nsdzOoXHn','Add engines field to package.json',false,1677094769000,1648201762000,'nqYkxAGMnzk7Y5STjZryV',NULL,'To prevent nodejs versions and that trigger this miniflare bug:

https://github.com/cloudflare/miniflare/issues/215

Remove when miniflare releases a new version.','public'),
	 ('gA9A9pgljdEh6_hv-cUov','Preview URLs',true,1677093054000,1647910333000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Once we''ve move to CF pages (#151 ) we could look into getting vercel-style preview URLs for PRs.

I asked on the Cloudflare Discord and (surprisingly) got a helpful response:

https://rocicorp.slack.com/archives/G013XFG80JC/p1647906508423839

There are at least two ways we could do this:

1. Use cloudflare''s "pages" product, which seems to have this built-in.
2. Generate new named "environments" at deploy time: https://developers.cloudflare.com/workers/platform/environments/
','public'),
	 ('L3U1O8LZ9aHznzFxgKe5u','Send client-side logs to datadog too',false,1677094769000,1647909438000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'See also: https://github.com/rocicorp/reflect-client/issues/12','public'),
	 ('iTorjUy7-7eZ3s2eLSrJI','---- Prioritized Customer Requests Above This Line ----',false,1677093602000,1647909156000,'OeVnr1y5bEM_Yg06sUFtD',NULL,NULL,'public'),
	 ('9Xg_d8fLu0FJk_3_WOkBb','`getLogLevel` -> `logLevel`',false,1677094770000,1647672065000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'There is no need for this parameter to be a function, because it does not need to change over the lifetime of a worker. Changing it to a plain string reduces boilerplate for customer and also enables simpler implementation strategies inside our own code.','public'),
	 ('G3Y8HOtttXVHq9lDqCO9R','Make console logger always enabled',false,1677094770000,1647671937000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'It takes four imports and a surprising amount of code to enable the DataDogLogger and the ConsoleLogger, something I think will be the most common configuration.

I''m struggling to imagine a case where one would not want console logging enabled. Can we force it to always be enabled and do the teeing internally so that the boilerplate required by customers can be reduced?','public'),
	 ('NnQSsduxnjkJa-3xMjLqA','Investigate whether Cloudflare LogPull contains info on worker crashes',false,1677094771000,1647373539000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We currently log from inside a worker using datadog''s rest api. However this doesn''t allow us to capture information about workers themselves crashing, e.g., due to sending > 1mb of push data.

We should investigate whether this information about worker crashes is accessible via Cloudflare LogPull. If it is, this could be a good debugging aide in the future (and should be added to the recipes section of this repo''s README).

If it''s not then we should feature request to Cloudflare about adding LogPull and LogPush support to workers.','public'),
	 ('hVF5Qjgb4NtorcRApYFf3','Print roomID into log at server startup',false,1677094771000,1647372670000,'OeVnr1y5bEM_Yg06sUFtD',NULL,NULL,'public'),
	 ('ymXYscvvL17Z-gPhreFrJ','Print roomID in connect handler',false,1677094772000,1647372600000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Here: https://github.com/rocicorp/reflect/blob/main/src/server/connect.ts#L61

This would help with debugging.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('yEFLQ6xLl-mn_IT5cSg3b','Accept an optional logger as a constructor argument',false,1677702981000,1647372464000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'This way clients can arrange to send client-side logs to e.g., datadog','public'),
	 ('C5xZ7-mdn6HvNY0DdaUpb','Another data loss report from Monday',false,1677094772000,1647076885000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'<img width="960" alt="Screen Shot 2022-03-12 at 1 18 00 AM" src="https://user-images.githubusercontent.com/80388/158012084-af52d8a6-948c-4ee9-acd6-2889dc7c37e5.png">

Noam said the customer did 25m of work which was lost on refresh.

This room ID doesn''t appear in the logs at all, and I don''t see anything abnormal in the logs from that period.

I asked Noam to check whether this room has any data on the server-side, haven''t heard back yet.

It''s not reproducible from what I understand. Still, this is a pretty awful bug for a customer to experience -- silently losing a big chunk of customer data.

Ideas, prioritized:
- We should capture client-side logs and send to datadog (https://docs.datadoghq.com/logs/log_collection/javascript/). Maybe there''s a clue in there.
- I think we are not logging enough information in `info` mode. For example, we should print the room ID on startup of the DO and also include the room ID in the connection request log.
- We should test ourselves whether [LogPull](https://developers.cloudflare.com/logs/logpull/) contains information about workers and durable objects for our instance. I don''t have clear info on whether or whether it should not according to the docs. If it does then maybe it will have information about platform-level exceptions. If it does *not* contain such information then we should put in a feature request with Cloudflare.
- Perhaps we should prioritize rocicorp/mono#310 more highly. I had originally thought that `onDisconnect` would be based on socket state, but maybe we should do a periodic mutation and check that it is applied and synced? That would be more foolproof and is what we were proposing Noam do but maybe we should just include it in Reflect?

Not sure what else we can do. Ideas?','public'),
	 ('AMwS1QGlGdcD-CUHX4aDt','Licensing done done',true,1690343109000,1646950633000,'yJ5hiysWE-LBcDfT44lR8',NULL,'Original issue rocicorp/mono#133

### Strictly Required to launch
- [x] Replicache
   - [x] add browser profile id and include it in active ping, write it to db 
   - [x] use TEST_LICENSE_KEY in tests
   - [x] complain loudly if they don''t pass a key in
   - [x] complain loudly if license is invalid 
   - [x] client should take a logger from Replicache and we should ensure we are logging appropriately for licensing calls
   - [x] enable licensing functionality by default
 - [x] ToS and privacy policy
   - [x] finished
   - [x] posted to web site
   - [x] linked to from get-license
   - [x] pricing is up to date on web site and linked to from get-license
- [x] collect remaining information needed in get-license: commercial, agree to ToS, email, marketing opt out, etc. and store it in the customer table
- [x] ensure website / docs explains licensing, TEST_LICENSE_KEY, etc.
- [x] db
   - [x] compact migrations, reset db
   - [x] ensure db backup policy makes sense & do a restore so we know how it works
- [x] productionization
  - [x] licensing server has a level logger
  - [x] licensing server log collection to datadog
  - [x] know the story for server rollout and rollback
  - [x] pre-prod staging environment (full clone of prod)
- [x] walk through & polish
  - [x] with aa
  - [x] ~with a customer or two~ not gonna do it
- [x] add at least a version bit to licensing api calls, maybe a little more structure for errors too?
- [x] get an answer to https://github.com/rocicorp/replicache/issues/909 and update code if necessary
- [x] ensure admin pages are robust against bad or garbage customer-supplied input
- [x] make sure i understand the forward/backward compatibility of the api types

### Needed for done done
- [ ] productionization
   - [ ] uniform error handling in server (and in client)
   - [ ] do some db migrations that require data (not just schema) migration (this assumes a pre-prod env)
   - [ ] do a db migration rollback for practice (same)
- [ ] Replicache
  - [x] implement kill switch and trigger it on invalid license
  - [ ] send version in active ping
  - [x] time TEST_LICENSE_KEY out after N minutes, triggering kill switch
  - [ ] add timeouts to licensing api calls
- [ ] store full datetime in active pings table instead of just date
- [ ] integration test 
- [ ] monitoring (add some minimal metrics)
- [ ] alerting
  - [ ] on errors
  - [ ] on any monitoring signals we think are useful
  - [ ] on flapping, crashes, or other signals we get from heroku
- [x] billing walk through with aaron and susan
  - [x] make any improvements/tweaks required
- [ ] move admin functionality to its own server for safety
- [ ] are we using heroku HA?
- [ ] split the client and server packages once the dust settles
- [x] upgrade sample apps to 10 and ensure the sample apps pass a key in (set in the env, not in the code)
- [ ] we probably want to prune the active table so that our backups don''t get linearly expensive. logical backups will start to fail according to heroku docs around 20GB.
- [x] (aboodman) outreach to existing customers telling them to upgrade, how to get a license, and TEST_LICENSE_KEY
','public'),
	 ('OF-X5p9YFMFBOXGZojNHw','------ PLAYABLE BETA ABOVE THIS LINE ------',false,1677093606000,1646772163000,'yJ5hiysWE-LBcDfT44lR8',NULL,NULL,'public'),
	 ('z47B8ust9TzFOX2qpjKtt','✂️✂️✂️ GA CUT LINE ✂️✂️✂️ 🛳🛳🛳 SHIP IT AFTER THIS LINE 🛳🛳🛳',false,1677704847000,1646770686000,'OeVnr1y5bEM_Yg06sUFtD',NULL,NULL,'public'),
	 ('VTkq5SJThomH_-HUdqHC9','Continuous unit testing',false,1677093607000,1646761594000,'OeVnr1y5bEM_Yg06sUFtD',NULL,':-/','public'),
	 ('boJ4amDiykt1QpBBZi4af','Website Refresh',false,1677091491000,1646707591000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We''ve learned so much from our users on the march to GA - everything from how to talk about the product to what the product should be. The website should get a new coat of paint.','public'),
	 ('doAMnYlz8vrRQsWxtY8DF','Replidraw: ✨click effects✨',false,1677093608000,1646707024000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'I would like to add water droplet like click effects to showcase 60fps.','public'),
	 ('_EIQUWiyG3kD9i7bsOoTV','Clean up Replidraw',true,1681243071000,1646706949000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Two cleanups I think we should do:

* We should use `clientID` from ReadTransaction, not pass it in. This code dates from before we had that feature of `ReadTransaction` and passing `clientID` all over the place is really messy.
* Generally the use of `useSubscribe` is just overdesigned. We should just have one subscribe at the top level like replicache-todo does and pass down raw data to the components. Use React''s `memo()` function to avoid re-renders when identity of input doesn''t change.

Somebody (aa, erik, cesar, greg) should do a pass of this code and make sure it''s a real nice example of using Reflect.','public'),
	 ('bUPHRFd58OwFTd73qpvqT','Implement a smarter diff strategy for fast-forward',true,1684383883000,1646706356000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'At connection, we "fast-forward" a client to the current state of the room. Currently this is done via brute force. Eventually we will need to do something smarter.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('yVj1zOD3d8Ac7AW0USItb','Replace replidraw with replidraw-do publicly',false,1677093608000,1646706257000,'OeVnr1y5bEM_Yg06sUFtD',NULL,NULL,'public'),
	 ('veg_CSOsSp-Kl1yhB11b0','Drive `late-mutation` and `late-poke` metrics both to 1:1000 on Replidraw (via manual testing)',false,1677093609000,1646706066000,'OeVnr1y5bEM_Yg06sUFtD',NULL,NULL,'public'),
	 ('zavlYe0tM2k8Cd1Hhe1oV','Add `late-poke` metric',false,1677093610000,1646705929000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We should have enough information in the poke to know whether we played it late. Add a metric to track this.','public'),
	 ('igeECdxpLjM57JF3NHlLT','Add `late-mutation` metric',false,1677093610000,1646705883000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We should have enough information (via source-provided timestamps) to know when we are processing a mutation late in the process loop. Add a metric to track when this happens.','public'),
	 ('0TotK1tuZFxFXthQ_WBbu','Programmatic access control on server-side',true,1677093056000,1646705730000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Expose `UserData` returned by `authHandler` to mutators. Optionally also allow providing this on client-side to do same thing optimistically.','public'),
	 ('2fVC187meiZ9ta39EpxRj','Nail down Perf Envelope',true,1677093057000,1646705499000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Reflect''s perf envelope for alpha:
* Rooms can be up to 25MB
* 50 concurrents in a room supported, each moving in 10% of frames
* 1:1000 frames dropped e2e

Then:

* Document envelope','public'),
	 ('ITE0vq4xtyRQcJCBsVCCc','Script in npm package to list room instances (and maybe delete?)',false,1677093498000,1646705410000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Thinking like:

```bash
npm run list-rooms ...
```','public'),
	 ('cnuWKNv7CiGiiiKF3hiaV','Recipe for dev/preview/prod',true,1677093065000,1646705368000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We should use Replidraw as a way to figure out how to do this and document it.','public'),
	 ('Ssw407hnzRxkq9qxjB4VI','Monitoring and Alerting',false,1677704948000,1646705323000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Theory: expand the existing logging interface and DataDog concrete impls we have.','public'),
	 ('aV6nM8f0Qr2Hc8A91q9CT','Wrap top-level entrypoints so that unexpected exceptions go to DataDog too',false,1677704986000,1646705287000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Right now, I believe that some unhandled exceptions will propagate to the console but won''t make it to DataDog.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('J9HZcO1Q86K3d75M1I7sM','Rate-limit reconnect',false,1677093500000,1646705220000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Can use similar strategy as in Replicache rate-limiting logic.','public'),
	 ('VRpbBmU-uZBULvFGhpwru','Publish reflect and reflect-client to NPM',false,1677093501000,1646705166000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Note: we are only planning to publish minimized/obsfucated builds, not source files.','public'),
	 ('OQPzDXIhsddK9TNefJIIf','Rework client API',false,1677093501000,1646705122000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Currently the Client API is a mishmash of Replicache and the quickly hacked together `Client` class. We need to combine into one `ReflectClient` or some such and and also hide all the stateless API details from Replicache that we don''t use.','public'),
	 ('lmMmnhdd5zR6wjTC5Blph','Replace zod with ???',true,1678376844000,1646705030000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Use superstruct because it is faster than zod. It is not as fast as
suretype but the code size of suretype is too large for our needs.

Closes rocicorp/mono#307','public'),
	 ('oDYo4rX4-Q5R2MApWlVkp','Integrate licensing from Replicache to Reflect server',false,1677093502000,1646704991000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Once licensing is integrated into Replicache (rocicorp/mono#133) we will need to do the same in Reflect.

This involves:

- adding the script to generate a license to the reflect package
- accepting license key as param to both client and server
- validating the license at startup in both client and server
- pinging periodically in both client and server','public'),
	 ('xDVlNUOoYCK1X8H78brz_','Enable licensing for Reflect',false,1677698531000,1646704962000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Spec here: https://www.notion.so/replicache/Reflect-Auth-CLI-v0-73d206a4dd8343aa91855f5bc4bac7c9.

There are basically two parts to this work:

1. Implement the `login` and `teams` commands in the `reflect` package, along with supporting schema and API changes in the licensing server (careful to not break existing Replicache usage!)
2. Plumb `licenseKey` through from `replicache` into `reflect` package','public'),
	 ('UL9p6OuZRrHsVCW4rUGVv','v0 marketing website',false,1681146778000,1646704792000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'https://www.notion.so/replicache/Reflect-Landing-Page-21f2981e8ce846a991c884bd76b01835','public'),
	 ('2XJzJy2BYngWWUF-cc7LY','Implement Repliear',false,1677091492000,1646451304000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We''ve talked for a long time about having a demo that shows off Replicache''s offline-first performance. As we created an homage to Figma for Multiplayer, I''d like to create an homage to Linear for offline-first.

I''m thinking:

* Runs on Next.js/Supabase using the shared typescript mutator pattern
* Uses the rowversion strategy on a per-repo basis
* Has a very small number of features:
  * Read/write plaintext/markdown body
  * Reporter
  * Create date
  * Last modified
  * Status
  * Append-only comments (each having an author and create date)
  * Labels
  * Search by:
    * text!
    * label
    * status
  * Sort by: last-modified, created
  * j/k navigation while in detail view
  * Styled to look like Linear
  * On startup asks to import from github to get a significant amount of data in there','public'),
	 ('-s-7JuRdK5po4Te_CmiqA','Server-side API to invalidate authentication',false,1677093503000,1646289836000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'An Auth API that supports invalidating auth tokens by user or room. See https://www.notion.so/replicache/Invalidating-Auth-shared-015bd173de8d45c4ab1c8d85f425f9f7','public'),
	 ('82oVVllmrIYHYPGbaDwXZ','Expose connect/disconnect events',false,1677093504000,1646203893000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Two use cases have surfaced for knowing when a client disconnects:

1. On the _source client_ (the one that disconnected), it would be nice to update the UI to tell the user they are disconnected and collaborators aren''t seeing their chaangs.

2. On the _destination client_ (or maybe on server?) it would be nice to proactively remove client state that''s no longer needed, rather than having to play timeout games.

This requires some API design. For (2) the most direct approach to me seems to be an event on the _server_ that can be used to make some state changes. But that feels very special case. We could expose an event on other clients when some client disappears, but that would give client A the ability to modify client B which we might not want.

I''m also not clear if there''s some utility in a mirror connect event?','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('mD8kei9d9ay7xVOozVo_U','It would be good to print the version of the server into the log at startup',false,1677093504000,1646175391000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Also to have a public API on `Client` that returns the version, so that web apps could print it up in the client at startup.','public'),
	 ('HflGUiig7qx-antN2p-Hb','canvas: drawing for long time crashes server',false,1677093505000,1645862940000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Splitting off from rocicorp/mono#301: Noam reports that even without copy/paste, he can still reproduce this crash just by drawing for a long period of time in the canvas app. Given the structure of the `drawLine()` mutation, this doesn''t seem like it would be because of the 1mb limit (because `drawLine()` only appends a point to a line, it doesn''t re-put the entire line).

I scoured the server-side logs in that bug and the only clue is the OOM reports (there are two of them in the log). So perhaps what is causing the crash in the case of drawing is unrelated to large uploads.','public'),
	 ('uTPaTRvYiHjNsjyQtr8Ou','Cloudflare log message: "Trace resource limit exceeded; subsequent logs not recorded."',false,1677093506000,1645846385000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'There are several occurrences of this message in the cloudflare logs from rocicorp/mono#301. It appears that the way the logging works with CF is that all the messages from a single socket connection get grouped together and we stop receiving messages from a connection after some number of bytes.

Unclear if this affects the datadog logging. If it doesn''t then maybe we don''t care about this. But without datadog logging, this prevents us from understanding much about what''s happening in the server as we stop seeing log messages fairly rapidly.','public'),
	 ('WPfQD0ViKhZJWWZIU78gP','1mb upload crashes durable object',false,1677093506000,1645846178000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Splitting off from rocicorp/mono#301: we have confirmed experimentally that we can''t send a > 1mb push to CF. We should split large pushes into multiple socket messages I suppose.','public'),
	 ('lBxK59WH_zVrsTABv5o5z','canvas: copy/paste of large numbers of objects fails',true,1677093068000,1645666250000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'*Note:* This bug is distilled from an earlier report and notes. Background here: https://www.notion.so/replicache/Data-Loss-During-Drawing-b6a0f836dc404cc89496b1d58aec5e33.

In the canvas app, if you scribble a bunch with the pencil tool, then copy/paste the scribble, then copy/paste all scribbles, then do that repeatedly... within about 10 or 16 copy paste cycles you notice that the copy/paste operation stops working. Meaning it doesn''t propagate to other clients open at same time.

Even odder, though, when you refresh the client, the data is lost entirely. It''s not present in the refreshed view either.

I''ve seen server logs that may or may not correspond to this event and they say the server exceeded its memory quota and was restarted. But I don''t *think* the number of mutations we are talking about are sufficient to do that?','public'),
	 ('bjCcZRrgs0o2pf6lCgHUZ','Need to string logger through worker too',false,1677093507000,1645168043000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Right now the DO is extensively logged, but the outer worker not at all. When debugging things it would be super useful to have the outer worker logged too -- particularly for e.g., auth issues.','public'),
	 ('uRhaQ6_Q635H4g3WwJvO5','Validate that stored user data is JSON before storing',true,1680637355000,1645087311000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We currently validate that stored data matched `userValueSchema` on read, but if that fails it''s already too late -- now the server won''t run :(. We should be validating on write (perhaps also on write) to prevent writing such data in the first place.
','public'),
	 ('qIS_bVLLjzfv0twuNsw0S','Feature request: some kind of script or tool to list room instances',false,1677093508000,1644313813000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Cloudflare has REST API for this, but it''s not that easy to use. Would be nice to have some kind of script in the npm package that lists rooms.','public'),
	 ('o67nWa46P-P9kWRsc9aDa','Feature request: a way to view and edit contents of room storage',true,1677093069000,1644303943000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Well this didn''t take long :-). One of our customers have requested a way to view/edit the contents of storage.

I would expect CF to be working on this, or absent that, somebody to have written the obvious npm package. But I didn''t find either.

Put in request with our contacts at CF and asked in their Discord. No answer yet.','public'),
	 ('w2pqemp2wzHziXK1Cslth','Collect client-side state of old clients from previous formats/schemas',true,1677091067000,1643910324000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'We can use the IDBDatabasesStore to find these old dbs.  
We can wait till all clients in an old db are older than 7 days and then delete the DB all at once (rather than GCing individual clients).
note: this implies we keep code that knows how to read and write the previous data format version in Replicache','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('xkMt_GYfh2tXtvtFrsswW','Implement server-side buffering',false,1677093508000,1643707326000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'The current code plays mutations in the next frame after receipt. This is not always the right thing to do. There can be variability in transmission time from client to server which can result in multiple mutations from a client showing up to server in same frame. We wouldn''t want to play those together. Also in the opposite direction variability in tx time can result in the server "dropping" an input frame from a client.

The server should have a per-client buffer of mutations that is sized such that only 1/100 input frames is dropped.','public'),
	 ('5vhM9LcujDBpEWXKEO-W_','Re-enable output gate (implies Implementing batched frames)',false,1677093509000,1643707191000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Right now in this codebase, every frame is flushed to storage individually. For cost reasons, we want to instead "turn" the game loop every fourth frame, and execute a batch of four frames all at once. Each frame will still send its own pokes, with timestamps so that they get played back on client at correct interval.

Previously this codebase had something like this, but it was behaving buggily and removed temporarily in https://github.com/rocicorp/replidraw-do/commit/aa97462d1f1857d328d72a7bbdc3d6a2eb9636d2. 

Note: We also will need to do this eventually for performance and correctness reasons. Workers are supposed to "gate" output over the web socket on writes to durable storage confirming. Right now, this gating doesn''t happen, which is a bug. But once the bug is fixed, all the sudden our output over the socket will slow way way down while waiting for storage confirms which take 20-30ms. This means that we will have to have a loop of 4 frames just to keep up with input.','public'),
	 ('1j8tnx0eVLybcbf8W7PkU','Design authentication',false,1677093509000,1643660941000,'OeVnr1y5bEM_Yg06sUFtD',NULL,NULL,'public'),
	 ('dvFMulgoDz1hRNO8pd9s5','Demonstrate that durable objects is a good backend for Replicache multiplayer',false,1677093510000,1643660443000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'- [x] Rip out the old scheduling / game loop code and do something very simple - see how that performs rocicorp/replidraw-do#1
- [ ] Re-implement the game loop (perhaps using setInterval)
- [ ]  Load test (concurrent clients in a room, max number of writes per room)
- [ ] Verify cost scales as expected under load','public'),
	 ('qaYILbO6MwgV0O_iESnt0','Pull "rep-client" down into Replicache proper',false,1677093511000,1643660085000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'multiplayer Replicache is socket-based and has a stateful slightly different protocol than local acceleration Replicache.  The client side of this protocol is currently implemented in the `rep-client` directory of this repo. We don''t want every project to have to include this literally, and also the implementation of this client is quite inefficient and indirect due to the fact that it has to be implemented on top of the stateless API that replicache provides.

Replicache should directly and efficiently support the multiplayer use case without any of this client boilerplate required in replidraw-do.

What to do with the stateless interface is up for debate. I have considered:

1. Pulling it into a separate repo (ala replicache-react)
2. Ditching it entirely (users have to do it themselves)
3. Having it as an optional feature of `replicache`

Currently leaning more toward 3. But in any case the goal for this bug here is that multiplayer apps don''t need to copy/paste the client and that the client is efficient.','public'),
	 ('rQo1DeZZgBnqWT7wvBXHS','Infinite loop when restarting wrangler dev',false,1677093511000,1643619617000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Reproduction:

1. `wrangler dev`
2. Open tab to app
3. Kill server
4. Restart server

This happens because the persistent state for `wrangler dev` is deleted automatically when the server restarts and so from the server''s perspective it receives a mutation which is from the future for the existing client on reboot, and it can never apply it so it just keeps trying forever.

This is related to https://github.com/rocicorp/replicache/issues/335 -- the client should have been told it was unknown. But also there shouldn''t be a way for our server to iloop.','public'),
	 ('CCsMsi5XESPApUiyOXfiM','Add a recipe page to docs about versioning data @arv',true,1677091068000,1643107991000,'nqYkxAGMnzk7Y5STjZryV',NULL,NULL,'public'),
	 ('KCAtXyO9hPZTNM4dJHwKp','Add recipe about how to detect that there are unsynced changes',false,1709536788000,1643040648000,'nqYkxAGMnzk7Y5STjZryV',NULL,'This can be done by having the client set a dirty bit and having the server clear it.

```ts
const rep = new Replicache({
  name: ''dummy'',
  mutators: {
    todo: wrapWithDirty(async (tx: WriteTransaction, args: string) => {
      await tx.put(''x'', args);
      return true;
    }),
  },
});

function wrapWithDirty<A extends JSONValue, R extends JSONValue>(
  f: (tx: WriteTransaction, arg: A) => MaybePromise<R>,
): (tx: WriteTransaction, arg: A) => MaybePromise<R> {
  return async (tx: WriteTransaction, args: A) => {
    const rv = await f(tx, args);
    await setDirty(tx);
    return rv;
  };
}

async function setDirty(tx: WriteTransaction): Promise<void> {
  await tx.put(''/dirty'', true);
}

async function isDirty(tx: ReadTransaction): Promise<boolean> {
  return (await tx.get(''/dirty'')) === true;
}
```

This is not super obvious and it does mean that the mutators have to call `setDirty` or wrap the mutators. It does make the API a bit less convenient and we might want to expose a cleaner way.
','public'),
	 ('oj7ViYmOhygtxCuG5Q1HN','gc has a bug when a head name is updated to the same hash',false,1677091480000,1641490066000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'If during a commit a head name is updated to the same hash  (through a single setHead or a series of setHead) we end up incorrectly increasing the ref count of that hash.

This can be fixed in `gc.ts` by checking that headChanges actually change the hash .  

Existing code in `gc.ts` computeRefCountUpdate:
```
  for (const changedHead of headChanges) {
    changedHead.old && oldHeads.push(changedHead.old);
    changedHead.new && newHeads.push(changedHead.new);
  }
```

fix:
```
  for (const changedHead of headChanges) {
    if (changedHead.old !== changedHead.new) {
      changedHead.old && oldHeads.push(changedHead.old);
      changedHead.new && newHeads.push(changedHead.new);
    }
  }
```

','public'),
	 ('t9yIshNCWN_5966ApVXM1','add rebase speed target to perf envelope and add test',true,1677091069000,1638479334000,'yJ5hiysWE-LBcDfT44lR8',NULL,'MP at 60fps implies a strict constraints on rebase in the client. We should:
- add a target to the perf envelope that captures this
- add a benchmark that tracks this metric','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('6OyQRI1-b_bc6mzgevMKg','update eslint config to enforce leading underscore for private methods.',false,1677091479000,1637716073000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'
_Originally posted by @arv in https://github.com/rocicorp/replicache/pull/724#discussion_r755607584_

cc @arv ','public'),
	 ('zHk5PvZbKetkVVQkX1Ynu','determine under what circumstances to accept an update in the MP world',false,1709599717000,1637292977000,'yJ5hiysWE-LBcDfT44lR8',NULL,'In the current world the PullResponse has the lastMutationID and cookie. We accept the new snapshot in the PullResponse if 
- [the new lmid is >= what we have](https://github.com/rocicorp/replicache/blob/16d2597163a88c65f51d6632bf1b03fd294769cf/src/sync/pull.ts#L115) AND
- [there is a patch or the cookie or lmid changes](https://github.com/rocicorp/replicache/blob/16d2597163a88c65f51d6632bf1b03fd294769cf/src/sync/pull.ts#L124) AND
- [the base snapshot hasn''t changed out from under us](https://github.com/rocicorp/replicache/blob/16d2597163a88c65f51d6632bf1b03fd294769cf/src/sync/pull.ts#L108) (indicating something else completed a pull while we were working)

Note that we''ll write a new snapshot if the server returns the same cookie or lmid as long as the patch is non-empty. The state associated with a lmid and a cookie is not unique. Intuitively this must be true because of out of band changes, but unfortunately it means that we can''t _know_ the client has the state we intend for it to have, because a cookie does not uniquely identify a state.

In the new world we''ll poke a series of updates composed of (more or less) a tuple of `(baseCookie, newCookie, lastMutationID, patch)`. We need to determine under which circumstances to accept an update, considering both the MP use case as well as the more traditional instant use cases (client does something like today''s pull, maybe without a cookie). 

I have an opinion about this that I will flesh out here, but I''m submitting this issue right now as-is so I have an issue to reference in code I am trying to merge.','public'),
	 ('izxfni_zPjeOA49N-oMWr','Consider changing entries for btrees',true,1677091070000,1637270184000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Right now the entries of a BTree Node looks like `[key: string, value: Hash | ReadonlyJSONValue][]`. This is a lot of array objects.

We could either have one array for the keys and one for the values or we could have an array of alternative keys and values.','public'),
	 ('yim3tSni-ncY5B1fEtazj','Reduce allocations in BTree read',true,1677091071000,1637270059000,'nqYkxAGMnzk7Y5STjZryV',NULL,'For B+Tree read operations I think we can work directly on the chunk data instead of the Node wrappers.

Currently the code depends on "virtual dispatch" since what needs to happen for an internal node vs a data node is slightly different. That "virtual dispatch" can be manually handled with "if statements".','public'),
	 ('YXDDa24OnTfb4ouoaA8aP','RFE: Run Perdag in a Worker',true,1677091071000,1637269908000,'nqYkxAGMnzk7Y5STjZryV',NULL,'We can/should run the perdag in the worker. ~~That would allow us to use the native hash functions (we can precompute the hash of the chunks in the persist operation)~~ (We actually precompute the hashes outside the IDB transactions with SDD)

According to this [SO post](https://stackoverflow.com/questions/10343913/how-to-create-a-web-worker-from-a-string) you can create a worker from a string but it is not clear what CSP policies this runs under.','public'),
	 ('y7I5m2BZbTaBw11cJKv4O','p95 performance benchmarks are noisy, increase tolerance for p95 but not p50',true,1677091072000,1636996283000,'Gg4MskWt3M-ttzzlrJ9jn',NULL,'This will require a bit of a refactor, so that the p50 and p95 benchmarks can have different tolerances, currently they share one.','public'),
	 ('yIUPmTFehw2TgOSSZNG_Y','we should consider tracking GC perf',true,1677091073000,1636677659000,'yJ5hiysWE-LBcDfT44lR8',NULL,'Per https://github.com/rocicorp/replicache/pull/681#issuecomment-966725207. I don''t think GC rises to the level of core perf envelope metric, but it''s clearly a component of core per metrics (eg, rebase perf). Seems like it would be useful to have a baseline.','public'),
	 ('J3-tsjZxV8PlJSH44m3w1','doc: Recipe for multi user app',true,1677091073000,1636136927000,'nqYkxAGMnzk7Y5STjZryV',NULL,'- Include user id in name
- Use local storage to allow switching while offline?','public'),
	 ('QuTzFD3KoD-vdbFGPROzl','Perf tests runs for too long?',true,1677091074000,1635372592000,'nqYkxAGMnzk7Y5STjZryV',NULL,'The tests were designed to run 5 iterations or 500ms seconds but I see tests running for 30s

https://github.com/rocicorp/replicache/blob/aa5d02439657400edfc55d837b57ae632de537b3/perf/perf.ts#L42

The way it is structured now, the sum is only summing up the time we are measuring. I think we should exit if we have 10 runs and a total of 5s','public'),
	 ('zceznydiV_dcsp1cGmsvd','Replidraw r3 bugs',false,1677091492000,1633969950000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'See https://github.com/rocicorp/replidraw/issues?q=is%3Aopen+is%3Aissue+milestone%3AR3','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('UgayxjXiqQZn8_Yt9F5qq','Consider using structuredClone function',false,1677091479000,1633383521000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Do some perf tests on the built in `structuredClone` function and use it instead of `json.ts` `deepClone` if faster.

It is available in Safari Developer Preview and Deno and is in a spec so it should come to other browsers soon:tm:','public'),
	 ('HO85ObWb4MZiZkq638vuk','Licensing',false,1690343136000,1632901452000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'**NOTE: Licensing done done list: rocicorp/mono#119**

In order to implement our billing model, we need to track the number of unique clients each customer uses Replicache with each month. 



This implies that Replicache will ping some central server on startup with an accountid/clientid pair so that we can count. Replicache generates the clientid internally already, and the accountid could be supplied as a constructor parameter.

However, we do **not** want it to be possible to accidentally (or maliciously) use someone else''s account id  and charge their credit card. Therefore, accounts should be somehow tied to domains -- and you should only be able to construct Replicache with an accountid if the owner of that account intends Replicache to be used on that domain. (As a nice side effect this would allow account holders to know which domains are generating Replicache usage).

Another requirement is that we do **not** want to report domain names Replicache is used on to some central service. It''s common for companies to have internal, semi-secret host names, and it would be bad practice to collect those.','public'),
	 ('4jSfGKGJZnmJB7wqIfWZo','Replicache Performance Specs',false,1677705190000,1632896484000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'This bug describes the current performance specifications of Replicache. This is intended to help developers understand how Replicache has been designed and tested to be used.

# Performance

## Data Sizes
- Up to 64MB per Replicache instance

## Scan
- 650 MB/s in release mode

## Reactive Loop
100 open subscriptions, 5 of which are dirty, each of which reads 10KB of data.
With 16 MB total data, all reads refresh after a write in:
- p50: 3.5ms
- p95: 6ms
With 64 MB total data:
- p50: 3.5ms
- p95: 25ms

## Populate 1MB
- With zero indexes: 25 MB/s
- With one index: 17 MB/s
- With two indexes: 13 MB/s

## Startup
- @100mb: p95 Read first 100kb < 150ms

# Correctness

## Offline

Pending but unpushed mutations are pushed to server when back online. For example, if pending mutations are applied in a tab while offline and that tab is closed (or crashes) while offline, they are sent next time the app runs (in any tab). The very last few moments of mutations (~5s worth) before tab close/crash may be lost.
','public'),
	 ('ZaW0CXuewcGZov8DUJ5HK','doc: Add "paging in data" recipe',false,1709536814000,1632728493000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'A start: https://roamresearch.com/#/app/aboodman/page/X3mhH5OCs','public'),
	 ('WtLd19bqTJ38S6scp1OA0','doc: error handling recipe',true,1677090551000,1632728420000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Add a recipe demonstrating how to handle errors in mutators.','public'),
	 ('k1RWJfF-aQu5SoB5E-q56','doc: server requirements',true,1690343026000,1632728384000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Add a page to documentation listing server requirements so you can easily determine if your db is compatible with Replicache','public'),
	 ('LZnu7lfrjDvi8k-3XbZox','doc: Document ExperimentalKVStore and related interfaces',true,1690342957000,1632727574000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'https://doc.replicache.dev/api/interfaces/ExperimentalKVStore -- none of the methods or the other related experimental interfaces are documented.','public'),
	 ('rmuRNbTUJ43LPAV_UbdzJ','doc: add "garbage collecting old data" recipe',false,1690342899000,1632727447000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'https://www.notion.so/Garbage-Collecting-Old-Data-6b3bb7f39a4447f7b77a465b013824b5','public'),
	 ('WV-Eq1VhQLCdOhcArGQKC','Doc: change isolation level of push to serialized',false,1690342830000,1632726928000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'We should officially required snapshot isolation or higher for our push handlers. I think this is a reasonable tradeoff since Replicache sends many mutations in a batch. Also most modern databases are going to higher levels of isolation anyway.

Let''s change the integration guide to explicitly use serialized for its transactions/

https://discord.com/channels/830183651022471199/830183651022471202/884871312361807922','public'),
	 ('awv6JbOAI38Ed3q8-Iy3c','Calling commit twice should throw',true,1677091076000,1632264884000,'nqYkxAGMnzk7Y5STjZryV',NULL,'Also commit should be private

We have some code paths where we call commit twice.','public');
INSERT INTO public.issue (id,title,"open",modified,created,"creatorID","assigneeID",description,visibility) VALUES
	 ('iRTV6NFibyB7ggIKJoW1R','Idea: DeepFrozenJSONValue',false,1709536838000,1630877666000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'https://github.com/rocicorp/replicache/pull/479 introduces a DeepReadonlyJSONValue, but the semantics are not quite ideal. You can pass a `JSONObject` to a parameter that is `DeepReadonlyJSONObject `, because the latter is just promising that *it* won''t mutate the data. It''s not saying that it wants a `JSONObject` that others can''t mutate.

@arv ways saying he thinks it''s possible to use the opaque type trick to get us semantics more like real immutability, where you can enforce that nobody accidentally passes you something that they will mutate behind your back.  I think that might be useful to us.','public'),
	 ('-guR4swz7EX7CeNnEQI3P','ExperimentalKVStore: Move withRead/withWrite to Replicache',true,1677090671000,1630665606000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'Why do clients have to implement this? It seems like given read(), write(), and release(), we can do this ourselves.','public'),
	 ('7bHPpjhEyp7j726iOKQab','Test don''t show details of mismatched maps',true,1677091077000,1630491141000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'I see errors like:

```
 ❌ maybe end try pull
      AssertionError: 2 pending but nothing to replay: expected {} to deeply equal {}
      + expected - actual
```

It looks like this has been fixed upstream in Chai, but a new release doesn''t exist yet: https://github.com/chaijs/chai/issues/1228. So all we need to do is roll Chai when a new release exists.','public'),
	 ('058B73_9XTp1ayBO4K1n3','Use a cursor to get consecutive KV values',true,1677091078000,1628806363000,'nqYkxAGMnzk7Y5STjZryV',NULL,'https://jlongster.com/future-sql-web

> Because we keep a single transaction open for reads over time, we can detect when sequential reads are happening and open a cursor. There’s a lot of interesting tradeoffs here because opening a cursor is actually super slow in some browsers, but iterating is a lot faster than many get requests. This backend will intelligently detect when several sequential reads happen and automatically switch to using a cursor.

For `dag::Store` we sometimes read/write consecutive keys:

https://github.com/rocicorp/repc/blob/main/src/dag/write.rs#L192-L200

and these keys are of the shape `''c/<hash>/d`,`''c/<hash>/m` and `''c/<hash>/r`. It seem like it would be beneficial to use a cursor in this case.','public'),
	 ('H-s5GYmv65OM8Cn6XhCvK','Consider making subscribe cancel function a PromiseLike',true,1677091079000,1619807318000,'nqYkxAGMnzk7Y5STjZryV',NULL,'subscribe is an async operation but we do not expose the promise so there is no (easy) way to tell when it is done.

We can make the returned function a "thenable"/`PromiseLike` by doing something like:

```ts
    const f: PromiseLike<void> & (() => void) = (): void => {
      this._subscriptions.delete(
        (s as unknown) as Subscription<JSONValue | undefined, unknown>,
      );
    };
    f.then = (a, b) => p.then(a, b);
    return f;
```


This would allow us to await the result. We can do more trickery to allow you do `const cancel = await rep.subscribe(...)` if we also want that.','public'),
	 ('3O2nk8ljXPknw7Hl_Azyj','If getPushAuth/getPullAuth set, let''s call it once before first push/pull',true,1677090669000,1618609999000,'OeVnr1y5bEM_Yg06sUFtD',NULL,'This is fairly minor but current behavior always results in one error. This is by design but it looks confusing/weird in the console until you remember what''s going on.

<img width="1030" alt="Screen Shot 2021-04-16 at 11 52 58 AM" src="https://user-images.githubusercontent.com/80388/115087681-52759a80-9eaa-11eb-90d0-4c6e54c624ac.png">
','public');
