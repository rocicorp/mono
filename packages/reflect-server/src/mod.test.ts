import {test, expect} from '@jest/globals';
//import { handleRequest } from "@/index";

test('should pass-through to durable object', async () => {
  const {roomDO} = getMiniflareBindings();
  const id = roomDO.newUniqueId();
  const storage = await getMiniflareDurableObjectStorage(id);
  expect(storage).toBeDefined();
  /*
  await storage.put("count", 10);

  const req = new Request("http://localhost/name/increment");
  const res = await handleRequest(req, env);
  expect(res.status).toBe(200);
  expect(await res.text()).toContain("⬆️ 11");

  const newValue = await storage.get("count");
  expect(newValue).toBe(11);
  */
});
