import * as z from 'zod';

export const entitySchema = z.object({
  id: z.string(),
});
