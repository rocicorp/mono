import { z } from "zod";

export const errorMessageSchema = z.tuple([z.literal("error"), z.string()]);

export type ErrorMessage = z.infer<typeof errorMessageSchema>;
