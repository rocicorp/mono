import { Mutation } from "../protocol/push";
import { ClientID } from "./client-state";

export type ClientMutation = Mutation & {
  clientID: ClientID;
};
