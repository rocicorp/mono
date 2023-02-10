import { PokeBody } from "../protocol/poke";
import { ClientID } from "./client-state";

export type ClientPokeBody = {
  clientID: ClientID;
  poke: PokeBody;
};
