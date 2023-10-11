import {entitySchema, generate, Update} from '@rocicorp/rails';
import type {ReadTransaction, WriteTransaction} from '@rocicorp/reflect';
import {z} from 'zod';

const DEAD_BOT_CONTROLLER_THRESHHOLD_MS = 5_000;

const botControllerKey = 'botControllerKey';
const botControllerSchema = z.object({
  clientID: z.string(),
  aliveTimestamp: z.number().default(0),
});
export type BotControllerModel = z.infer<typeof botControllerSchema>;

export const getBotController = async (
  tx: ReadTransaction,
): Promise<BotControllerModel | undefined> => {
  const botController = await tx.get(botControllerKey);
  return botController === undefined
    ? botController
    : botControllerSchema.parse(botController);
};
const setBotController = (tx: WriteTransaction, value: BotControllerModel) =>
  tx.set(botControllerKey, value);

const deleteBotController = (tx: WriteTransaction) => tx.del(botControllerKey);

export const clientModelSchema = entitySchema.extend({
  selectedPieceID: z.string(),
  x: z.number(),
  y: z.number(),
  color: z.string(),
  location: z.union([z.string(), z.null()]),
  focused: z.boolean(),
  // If non-empty, this client is a bot controlled
  // by the client with this id.
  botControllerID: z.string(),
  // True if this client is a bot that was manually triggered
  // (in which case botControllerID is the client that manually
  // triggered it)
  manuallyTriggeredBot: z.boolean(),
});

// Export generated interface.
export type ClientModel = z.infer<typeof clientModelSchema>;
export type ClientModelUpdate = Update<ClientModel>;
const clientGenerateResult = generate('client', clientModelSchema);

export const {
  get: getClient,
  has: hasClient,
  init: initClient,
  list: listClients,
} = clientGenerateResult;

export const deleteClient = async (tx: WriteTransaction, id: string) => {
  const botController = await getBotController(tx);
  if (tx.environment === 'server') {
    const clients = await listClients(tx);
    let potentialNewBotControllerClient = undefined;
    for (const client of clients) {
      if (client.id !== id && client.botControllerID === '') {
        potentialNewBotControllerClient = client;
      }
      if (client.botControllerID === id) {
        await clientGenerateResult.delete(tx, client.id);
      }
    }
    if (botController?.clientID === id) {
      // Set new bot controller.
      if (potentialNewBotControllerClient) {
        console.log(
          'Bot controller deleted',
          id,
          'assigning',
          potentialNewBotControllerClient.id,
        );
        await setBotController(tx, {
          clientID: potentialNewBotControllerClient.id,
          aliveTimestamp: Date.now(),
        });
      } else {
        console.log('Bot controller deleted', id, 'no client to assign');
        await deleteBotController(tx);
      }
    }
  }
  await clientGenerateResult.delete(tx, id);
};

function canModifyClient(
  tx: WriteTransaction,
  client: ClientModel,
  botController: BotControllerModel | undefined,
) {
  if (!client.botControllerID) {
    return true;
  }
  if (client.manuallyTriggeredBot) {
    return tx.clientID === client.botControllerID;
  }
  return (
    tx.clientID === client.botControllerID &&
    tx.clientID === botController?.clientID
  );
}

async function ensureAliveBotController(tx: WriteTransaction) {
  let botController = await getBotController(tx);
  if (tx.environment === 'server') {
    if (!botController) {
      console.log('No bot controller, assigning', tx.clientID);
      botController = {
        clientID: tx.clientID,
        aliveTimestamp: Date.now(),
      };
      await setBotController(tx, botController);
    } else if (
      botController.clientID !== tx.clientID &&
      (!botController.aliveTimestamp ||
        Date.now() - botController.aliveTimestamp >
          DEAD_BOT_CONTROLLER_THRESHHOLD_MS)
    ) {
      console.log(
        'Dead bot controller',
        botController.clientID,
        'assigning',
        tx.clientID,
      );
      // Delete non-manual bots controlled by dead bot controller
      const clients = await listClients(tx);
      for (const client of clients) {
        if (
          !client.manuallyTriggeredBot &&
          client.botControllerID === botController.clientID
        ) {
          await clientGenerateResult.delete(tx, client.id);
        }
      }
      botController = {
        clientID: tx.clientID,
        aliveTimestamp: Date.now(),
      };
      await setBotController(tx, botController);
    }
  }
  return botController;
}

/**
 * Returns whether or not the client was updated.
 */
export const updateClient = async (
  tx: WriteTransaction,
  update: ClientModelUpdate,
) => {
  const client = await getClient(tx, update.id);
  if (!client) {
    // pass through for default error messaging
    await clientGenerateResult.update(tx, update);
    return false;
  }
  const botController = await ensureAliveBotController(tx);
  if (!canModifyClient(tx, client, botController)) {
    return false;
  }
  if (
    tx.environment === 'server' &&
    client.botControllerID &&
    !client.manuallyTriggeredBot
  ) {
    await setBotController(tx, {
      clientID: tx.clientID,
      aliveTimestamp: Date.now(),
    });
  }
  await clientGenerateResult.update(tx, update);
  return true;
};

export const putClient = async (tx: WriteTransaction, value: ClientModel) => {
  const botController = await ensureAliveBotController(tx);
  if (!canModifyClient(tx, value, botController)) {
    return;
  }
  await clientGenerateResult.put(tx, value);
};

export const ensureClient = async (
  tx: WriteTransaction,
  value: ClientModel,
) => {
  const client = await getClient(tx, value.id);
  if (client) {
    return;
  }
  await putClient(tx, value);
};
