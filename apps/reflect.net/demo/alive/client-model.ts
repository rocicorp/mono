import {generate, Update} from '@rocicorp/rails';
import type {ReadTransaction, WriteTransaction} from '@rocicorp/reflect';
import * as z from 'zod';
import {colorToString, idToColor} from './colors';
import {entitySchema} from './entity-schema.js';

const DEAD_BOT_CONTROLLER_THRESHOLD_MS = 5_000;

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
});

export type ClientModel = z.infer<typeof clientModelSchema>;
export type ClientModelUpdate = Update<ClientModel>;
// TODO(arv): Use new presence state rails functionality.
const clientGenerateResult = generate('-/p', v => clientModelSchema.parse(v));

export const {
  get: getClient,
  has: hasClient,
  list: listClients,
} = clientGenerateResult;

export const botModelSchema = clientModelSchema.extend({
  // If non-empty, this client is a bot controlled
  // by the client with this id.
  botControllerID: z.string(),
  // True if this client is a bot that was manually triggered
  // (in which case botControllerID is the client that manually
  // triggered it)
  manuallyTriggeredBot: z.boolean(),
});

export type BotModel = z.infer<typeof botModelSchema>;
export type BotModelUpdate = Update<BotModel>;
const botGenerateResult = generate('bot', v => botModelSchema.parse(v));

export const {
  get: getBot,
  list: listBots,
  delete: deleteBot,
} = botGenerateResult;

export const ensureNotBotController = async (
  tx: WriteTransaction,
  clientID: string,
) => {
  if (tx.location === 'server') {
    const botController = await getBotController(tx);
    await deleteBotsControlledBy(tx, clientID);
    if (botController?.clientID === clientID) {
      const clients = await listClients(tx);
      let potentialNewBotControllerClient = undefined;
      for (const client of clients) {
        if (client.id !== clientID) {
          potentialNewBotControllerClient = client;
        }
      }
      // Set new bot controller.
      if (potentialNewBotControllerClient) {
        console.log(
          'Bot controller unassigned',
          clientID,
          'assigning',
          potentialNewBotControllerClient.id,
        );
        await setBotController(tx, {
          clientID: potentialNewBotControllerClient.id,
          aliveTimestamp: Date.now(),
        });
      } else {
        console.log(
          'Bot controller unassigned',
          clientID,
          'no client to assign',
        );
        await deleteBotController(tx);
      }
    }
  }
};

function canModifyBot(
  tx: WriteTransaction,
  bot: BotModel,
  botController: BotControllerModel | undefined,
) {
  if (bot.manuallyTriggeredBot) {
    return tx.clientID === bot.botControllerID;
  }
  return (
    tx.clientID === bot.botControllerID &&
    tx.clientID === botController?.clientID
  );
}

async function ensureAliveBotController(tx: WriteTransaction) {
  let botController = await getBotController(tx);
  if (tx.location === 'server') {
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
          DEAD_BOT_CONTROLLER_THRESHOLD_MS)
    ) {
      console.log(
        'Dead bot controller',
        botController.clientID,
        'assigning',
        tx.clientID,
      );
      await deleteBotsControlledBy(tx, botController.clientID);
      botController = {
        clientID: tx.clientID,
        aliveTimestamp: Date.now(),
      };
      await setBotController(tx, botController);
    }
  }
  return botController;
}

export const updateClient = async (
  tx: WriteTransaction,
  update: Omit<ClientModelUpdate, 'id'>,
) => {
  await ensureAliveBotController(tx);
  await clientGenerateResult.update(tx, {
    id: tx.clientID,
    ...update,
  });
};

export const initClient = async (
  tx: WriteTransaction,
  args: {focused: boolean},
) => {
  const id = tx.clientID;
  const {focused} = args;
  const client = {
    id,
    selectedPieceID: '',
    // off the page, so not visible till user moves cursor
    // avoids cursors stacking up at 0,0
    x: Number.MIN_SAFE_INTEGER,
    y: 0,
    color: colorToString(idToColor(id)),
    location: null,
    focused,
    botControllerID: '',
    manuallyTriggeredBot: false,
  };
  await ensureAliveBotController(tx);
  await clientGenerateResult.set(tx, client);
};

/**
 * Returns whether or not the bot was updated.
 */
export const updateBot = async (
  tx: WriteTransaction,
  update: BotModelUpdate,
) => {
  const bot = await getBot(tx, update.id);
  if (!bot) {
    // pass through for default error messaging
    await botGenerateResult.update(tx, update);
    return false;
  }
  const botController = await ensureAliveBotController(tx);
  if (!canModifyBot(tx, bot, botController)) {
    return false;
  }
  if (tx.location === 'server' && !bot.manuallyTriggeredBot) {
    await setBotController(tx, {
      clientID: tx.clientID,
      aliveTimestamp: Date.now(),
    });
  }
  await botGenerateResult.update(tx, update);
  return true;
};

export const setBot = async (tx: WriteTransaction, value: BotModel) => {
  const botController = await ensureAliveBotController(tx);
  if (!canModifyBot(tx, value, botController)) {
    return;
  }
  await botGenerateResult.set(tx, value);
};

async function deleteBotsControlledBy(tx: WriteTransaction, clientID: string) {
  const bots = await listBots(tx);
  for (const bot of bots) {
    if (bot.botControllerID === clientID) {
      await botGenerateResult.delete(tx, bot.id);
    }
  }
}
