import type {Socket} from '../util/socket.js';
import type {PongMessage} from 'reflect-protocol';
import type {LogContext} from '@rocicorp/logger';
import {send} from '../util/socket.js';

/**
 * handles the 'ping' upstream message by sending a pong!
 * @param ws socket connection to requesting client
 * @returns
 */
export function handlePing(lc: LogContext, ws: Socket) {
  lc.debug?.('handling ping');
  const pongMessage: PongMessage = ['pong', {}];
  send(ws, pongMessage);
}
