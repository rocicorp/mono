import type {Enum} from '../../../shared/src/enum.ts';
import * as ConnectionStatusEnum from './connection-status-enum.ts';

export {ConnectionStatusEnum as ConnectionStatus};
export type ConnectionStatus = Enum<typeof ConnectionStatusEnum>;
