import {randomInt} from 'crypto';
import type {Firestore, Transaction} from 'firebase-admin/firestore';
import {logger} from 'firebase-functions';
import {defineString} from 'firebase-functions/params';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  ensureTeamRequestSchema,
  ensureTeamResponseSchema,
} from 'mirror-protocol/src/team.js';
import {
  membershipDataConverter,
  teamMembershipPath,
} from 'mirror-schema/src/membership.js';
import {
  sanitizeForLabel,
  teamDataConverter,
  teamPath,
  teamLabelIndexDataConverter,
  teamLabelIndexPath,
} from 'mirror-schema/src/team.js';
import {userDataConverter, userPath} from 'mirror-schema/src/user.js';
import {must} from 'shared/src/must.js';
import {newTeamID} from '../../ids.js';
import {userAuthorization} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';

const cloudflareAccountId = defineString('CLOUDFLARE_ACCOUNT_ID');

export const DEFAULT_MAX_APPS = null;

export const ensure = (firestore: Firestore) =>
  validateSchema(ensureTeamRequestSchema, ensureTeamResponseSchema)
    .validate(userAuthorization())
    .handle(async (req, context) => {
      const {userID} = context;
      const {name} = req;

      const userDocRef = firestore
        .doc(userPath(userID))
        .withConverter(userDataConverter);

      const teamID = await firestore.runTransaction(async txn => {
        const userDoc = await txn.get(userDocRef);
        if (!userDoc.exists) {
          throw new HttpsError('not-found', `User ${userID} does not exist`);
        }

        const user = must(userDoc.data());
        const {email} = user;

        const teamIDs = Object.keys(user.roles);
        if (teamIDs.length > 1) {
          throw new HttpsError(
            'internal',
            'User is part of multiple teams, but only one team is supported at this time',
          );
        }
        if (teamIDs.length === 1) {
          return teamIDs[0];
        }

        const teamID = newTeamID();
        const label = await getLabel(firestore, txn, name);

        logger.info(
          `Creating team "${name}" (${teamID}) at ${label}.reflect-server.net for user ${userID}`,
        );
        txn.create(
          firestore.doc(teamPath(teamID)).withConverter(teamDataConverter),
          {
            name,
            label,
            defaultCfID: cloudflareAccountId.value(),
            numAdmins: 1,
            numMembers: 0,
            numInvites: 0,
            numApps: 0,
            maxApps: DEFAULT_MAX_APPS,
          },
        );
        txn.create(
          firestore
            .doc(teamMembershipPath(teamID, userID))
            .withConverter(membershipDataConverter),
          {email, role: 'admin'},
        );
        txn.update(userDocRef, {roles: {[teamID]: 'admin'}});
        txn.create(
          firestore
            .doc(teamLabelIndexPath(label))
            .withConverter(teamLabelIndexDataConverter),
          {teamID},
        );
        return teamID;
      });
      return {teamID, success: true};
    });

async function getLabel(
  firestore: Firestore,
  txn: Transaction,
  name: string,
): Promise<string> {
  // Try up to 5 times, adding a random number fo the label if it is taken.
  for (let i = 0; i < 5; i++) {
    const label =
      i === 0
        ? sanitizeForLabel(name)
        : `${sanitizeForLabel(name)}${randomInt(10000)}`;
    const entry = await txn.get(firestore.doc(teamLabelIndexPath(label)));
    if (!entry.exists) {
      return label;
    }
    logger.info(
      `Team with label ${label} (${entry.ref.path}) already exists. Adding a random suffix.`,
      entry.data(),
    );
  }
  throw new HttpsError(
    'resource-exhausted',
    `Failed to generate a random label for ${name}`,
  );
}
