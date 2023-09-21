import * as v from 'shared/src/valita.js';
import {firestoreDataConverter} from './converter.js';
import * as path from './path.js';

export const teamSchema = v.object({
  name: v.string(),

  // A globally unique label, by default derived from the `name`, that is used as
  // the suffix for the hostname where apps are hosted, e.g.
  // https://app-name-teamlabel.reflect-server.net
  //
  // A team label is restricted to alphanumeric characters and must start with a letter
  // (in case they need to stand alone as the hostname in the future). Unlike full
  // subdomains, hyphens are not allowed because a hyphen is used as the delimiter
  // between the app name and the team label in the local name of the hostname.
  //
  // The default value is a sanitized version of the Team `name`,
  // with a random integer suffix added in the case of collisions. In the future, users will
  // have the ability to change the team name and label.
  //
  // This field is denormalized to all of the Team's apps to simplify deployment logic.
  label: v.string(),

  /** @deprecated TODO(darick): Remove with the cli migration code. */
  subdomain: v.string().optional(),

  defaultCfID: v.string(),

  // Number of memberships of role 'admin'.
  // A team must have at least one admin.
  numAdmins: v.number(),
  // Number of memberships of role 'member'.
  numMembers: v.number(),
  numInvites: v.number(),

  numApps: v.number(),
  // null means default max
  maxApps: v.union(v.number(), v.null()),
});

export type Team = v.Infer<typeof teamSchema>;

export const teamDataConverter = firestoreDataConverter(teamSchema);

export const TEAM_COLLECTION = 'teams';

export function teamPath(teamID: string): string {
  return path.join(TEAM_COLLECTION, teamID);
}

export const teamLabelIndexSchema = v.object({
  teamID: v.string(),
});

export type TeamLabelIndex = v.Infer<typeof teamLabelIndexSchema>;

export const teamLabelIndexDataConverter =
  firestoreDataConverter(teamLabelIndexSchema);

export const TEAM_LABEL_INDEX_COLLECTION = 'teamLabels';

export function teamLabelIndexPath(label: string): string {
  return path.join(TEAM_LABEL_INDEX_COLLECTION, label);
}

const VALID_LABEL = /^[a-z]([a-z0-9]*)$/;

export function isValidLabel(name: string): boolean {
  return VALID_LABEL.test(name);
}

export function sanitizeForLabel(orig: string): string {
  return orig
    .toLocaleLowerCase()
    .replaceAll(/[^a-z0-9]+/g, '') // Remove any illegal characters
    .replaceAll(/^[0-9]*/g, ''); // Remove leading digits or hyphens
}

const VALID_SUBDOMAIN = /^[a-z]([a-z0-9-])*[a-z0-9]$/;

export function isValidSubdomain(name: string): boolean {
  return VALID_SUBDOMAIN.test(name);
}

export function sanitizeForSubdomain(orig: string): string {
  return orig
    .toLocaleLowerCase()
    .replaceAll(/[^a-z0-9-]+/g, '-') // Replace any sequences of illegal characters with a hyphens
    .replaceAll(/^[0-9-]*/g, '') // Remove leading digits or hyphens
    .replaceAll(/[-]*$/g, ''); // Remove trailing hyphens
}

export const appNameIndexSchema = v.object({
  appID: v.string(),
});

export type AppNameIndex = v.Infer<typeof appNameIndexSchema>;

export const appNameIndexDataConverter =
  firestoreDataConverter(appNameIndexSchema);

export const APP_NAME_INDEX_COLLECTION_ID = 'appNames';

export function appNameIndexPath(teamID: string, appName: string): string {
  return path.append(teamPath(teamID), APP_NAME_INDEX_COLLECTION_ID, appName);
}
