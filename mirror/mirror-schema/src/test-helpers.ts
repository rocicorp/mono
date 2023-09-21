import type {Firestore} from '@google-cloud/firestore';
import type firebase from 'firebase/compat/app';
import {firebaseStub} from 'firestore-jest-mock/mocks/firebase.js';
import {App, appDataConverter, appPath} from 'mirror-schema/src/app.js';
import {
  Membership,
  Role,
  membershipDataConverter,
  teamMembershipPath,
} from 'mirror-schema/src/membership.js';
import {
  teamDataConverter,
  teamPath,
  appNameIndexPath,
  appNameIndexDataConverter,
  sanitizeForLabel,
  type AppNameIndex,
  type Team,
} from 'mirror-schema/src/team.js';
import {
  userDataConverter,
  userPath,
  type User,
} from 'mirror-schema/src/user.js';
import {must} from 'shared/src/must.js';
import {DeploymentSecrets, defaultOptions} from './deployment.js';

// The server and (v8) client Firestore interfaces are largely the same.
// Have the jest mock implement both, which should largely work for our testing purposes.
type AllTheFirestores = Firestore & firebase.default.firestore.Firestore;

export function fakeFirestore(): AllTheFirestores {
  return firebaseStub(
    {database: {}},
    {mutable: true},
  ).firestore() as unknown as AllTheFirestores;
}

export async function setUser(
  firestore: Firestore,
  userID: string,
  email: string,
  name = 'Foo Bar',
  roles: Record<string, Role> = {},
): Promise<User> {
  const user: User = {
    email,
    name,
    roles,
  };
  await firestore
    .doc(userPath(userID))
    .withConverter(userDataConverter)
    .set(user);
  return user;
}

export async function getUser(
  firestore: Firestore,
  userID: string,
): Promise<User> {
  const userDoc = await firestore
    .doc(userPath(userID))
    .withConverter(userDataConverter)
    .get();
  return must(userDoc.data());
}

export async function setTeam(
  firestore: Firestore,
  teamID: string,
  team: Partial<Team>,
): Promise<Team> {
  const {
    name = `Name of ${teamID}`,
    label = sanitizeForLabel(name),
    defaultCfID = 'default-cloudflare-id',
    numAdmins = 0,
    numMembers = 0,
    numInvites = 0,
    numApps = 0,
    maxApps = null,
  } = team;
  const newTeam: Team = {
    name,
    label,
    defaultCfID,
    numAdmins,
    numMembers,
    numInvites,
    numApps,
    maxApps,
  };
  await firestore
    .doc(teamPath(teamID))
    .withConverter(teamDataConverter)
    // Work around bug in
    .set(newTeam, {merge: true});
  return newTeam;
}

export async function getTeam(
  firestore: Firestore,
  teamID: string,
): Promise<Team> {
  const teamDoc = await firestore
    .doc(teamPath(teamID))
    .withConverter(teamDataConverter)
    .get();
  return must(teamDoc.data());
}

export async function setMembership(
  firestore: Firestore,
  teamID: string,
  userID: string,
  email: string,
  role: Role,
): Promise<Membership> {
  const membership: Membership = {
    email,
    role,
  };
  await firestore
    .doc(teamMembershipPath(teamID, userID))
    .withConverter(membershipDataConverter)
    .set(membership);
  return membership;
}

export async function getMembership(
  firestore: Firestore,
  teamID: string,
  userID: string,
): Promise<Membership> {
  const membershipDoc = await firestore
    .doc(teamMembershipPath(teamID, userID))
    .withConverter(membershipDataConverter)
    .get();
  return must(membershipDoc.data());
}

export async function getApp(
  firestore: Firestore,
  appID: string,
): Promise<App> {
  const appDoc = await firestore
    .doc(appPath(appID))
    .withConverter(appDataConverter)
    .get();
  return must(appDoc.data());
}

export async function setApp(
  firestore: Firestore,
  appID: string,
  app: Partial<App>,
): Promise<App> {
  const {
    name = `Name of ${appID}`,
    teamID = 'team-id',
    teamLabel = 'teamlabel',
    cfID = 'default-cloudflare-id',
    cfScriptName = 'cf-script-name',
    serverReleaseChannel = 'stable',
  } = app;
  const newApp: App = {
    name,
    teamID,
    teamLabel,
    cfID,
    cfScriptName,
    serverReleaseChannel,
    deploymentOptions: defaultOptions(),
  };
  await firestore
    .doc(appPath(appID))
    .withConverter(appDataConverter)
    .set(newApp);
  return newApp;
}

export async function getAppName(
  firestore: Firestore,
  teamID: string,
  appName: string,
): Promise<AppNameIndex> {
  const appNameDoc = await firestore
    .doc(appNameIndexPath(teamID, appName))
    .withConverter(appNameIndexDataConverter)
    .get();
  return must(appNameDoc.data());
}

export async function setAppName(
  firestore: Firestore,
  teamID: string,
  appID: string,
  name: NamedCurve,
): Promise<void> {
  await firestore
    .doc(appNameIndexPath(teamID, name))
    .withConverter(appNameIndexDataConverter)
    .set({appID});
}

export function dummySecrets(): DeploymentSecrets {
  return {
    /* eslint-disable @typescript-eslint/naming-convention */
    REFLECT_AUTH_API_KEY: 'dummy1',
    DATADOG_LOGS_API_KEY: 'dummy2',
    DATADOG_METRICS_API_KEY: 'dummy3',
    /* eslint-enable @typescript-eslint/naming-convention */
  };
}
