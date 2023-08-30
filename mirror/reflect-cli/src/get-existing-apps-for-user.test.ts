import {expect, test} from '@jest/globals';
import {
  fakeFirestore,
  setApp,
  setTeam,
  setUser,
} from 'mirror-schema/src/test-helpers.js';
import {defaultOptions} from 'mirror-schema/src/deployment.js';
import {getExistingAppsForUser} from './get-existing-apps-for-user.js';

test('list when no team', async () => {
  const firestore = fakeFirestore();
  const userID = 'foo';
  const email = 'foo@bar.com';
  await setUser(firestore, userID, email, 'Foo Bar', {});

  const apps = await getExistingAppsForUser(firestore, userID);

  expect(apps).toEqual([]);
});

test('list when missing team', async () => {
  const firestore = fakeFirestore();
  const userID = 'foo';
  const email = 'foo@bar.com';
  const teamID = 'fooTeam';
  await setUser(firestore, userID, email, 'Foo Bar', {[teamID]: 'admin'});

  const apps = await getExistingAppsForUser(firestore, userID);

  expect(apps).toEqual([]);
});

test('list with one team but no apps', async () => {
  const firestore = fakeFirestore();
  const userID = 'foo';
  const email = 'foo@bar.com';
  const teamID = 'fooTeam';
  await setUser(firestore, userID, email, 'Foo Bar', {[teamID]: 'admin'});
  await setTeam(firestore, teamID, {name: 'Foo Team'});

  const apps = await getExistingAppsForUser(firestore, userID);

  expect(apps).toEqual([]);
});

test('list with multiple teams but no apps', async () => {
  const firestore = fakeFirestore();
  const userID = 'foo';
  const email = 'foo@bar.com';
  const teamID1 = 'team-1';
  const teamID2 = 'team-2';
  await setUser(firestore, userID, email, 'Foo Bar', {
    [teamID1]: 'admin',
    [teamID2]: 'member',
  });
  await setTeam(firestore, teamID1, {name: 'Team 1'});
  await setTeam(firestore, teamID2, {name: 'Team 2'});

  const apps = await getExistingAppsForUser(firestore, userID);

  expect(apps).toEqual([]);
});

test('list with one teams and one app', async () => {
  const firestore = fakeFirestore();
  const userID = 'foo';
  const email = 'foo@bar.com';
  const teamID = 'team-id';
  const teamSubdomain = 'team-sub';

  await setUser(firestore, userID, email, 'Foo Bar', {
    [teamID]: 'admin',
  });
  await setTeam(firestore, teamID, {name: 'Team Name'});
  await setApp(firestore, 'app-id', {teamID, teamSubdomain});

  const apps = await getExistingAppsForUser(firestore, userID);

  expect(apps).toEqual([
    {
      appID: 'app-id',
      cfID: 'default-cloudflare-id',
      cfScriptName: 'cf-script-name',
      name: 'Name of app-id',
      serverReleaseChannel: 'stable',
      teamID,
      teamSubdomain,
      deploymentOptions: defaultOptions(),
    },
  ]);
});

test('list with one teams and two apps', async () => {
  const firestore = fakeFirestore();
  const userID = 'foo';
  const email = 'foo@bar.com';
  const teamID = 'team-id';
  const teamSubdomain = 'team-sub';

  await setUser(firestore, userID, email, 'Foo Bar', {
    [teamID]: 'admin',
  });
  await setTeam(firestore, teamID, {name: 'Team Name'});
  await setApp(firestore, 'app-id-1', {teamID, teamSubdomain});
  await setApp(firestore, 'app-id-2', {
    teamID,
    teamSubdomain,
    serverReleaseChannel: 'canary',
  });

  const apps = await getExistingAppsForUser(firestore, userID);

  expect(apps).toEqual([
    {
      appID: 'app-id-1',
      cfID: 'default-cloudflare-id',
      cfScriptName: 'cf-script-name',
      name: 'Name of app-id-1',
      serverReleaseChannel: 'stable',
      teamID,
      teamSubdomain,
      deploymentOptions: defaultOptions(),
    },
    {
      appID: 'app-id-2',
      cfID: 'default-cloudflare-id',
      cfScriptName: 'cf-script-name',
      name: 'Name of app-id-2',
      serverReleaseChannel: 'canary',
      teamID,
      teamSubdomain,
      deploymentOptions: defaultOptions(),
    },
  ]);
});

test('list with two teams and two apps total', async () => {
  const firestore = fakeFirestore();
  const userID = 'foo';
  const email = 'foo@bar.com';
  const teamID1 = 'team-id-1';
  const teamSubdomain1 = 'team-sub-1';
  const teamID2 = 'team-id-2';
  const teamSubdomain2 = 'team-sub-2';

  await setUser(firestore, userID, email, 'Foo Bar', {
    [teamID1]: 'admin',
    [teamID2]: 'member',
  });
  await setTeam(firestore, teamID1, {name: 'Team Name 1'});
  await setTeam(firestore, teamID2, {name: 'Team Name 2'});
  await setApp(firestore, 'app-id-1', {
    teamID: teamID1,
    teamSubdomain: teamSubdomain1,
  });
  await setApp(firestore, 'app-id-2', {
    teamID: teamID2,
    teamSubdomain: teamSubdomain2,
    serverReleaseChannel: 'canary',
  });

  const apps = await getExistingAppsForUser(firestore, userID);

  expect(apps).toEqual([
    {
      appID: 'app-id-1',
      cfID: 'default-cloudflare-id',
      cfScriptName: 'cf-script-name',
      name: 'Name of app-id-1',
      serverReleaseChannel: 'stable',
      teamID: teamID1,
      teamSubdomain: teamSubdomain1,
      deploymentOptions: defaultOptions(),
    },
    {
      appID: 'app-id-2',
      cfID: 'default-cloudflare-id',
      cfScriptName: 'cf-script-name',
      name: 'Name of app-id-2',
      serverReleaseChannel: 'canary',
      teamID: teamID2,
      teamSubdomain: teamSubdomain2,
      deploymentOptions: defaultOptions(),
    },
  ]);
});

test('list with two teams and 4 apps total', async () => {
  const firestore = fakeFirestore();
  const userID = 'foo';
  const email = 'foo@bar.com';
  const teamID1 = 'team-id-1';
  const teamSubdomain1 = 'team-sub-1';
  const teamID2 = 'team-id-2';
  const teamSubdomain2 = 'team-sub-2';

  await setUser(firestore, userID, email, 'Foo Bar', {
    [teamID1]: 'admin',
    [teamID2]: 'member',
  });
  await setTeam(firestore, teamID1, {name: 'Team Name 1'});
  await setTeam(firestore, teamID2, {name: 'Team Name 2'});
  await setApp(firestore, 'app-id-1', {
    teamID: teamID1,
    teamSubdomain: teamSubdomain1,
  });
  await setApp(firestore, 'app-id-2', {
    teamID: teamID2,
    teamSubdomain: teamSubdomain2,
    serverReleaseChannel: 'canary',
  });
  await setApp(firestore, 'app-id-3', {
    teamID: teamID1,
    teamSubdomain: teamSubdomain1,
  });
  await setApp(firestore, 'app-id-4', {
    teamID: teamID2,
    teamSubdomain: teamSubdomain2,
    serverReleaseChannel: 'canary',
  });

  const apps = await getExistingAppsForUser(firestore, userID);

  expect(apps).toEqual([
    {
      appID: 'app-id-1',
      cfID: 'default-cloudflare-id',
      cfScriptName: 'cf-script-name',
      name: 'Name of app-id-1',
      serverReleaseChannel: 'stable',
      teamID: teamID1,
      teamSubdomain: teamSubdomain1,
      deploymentOptions: defaultOptions(),
    },
    {
      appID: 'app-id-2',
      cfID: 'default-cloudflare-id',
      cfScriptName: 'cf-script-name',
      name: 'Name of app-id-2',
      serverReleaseChannel: 'canary',
      teamID: teamID2,
      teamSubdomain: teamSubdomain2,
      deploymentOptions: defaultOptions(),
    },
    {
      appID: 'app-id-3',
      cfID: 'default-cloudflare-id',
      cfScriptName: 'cf-script-name',
      name: 'Name of app-id-3',
      serverReleaseChannel: 'stable',
      teamID: teamID1,
      teamSubdomain: teamSubdomain1,
      deploymentOptions: defaultOptions(),
    },
    {
      appID: 'app-id-4',
      cfID: 'default-cloudflare-id',
      cfScriptName: 'cf-script-name',
      name: 'Name of app-id-4',
      serverReleaseChannel: 'canary',
      teamID: teamID2,
      teamSubdomain: teamSubdomain2,
      deploymentOptions: defaultOptions(),
    },
  ]);
});
