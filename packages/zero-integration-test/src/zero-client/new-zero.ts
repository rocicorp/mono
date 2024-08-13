import {QueryDefs, Zero} from 'zero-client';
import {QueryParseDefs} from 'zero-client/src/client/options.js';
import {nanoid} from 'zero-client/src/util/nanoid.js';
import type {
  Album,
  Artist,
  Playlist,
  Track,
  TrackArtist,
} from 'zql/src/zql/test-helpers/create-data.js';

export function newZero<QD extends QueryDefs>(
  queries: QueryParseDefs<QD>,
): Zero<QD> {
  const z = new Zero({
    userID: 'user-' + nanoid(),
    queries,
  });
  return z;
}

export type Z = ReturnType<typeof newZero>;

export async function bulkSet(
  z: Z,
  items: {
    tracks?: readonly Track[] | undefined;
    albums?: readonly Album[] | undefined;
    artists?: readonly Artist[] | undefined;
    playlists?: readonly Playlist[] | undefined;
    trackArtists?: readonly TrackArtist[] | undefined;
  },
) {
  const promises: Promise<void>[] = [];
  await z.mutate(async tx => {
    for (const track of items.tracks ?? []) {
      promises.push(tx.track.create(track));
    }
    for (const album of items.albums ?? []) {
      promises.push(tx.album.create(album));
    }
    for (const artist of items.artists ?? []) {
      promises.push(tx.artist.create(artist));
    }
    for (const playlist of items.playlists ?? []) {
      promises.push(tx.playlist.create(playlist));
    }
    for (const trackArtist of items.trackArtists ?? []) {
      promises.push(tx.trackArtist.create(trackArtist));
    }
    await Promise.all(promises);
  });
}

export async function bulkRemove(
  z: Z,
  items: {
    tracks?: Track[] | undefined;
    albums?: Album[] | undefined;
    artists?: Artist[] | undefined;
    playlists?: Playlist[] | undefined;
    trackArtists?: TrackArtist[] | undefined;
  },
) {
  await z.mutate(async tx => {
    const promises: Promise<void>[] = [];
    for (const track of items.tracks ?? []) {
      promises.push(tx.track.delete({id: track.id}));
    }
    for (const album of items.albums ?? []) {
      promises.push(tx.album.delete(album));
    }
    for (const artist of items.artists ?? []) {
      promises.push(tx.artist.delete(artist));
    }
    for (const playlist of items.playlists ?? []) {
      promises.push(tx.playlist.delete(playlist));
    }
    for (const trackArtist of items.trackArtists ?? []) {
      promises.push(tx.trackArtist.delete(trackArtist));
    }
    await Promise.all(promises);
  });
}
