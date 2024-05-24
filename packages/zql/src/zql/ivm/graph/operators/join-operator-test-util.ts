import {expect, vi} from 'vitest';
import {DifferenceStream} from '../difference-stream.js';
import {createPullResponseMessage, PullMsg} from '../message.js';

export type Track = {
  id: string;
  title: string;
  length: number;
  albumId: string;
};

export type Album = {
  id: string;
  title: string;
  artistId: string;
};

export type Artist = {
  id: string;
  name: string;
};

export type Playlist = {
  id: string;
  name: string;
};

export type TrackArtist = {
  id: `${TrackArtist['trackId']}-${TrackArtist['artistId']}`;
  trackId: string;
  artistId: string;
};

export type PlaylistTrack = {
  id: `${PlaylistTrack['playlistId']}-${PlaylistTrack['trackId']}`;
  playlistId: string;
  trackId: string;
  position: number;
};

export function orderIsRemovedFromRequest(join: 'leftJoin' | 'join') {
  const trackInput = new DifferenceStream<Track>();
  const albumInput = new DifferenceStream<Album>();
  const output = trackInput[join](
    {
      aTable: 'track',
      aPrimaryKeyColumns: ['id'],
      aJoinColumn: ['track', 'albumId'],

      b: albumInput,
      bTable: 'album',
      bAs: 'album',
      bPrimaryKeyColumns: ['id'],
      bJoinColumn: ['album', 'id'],
    },
    undefined,
  );

  const trackInputSpy = vi.spyOn(trackInput, 'messageUpstream');
  const albumInputSpy = vi.spyOn(albumInput, 'messageUpstream');

  const msg: PullMsg = {
    id: 1,
    hoistedConditions: [],
    type: 'pull',
    order: [[['intentional-nonsense', 'x']], 'asc'],
  };
  const listener = {
    commit() {},
    newDifference() {},
  };
  output.messageUpstream(msg, listener);

  expect(trackInputSpy).toHaveBeenCalledOnce();
  expect(albumInputSpy).toHaveBeenCalledOnce();

  expect(trackInputSpy.mock.calls[0][0]).toEqual(msg);
  expect(albumInputSpy.mock.calls[0][0]).toEqual({...msg, order: undefined});
}

export function orderIsRemovedFromReply(join: 'leftJoin' | 'join') {
  const trackInput = new DifferenceStream<Track>();
  const albumInput = new DifferenceStream<Album>();
  const output = trackInput[join](
    {
      aTable: 'track',
      aPrimaryKeyColumns: ['id'],
      aJoinColumn: ['track', 'albumId'],

      b: albumInput,
      bTable: 'album',
      bAs: 'album',
      bPrimaryKeyColumns: ['id'],
      bJoinColumn: ['album', 'id'],
    },
    undefined,
  );

  const outputSpy = vi.spyOn(output, 'newDifference');
  const msg: PullMsg = {
    id: 1,
    hoistedConditions: [],
    type: 'pull',
    order: [[['intentional-nonsense', 'x']], 'asc'],
  };
  const listener = {
    commit() {},
    newDifference() {},
  };
  output.messageUpstream(msg, listener);
  const trackReply = createPullResponseMessage(msg, 'track', [
    [['track', 'id']],
    'asc',
  ]);
  const albumReply = createPullResponseMessage(msg, 'title', [
    [['title', 'id']],
    'asc',
  ]);

  trackInput.newDifference(1, [], trackReply);

  // join buffers until both replies are received.
  expect(outputSpy).toHaveBeenCalledTimes(0);

  albumInput.newDifference(1, [], albumReply);

  expect(outputSpy).toHaveBeenCalledTimes(1);
  expect(outputSpy.mock.calls[0][0]).toEqual(1);
  expect([...outputSpy.mock.calls[0][1]]).toEqual([]);
  expect(outputSpy.mock.calls[0][2]).toEqual(trackReply);
}
