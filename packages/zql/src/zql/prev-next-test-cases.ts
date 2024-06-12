export const cases = [
  {
    name: 'overlap title completely, overlap length partially.',
    tracks: [
      {
        title: 'a',
        length: 0,
        albumId: '001',
        id: '001',
      },
      {
        title: 'a',
        length: 0,
        albumId: '001',
        id: '002',
      },
      {
        title: 'a',
        length: 0,
        albumId: '001',
        id: '003',
      },
      {
        title: 'a',
        length: 1,
        albumId: '001',
        id: '004',
      },
      {
        title: 'a',
        length: 1,
        albumId: '001',
        id: '005',
      },
    ],
  },
  {
    name: 'overlap title completely, overlap length completely.',
    tracks: [
      {
        title: 'a',
        length: 0,
        albumId: '001',
        id: '001',
      },
      {
        title: 'a',
        length: 0,
        albumId: '001',
        id: '002',
      },
      {
        title: 'a',
        length: 0,
        albumId: '001',
        id: '003',
      },
      {
        title: 'a',
        length: 0,
        albumId: '001',
        id: '004',
      },
      {
        title: 'a',
        length: 0,
        albumId: '001',
        id: '005',
      },
    ],
  },
  {
    name: 'overlap title partially, overlap length completely.',
    tracks: [
      {
        title: 'a',
        length: 0,
        albumId: '001',
        id: '001',
      },
      {
        title: 'a',
        length: 0,
        albumId: '001',
        id: '002',
      },
      {
        title: 'a',
        length: 0,
        albumId: '001',
        id: '003',
      },
      {
        title: 'b',
        length: 0,
        albumId: '001',
        id: '004',
      },
      {
        title: 'b',
        length: 0,
        albumId: '001',
        id: '005',
      },
    ],
  },
  {
    name: 'overlap title partially, overlap length partially.',
    tracks: [
      {
        title: 'a',
        length: 0,
        albumId: '001',
        id: '001',
      },
      {
        title: 'a',
        length: 0,
        albumId: '001',
        id: '002',
      },
      {
        title: 'a',
        length: 1,
        albumId: '001',
        id: '003',
      },
      {
        title: 'a',
        length: 1,
        albumId: '001',
        id: '004',
      },
      {
        title: 'b',
        length: 1,
        albumId: '001',
        id: '005',
      },
    ],
  },
  {
    name: 'no overlap',
    tracks: [
      {
        title: 'a',
        length: 1,
        albumId: '001',
        id: '001',
      },
      {
        title: 'b',
        length: 2,
        albumId: '001',
        id: '002',
      },
      {
        title: 'c',
        length: 3,
        albumId: '001',
        id: '003',
      },
      {
        title: 'd',
        length: 4,
        albumId: '001',
        id: '004',
      },
      {
        title: 'e',
        length: 5,
        albumId: '001',
        id: '005',
      },
    ],
  },
] as const;
