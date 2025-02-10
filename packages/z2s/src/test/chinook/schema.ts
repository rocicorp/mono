/* eslint-disable @typescript-eslint/naming-convention */
import {relationships} from '../../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';

// auto-generated from `Chinook_PostgreSql.sql` by Claude
// Table definitions
const album = table('album')
  .columns({
    album_id: number(),
    title: string(),
    artist_id: number(),
  })
  .primaryKey('album_id');

const artist = table('artist')
  .columns({
    artist_id: number(),
    name: string().optional(),
  })
  .primaryKey('artist_id');

const customer = table('customer')
  .columns({
    customer_id: number(),
    first_name: string(),
    last_name: string(),
    company: string().optional(),
    address: string().optional(),
    city: string().optional(),
    state: string().optional(),
    country: string().optional(),
    postal_code: string().optional(),
    phone: string().optional(),
    fax: string().optional(),
    email: string(),
    support_rep_id: number().optional(),
  })
  .primaryKey('customer_id');

const employee = table('employee')
  .columns({
    employee_id: number(),
    last_name: string(),
    first_name: string(),
    title: string().optional(),
    reports_to: number().optional(),
    birth_date: number().optional(), // TIMESTAMP as number
    hire_date: number().optional(), // TIMESTAMP as number
    address: string().optional(),
    city: string().optional(),
    state: string().optional(),
    country: string().optional(),
    postal_code: string().optional(),
    phone: string().optional(),
    fax: string().optional(),
    email: string().optional(),
  })
  .primaryKey('employee_id');

const genre = table('genre')
  .columns({
    genre_id: number(),
    name: string().optional(),
  })
  .primaryKey('genre_id');

const invoice = table('invoice')
  .columns({
    invoice_id: number(),
    customer_id: number(),
    invoice_date: number(), // TIMESTAMP as number
    billing_address: string().optional(),
    billing_city: string().optional(),
    billing_state: string().optional(),
    billing_country: string().optional(),
    billing_postal_code: string().optional(),
    total: number(),
  })
  .primaryKey('invoice_id');

const invoice_line = table('invoice_line')
  .columns({
    invoice_line_id: number(),
    invoice_id: number(),
    track_id: number(),
    unit_price: number(),
    quantity: number(),
  })
  .primaryKey('invoice_line_id');

const media_type = table('media_type')
  .columns({
    media_type_id: number(),
    name: string().optional(),
  })
  .primaryKey('media_type_id');

const playlist = table('playlist')
  .columns({
    playlist_id: number(),
    name: string().optional(),
  })
  .primaryKey('playlist_id');

const playlist_track = table('playlist_track')
  .columns({
    playlist_id: number(),
    track_id: number(),
  })
  .primaryKey('playlist_id', 'track_id');

const track = table('track')
  .columns({
    track_id: number(),
    name: string(),
    album_id: number().optional(),
    media_type_id: number(),
    genre_id: number().optional(),
    composer: string().optional(),
    milliseconds: number(),
    bytes: number().optional(),
    unit_price: number(),
  })
  .primaryKey('track_id');

// Relationships
const albumRelationships = relationships(album, ({one}) => ({
  artist: one({
    sourceField: ['artist_id'],
    destField: ['artist_id'],
    destSchema: artist,
  }),
}));

const customerRelationships = relationships(customer, ({one}) => ({
  supportRep: one({
    sourceField: ['support_rep_id'],
    destField: ['employee_id'],
    destSchema: employee,
  }),
}));

const employeeRelationships = relationships(employee, ({one}) => ({
  reportsTo: one({
    sourceField: ['reports_to'],
    destField: ['employee_id'],
    destSchema: employee,
  }),
}));

const invoiceRelationships = relationships(invoice, ({one}) => ({
  customer: one({
    sourceField: ['customer_id'],
    destField: ['customer_id'],
    destSchema: customer,
  }),
}));

const trackRelationships = relationships(track, ({one, many}) => ({
  album: one({
    sourceField: ['album_id'],
    destField: ['album_id'],
    destSchema: album,
  }),
  genre: one({
    sourceField: ['genre_id'],
    destField: ['genre_id'],
    destSchema: genre,
  }),
  mediaType: one({
    sourceField: ['media_type_id'],
    destField: ['media_type_id'],
    destSchema: media_type,
  }),
  playlists: many(
    {
      sourceField: ['track_id'],
      destField: ['track_id'],
      destSchema: playlist_track,
    },
    {
      sourceField: ['playlist_id'],
      destField: ['playlist_id'],
      destSchema: playlist,
    },
  ),
}));

const playlistRelationships = relationships(playlist, ({many}) => ({
  tracks: many(
    {
      sourceField: ['playlist_id'],
      destField: ['playlist_id'],
      destSchema: playlist_track,
    },
    {
      sourceField: ['track_id'],
      destField: ['track_id'],
      destSchema: track,
    },
  ),
}));

export const schema = createSchema(1, {
  tables: [
    album,
    artist,
    customer,
    employee,
    genre,
    invoice,
    invoice_line,
    media_type,
    playlist,
    playlist_track,
    track,
  ],
  relationships: [
    albumRelationships,
    customerRelationships,
    employeeRelationships,
    invoiceRelationships,
    trackRelationships,
    playlistRelationships,
  ],
});
