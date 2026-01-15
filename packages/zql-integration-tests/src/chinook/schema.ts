import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';

// auto-generated from `Chinook_PostgreSql.sql` by Claude
// Table definitions
const album = table('album')
  .columns({
    id: number().from('album_id'),
    title: string(),
    artistId: number().from('artist_id'),
  })
  .primaryKey('id');

const artist = table('artist')
  .columns({
    id: number().from('artist_id'),
    name: string().optional(),
  })
  .primaryKey('id');

const customer = table('customer')
  .columns({
    id: number().from('customer_id'),
    firstName: string().from('first_name'),
    lastName: string().from('last_name'),
    company: string().optional(),
    address: string().optional(),
    city: string().optional(),
    state: string().optional(),
    country: string().optional(),
    postalCode: string().optional().from('postal_code'),
    phone: string().optional(),
    fax: string().optional(),
    email: string(),
    supportRepId: number().optional().from('support_rep_id'),
  })
  .primaryKey('id');

const employee = table('employee')
  .columns({
    id: number().from('employee_id'),
    lastName: string().from('last_name'),
    firstName: string().from('first_name'),
    title: string().optional(),
    reportsTo: number().optional().from('reports_to'),
    birthDate: number().optional().from('birth_date'),
    hireDate: number().optional().from('hire_date'),
    address: string().optional(),
    city: string().optional(),
    state: string().optional(),
    country: string().optional(),
    postalCode: string().optional().from('postal_code'),
    phone: string().optional(),
    fax: string().optional(),
    email: string().optional(),
  })
  .primaryKey('id');

const genre = table('genre')
  .columns({
    id: number().from('genre_id'),
    name: string().optional(),
  })
  .primaryKey('id');

const invoice = table('invoice')
  .columns({
    id: number().from('invoice_id'),
    customerId: number().from('customer_id'),
    invoiceDate: number().from('invoice_date'),
    billingAddress: string().optional().from('billing_address'),
    billingCity: string().optional().from('billing_city'),
    billingState: string().optional().from('billing_state'),
    billingCountry: string().optional().from('billing_country'),
    billingPostalCode: string().optional().from('billing_postal_code'),
    total: number(),
  })
  .primaryKey('id');

const invoiceLine = table('invoiceLine')
  .from('invoice_line')
  .columns({
    id: number().from('invoice_line_id'),
    invoiceId: number().from('invoice_id'),
    trackId: number().from('track_id'),
    unitPrice: number().from('unit_price'),
    quantity: number(),
  })
  .primaryKey('id');

const mediaType = table('mediaType')
  .from('media_type')
  .columns({
    id: number().from('media_type_id'),
    name: string().optional(),
  })
  .primaryKey('id');

const playlist = table('playlist')
  .columns({
    id: number().from('playlist_id'),
    name: string().optional(),
  })
  .primaryKey('id');

const playlistTrack = table('playlistTrack')
  .from('playlist_track')
  .columns({
    playlistId: number().from('playlist_id'),
    trackId: number().from('track_id'),
  })
  .primaryKey('playlistId', 'trackId');

const track = table('track')
  .columns({
    id: number().from('track_id'),
    name: string(),
    albumId: number().optional().from('album_id'),
    mediaTypeId: number().from('media_type_id'),
    genreId: number().optional().from('genre_id'),
    composer: string().optional(),
    milliseconds: number(),
    bytes: number().optional(),
    unitPrice: number().from('unit_price'),
  })
  .primaryKey('id');

// Relationships
const albumRelationships = relationships(album, ({one, many}) => ({
  artist: one({
    sourceField: ['artistId'],
    destField: ['id'],
    destSchema: artist,
  }),
  tracks: many({
    sourceField: ['id'],
    destField: ['albumId'],
    destSchema: track,
  }),
}));

const artistRelationships = relationships(artist, ({many}) => ({
  albums: many({
    sourceField: ['id'],
    destField: ['artistId'],
    destSchema: album,
  }),
}));

const invoiceLineRelationships = relationships(invoiceLine, ({one}) => ({
  track: one({
    sourceField: ['trackId'],
    destField: ['id'],
    destSchema: track,
  }),
  invoice: one({
    sourceField: ['invoiceId'],
    destField: ['id'],
    destSchema: invoice,
  }),
  customer: one(
    {
      sourceField: ['invoiceId'],
      destField: ['id'],
      destSchema: invoice,
    },
    {
      sourceField: ['customerId'],
      destField: ['id'],
      destSchema: customer,
    },
  ),
}));

const customerRelationships = relationships(customer, ({one}) => ({
  supportRep: one({
    sourceField: ['supportRepId'],
    destField: ['id'],
    destSchema: employee,
  }),
}));

const employeeRelationships = relationships(employee, ({one}) => ({
  reportsToEmployee: one({
    sourceField: ['reportsTo'],
    destField: ['id'],
    destSchema: employee,
  }),
}));

const invoiceRelationships = relationships(invoice, ({one, many}) => ({
  customer: one({
    sourceField: ['customerId'],
    destField: ['id'],
    destSchema: customer,
  }),
  lines: many({
    sourceField: ['id'],
    destField: ['invoiceId'],
    destSchema: invoiceLine,
  }),
}));

const trackRelationships = relationships(track, ({one, many}) => ({
  album: one({
    sourceField: ['albumId'],
    destField: ['id'],
    destSchema: album,
  }),
  genre: one({
    sourceField: ['genreId'],
    destField: ['id'],
    destSchema: genre,
  }),
  mediaType: one({
    sourceField: ['mediaTypeId'],
    destField: ['id'],
    destSchema: mediaType,
  }),
  playlists: many(
    {
      sourceField: ['id'],
      destField: ['trackId'],
      destSchema: playlistTrack,
    },
    {
      sourceField: ['playlistId'],
      destField: ['id'],
      destSchema: playlist,
    },
  ),
  playlistTrackJunction: many({
    sourceField: ['id'],
    destField: ['trackId'],
    destSchema: playlistTrack,
  }),
  invoiceLines: many({
    sourceField: ['id'],
    destField: ['trackId'],
    destSchema: invoiceLine,
  }),
}));

const playlistRelationships = relationships(playlist, ({many}) => ({
  tracks: many(
    {
      sourceField: ['id'],
      destField: ['playlistId'],
      destSchema: playlistTrack,
    },
    {
      sourceField: ['trackId'],
      destField: ['id'],
      destSchema: track,
    },
  ),
}));

const genreRelationships = relationships(genre, ({many}) => ({
  tracks: many({
    sourceField: ['id'],
    destField: ['genreId'],
    destSchema: track,
  }),
}));

const mediaTypeRelationships = relationships(mediaType, ({many}) => ({
  tracks: many({
    sourceField: ['id'],
    destField: ['mediaTypeId'],
    destSchema: track,
  }),
}));

export const schema = createSchema({
  tables: [
    album,
    artist,
    customer,
    employee,
    genre,
    invoice,
    invoiceLine,
    mediaType,
    playlist,
    playlistTrack,
    track,
  ],
  relationships: [
    albumRelationships,
    artistRelationships,
    customerRelationships,
    employeeRelationships,
    invoiceRelationships,
    invoiceLineRelationships,
    trackRelationships,
    playlistRelationships,
    genreRelationships,
    mediaTypeRelationships,
  ],
});

export const builder = createBuilder(schema);
