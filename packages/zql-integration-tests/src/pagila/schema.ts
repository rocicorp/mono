import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';

// =============================================================================
// Table definitions
// =============================================================================

// Reference tables
const language = table('language')
  .columns({
    id: number().from('language_id'),
    name: string(),
    lastUpdate: number().from('last_update'),
  })
  .primaryKey('id');

const country = table('country')
  .columns({
    id: number().from('country_id'),
    country: string(),
    lastUpdate: number().from('last_update'),
  })
  .primaryKey('id');

const city = table('city')
  .columns({
    id: number().from('city_id'),
    city: string(),
    countryId: number().from('country_id'),
    lastUpdate: number().from('last_update'),
  })
  .primaryKey('id');

const address = table('address')
  .columns({
    id: number().from('address_id'),
    address: string(),
    address2: string().optional(),
    district: string(),
    cityId: number().from('city_id'),
    postalCode: string().optional().from('postal_code'),
    phone: string(),
    lastUpdate: number().from('last_update'),
  })
  .primaryKey('id');

// Content tables
const category = table('category')
  .columns({
    id: number().from('category_id'),
    name: string(),
    lastUpdate: number().from('last_update'),
  })
  .primaryKey('id');

const actor = table('actor')
  .columns({
    id: number().from('actor_id'),
    firstName: string().from('first_name'),
    lastName: string().from('last_name'),
    lastUpdate: number().from('last_update'),
  })
  .primaryKey('id');

const film = table('film')
  .columns({
    id: number().from('film_id'),
    title: string(),
    description: string().optional(),
    releaseYear: number().optional().from('release_year'),
    languageId: number().from('language_id'),
    originalLanguageId: number().optional().from('original_language_id'),
    rentalDuration: number().from('rental_duration'),
    rentalRate: number().from('rental_rate'),
    length: number().optional(),
    replacementCost: number().from('replacement_cost'),
    rating: string().optional(), // mpaa_rating enum → string
    lastUpdate: number().from('last_update'),
    // Note: special_features (text[]) and fulltext (tsvector) are excluded
  })
  .primaryKey('id');

// Junction tables
const filmActor = table('filmActor')
  .from('film_actor')
  .columns({
    actorId: number().from('actor_id'),
    filmId: number().from('film_id'),
    lastUpdate: number().from('last_update'),
  })
  .primaryKey('actorId', 'filmId');

const filmCategory = table('filmCategory')
  .from('film_category')
  .columns({
    filmId: number().from('film_id'),
    categoryId: number().from('category_id'),
    lastUpdate: number().from('last_update'),
  })
  .primaryKey('filmId', 'categoryId');

// Business tables
const store = table('store')
  .columns({
    id: number().from('store_id'),
    managerStaffId: number().from('manager_staff_id'),
    addressId: number().from('address_id'),
    lastUpdate: number().from('last_update'),
  })
  .primaryKey('id');

const staff = table('staff')
  .columns({
    id: number().from('staff_id'),
    firstName: string().from('first_name'),
    lastName: string().from('last_name'),
    addressId: number().from('address_id'),
    email: string().optional(),
    storeId: number().from('store_id'),
    active: boolean(),
    username: string(),
    password: string().optional(),
    lastUpdate: number().from('last_update'),
    // Note: picture (bytea) is excluded
  })
  .primaryKey('id');

const customer = table('customer')
  .columns({
    id: number().from('customer_id'),
    storeId: number().from('store_id'),
    firstName: string().from('first_name'),
    lastName: string().from('last_name'),
    email: string().optional(),
    addressId: number().from('address_id'),
    activebool: boolean(),
    createDate: number().from('create_date'),
    lastUpdate: number().optional().from('last_update'),
    active: number().optional(),
  })
  .primaryKey('id');

// Inventory and rental tables
const inventory = table('inventory')
  .columns({
    id: number().from('inventory_id'),
    filmId: number().from('film_id'),
    storeId: number().from('store_id'),
    lastUpdate: number().from('last_update'),
  })
  .primaryKey('id');

const rental = table('rental')
  .columns({
    id: number().from('rental_id'),
    rentalDate: number().from('rental_date'),
    inventoryId: number().from('inventory_id'),
    customerId: number().from('customer_id'),
    returnDate: number().optional().from('return_date'),
    staffId: number().from('staff_id'),
    lastUpdate: number().from('last_update'),
  })
  .primaryKey('id');

const payment = table('payment')
  .columns({
    id: number().from('payment_id'),
    customerId: number().from('customer_id'),
    staffId: number().from('staff_id'),
    rentalId: number().optional().from('rental_id'),
    amount: number(),
    paymentDate: number().from('payment_date'),
  })
  .primaryKey('paymentDate', 'id');

// =============================================================================
// Relationships
// =============================================================================

// Geographic hierarchy: address → city → country
const countryRelationships = relationships(country, ({many}) => ({
  cities: many({
    sourceField: ['id'],
    destField: ['countryId'],
    destSchema: city,
  }),
}));

const cityRelationships = relationships(city, ({one, many}) => ({
  country: one({
    sourceField: ['countryId'],
    destField: ['id'],
    destSchema: country,
  }),
  addresses: many({
    sourceField: ['id'],
    destField: ['cityId'],
    destSchema: address,
  }),
}));

const addressRelationships = relationships(address, ({one, many}) => ({
  city: one({
    sourceField: ['cityId'],
    destField: ['id'],
    destSchema: city,
  }),
  customers: many({
    sourceField: ['id'],
    destField: ['addressId'],
    destSchema: customer,
  }),
  staff: many({
    sourceField: ['id'],
    destField: ['addressId'],
    destSchema: staff,
  }),
  stores: many({
    sourceField: ['id'],
    destField: ['addressId'],
    destSchema: store,
  }),
}));

// Film relationships
const languageRelationships = relationships(language, ({many}) => ({
  films: many({
    sourceField: ['id'],
    destField: ['languageId'],
    destSchema: film,
  }),
  filmsOriginal: many({
    sourceField: ['id'],
    destField: ['originalLanguageId'],
    destSchema: film,
  }),
}));

const filmRelationships = relationships(film, ({one, many}) => ({
  language: one({
    sourceField: ['languageId'],
    destField: ['id'],
    destSchema: language,
  }),
  originalLanguage: one({
    sourceField: ['originalLanguageId'],
    destField: ['id'],
    destSchema: language,
  }),
  // Many-to-many: film → actors via film_actor
  actors: many(
    {
      sourceField: ['id'],
      destField: ['filmId'],
      destSchema: filmActor,
    },
    {
      sourceField: ['actorId'],
      destField: ['id'],
      destSchema: actor,
    },
  ),
  // Many-to-many: film → categories via film_category
  categories: many(
    {
      sourceField: ['id'],
      destField: ['filmId'],
      destSchema: filmCategory,
    },
    {
      sourceField: ['categoryId'],
      destField: ['id'],
      destSchema: category,
    },
  ),
  inventory: many({
    sourceField: ['id'],
    destField: ['filmId'],
    destSchema: inventory,
  }),
  filmActorJunction: many({
    sourceField: ['id'],
    destField: ['filmId'],
    destSchema: filmActor,
  }),
  filmCategoryJunction: many({
    sourceField: ['id'],
    destField: ['filmId'],
    destSchema: filmCategory,
  }),
}));

const actorRelationships = relationships(actor, ({many}) => ({
  // Many-to-many: actor → films via film_actor
  films: many(
    {
      sourceField: ['id'],
      destField: ['actorId'],
      destSchema: filmActor,
    },
    {
      sourceField: ['filmId'],
      destField: ['id'],
      destSchema: film,
    },
  ),
  filmActorJunction: many({
    sourceField: ['id'],
    destField: ['actorId'],
    destSchema: filmActor,
  }),
}));

const categoryRelationships = relationships(category, ({many}) => ({
  // Many-to-many: category → films via film_category
  films: many(
    {
      sourceField: ['id'],
      destField: ['categoryId'],
      destSchema: filmCategory,
    },
    {
      sourceField: ['filmId'],
      destField: ['id'],
      destSchema: film,
    },
  ),
  filmCategoryJunction: many({
    sourceField: ['id'],
    destField: ['categoryId'],
    destSchema: filmCategory,
  }),
}));

// Store and staff relationships
const storeRelationships = relationships(store, ({one, many}) => ({
  address: one({
    sourceField: ['addressId'],
    destField: ['id'],
    destSchema: address,
  }),
  manager: one({
    sourceField: ['managerStaffId'],
    destField: ['id'],
    destSchema: staff,
  }),
  staff: many({
    sourceField: ['id'],
    destField: ['storeId'],
    destSchema: staff,
  }),
  customers: many({
    sourceField: ['id'],
    destField: ['storeId'],
    destSchema: customer,
  }),
  inventory: many({
    sourceField: ['id'],
    destField: ['storeId'],
    destSchema: inventory,
  }),
}));

const staffRelationships = relationships(staff, ({one, many}) => ({
  address: one({
    sourceField: ['addressId'],
    destField: ['id'],
    destSchema: address,
  }),
  store: one({
    sourceField: ['storeId'],
    destField: ['id'],
    destSchema: store,
  }),
  rentals: many({
    sourceField: ['id'],
    destField: ['staffId'],
    destSchema: rental,
  }),
  payments: many({
    sourceField: ['id'],
    destField: ['staffId'],
    destSchema: payment,
  }),
}));

// Customer relationships
const customerRelationships = relationships(customer, ({one, many}) => ({
  address: one({
    sourceField: ['addressId'],
    destField: ['id'],
    destSchema: address,
  }),
  store: one({
    sourceField: ['storeId'],
    destField: ['id'],
    destSchema: store,
  }),
  rentals: many({
    sourceField: ['id'],
    destField: ['customerId'],
    destSchema: rental,
  }),
  payments: many({
    sourceField: ['id'],
    destField: ['customerId'],
    destSchema: payment,
  }),
}));

// Inventory relationships
const inventoryRelationships = relationships(inventory, ({one, many}) => ({
  film: one({
    sourceField: ['filmId'],
    destField: ['id'],
    destSchema: film,
  }),
  store: one({
    sourceField: ['storeId'],
    destField: ['id'],
    destSchema: store,
  }),
  rentals: many({
    sourceField: ['id'],
    destField: ['inventoryId'],
    destSchema: rental,
  }),
}));

// Rental relationships
const rentalRelationships = relationships(rental, ({one, many}) => ({
  inventory: one({
    sourceField: ['inventoryId'],
    destField: ['id'],
    destSchema: inventory,
  }),
  customer: one({
    sourceField: ['customerId'],
    destField: ['id'],
    destSchema: customer,
  }),
  staff: one({
    sourceField: ['staffId'],
    destField: ['id'],
    destSchema: staff,
  }),
  payments: many({
    sourceField: ['id'],
    destField: ['rentalId'],
    destSchema: payment,
  }),
  // Multi-hop: rental → inventory → film
  film: one(
    {
      sourceField: ['inventoryId'],
      destField: ['id'],
      destSchema: inventory,
    },
    {
      sourceField: ['filmId'],
      destField: ['id'],
      destSchema: film,
    },
  ),
}));

// Payment relationships
const paymentRelationships = relationships(payment, ({one}) => ({
  customer: one({
    sourceField: ['customerId'],
    destField: ['id'],
    destSchema: customer,
  }),
  staff: one({
    sourceField: ['staffId'],
    destField: ['id'],
    destSchema: staff,
  }),
  rental: one({
    sourceField: ['rentalId'],
    destField: ['id'],
    destSchema: rental,
  }),
  // Note: Multi-hop payment → rental → inventory → film exceeds 2 hops,
  // so we navigate through rental.inventory.film instead
}));

// =============================================================================
// Schema export
// =============================================================================

export const schema = createSchema({
  tables: [
    // Reference tables
    language,
    country,
    city,
    address,
    // Content tables
    category,
    actor,
    film,
    // Junction tables
    filmActor,
    filmCategory,
    // Business tables
    store,
    staff,
    customer,
    // Transaction tables
    inventory,
    rental,
    payment,
  ],
  relationships: [
    // Geographic
    countryRelationships,
    cityRelationships,
    addressRelationships,
    // Content
    languageRelationships,
    filmRelationships,
    actorRelationships,
    categoryRelationships,
    // Business
    storeRelationships,
    staffRelationships,
    customerRelationships,
    // Transactions
    inventoryRelationships,
    rentalRelationships,
    paymentRelationships,
  ],
});

export const builder = createBuilder(schema);
