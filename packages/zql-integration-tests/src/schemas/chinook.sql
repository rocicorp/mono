
/*******************************************************************************
   Chinook Database - Version 1.4.5
   Script: Chinook_PostgreSql.sql
   Description: Creates and populates the Chinook database.
   DB Server: PostgreSql
   Author: Luis Rocha
   License: https://github.com/lerocha/chinook-database/blob/master/LICENSE.md
********************************************************************************/


/*******************************************************************************
   Create Tables
********************************************************************************/
CREATE TABLE album
(
    album_id INT NOT NULL,
    title VARCHAR(160) NOT NULL,
    artist_id INT NOT NULL,
    CONSTRAINT album_pkey PRIMARY KEY  (album_id)
);

CREATE TABLE artist
(
    artist_id INT NOT NULL,
    name VARCHAR(120),
    CONSTRAINT artist_pkey PRIMARY KEY  (artist_id)
);

CREATE TABLE customer
(
    customer_id INT NOT NULL,
    first_name VARCHAR(40) NOT NULL,
    last_name VARCHAR(20) NOT NULL,
    company VARCHAR(80),
    address VARCHAR(70),
    city VARCHAR(40),
    state VARCHAR(40),
    country VARCHAR(40),
    postal_code VARCHAR(10),
    phone VARCHAR(24),
    fax VARCHAR(24),
    email VARCHAR(60) NOT NULL,
    support_rep_id INT,
    CONSTRAINT customer_pkey PRIMARY KEY  (customer_id)
);

CREATE TABLE employee
(
    employee_id INT NOT NULL,
    last_name VARCHAR(20) NOT NULL,
    first_name VARCHAR(20) NOT NULL,
    title VARCHAR(30),
    reports_to INT,
    birth_date TIMESTAMP,
    hire_date TIMESTAMP,
    address VARCHAR(70),
    city VARCHAR(40),
    state VARCHAR(40),
    country VARCHAR(40),
    postal_code VARCHAR(10),
    phone VARCHAR(24),
    fax VARCHAR(24),
    email VARCHAR(60),
    CONSTRAINT employee_pkey PRIMARY KEY  (employee_id)
);

CREATE TABLE genre
(
    genre_id INT NOT NULL,
    name VARCHAR(120),
    CONSTRAINT genre_pkey PRIMARY KEY  (genre_id)
);

CREATE TABLE invoice
(
    invoice_id INT NOT NULL,
    customer_id INT NOT NULL,
    invoice_date TIMESTAMP NOT NULL,
    billing_address VARCHAR(70),
    billing_city VARCHAR(40),
    billing_state VARCHAR(40),
    billing_country VARCHAR(40),
    billing_postal_code VARCHAR(10),
    total NUMERIC(10,2) NOT NULL,
    CONSTRAINT invoice_pkey PRIMARY KEY  (invoice_id)
);

CREATE TABLE invoice_line
(
    invoice_line_id INT NOT NULL,
    invoice_id INT NOT NULL,
    track_id INT NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    quantity INT NOT NULL,
    CONSTRAINT invoice_line_pkey PRIMARY KEY  (invoice_line_id)
);

CREATE TABLE media_type
(
    media_type_id INT NOT NULL,
    name VARCHAR(120),
    CONSTRAINT media_type_pkey PRIMARY KEY  (media_type_id)
);

CREATE TABLE playlist
(
    playlist_id INT NOT NULL,
    name VARCHAR(120),
    CONSTRAINT playlist_pkey PRIMARY KEY  (playlist_id)
);

CREATE TABLE playlist_track
(
    playlist_id INT NOT NULL,
    track_id INT NOT NULL,
    CONSTRAINT playlist_track_pkey PRIMARY KEY  (playlist_id, track_id)
);

CREATE TABLE track
(
    track_id INT NOT NULL,
    name VARCHAR(200) NOT NULL,
    album_id INT,
    media_type_id INT NOT NULL,
    genre_id INT,
    composer VARCHAR(220),
    milliseconds INT NOT NULL,
    bytes INT,
    unit_price NUMERIC(10,2) NOT NULL,
    CONSTRAINT track_pkey PRIMARY KEY  (track_id)
);



/*******************************************************************************
   Create Primary Key Unique Indexes
********************************************************************************/

/*******************************************************************************
   Create Foreign Keys
********************************************************************************/
ALTER TABLE album ADD CONSTRAINT album_artist_id_fkey
    FOREIGN KEY (artist_id) REFERENCES artist (artist_id) ON DELETE NO ACTION ON UPDATE NO ACTION;

CREATE INDEX album_artist_id_idx ON album (artist_id);

ALTER TABLE customer ADD CONSTRAINT customer_support_rep_id_fkey
    FOREIGN KEY (support_rep_id) REFERENCES employee (employee_id) ON DELETE NO ACTION ON UPDATE NO ACTION;

CREATE INDEX customer_support_rep_id_idx ON customer (support_rep_id);

ALTER TABLE employee ADD CONSTRAINT employee_reports_to_fkey
    FOREIGN KEY (reports_to) REFERENCES employee (employee_id) ON DELETE NO ACTION ON UPDATE NO ACTION;

CREATE INDEX employee_reports_to_idx ON employee (reports_to);

ALTER TABLE invoice ADD CONSTRAINT invoice_customer_id_fkey
    FOREIGN KEY (customer_id) REFERENCES customer (customer_id) ON DELETE NO ACTION ON UPDATE NO ACTION;

CREATE INDEX invoice_customer_id_idx ON invoice (customer_id);

ALTER TABLE invoice_line ADD CONSTRAINT invoice_line_invoice_id_fkey
    FOREIGN KEY (invoice_id) REFERENCES invoice (invoice_id) ON DELETE NO ACTION ON UPDATE NO ACTION;

CREATE INDEX invoice_line_invoice_id_idx ON invoice_line (invoice_id);

ALTER TABLE invoice_line ADD CONSTRAINT invoice_line_track_id_fkey
    FOREIGN KEY (track_id) REFERENCES track (track_id) ON DELETE NO ACTION ON UPDATE NO ACTION;

CREATE INDEX invoice_line_track_id_idx ON invoice_line (track_id);

ALTER TABLE playlist_track ADD CONSTRAINT playlist_track_playlist_id_fkey
    FOREIGN KEY (playlist_id) REFERENCES playlist (playlist_id) ON DELETE NO ACTION ON UPDATE NO ACTION;

CREATE INDEX playlist_track_playlist_id_idx ON playlist_track (playlist_id);

ALTER TABLE playlist_track ADD CONSTRAINT playlist_track_track_id_fkey
    FOREIGN KEY (track_id) REFERENCES track (track_id) ON DELETE NO ACTION ON UPDATE NO ACTION;

CREATE INDEX playlist_track_track_id_idx ON playlist_track (track_id);

ALTER TABLE track ADD CONSTRAINT track_album_id_fkey
    FOREIGN KEY (album_id) REFERENCES album (album_id) ON DELETE NO ACTION ON UPDATE NO ACTION;

CREATE INDEX track_album_id_idx ON track (album_id);

ALTER TABLE track ADD CONSTRAINT track_genre_id_fkey
    FOREIGN KEY (genre_id) REFERENCES genre (genre_id) ON DELETE NO ACTION ON UPDATE NO ACTION;

CREATE INDEX track_genre_id_idx ON track (genre_id);

ALTER TABLE track ADD CONSTRAINT track_media_type_id_fkey
    FOREIGN KEY (media_type_id) REFERENCES media_type (media_type_id) ON DELETE NO ACTION ON UPDATE NO ACTION;

CREATE INDEX track_media_type_id_idx ON track (media_type_id);
