-- schema.sql -*- mode: sql; sql-product: postgres; -*-
--
-- data storage for bitmark blockchain data

-- the installation script will ignore this line
\echo "--- use the: 'install-schema' script rather than loading this file directly ---" \q

-- initial setup
\connect postgres

-- note: the install-schema will use the password from etc/updaterd.conf
--       in place of the tag below when loading this file into the database
CREATE USER @CHANGE-TO-USERNAME@ ENCRYPTED PASSWORD '@CHANGE-TO-SECURE-PASSWORD@';
ALTER ROLE @CHANGE-TO-USERNAME@ ENCRYPTED PASSWORD '@CHANGE-TO-SECURE-PASSWORD@';

-- drop/create database is controlled by install-schema options
--@DROP@DROP DATABASE IF EXISTS @CHANGE-TO-DBNAME@;
--@CREATE@CREATE DATABASE @CHANGE-TO-DBNAME@;

-- connect to the database
\connect @CHANGE-TO-DBNAME@

-- drop schema and all its objects, create the schema and use it by default
DROP SCHEMA IF EXISTS blockchain CASCADE;
CREATE SCHEMA IF NOT EXISTS blockchain;

SET search_path = blockchain;                              -- everything in this schema for schema loading
ALTER ROLE @CHANGE-TO-USERNAME@ SET search_path TO blockchain, PUBLIC; -- ensure user sees the schema first

--- grant to @CHANGE-TO-USERNAME@ ---
GRANT CONNECT ON DATABASE @CHANGE-TO-DBNAME@ TO @CHANGE-TO-USERNAME@;
GRANT USAGE ON SCHEMA blockchain TO @CHANGE-TO-USERNAME@;
ALTER DEFAULT PRIVILEGES IN SCHEMA blockchain GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO @CHANGE-TO-USERNAME@;
ALTER DEFAULT PRIVILEGES IN SCHEMA blockchain GRANT SELECT, UPDATE ON SEQUENCES TO @CHANGE-TO-USERNAME@;


-- bitmark blocks
DROP TABLE IF EXISTS block;

CREATE TABLE block (
  block_number INT8 PRIMARY KEY NOT NULL,
  block_hash TEXT NOT NULL,
  block_created_at TIMESTAMP WITH TIME ZONE DEFAULT NULL
);

-- dummy negative one block as anchor for possibly expiring records during a fork
INSERT INTO block (block_number, block_hash, block_created_at) VALUES (-1, '*fork-expiry-anchor*', NULL);

-- dummy zero block as anchor for pending records
INSERT INTO block (block_number, block_hash, block_created_at) VALUES (0, '*pending-anchor*', NULL);

-- genesis block
INSERT INTO block (block_number, block_hash, block_created_at) VALUES (1, '*genesis-reserved*', NULL);


-- status enumeration
CREATE TYPE status_type AS ENUM ('queuing', 'pending', 'verified', 'confirmed');

-- head enumeration
CREATE TYPE head_type AS ENUM ('prior', 'moved', 'head');


-- assets

DROP SEQUENCE IF EXISTS asset_seq;
CREATE SEQUENCE asset_seq;

DROP TABLE IF EXISTS asset;

CREATE TABLE asset (
  asset_id TEXT PRIMARY KEY NOT NULL,
  asset_name TEXT NOT NULL,
  asset_fingerprint TEXT NOT NULL,
  asset_metadata JSONB NOT NULL,
  asset_raw_metadata BYTEA,
  asset_registrant TEXT NOT NULL,
  asset_sequence INT8 DEFAULT nextval('asset_seq'),
  asset_signature TEXT NOT NULL,
  asset_status status_type NOT NULL DEFAULT 'pending',
  asset_block_number INT8 DEFAULT 0 REFERENCES block(block_number) ON DELETE CASCADE,
  asset_block_offset INT8 DEFAULT 0,
  asset_expires_at TIMESTAMP WITH TIME ZONE DEFAULT NULL
);

-- for asset query
DROP INDEX IF EXISTS asset_sequence_index;
CREATE INDEX asset_sequence_index ON asset(asset_sequence);

-- for registrant paging
DROP INDEX IF EXISTS asset_registrant_sequence_index;
CREATE UNIQUE INDEX asset_registrant_sequence_index ON asset(asset_registrant, asset_sequence);

-- for fast expiry
DROP INDEX IF EXISTS asset_expires_at_index;
CREATE INDEX asset_expires_at_index ON asset(asset_expires_at) WHERE asset_expires_at IS NOT NULL AND asset_block_number <= 0;

-- for block fork
DROP INDEX IF EXISTS asset_block_number_index;
CREATE INDEX asset_block_number_index ON asset(asset_block_number) WHERE asset_block_number IS NOT NULL;


-- merged issuses and transfers to for transactions

DROP SEQUENCE IF EXISTS tx_seq;
CREATE SEQUENCE tx_seq;

DROP TABLE IF EXISTS TRANSACTION;

CREATE TABLE TRANSACTION (
  tx_id TEXT PRIMARY KEY NOT NULL,
  tx_owner TEXT NOT NULL DEFAULT '',
  tx_sequence INT8  DEFAULT nextval('tx_seq'),
  tx_signature TEXT NOT NULL,
  tx_countersignature TEXT NOT NULL DEFAULT '',
  -- note: in the case of a block asset_id will be NULL
  tx_asset_id TEXT REFERENCES asset(asset_id),
  -- note: in the case of an issue record the previous_id will be NULL
  tx_bitmark_id TEXT REFERENCES TRANSACTION(tx_id) ON DELETE CASCADE,
  tx_previous_id TEXT REFERENCES TRANSACTION(tx_id) ON DELETE CASCADE,
  tx_head head_type NOT NULL,
  tx_status status_type NOT NULL,
  tx_payments JSONB,  -- NULL for Asset, set for Block Ownership
  tx_pay_id TEXT NOT NULL,
  tx_shares_info JSONB,    -- only set for share grant and swap transaction
  tx_block_number INT8 REFERENCES block(block_number) ON DELETE CASCADE,
  tx_block_offset INT8 DEFAULT 0,
  tx_edition INT8 DEFAULT NULL,
  tx_expires_at TIMESTAMP WITH TIME ZONE DEFAULT NULL,
  tx_modified_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- for query bitmark
DROP INDEX IF EXISTS transaction_issue_index;
CREATE UNIQUE INDEX transaction_issue_index ON TRANSACTION(tx_bitmark_id) WHERE tx_previous_id IS NULL;

-- for query ownership
DROP INDEX IF EXISTS transaction_tx_index;
DROP INDEX IF EXISTS transaction_tx_index_head_and_moved;
-- transaction_tx_index can not be set to UNIQUE because we insert a new 'head' transaction
-- and update the previous on to 'prior'
CREATE INDEX transaction_tx_index ON TRANSACTION(tx_bitmark_id) WHERE tx_head = 'head';
CREATE INDEX transaction_tx_index_head_and_moved ON TRANSACTION(tx_bitmark_id) WHERE  tx_head = ANY ('{head,moved}'::head_type[]);

-- for transaction query
DROP INDEX IF EXISTS tx_sequence_index;
CREATE INDEX tx_sequence_index ON TRANSACTION(tx_sequence);

-- for owner paging
DROP INDEX IF EXISTS tx_owner_sequence_index;
CREATE UNIQUE INDEX tx_owner_sequence_index ON TRANSACTION(tx_owner, tx_sequence);

-- for block owner paging
DROP INDEX IF EXISTS tx_block_owner_index;
CREATE UNIQUE INDEX tx_block_owner_index ON TRANSACTION(tx_owner, tx_sequence)
       WHERE tx_asset_id IS NULL
          AND tx_head = 'head'
          AND tx_status = 'confirmed';

-- for setting edition
DROP INDEX IF EXISTS tx_edition_index;
CREATE UNIQUE INDEX tx_edition_index ON TRANSACTION(tx_owner, tx_asset_id, tx_block_number, tx_block_offset)
       WHERE tx_previous_id IS NULL
         AND tx_block_number > 0;

-- for fast expiry
DROP INDEX IF EXISTS tx_expires_at_index;
CREATE INDEX tx_expires_at_index ON TRANSACTION(tx_expires_at) WHERE tx_expires_at IS NOT NULL AND tx_block_number <= 0;

-- for block fork
DROP INDEX IF EXISTS tx_block_number_index;
CREATE INDEX tx_block_number_index ON TRANSACTION(tx_block_number) WHERE tx_block_number IS NOT NULL;
DROP INDEX IF EXISTS tx_block_number_previous_index;
CREATE INDEX tx_block_number_previous_index ON TRANSACTION(tx_block_number) WHERE tx_previous_id IS NOT NULL;


DROP SEQUENCE IF EXISTS share_seq;
CREATE SEQUENCE share_seq;

CREATE TYPE share_type AS ENUM ('increment', 'decrement', 'summation');

-- share table
DROP TABLE IF EXISTS SHARE;

CREATE TABLE SHARE (
  share_id TEXT REFERENCES TRANSACTION(tx_id) NOT NULL,
  share_owner TEXT NOT NULL DEFAULT '',
  share_quantity INTEGER NOT NULL DEFAULT 0,
  share_sequence INT8 DEFAULT nextval('share_seq'),
  share_status status_type NOT NULL,
  share_tx_id TEXT REFERENCES TRANSACTION(tx_id) ON DELETE CASCADE, -- for tracking pending balances
  share_block_number INT8 REFERENCES block(block_number) ON DELETE CASCADE,
  share_type share_type NOT NULL,
  share_modified_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  share_expires_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

DROP INDEX IF EXISTS unique_share_id_owner_tx_id;
CREATE UNIQUE INDEX unique_share_id_owner_tx_id ON SHARE (share_id, share_owner, share_type, share_tx_id);

DROP INDEX IF EXISTS unique_share_id_owner_summation;
CREATE UNIQUE INDEX unique_share_id_owner_summation ON SHARE (share_id, share_owner) WHERE share_type = 'summation' ;


-- events
DROP TABLE IF EXISTS event;

CREATE TABLE event (
  ID SERIAL PRIMARY KEY,
  NAME TEXT NOT NULL,
  VALUE TEXT NOT NULL,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  expires_at TIMESTAMP WITH TIME ZONE DEFAULT NULL,
  processing_at TIMESTAMP WITH TIME ZONE DEFAULT NULL,
  notified BOOLEAN DEFAULT FALSE
);

-- functions
-- ---------

-- get unnotified events

DROP FUNCTION IF EXISTS get_unnotified_events();

CREATE FUNCTION get_unnotified_events() RETURNS TABLE (_id INT, _name TEXT, _value TEXT, _updated_at TIMESTAMP WITH TIME ZONE)  AS $$
DECLARE
  _local_time_now TIMESTAMP WITH TIME ZONE := now();
BEGIN
  RETURN QUERY
  SELECT ID, NAME, VALUE, updated_at
  FROM event
  WHERE notified = FALSE AND expires_at > _local_time_now
  ORDER BY ID;
END;
$$ LANGUAGE plpgsql;


-- set an event to be processing

DROP FUNCTION IF EXISTS set_event_processing(INT);
CREATE FUNCTION set_event_processing(_event_id INT) RETURNS VOID AS $$
DECLARE
  _local_time_now TIMESTAMP WITH TIME ZONE := now();
BEGIN
  UPDATE event
  SET processing_at = _local_time_now
  WHERE notified = FALSE AND (processing_at IS NULL OR _local_time_now > (processing_at + INTERVAL '2 mins')) AND ID = _event_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'not found';
  END IF;
END;
$$ LANGUAGE plpgsql;


-- get an unnotified event

DROP FUNCTION IF EXISTS get_unnotified_event_by_id(INT);

CREATE FUNCTION get_unnotified_event_by_id(_event_id INT) RETURNS TABLE (_id INT, _name TEXT, _value TEXT, _updated_at TIMESTAMP WITH TIME ZONE) AS $$
DECLARE
  _local_time_now TIMESTAMP WITH TIME ZONE := now();
BEGIN
  RETURN QUERY
  SELECT ID, NAME, VALUE, updated_at
  FROM event
  WHERE notified = FALSE AND expires_at > _local_time_now AND ID = _event_id;
END;
$$ LANGUAGE plpgsql;


-- set events notified

DROP FUNCTION IF EXISTS set_event_notified(INT);

CREATE FUNCTION set_event_notified(_id INT) RETURNS VOID AS $$
BEGIN
  UPDATE event
  SET notified = TRUE
  WHERE ID = _id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'event not found';
  END IF;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS set_event_notified(TEXT, TEXT);

CREATE FUNCTION set_event_notified(_name TEXT, _value TEXT) RETURNS VOID AS $$
BEGIN
  UPDATE event
  SET notified = TRUE
  WHERE NAME = _name AND VALUE = _value;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'event not found';
  END IF;
END;
$$ LANGUAGE plpgsql;


-- constants for expiry times

DROP FUNCTION IF EXISTS expires_at();

CREATE FUNCTION expires_at() RETURNS TIMESTAMP WITH TIME ZONE AS $$
DECLARE
  _local_expires_at TIMESTAMP WITH TIME ZONE := now() + INTERVAL '24 hours';
BEGIN
  RETURN _local_expires_at;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS long_expires_at();

CREATE FUNCTION long_expires_at() RETURNS TIMESTAMP WITH TIME ZONE AS $$
DECLARE
  _local_expires_at TIMESTAMP WITH TIME ZONE := now() + INTERVAL '90 days';
BEGIN
  RETURN _local_expires_at;
END;
$$ LANGUAGE plpgsql;


-- expire old records

DROP FUNCTION IF EXISTS expire_records();

CREATE FUNCTION expire_records() RETURNS VOID AS $$
DECLARE
  _local_time_now TIMESTAMP WITH TIME ZONE := now();
  _local_long_expires_at TIMESTAMP WITH TIME ZONE := long_expires_at();
  _local_tx RECORD;
BEGIN
  -- convert block_number = 0 to long expiry time
  -- IF   tx was 'head'            -> 'moved'(gone)
  -- THEN prev_tx('prior'/'moved') -> 'head'
  -- exclude 'queuing' transactions
  FOR _local_tx IN SELECT * FROM TRANSACTION
    WHERE tx_expires_at IS NOT NULL AND tx_block_number = 0
      AND tx_status <> 'queuing'
      AND tx_expires_at < _local_time_now
  LOOP
    IF 'head' = _local_tx.tx_head THEN
      UPDATE TRANSACTION SET tx_head = 'head' WHERE tx_id = _local_tx.tx_previous_id;
    END IF;
    UPDATE TRANSACTION
      SET tx_expires_at = _local_long_expires_at,
          tx_modified_at = _local_time_now,
          tx_head = 'moved',
          tx_block_number = -1
      WHERE tx_id = _local_tx.tx_id;
  END LOOP;

  -- clean up assets
  -- exclude 'queuing' assets
  UPDATE asset
    SET asset_expires_at = _local_long_expires_at,
        asset_block_number = -1
    WHERE asset_expires_at IS NOT NULL AND asset_block_number = 0
      AND asset_status <> 'queuing'
      AND asset_expires_at < _local_time_now;

  -- delete block_number = -1 after long expiry time
  DELETE FROM TRANSACTION
    WHERE tx_expires_at IS NOT NULL AND tx_block_number < 0
      AND tx_expires_at < _local_time_now;
  DELETE FROM asset
    WHERE asset_expires_at IS NOT NULL AND asset_block_number < 0
      AND asset_expires_at < _local_time_now;
END;
$$ LANGUAGE plpgsql;


-- iso8601 timestamp

DROP FUNCTION IF EXISTS iso8601_timestamp(TIMESTAMP WITH TIME ZONE);

CREATE FUNCTION iso8601_timestamp(ts TIMESTAMP WITH TIME ZONE) RETURNS TEXT AS $$
BEGIN
  RETURN to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"');
END;
$$ LANGUAGE plpgsql;


-- get the current block height
-- i.e.the highest block number on file

DROP FUNCTION IF EXISTS get_block_height();

CREATE FUNCTION get_block_height() RETURNS INT8 AS $$
DECLARE
  _local_block_number INT8;
BEGIN
  SELECT block_number INTO _local_block_number
    FROM block
    ORDER BY block_number DESC LIMIT 1;
  IF NOT FOUND THEN
    RETURN 0;
  END IF;
  RETURN _local_block_number;
END;
$$ LANGUAGE plpgsql;


-- get the digest of a specific block

DROP FUNCTION IF EXISTS get_block_digest(INT8);

CREATE FUNCTION get_block_digest(_block_number INT8) RETURNS TEXT AS $$
DECLARE
  _local_hash TEXT;
BEGIN
  SELECT block_hash INTO  _local_hash
    FROM block
    WHERE block_number = _block_number LIMIT 1;
  IF NOT FOUND THEN
    RETURN NULL;
  END IF;
  RETURN _local_hash;
END;
$$ LANGUAGE plpgsql;


-- remove blocks before fork recovery

DROP FUNCTION IF EXISTS delete_down_to_block(INT8);

CREATE FUNCTION delete_down_to_block(_low_block_number INT8) RETURNS VOID AS $$
DECLARE
  _share_row RECORD;
  _share_multiplier INTEGER;
  _local_time_now TIMESTAMP WITH TIME ZONE := now();
  _local_expires_at TIMESTAMP WITH TIME ZONE := expires_at();
BEGIN
  FOR _block_number IN REVERSE get_block_height() .. _low_block_number LOOP

    UPDATE TRANSACTION SET tx_head = 'head'
      WHERE tx_id IN (
        SELECT tx_previous_id
          FROM TRANSACTION
          WHERE tx_block_number = _block_number AND tx_previous_id IS NOT NULL
      );

    UPDATE TRANSACTION
      SET tx_block_number = -1,
          tx_modified_at = _local_time_now,
          tx_expires_at = _local_expires_at,
          tx_head = 'moved',
          tx_status = 'pending'
      WHERE tx_block_number = _block_number;
    UPDATE asset
      SET asset_block_number = -1,
          asset_expires_at = _local_expires_at
      WHERE asset_block_number = _block_number;
    -- mark block number of reverted share records to -1

    -- revert the share balance
    FOR _share_row IN SELECT * FROM SHARE WHERE share_block_number = _block_number LOOP
      IF _share_row.share_type = 'increment' THEN
        _share_multiplier := -1;
      ELSIF _share_row.share_type = 'decrement' THEN
        _share_multiplier := 1;
      END IF;
      UPDATE SHARE
        SET share_quantity = share_quantity + _share_row.share_quantity * _share_multiplier
        WHERE share_id = _share_row.share_id AND share_owner = _share_row.share_owner AND share_type = 'summation';
    END LOOP;

    UPDATE SHARE
      SET share_block_number = -1,
          share_modified_at = _local_time_now,
          share_expires_at = _local_expires_at
      WHERE share_block_number = _block_number;

    DELETE FROM block
      WHERE block_number = _block_number;
  END LOOP;
END;
$$ LANGUAGE plpgsql;



-- insert a block

DROP FUNCTION IF EXISTS insert_block(INT8, TEXT, TIMESTAMP WITH TIME ZONE);

CREATE FUNCTION insert_block(_block_number INT8,
                             _hash TEXT,
                             _created_at TIMESTAMP WITH TIME ZONE)
                RETURNS VOID AS $$
DECLARE
  _local_hash TEXT := '';
BEGIN
  LOOP
    SELECT block_hash INTO _local_hash FROM block WHERE block_number = _block_number LIMIT 1;
    IF NOT FOUND THEN
      -- try to insert the record
      BEGIN
        INSERT INTO block (block_number, block_hash, block_created_at)
                    VALUES (_block_number, _hash, _created_at);
        EXIT;

      EXCEPTION WHEN unique_violation THEN
        -- if another thread inserts the same record concurrently
        -- then will receive a unique-key exception
        -- do nothing, just loop to try the UPDATE again
      END;

    ELSE
      RAISE EXCEPTION 'a block of the same height has already exist';
      EXIT;
    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql;


-- insert an asset

DROP FUNCTION IF EXISTS insert_asset(TEXT, TEXT, TEXT, JSONB, TEXT, TEXT, status_type, INT8, INT8);

CREATE FUNCTION insert_asset(_asset_id TEXT,
                             _name TEXT,
                             _fingerprint TEXT,
                             _metadata JSONB,
                             _registrant TEXT,
                             _signature TEXT,
                             _status status_type,
                             _block_number INT8,
                             _block_offset INT8)
                RETURNS VOID AS $$
DECLARE
  _local_status status_type;
  _local_block_number INT8;
  _local_expires_at TIMESTAMP WITH TIME ZONE := expires_at();
BEGIN
  IF _block_number > 0 THEN
    _local_expires_at := NULL;
  END IF;

  LOOP
    SELECT asset_status, asset_block_number INTO _local_status, _local_block_number FROM asset WHERE asset_id = _asset_id LIMIT 1;
    IF NOT FOUND THEN
      -- absent, try to insert the record
      BEGIN

        INSERT INTO asset(asset_id, asset_name, asset_fingerprint, asset_metadata,
                          asset_registrant, asset_signature, asset_status,
                          asset_block_number, asset_block_offset,
                          asset_expires_at)
          VALUES (_asset_id, _name, _fingerprint, _metadata,
                  _registrant, _signature, _status,
                  _block_number, _block_offset,
                  _local_expires_at);
        EXIT;

      EXCEPTION WHEN unique_violation THEN
        -- if another thread inserts the same record concurrently
        -- then will receive a unique-key exception
        -- do nothing, just loop to try the UPDATE again
      END;

    ELSE
      -- already exists
      IF _local_block_number <= 0 AND (_status <> _local_status OR _block_number <> _local_block_number) THEN
        UPDATE asset
          SET asset_status = _status,
              asset_sequence = nextval('asset_seq'),
              asset_block_number = _block_number,
              asset_block_offset = _block_offset,
              asset_expires_at = _local_expires_at
          WHERE asset_id = _asset_id;
      END IF;
      EXIT;

    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql;


-- insert a transaction

DROP FUNCTION IF EXISTS insert_transaction(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, status_type, TEXT, INT8, INT8);
DROP FUNCTION IF EXISTS insert_transaction(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, status_type, JSONB, TEXT, INT8, INT8);

CREATE FUNCTION insert_transaction(_tx_id TEXT,
                               _owner TEXT,
                               _signature TEXT,
                               _countersignature TEXT,
                               _asset_id TEXT,     -- NULL for non-issue or block ownership
                               _previous_id TEXT,  -- NULL for an issue
                               _status status_type,
                               _payments JSONB,    -- non-NULL fo block ownership
                               _pay_id TEXT,
                               _block_number INT8, -- owned block if corresponds to a foundation record
                               _block_offset INT8)
                RETURNS VOID AS $$
DECLARE
  _local_status status_type;
  _local_prior head_type := 'moved';
  _local_head head_type := 'head';
  _local_block_number INT8 := 0;
  _local_bitmark_id TEXT := NULL;
  _local_previous_id TEXT := NULL;
  _local_expires_at TIMESTAMP WITH TIME ZONE := expires_at();
BEGIN
  IF _block_number > 0 THEN
    _local_expires_at := NULL;
    _local_prior := 'prior';
    _local_head := 'head';
  END IF;

  IF _payments = '{}' THEN
    RAISE EXCEPTION '_payments is empty object';
  END IF;

  LOOP

    SELECT tx_status, tx_block_number, tx_previous_id
      INTO _local_status, _local_block_number, _local_previous_id
      FROM TRANSACTION WHERE tx_id = _tx_id LIMIT 1;
    IF NOT FOUND THEN
      -- absent, try to insert the record
      IF _previous_id IS NOT NULL THEN
        SELECT tx_bitmark_id, tx_asset_id, tx_id INTO _local_bitmark_id, _asset_id, _local_previous_id
          FROM TRANSACTION
          WHERE tx_id = _previous_id LIMIT 1;
        IF NOT FOUND THEN
          -- ***** FIX THIS: need an error indication here
          EXIT;
        END IF;
      ELSE        -- this is an issue
        _local_bitmark_id := _tx_id;  -- so make its bitmark_id POINT TO itself
      END IF;
      BEGIN
        INSERT INTO TRANSACTION(tx_id, tx_owner, tx_signature, tx_countersignature,
                            tx_asset_id, tx_bitmark_id, tx_previous_id,
                            tx_head, tx_status,
                            tx_block_number, tx_block_offset,
                            tx_payments,
                            tx_pay_id,
                            tx_expires_at)
               VALUES (_tx_id, _owner, _signature, _countersignature,
                       _asset_id, _local_bitmark_id, _previous_id,
                       _local_head, _status,
                       _block_number, _block_offset,
                       _payments,
                       _pay_id,
                       _local_expires_at);
        EXIT;

      EXCEPTION WHEN unique_violation THEN
        -- if another thread inserts the same record concurrently
        -- then will receive a unique-key exception
        -- do nothing, and loop to try the UPDATE again
      END;

    ELSE
      -- already exists, update
      IF _local_block_number <= 0 AND (_status <> _local_status OR _block_number <> _local_block_number) THEN
        UPDATE TRANSACTION
          SET tx_status = _status,
              tx_sequence = nextval('tx_seq'),
              tx_block_number = _block_number,
              tx_block_offset = _block_offset,
              tx_head = _local_head,
              tx_expires_at = _local_expires_at,
              tx_modified_at = now()
          WHERE tx_id = _tx_id;
      END IF;
      EXIT;

    END IF;
  END LOOP;

  -- adjust head indication on the previous record
  IF _local_previous_id IS NOT NULL THEN
    UPDATE TRANSACTION
      SET tx_head = _local_prior,
          tx_modified_at = now()
      WHERE tx_id = _local_previous_id;
  END IF;
END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS insert_share_transaction(TEXT, INTEGER, TEXT, TEXT, TEXT, status_type, INT8, INT8);
CREATE FUNCTION insert_share_transaction(_tx_id TEXT,
                                         _quantity INTEGER,
                                         _signature TEXT,
                                         _previous_id TEXT,  -- NULL for an issue
                                         _pay_id TEXT,
                                         _status status_type,
                                         _block_number INT8, -- owned block if corresponds to a foundation record
                                         _block_offset INT8)
                RETURNS VOID AS $$
DECLARE
  ROW RECORD;
  _local_time_now TIMESTAMP WITH TIME ZONE := now();
  _local_updated BOOLEAN := FALSE;
  _local_owner TEXT := NULL;
  _local_prior head_type := 'moved';
  _local_head head_type := 'head';
  _local_bitmark_id TEXT := NULL;
  _local_asset_id TEXT := NULL;
  _local_expires_at TIMESTAMP WITH TIME ZONE := expires_at();
BEGIN
  IF _block_number > 0 THEN
    _local_expires_at := NULL;
    _local_prior := 'prior';
    _local_head := 'head';
  END IF;

  SELECT tx_owner, tx_bitmark_id, tx_asset_id
    INTO _local_owner, _local_bitmark_id, _local_asset_id
    FROM TRANSACTION WHERE tx_id = _previous_id LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'previous transaction is not found';
  END IF;

  BEGIN
    -- insert both transaction and share record at the same time.
    INSERT INTO TRANSACTION (tx_id, tx_owner, tx_signature, tx_asset_id, tx_bitmark_id, tx_previous_id,
                             tx_head, tx_status, tx_block_number, tx_block_offset,
                             tx_shares_info, tx_pay_id, tx_expires_at)
           VALUES (_tx_id, _local_owner, _signature, _local_asset_id, _local_bitmark_id, _previous_id,
                   _local_head, _status, _block_number, _block_offset,
                   jsonb_build_object('new', _local_owner, 'share_id', _local_bitmark_id, 'quantity', _quantity), _pay_id, _local_expires_at);
    INSERT INTO SHARE (share_id, share_owner, share_quantity, share_status, share_tx_id, share_block_number, share_type, share_modified_at, share_expires_at)
           VALUES (_local_bitmark_id, _local_owner, _quantity, _status, _tx_id, _block_number, 'increment', _local_time_now, _local_expires_at);
    _local_updated = TRUE;
  EXCEPTION
    WHEN unique_violation THEN
      -- update the transaction status only when the tx_status if confirmed
      UPDATE TRANSACTION
          SET tx_status = _status,
              tx_head = _local_head,
              tx_block_number = _block_number,
              tx_block_offset = _block_offset,
              tx_pay_id = _pay_id,
              tx_expires_at = _local_expires_at
          WHERE tx_id = _tx_id AND tx_status <> 'confirmed';
      -- update share status if the transaction has updated
      IF FOUND THEN
        UPDATE SHARE
          SET share_status = _status,
              share_block_number = _block_number,
              share_modified_at = _local_time_now,
              share_expires_at = _local_expires_at
          WHERE share_tx_id = _tx_id;
        _local_updated = TRUE;
      END IF;
    WHEN OTHERS THEN
      RAISE;
  END;

  -- create / update an overall balance record
  IF _local_updated AND _status = 'confirmed' THEN
    INSERT INTO SHARE (share_id, share_owner, share_quantity, share_status, share_type, share_modified_at)
            VALUES (_local_bitmark_id, _local_owner, _quantity, _status, 'summation', _local_time_now)
            ON CONFLICT (share_id, share_owner) WHERE share_type = 'summation' DO UPDATE
            SET share_quantity = SHARE.share_quantity + _quantity,
                share_modified_at = _local_time_now;
  END IF;

  UPDATE TRANSACTION
    SET tx_head = _local_prior,
        tx_modified_at = _local_time_now
    WHERE tx_id = _previous_id;
END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS insert_grant_transaction(TEXT, TEXT, INTEGER, TEXT, TEXT, TEXT, TEXT, TEXT, JSONB, status_type, INT8, INT8);
CREATE FUNCTION insert_grant_transaction(_tx_id TEXT,
                                         _share_id TEXT,
                                         _quantity INTEGER,
                                         _owner TEXT,
                                         _recipient TEXT,
                                         _signature TEXT,
                                         _countersignature TEXT,
                                         _pay_id TEXT,
                                         _shares JSONB,
                                         _status status_type,
                                         _block_number INT8, -- owned block if corresponds to a foundation record
                                         _block_offset INT8)
                RETURNS VOID AS $$
DECLARE
  ROW RECORD;
  _local_time_now TIMESTAMP WITH TIME ZONE := now();
  _local_updated BOOLEAN := FALSE;
  _local_head head_type := 'head';
  _local_asset_id TEXT := NULL;
  _local_expires_at TIMESTAMP WITH TIME ZONE := expires_at();
BEGIN
  IF _block_number > 0 THEN -- confirmed
    _local_expires_at := NULL;
  END IF;

  SELECT tx_asset_id INTO _local_asset_id FROM TRANSACTION WHERE tx_id = _share_id LIMIT 1;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'share transaction is not found';
  END IF;

  BEGIN
    -- insert both transaction and share record at the same time.
    INSERT INTO TRANSACTION (tx_id, tx_owner, tx_signature, tx_countersignature, tx_head, tx_status,
                             tx_block_number, tx_block_offset, tx_pay_id, tx_shares_info, tx_expires_at)
           VALUES (_tx_id, _owner, _signature, _countersignature, _local_head, _status,
                   _block_number, _block_offset, _pay_id, _shares, _local_expires_at);
    -- insert both increment and decrement share records. i.e. move from A(-) to B(+)
    INSERT INTO SHARE (share_id, share_owner, share_quantity, share_status, share_tx_id, share_block_number, share_type, share_modified_at, share_expires_at)
           VALUES (_share_id, _recipient, _quantity, _status, _tx_id, _block_number, 'increment', _local_time_now, _local_expires_at);
    INSERT INTO SHARE (share_id, share_owner, share_quantity, share_status, share_tx_id, share_block_number, share_type, share_modified_at, share_expires_at)
           VALUES (_share_id, _owner, _quantity, _status, _tx_id, _block_number, 'decrement', _local_time_now, _local_expires_at);
    _local_updated = TRUE;
  EXCEPTION
    WHEN unique_violation THEN
      -- update the transaction status only when the tx_status if confirmed
      UPDATE TRANSACTION
          SET tx_status = _status,
              tx_head = _local_head,
              tx_block_number = _block_number,
              tx_block_offset = _block_offset,
              tx_pay_id = _pay_id,
              tx_expires_at = _local_expires_at
          WHERE tx_id = _tx_id AND tx_status <> 'confirmed';
      -- update share status if the transaction has updated
      IF FOUND THEN
        UPDATE SHARE
          SET share_status = _status,
              share_block_number = _block_number,
              share_modified_at = _local_time_now,
              share_expires_at = _local_expires_at
          WHERE share_tx_id = _tx_id;
        _local_updated = TRUE;
      END IF;
    WHEN OTHERS THEN
      RAISE;
  END;

  -- create / update an overall balance record
  IF _local_updated AND _status = 'confirmed' THEN
    -- update the final values. i.e. move from A(-) to B(+)
    INSERT INTO SHARE (share_id, share_owner, share_quantity, share_status, share_type, share_modified_at)
           VALUES (_share_id, _recipient, _quantity, _status, 'summation', _local_time_now)
           ON CONFLICT (share_id, share_owner) WHERE share_type = 'summation' DO UPDATE
           SET share_quantity = SHARE.share_quantity + _quantity,
               share_modified_at = _local_time_now;
    UPDATE SHARE
           SET share_quantity = share_quantity - _quantity,
               share_modified_at = _local_time_now
           WHERE share_id = _share_id AND share_owner = _owner AND share_type = 'summation';
  END IF;
END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS insert_swap_transaction(TEXT, TEXT, INTEGER, TEXT, TEXT, INTEGER, TEXT, TEXT, TEXT, TEXT, JSONB, status_type, INT8, INT8);
CREATE FUNCTION insert_swap_transaction(_tx_id TEXT,
                                        _share_one TEXT,
                                        _quantity_one INTEGER,
                                        _owner_one TEXT,
                                        _share_two TEXT,
                                        _quantity_two INTEGER,
                                        _owner_two TEXT,
                                        _signature TEXT,
                                        _countersignature TEXT,
                                        _pay_id TEXT,
                                        _swaps JSONB,
                                        _status status_type,
                                        _block_number INT8, -- owned block if corresponds to a foundation record
                                        _block_offset INT8)
                RETURNS VOID AS $$
DECLARE
  ROW RECORD;
  _local_time_now TIMESTAMP WITH TIME ZONE := now();
  _local_updated BOOLEAN := FALSE;
  _local_head head_type := 'head';
  _local_expires_at TIMESTAMP WITH TIME ZONE := expires_at();
BEGIN
  BEGIN
    -- insert both transaction and share record at the same time.
    INSERT INTO TRANSACTION (tx_id, tx_owner, tx_signature, tx_countersignature,
                             tx_head, tx_status, tx_block_number, tx_block_offset, tx_pay_id, tx_shares_info, tx_expires_at)
           VALUES (_tx_id, _owner_one, _signature, _countersignature,
                   _local_head, _status, _block_number, _block_offset, _pay_id, _swaps, _local_expires_at);
    -- insert both increment and decrement share records. i.e. move from A(-) to B(+)
    INSERT INTO SHARE (share_id, share_owner, share_quantity, share_status, share_tx_id, share_block_number, share_type, share_modified_at, share_expires_at)
           VALUES (_share_one, _owner_two, _quantity_one, _status, _tx_id, _block_number, 'increment', _local_time_now, _local_expires_at);
    INSERT INTO SHARE (share_id, share_owner, share_quantity, share_status, share_tx_id, share_block_number, share_type, share_modified_at, share_expires_at)
           VALUES (_share_one, _owner_one, _quantity_one, _status, _tx_id, _block_number, 'decrement', _local_time_now, _local_expires_at);
    INSERT INTO SHARE (share_id, share_owner, share_quantity, share_status, share_tx_id, share_block_number, share_type, share_modified_at, share_expires_at)
           VALUES (_share_two, _owner_one, _quantity_two, _status, _tx_id, _block_number, 'increment', _local_time_now, _local_expires_at);
    INSERT INTO SHARE (share_id, share_owner, share_quantity, share_status, share_tx_id, share_block_number, share_type, share_modified_at, share_expires_at)
           VALUES (_share_two, _owner_two, _quantity_two, _status, _tx_id, _block_number, 'decrement', _local_time_now, _local_expires_at);
    _local_updated = TRUE;
  EXCEPTION
    WHEN unique_violation THEN
      -- update the transaction status only when the tx_status if confirmed
      UPDATE TRANSACTION
          SET tx_status = _status,
              tx_head = _local_head,
              tx_block_number = _block_number,
              tx_block_offset = _block_offset,
              tx_pay_id = _pay_id,
              tx_expires_at = _local_expires_at
          WHERE tx_id = _tx_id AND tx_status <> 'confirmed';
      -- update share status if the transaction has updated
      IF FOUND THEN
        UPDATE SHARE
          SET share_status = _status,
              share_block_number = _block_number,
              share_modified_at = _local_time_now,
              share_expires_at = _local_expires_at
          WHERE share_tx_id = _tx_id;
        _local_updated = TRUE;
      END IF;
    WHEN OTHERS THEN
      RAISE;
  END;

  -- create / update an overall balance record
  IF _local_updated AND _status = 'confirmed' THEN
    -- update the final values. i.e. move from A(-) to B(+)

    UPDATE SHARE
           SET share_quantity = SHARE.share_quantity - _quantity_one,
               share_modified_at = now()
           WHERE share_id = _share_one AND share_owner = _owner_one AND share_type = 'summation';

    INSERT INTO SHARE (share_id, share_owner, share_quantity, share_status, share_type, share_modified_at)
           VALUES (_share_one, _owner_two, _quantity_one, _status, 'summation', now())
           ON CONFLICT (share_id, share_owner) WHERE share_type = 'summation' DO UPDATE
           SET share_quantity = SHARE.share_quantity + _quantity_one,
               share_modified_at = now();

    UPDATE SHARE
           SET share_quantity = SHARE.share_quantity - _quantity_two,
               share_modified_at = now()
           WHERE share_id = _share_two AND share_owner = _owner_two AND share_type = 'summation';

    INSERT INTO SHARE (share_id, share_owner, share_quantity, share_status, share_type, share_modified_at)
           VALUES (_share_two, _owner_one, _quantity_two, _status, 'summation', now())
           ON CONFLICT (share_id, share_owner) WHERE share_type = 'summation' DO UPDATE
           SET share_quantity = SHARE.share_quantity + _quantity_two,
               share_modified_at = now();
  END IF;
END;
$$ LANGUAGE plpgsql;


-- notifications

DROP FUNCTION IF EXISTS notify_new_block(TEXT);
CREATE FUNCTION notify_new_block(_block_number TEXT) RETURNS VOID AS $$
DECLARE
  _local_expires_at TIMESTAMP WITH TIME ZONE := expires_at() + INTERVAL '2 days';
  _event_id INT;
  _record RECORD;
BEGIN
  SELECT * INTO _record FROM event WHERE NAME = 'new_block' AND VALUE = _block_number FOR UPDATE;
  IF NOT FOUND THEN
    -- try to insert an block event
    BEGIN
      INSERT INTO event (NAME, VALUE, expires_at)
                  VALUES ('new_block', _block_number, _local_expires_at)
                  RETURNING event.ID INTO _event_id;
      PERFORM pg_notify('new_block', _event_id::TEXT);
    EXCEPTION WHEN unique_violation THEN
    END;
  ELSE
    UPDATE event SET expires_at = _local_expires_at, notified = false, updated_at = now()
    WHERE NAME = 'new_block' AND VALUE = _block_number
    RETURNING event.ID INTO _event_id;
    
    PERFORM pg_notify('new_block', _event_id::TEXT);
  END IF;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS notify_new_assets(TEXT);
CREATE FUNCTION notify_new_assets(_asset_ids TEXT) RETURNS VOID AS $$
DECLARE
  _local_expires_at TIMESTAMP WITH TIME ZONE := expires_at() + INTERVAL '2 days';
  _event_id INT;
  _record RECORD;
BEGIN
  SELECT * INTO _record FROM event WHERE NAME = 'new_assets' AND VALUE = _asset_ids FOR UPDATE;
  IF NOT FOUND THEN
    -- try to insert an block event
    BEGIN
      INSERT INTO event (NAME, VALUE, expires_at)
                  VALUES ('new_assets', _asset_ids, _local_expires_at)
                  RETURNING event.ID INTO _event_id;
      PERFORM pg_notify('new_assets', _event_id::TEXT);
    EXCEPTION WHEN unique_violation THEN
    END;
  ELSE
    UPDATE event SET expires_at = _local_expires_at WHERE NAME = 'new_assets' AND VALUE = _asset_ids;
  END IF;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS notify_new_issues(TEXT);
CREATE FUNCTION notify_new_issues(_issue_ids TEXT) RETURNS VOID AS $$
DECLARE
  _local_expires_at TIMESTAMP WITH TIME ZONE := expires_at() + INTERVAL '2 days';
  _event_id INT;
  _record RECORD;
BEGIN
  SELECT * INTO _record FROM event WHERE NAME = 'new_issues' AND VALUE = _issue_ids FOR UPDATE;
  IF NOT FOUND THEN
    -- try to insert an block event
    BEGIN
      INSERT INTO event (NAME, VALUE, expires_at)
                  VALUES ('new_issues', _issue_ids, _local_expires_at)
                  RETURNING event.ID INTO _event_id;
      PERFORM pg_notify('new_issues', _event_id::TEXT);
    EXCEPTION WHEN unique_violation THEN
    END;
  ELSE
    UPDATE event SET expires_at = _local_expires_at WHERE NAME = 'new_issues' AND VALUE = _issue_ids;
  END IF;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS notify_new_transfers(TEXT);
CREATE FUNCTION notify_new_transfers(_transfer_ids TEXT) RETURNS VOID AS $$
DECLARE
  _local_expires_at TIMESTAMP WITH TIME ZONE := expires_at() + INTERVAL '2 days';
  _event_id INT;
  _record RECORD;
BEGIN
  SELECT * INTO _record FROM event WHERE NAME = 'new_transfers' AND VALUE = _transfer_ids FOR UPDATE;
  IF NOT FOUND THEN
    -- try to insert an block event
    BEGIN
      INSERT INTO event (NAME, VALUE, expires_at)
                  VALUES ('new_transfers', _transfer_ids, _local_expires_at)
                  RETURNING event.ID INTO _event_id;
      PERFORM pg_notify('new_transfers', _event_id::TEXT);
    EXCEPTION WHEN unique_violation THEN
    END;
  ELSE
    UPDATE event SET expires_at = _local_expires_at WHERE NAME = 'new_transfers' AND VALUE = _transfer_ids;
  END IF;
END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS notify_pending_transaction(TEXT);
CREATE FUNCTION notify_pending_transaction(_tx_id TEXT) RETURNS VOID AS $$
DECLARE
  _local_expires_at TIMESTAMP WITH TIME ZONE := expires_at() + INTERVAL '2 days';
  _event_id INT;
  _record RECORD;
BEGIN
  SELECT * INTO _record FROM event WHERE NAME = 'new_pending_transaction' AND VALUE = _tx_id FOR UPDATE;
  IF NOT FOUND THEN
    -- try to insert a pending tx event
    BEGIN
      INSERT INTO event (NAME, VALUE, expires_at)
                  VALUES ('new_pending_transaction', _tx_id, _local_expires_at)
                  RETURNING event.ID INTO _event_id;
      PERFORM pg_notify('new_pending_transaction', _event_id::TEXT);
    EXCEPTION WHEN unique_violation THEN
    END;
  ELSE
    UPDATE event SET expires_at = _local_expires_at, notified = false, updated_at = now()
    WHERE NAME = 'new_pending_transaction' AND VALUE = _tx_id
    RETURNING event.ID INTO _event_id;

    PERFORM pg_notify('new_pending_transaction', _event_id::TEXT);
  END IF;
END;
$$ LANGUAGE plpgsql;

-- queries

-- query assets
-- e.g. SELECT * FROM assets_for_registrant('account...');          -- to use defaults
--      SELECT * FROM assets_for_registrant('account...', 15, 50);  -- 15 was sequence value from last entry

DROP FUNCTION IF EXISTS assets_for_registrant(TEXT, INT8, INT8);

CREATE FUNCTION assets_for_registrant(_registrant TEXT, _start INT8 DEFAULT 0, _count INT8 DEFAULT 10) RETURNS SETOF blockchain.asset AS $$
BEGIN
  RETURN QUERY SELECT * FROM blockchain.asset
    WHERE asset_registrant = _registrant
      AND asset_sequence > _start
      ORDER BY (asset_registrant, asset_sequence)
      LIMIT _count;
END;
$$ LANGUAGE plpgsql;


-- query transactions
-- e.g. SELECT * FROM transactions_for_owner('khdkfsdhk...');           -- to use defaults
--      SELECT * FROM transactions_for_owner('khdkfsdhk...', 15, 50);   -- 15 was sequence value from last entry

DROP FUNCTION IF EXISTS transactions_for_owner(TEXT, INT8, INT8);

CREATE FUNCTION transactions_for_owner(_owner TEXT, _start INT8 DEFAULT 0, _count INT8 DEFAULT 10) RETURNS SETOF blockchain.TRANSACTION AS $$
BEGIN
  RETURN QUERY SELECT * FROM blockchain.TRANSACTION
    WHERE tx_owner = _owner
      AND tx_sequence > _start
      ORDER BY (tx_sequence)
      LIMIT _count;
END;
$$ LANGUAGE plpgsql;


-- query block ownership
-- e.g. SELECT * FROM blocks_owned_by('khdkfsdhk...');           -- to use defaults
--      SELECT * FROM blocks_owned_by('khdkfsdhk...', 15, 50);   -- 15 was sequence value from last entry

DROP FUNCTION IF EXISTS blocks_owned_by(TEXT, INT8, INT8);

CREATE FUNCTION blocks_owned_by(_owner TEXT, _start INT8 DEFAULT 0, _count INT8 DEFAULT 10)
  RETURNS TABLE (
    block_number INT8,
    tx_id TEXT,
    tx_sequence INT8 ,
    tx_bitmark_id TEXT,
    tx_payments JSONB,
    tx_modified_at TIMESTAMP WITH TIME ZONE
  ) AS $$
BEGIN
  RETURN QUERY SELECT
      ir.tx_block_number AS obn,
      btt.tx_id, btt.tx_sequence,
      btt.tx_bitmark_id,
      btt.tx_payments,
      btt.tx_modified_at
    FROM blockchain.TRANSACTION AS btt
      JOIN blockchain.TRANSACTION AS ir
        ON btt.tx_bitmark_id = ir.tx_id
    WHERE btt.tx_owner = _owner
      AND btt.tx_sequence > _start
      AND btt.tx_asset_id IS NULL
      AND btt.tx_head = 'head'
      AND btt.tx_status = 'confirmed'
    ORDER BY (btt.tx_sequence)
    LIMIT _count;
END;
$$ LANGUAGE plpgsql;


-- set edition value on a particular owner's asset in this block

DROP FUNCTION IF EXISTS set_edition_value(TEXT, TEXT, INT8);

CREATE FUNCTION set_edition_value(_owner TEXT, _asset_id TEXT, _block_number INT8) RETURNS VOID AS $$
DECLARE
  _current_max_edition INT;
  _local_tx RECORD;
BEGIN
  SELECT MAX(tx_edition) INTO _current_max_edition
  FROM blockchain.transaction
  WHERE
    tx_owner = _owner AND
    tx_asset_id = _asset_id AND
    tx_previous_id IS NULL AND
    tx_block_number > 0 AND
    tx_block_number < _block_number;

  IF _current_max_edition IS NULL THEN
    _current_max_edition = -1;
  END IF;

  FOR _local_tx IN
    SELECT * FROM blockchain.TRANSACTION
    WHERE tx_owner = _owner
      AND tx_asset_id = _asset_id
      AND tx_previous_id IS NULL
      AND tx_block_number = _block_number
    ORDER BY tx_block_number, tx_block_offset ASC
  LOOP
    _current_max_edition := _current_max_edition + 1;
    UPDATE blockchain.TRANSACTION
    SET tx_edition = _current_max_edition
    WHERE tx_id = _local_tx.tx_id;
  END LOOP;
END;
$$ LANGUAGE plpgsql;


-- update editions of bitmarks issued in this block

DROP FUNCTION IF EXISTS update_editions(INT8);

CREATE FUNCTION update_editions(_block_number INT8) RETURNS VOID AS $$
DECLARE
  _local_tx RECORD;
  _local_first_block INT8 := 2;  -- genesis is 1
BEGIN
  IF _block_number < _local_first_block THEN
    RETURN;
  END IF;
  FOR _local_tx IN
    SELECT DISTINCT tx_owner, tx_asset_id, tx_previous_id, tx_block_number FROM blockchain.TRANSACTION
    WHERE tx_previous_id IS NULL
      AND tx_asset_id IS NOT NULL
      AND tx_block_number = _block_number
  LOOP
     PERFORM blockchain.set_edition_value(_local_tx.tx_owner, _local_tx.tx_asset_id, _block_number);
  END LOOP;
END;
$$ LANGUAGE plpgsql;



-- finished
SET search_path TO DEFAULT;
