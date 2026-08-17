// =============================================================================
// HCL view of the same schema, for the "SQL vs HCL" comparison in the demo.
// =============================================================================
//
// READ THIS BEFORE YOU TRUST THIS FILE
//
// Do not hand-write ClickHouse HCL. Generate it:
//
//     ./scripts/gen-hcl.sh cloud
//
// which runs `atlas schema inspect --format '{{ hcl . }}'` against a database
// that already matches schema/sql/schema.sql, and overwrites this file with
// Atlas's own canonical representation.
//
// Two reasons this matters:
//
//   1. HCL attribute names for ClickHouse-specific features (partition key,
//      sort key, table settings, MV target) move between Atlas versions.
//      Generated HCL is correct for the Atlas binary you actually have.
//
//   2. Round-tripping is the cheapest parity test you will ever run. If you
//      inspect a table and the HCL comes back missing your codec, your TTL,
//      or your projection, then Atlas does not model that attribute yet, and
//      it will not defend it against drift. Better to learn that here than
//      in a plan against production.
//
// The block below is an ILLUSTRATIVE excerpt using constructs documented on
// atlasgo.io. Treat it as a shape, not as a working file.
// =============================================================================

schema "adtech" {
  // ClickHouse Cloud reports this back as `Shared`. Local OSS uses `Atomic`.
  // This single line is the clearest example of Cloud/OSS engine divergence.
  engine = sql("Atomic")
}

table "ad_events" {
  schema = schema.adtech
  engine = MergeTree
  ttl    = sql("event_date + toIntervalMonth(13)")

  column "event_time" {
    null = false
    type = DateTime
    // Codecs and other storage hints are exactly the kind of attribute worth
    // verifying round-trips before you rely on HCL as your source of truth.
  }
  column "event_date" {
    null    = false
    type    = Date
    default = sql("toDate(event_time)")
  }
  column "event_type" {
    null = false
    type = sql("LowCardinality(String)")
  }
  column "advertiser_id" {
    null = false
    type = UInt32
  }
  column "campaign_id" {
    null = false
    type = UInt32
  }
  column "creative_id" {
    null = false
    type = UInt32
  }
  column "placement_id" {
    null = false
    type = UInt32
  }
  column "user_id" {
    null = false
    type = UInt64
  }
  column "bid_price_usd" {
    null = false
    type = sql("Decimal(10, 6)")
  }
  column "revenue_usd" {
    null = false
    type = sql("Decimal(10, 6)")
  }

  primary_key {
    columns = [column.advertiser_id, column.campaign_id, column.event_time]
  }
}

// -----------------------------------------------------------------------------
// Verdict for the meeting: for ClickHouse specifically, plain SQL is the better
// source of truth for most data teams.
//
//   - Your engineers already read and write ClickHouse DDL. No new dialect.
//   - Every ClickHouse feature is expressible, including ones Atlas has not
//     modelled in HCL yet. There is no expressiveness ceiling.
//   - Code review happens on the exact text the database will see.
//
// HCL earns its place when you want one schema language across ClickHouse,
// Postgres and MySQL, or when you want to template a schema across tenants or
// clusters. If that is not you, stay on SQL.
// -----------------------------------------------------------------------------
