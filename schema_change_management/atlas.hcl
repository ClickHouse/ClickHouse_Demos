// =============================================================================
// Atlas project config for the ClickHouse change-management demo.
//
// Everything secret comes from the environment, never from this file.
// Source .env before running any atlas command:
//
//     set -a && source .env && set +a
//
// Three environments, deliberately:
//
//   local  - ClickHouse OSS in Docker. Break this one freely. Rehearse here.
//   cloud  - the ClickHouse Cloud service you demo against.
//   ci     - no target database at all. Lints the migration directory only.
//            This is the environment your pull requests should run in.
// =============================================================================

// -----------------------------------------------------------------------------
// Shared lint policy.
//
// Be precise about what this block does, because a customer will read the Atlas
// docs during the meeting:
//
//   `error = true` is the DEFAULT for the destructive analyzer. Destructive
//   changes already fail `migrate lint` with exit code 1. Stating it here is a
//   declaration of intent — it means nobody can quietly set it to false in a
//   later PR without that showing up in review. It does not add enforcement.
//
//   `force = true` is the part that does. It prevents an author from silencing
//   the check with an inline `-- atlas:nolint` comment, and guarantees the
//   analyzer runs even when explicitly excluded. Atlas Pro. Use it for the
//   handful of rules you are willing to defend in a design review, not for
//   everything.
// -----------------------------------------------------------------------------
lint {
  destructive {
    error = true
    // force = true
  }
}

env "local" {
  url = getenv("CH_LOCAL_URL")
  dev = getenv("CH_DEV_URL")

  schema {
    src = "file://schema/sql/schema.sql"
  }

  migration {
    dir = "file://migrations"
  }

  // Keep generated plans readable. You are going to review these by hand.
  format {
    migrate {
      diff = "{{ sql . \"  \" }}"
    }
  }
}

env "cloud" {
  url = getenv("CH_CLOUD_URL")

  // The dev database is a scratch instance Atlas wipes on every run. It is how
  // Atlas validates a plan before touching your target.
  //
  // Two valid choices, and the trade-off is worth naming in the meeting:
  //
  //   docker://clickhouse/<ver>/dev
  //     Fast, free, offline. But it is ClickHouse OSS, so it does not know
  //     SharedMergeTree, and its version will not match Cloud exactly.
  //
  //   a second ClickHouse Cloud service
  //     Exact engine and version parity. Idles when unused, so cost is small.
  //     This is the right answer for anything you will run in production.
  dev = getenv("CH_DEV_URL")

  schema {
    src = "file://schema/sql/schema.sql"
  }

  migration {
    dir = "file://migrations"
  }

  format {
    migrate {
      diff = "{{ sql . \"  \" }}"
    }
  }
}

env "ci" {
  dev = getenv("CH_DEV_URL")

  schema {
    src = "file://schema/sql/schema.sql"
  }

  migration {
    dir = "file://migrations"
  }
}
