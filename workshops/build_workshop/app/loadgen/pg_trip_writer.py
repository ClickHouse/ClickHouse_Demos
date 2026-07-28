from __future__ import annotations

import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg import errors, sql
from config import env, load_postgres_config

# Structured stdout logging so container logs are parseable by the ClickStack
# collector (filelog receiver) instead of bare prints. Level from LOG_LEVEL.
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logger = logging.getLogger("loadgen")


POSTGRES = load_postgres_config()
PGHOST = POSTGRES.host
PGPORT = POSTGRES.port
PGDATABASE = POSTGRES.database
PGUSER = POSTGRES.user
PGPASSWORD = POSTGRES.password
PGSSLMODE = POSTGRES.sslmode
# Publication the CDC ClickPipe subscribes to. On the participant's own managed
# Postgres the loadgen creates it (the connection user is the instance admin);
# on an instructor-provided managed instance the instructor pre-creates it and the scoped
# role lacks CREATE rights, so ensure_publication() below is a logged no-op.
PG_PUBLICATION = POSTGRES.publication

RATE_PER_SEC = float(env("RATE_PER_SEC", "5"))
BATCH_SIZE = int(env("BATCH_SIZE", "25"))


def ensure_publication(conn: psycopg.Connection, pub_name: str) -> None:
    """Idempotently ensure the CDC publication exists for public.realtime_trips.

    Removes any psql prerequisite for participants: on their own managed Postgres
    the loadgen creates the publication; on an instructor-provided managed instance it is
    pre-created and the scoped role cannot create it, so we swallow the
    insufficient-privilege error and log clearly. Either way the loadgen keeps
    running and the ClickPipe finds a publication to subscribe to.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (pub_name,))
        if cur.fetchone():
            logger.info(f"[loadgen] publication {pub_name} already exists; leaving as-is")
            return

    create_pub = sql.SQL("CREATE PUBLICATION {} FOR TABLE {}").format(
        sql.Identifier(pub_name),
        sql.Identifier("public", "realtime_trips"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(create_pub)
        logger.info(f"[loadgen] created publication {pub_name} for public.realtime_trips")
    except errors.DuplicateObject:
        # Lost a race with another writer; the publication now exists.
        logger.info(f"[loadgen] publication {pub_name} created concurrently; leaving as-is")
    except errors.InsufficientPrivilege:
        # Managed instructor-fallback path: the scoped role cannot CREATE PUBLICATION, but the
        # instructor pre-created it out of band, so this is expected and harmless.
        logger.warning(
            f"[loadgen] no privilege to create publication {pub_name}; assuming it was "
            "pre-created (managed instructor fallback) and continuing"
        )


def pick_zone_id() -> int:
    # Very rough weighting toward Manhattan-ish IDs, but still covers full range.
    # TLC taxi zones are typically 1..263.
    r = random.random()
    if r < 0.65:
        return random.randint(140, 250)
    return random.randint(1, 263)


def main() -> None:
    logger.info(f"[loadgen] connecting: {PGHOST}:{PGPORT} db={PGDATABASE} user={PGUSER}")

    # Create table if missing (idempotent). Debezium will capture changes.
    create_sql = """
    CREATE TABLE IF NOT EXISTS realtime_trips (
      id bigserial PRIMARY KEY,
      pickup_datetime timestamptz NOT NULL,
      dropoff_datetime timestamptz NOT NULL,
      pickup_location_id integer NOT NULL,
      dropoff_location_id integer NOT NULL,
      passenger_count smallint NOT NULL,
      trip_distance double precision NOT NULL,
      fare_amount double precision NOT NULL,
      tip_amount double precision NOT NULL,
      total_amount double precision NOT NULL,
      payment_type smallint NOT NULL,
      vendor_id smallint NOT NULL,
      car_type text NOT NULL DEFAULT 'yellow',
      created_at timestamptz NOT NULL DEFAULT now()
    );
    """

    insert_sql = """
    INSERT INTO realtime_trips
      (pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id,
       passenger_count, trip_distance, fare_amount, tip_amount, total_amount,
       payment_type, vendor_id, car_type)
    VALUES
      (%(pickup_datetime)s, %(dropoff_datetime)s, %(pickup_location_id)s, %(dropoff_location_id)s,
       %(passenger_count)s, %(trip_distance)s, %(fare_amount)s, %(tip_amount)s, %(total_amount)s,
       %(payment_type)s, %(vendor_id)s, %(car_type)s)
    """

    delay = max(0.01, BATCH_SIZE / max(0.001, RATE_PER_SEC))
    now = datetime.now(timezone.utc)

    with psycopg.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        sslmode=PGSSLMODE,
        autocommit=True,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(create_sql)
            logger.info(f"[loadgen] ensured realtime_trips table exists")
        ensure_publication(conn, PG_PUBLICATION)

        while True:
            rows = []
            for _ in range(BATCH_SIZE):
                pickup = now + timedelta(seconds=random.randint(-30, 0))
                duration_s = max(30, int(random.gauss(12 * 60, 6 * 60)))
                dropoff = pickup + timedelta(seconds=duration_s)

                dist = max(0.2, random.gauss(2.5, 1.8))
                fare = max(2.5, dist * random.uniform(2.5, 4.5))
                tip = 0.0 if random.random() < 0.2 else fare * random.uniform(0.10, 0.35)
                total = fare + tip + random.uniform(0, 3.0)

                rows.append(
                    {
                        "pickup_datetime": pickup,
                        "dropoff_datetime": dropoff,
                        "pickup_location_id": pick_zone_id(),
                        "dropoff_location_id": pick_zone_id(),
                        "passenger_count": random.choice([1, 1, 1, 2, 2, 3]),
                        "trip_distance": float(round(dist, 3)),
                        "fare_amount": float(round(fare, 2)),
                        "tip_amount": float(round(tip, 2)),
                        "total_amount": float(round(total, 2)),
                        "payment_type": random.choice([1, 1, 1, 2, 2, 1]),
                        "vendor_id": random.choice([1, 2]),
                        "car_type": random.choice(["yellow", "green"]),
                    }
                )

            with conn.cursor() as cur:
                cur.executemany(insert_sql, rows)
            now = datetime.now(timezone.utc)
            logger.info(f"[loadgen] inserted {len(rows)} trips @ {now.isoformat(timespec='seconds')}")
            time.sleep(delay)


if __name__ == "__main__":
    random.seed()
    main()
