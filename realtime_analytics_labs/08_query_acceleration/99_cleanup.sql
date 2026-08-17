-- ============================================================
-- Section 08: Cleanup
-- ============================================================
-- Drops all objects created during the Query Acceleration labs.
-- Run this after the section to reset your environment.
-- ============================================================

SELECT '---- Cleaning up Section 08: Query Acceleration ----' AS step;

DROP DATABASE IF EXISTS nyc_taxi_perf;

SELECT '[OK] Section 08 cleanup complete. Database nyc_taxi_perf dropped.' AS status;
