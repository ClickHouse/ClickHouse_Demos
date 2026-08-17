-- ============================================================
-- Section 05: Cleanup
-- ============================================================
-- Drops all objects created during the Analyzing Data labs.
-- Run this after the section to reset your environment.
-- ============================================================

SELECT '---- Cleaning up Section 05: Analyzing Data ----' AS step;

DROP DATABASE IF EXISTS nyc_taxi_analytics;

SELECT '[OK] Section 05 cleanup complete. Database nyc_taxi_analytics dropped.' AS status;
