-- Availability supplies full known-price intervals, including eligible zero-sale hours.
-- Only tagged ample-stock calibration sales are labels. Returns stay in economic reports.
SELECT a.sku, a.store_id, a.hour, a.offered_price_minor, a.reference_price_minor,
       a.exposure_hours, a.available_units, a.open,
       true AS single_price, s.units AS units
FROM __DATABASE__.__AVAILABILITY__ AS a
LEFT JOIN (
  SELECT sku, store_id, toStartOfHour(event_time) AS hour, sum(quantity) AS units,
         uniqExact(offered_price_minor) AS prices, max(offered_price_minor) AS price
  FROM __DATABASE__.__RAW__
  WHERE sandbox_id = {sandbox:String} AND calibration AND event_type = 'sale' AND quantity > 0
  GROUP BY sku, store_id, hour
) AS s ON a.sku = s.sku AND a.store_id = s.store_id AND a.hour = s.hour
WHERE a.open AND a.calibration AND a.exposure_hours = 1 AND a.available_units > s.units
  AND s.prices <= 1 AND (s.prices = 0 OR s.price = a.offered_price_minor)
ORDER BY a.hour, a.sku, a.store_id;
