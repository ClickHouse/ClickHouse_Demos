-- 1. Current probability and freshness
SELECT
    m.token_id,
    m.question,
    m.outcome,
    round(argMax(t.midpoint, t.event_at) * 100, 2) AS probability_percent,
    max(t.event_at) AS last_update
FROM polymarket.price_ticks AS t
INNER JOIN
(
    SELECT token_id, question, outcome
    FROM polymarket.markets FINAL
) AS m ON m.token_id = t.token_id
WHERE t.midpoint > 0
  AND t.event_at >= now() - INTERVAL 30 MINUTE
GROUP BY m.token_id, m.question, m.outcome
ORDER BY m.question, m.outcome;

-- 2. Five-minute movers
WITH now() AS current_time
SELECT
    m.token_id,
    m.question,
    m.outcome,
    round(argMaxIf(t.midpoint, t.event_at, t.event_at > current_time - INTERVAL 1 MINUTE) * 100, 2) AS now_percent,
    round(argMaxIf(t.midpoint, t.event_at, t.event_at <= current_time - INTERVAL 5 MINUTE) * 100, 2) AS five_minutes_ago_percent,
    round(now_percent - five_minutes_ago_percent, 2) AS move_points
FROM polymarket.price_ticks AS t
INNER JOIN
(
    SELECT token_id, question, outcome
    FROM polymarket.markets FINAL
) AS m ON m.token_id = t.token_id
WHERE t.midpoint > 0
  AND t.event_at >= current_time - INTERVAL 15 MINUTE
GROUP BY m.token_id, m.question, m.outcome
HAVING now_percent > 0 AND five_minutes_ago_percent > 0
ORDER BY abs(move_points) DESC;

-- 3. Spread and freshness
SELECT
    m.token_id,
    m.question,
    m.outcome,
    round(argMax(t.best_bid, t.event_at) * 100, 2) AS bid_percent,
    round(argMax(t.best_ask, t.event_at) * 100, 2) AS ask_percent,
    round(ask_percent - bid_percent, 2) AS spread_points,
    dateDiff('second', max(t.event_at), now()) AS age_seconds
FROM polymarket.price_ticks AS t
INNER JOIN
(
    SELECT token_id, question, outcome
    FROM polymarket.markets FINAL
) AS m ON m.token_id = t.token_id
WHERE t.best_bid > 0
  AND t.best_ask > 0
  AND t.event_at >= now() - INTERVAL 30 MINUTE
GROUP BY m.token_id, m.question, m.outcome
ORDER BY spread_points DESC;

-- 4. Trade-volume velocity
SELECT
    condition_id,
    token_id,
    title,
    outcome,
    round(sumIf(price * size, event_at >= now() - INTERVAL 5 MINUTE), 2) AS current_5m_usd,
    round(sumIf(
        price * size,
        event_at >= now() - INTERVAL 10 MINUTE
          AND event_at < now() - INTERVAL 5 MINUTE
    ), 2) AS previous_5m_usd,
    round(current_5m_usd / greatest(previous_5m_usd, 0.01), 2) AS velocity_ratio
FROM polymarket.trades_clean
WHERE event_at >= now() - INTERVAL 10 MINUTE
GROUP BY condition_id, token_id, title, outcome
ORDER BY current_5m_usd DESC;

-- 5. One-minute quote-midpoint OHLC
SELECT
    minute,
    token_id,
    round(argMinMerge(open) * 100, 2) AS open_percent,
    round(maxMerge(high) * 100, 2) AS high_percent,
    round(minMerge(low) * 100, 2) AS low_percent,
    round(argMaxMerge(close) * 100, 2) AS close_percent,
    countMerge(updates) AS updates
FROM polymarket.market_midpoints_1m
WHERE minute >= now() - INTERVAL 30 MINUTE
GROUP BY minute, token_id
ORDER BY minute DESC, token_id
LIMIT 30;
