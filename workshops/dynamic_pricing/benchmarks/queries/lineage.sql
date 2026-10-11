SELECT snapshot_id, sandbox_id, generation, scenario_cutoff, captured_at,
       stock_units, current_price_minor, snapshot_json
FROM pricing.input_snapshots FINAL
WHERE sandbox_id={sandbox:String} AND generation={generation:UInt32}
ORDER BY captured_at DESC LIMIT 20;
