-- Execute with database=market_integrity. FINAL gives logical rows; synthetic fixtures are excluded.
SELECT 'market_metrics' AS table_name, count() AS rows FROM market_metrics FINAL WHERE NOT is_synthetic
UNION ALL SELECT 'cross_venue_metrics', count() FROM cross_venue_metrics FINAL WHERE NOT is_synthetic
UNION ALL SELECT 'derivatives_metrics', count() FROM derivatives_metrics FINAL WHERE NOT is_synthetic
UNION ALL SELECT 'market_anomalies', count() FROM market_anomalies FINAL WHERE NOT is_synthetic
UNION ALL SELECT 'market_integrity', count() FROM market_integrity FINAL WHERE NOT is_synthetic;

SELECT timestamp, exchange, symbol, window, trade_count, quote_volume, vwap, spread_bps, data_quality_flags FROM market_metrics FINAL WHERE NOT is_synthetic ORDER BY timestamp DESC LIMIT 10;
SELECT timestamp, window, active_venue_count, median_price, dispersion_bps, confirmation_ratio, confirmation_state FROM cross_venue_metrics FINAL WHERE NOT is_synthetic ORDER BY timestamp DESC LIMIT 10;
SELECT timestamp, exchange, window, perpetual_price, open_interest, funding_rate, basis_bps, perpetual_to_spot_volume_ratio, data_quality_flags FROM derivatives_metrics FINAL WHERE NOT is_synthetic ORDER BY timestamp DESC LIMIT 10;
SELECT detected_at, event_id, event_type, severity, direction, evidence FROM market_anomalies FINAL WHERE NOT is_synthetic ORDER BY detected_at DESC LIMIT 10;
SELECT calculated_at, event_id, integrity_score, overall_confidence, classification, components FROM market_integrity FINAL WHERE NOT is_synthetic ORDER BY calculated_at DESC LIMIT 10;
SELECT calculated_at, event_id, canonical_pair, move_window, integrity_score, overall_confidence, evidence_coverage FROM market_integrity FINAL WHERE NOT is_synthetic ORDER BY calculated_at DESC LIMIT 100;
SELECT classification, count() AS events FROM market_integrity FINAL WHERE NOT is_synthetic GROUP BY classification ORDER BY events DESC;
SELECT calculated_at, event_id, integrity_score, overall_confidence, classification, components FROM market_integrity FINAL WHERE NOT is_synthetic AND overall_confidence < 0.5 ORDER BY calculated_at DESC LIMIT 20;
SELECT timestamp, window, dispersion_bps, venue_prices, venue_deviation_bps FROM cross_venue_metrics FINAL WHERE NOT is_synthetic ORDER BY timestamp DESC LIMIT 10;

-- Physical duplicate visibility before background merges, not a permanent duplicate ledger.
SELECT event_id, count() AS physical_rows FROM market_integrity WHERE NOT is_synthetic GROUP BY event_id HAVING count() > 1 ORDER BY physical_rows DESC LIMIT 20;
