https://www.nseindia.com/static/market-data/securities-available-for-trading

-- ============================
-- INDIA EQUITY (YAHOO)
-- ============================
SELECT
    'india_equity_yahoo' AS asset_type,
    s.symbol_id,
    s.yahoo_symbol,
    s.name
FROM india_equity_symbols s
LEFT JOIN india_equity_yahoo_price_data p
       ON s.symbol_id = p.symbol_id
WHERE p.symbol_id IS NULL

UNION ALL

-- ============================
-- INDIA EQUITY (NSE)
-- ============================
SELECT
    'india_equity_nse' AS asset_type,
    s.symbol_id,
    s.yahoo_symbol,
    s.name
FROM india_equity_symbols s
LEFT JOIN india_equity_nse_price_data p
       ON s.symbol_id = p.symbol_id
WHERE p.symbol_id IS NULL

UNION ALL

-- ============================
-- INDIA EQUITY (TEST)
-- ============================
SELECT
    'india_equity_test' AS asset_type,
    s.symbol_id,
    s.yahoo_symbol,
    s.name
FROM india_equity_symbols s
LEFT JOIN india_equity_test_price_data p
       ON s.symbol_id = p.symbol_id
WHERE p.symbol_id IS NULL

UNION ALL

-- ============================
-- INDIA INDEX
-- ============================
SELECT
    'india_index' AS asset_type,
    s.symbol_id,
    s.yahoo_symbol,
    s.name
FROM india_index_symbols s
LEFT JOIN india_index_price_data p
       ON s.symbol_id = p.symbol_id
WHERE p.symbol_id IS NULL

UNION ALL

-- ============================
-- USA EQUITY
-- ============================
SELECT
    'usa_equity' AS asset_type,
    s.symbol_id,
    s.yahoo_symbol,
    s.name
FROM usa_equity_symbols s
LEFT JOIN usa_equity_price_data p
       ON s.symbol_id = p.symbol_id
WHERE p.symbol_id IS NULL

UNION ALL

-- ============================
-- GLOBAL INDEX
-- ============================
SELECT
    'global_index' AS asset_type,
    s.symbol_id,
    s.yahoo_symbol,
    s.name
FROM global_index_symbols s
LEFT JOIN global_index_price_data p
       ON s.symbol_id = p.symbol_id
WHERE p.symbol_id IS NULL

UNION ALL

-- ============================
-- COMMODITY
-- ============================
SELECT
    'commodity' AS asset_type,
    s.symbol_id,
    s.yahoo_symbol,
    s.name
FROM commodity_symbols s
LEFT JOIN commodity_price_data p
       ON s.symbol_id = p.symbol_id
WHERE p.symbol_id IS NULL

UNION ALL

-- ============================
-- CRYPTO
-- ============================
SELECT
    'crypto' AS asset_type,
    s.symbol_id,
    s.yahoo_symbol,
    s.name
FROM crypto_symbols s
LEFT JOIN crypto_price_data p
       ON s.symbol_id = p.symbol_id
WHERE p.symbol_id IS NULL

UNION ALL

-- ============================
-- FOREX
-- ============================
SELECT
    'forex' AS asset_type,
    s.symbol_id,
    s.yahoo_symbol,
    s.name
FROM forex_symbols s
LEFT JOIN forex_price_data p
       ON s.symbol_id = p.symbol_id
WHERE p.symbol_id IS NULL

ORDER BY asset_type, yahoo_symbol;
______________________________________________________________________________________________________________________________
SELECT
    s.yahoo_symbol AS symbol,

    CASE
        WHEN COUNT(*) FILTER (
            WHERE p.date = date_trunc('week', current_date)::date
        ) > 0 THEN 'Yes' ELSE 'No'
    END AS "Monday",

    CASE
        WHEN COUNT(*) FILTER (
            WHERE p.date = (date_trunc('week', current_date) + INTERVAL '1 day')::date
        ) > 0 THEN 'Yes' ELSE 'No'
    END AS "Tuesday",

    CASE
        WHEN COUNT(*) FILTER (
            WHERE p.date = (date_trunc('week', current_date) + INTERVAL '2 day')::date
        ) > 0 THEN 'Yes' ELSE 'No'
    END AS "Wednesday",

    CASE
        WHEN COUNT(*) FILTER (
            WHERE p.date = (date_trunc('week', current_date) + INTERVAL '3 day')::date
        ) > 0 THEN 'Yes' ELSE 'No'
    END AS "Thursday",

    CASE
        WHEN COUNT(*) FILTER (
            WHERE p.date = (date_trunc('week', current_date) + INTERVAL '4 day')::date
        ) > 0 THEN 'Yes' ELSE 'No'
    END AS "Friday"

FROM india_equity_symbols s
LEFT JOIN india_equity_yahoo_price_data p
    ON p.symbol_id = s.symbol_id
   AND p.timeframe = '1wk'
   AND p.date >= date_trunc('week', current_date)::date
   AND p.date <  (date_trunc('week', current_date) + INTERVAL '5 day')::date

GROUP BY s.yahoo_symbol
ORDER BY s.yahoo_symbol;