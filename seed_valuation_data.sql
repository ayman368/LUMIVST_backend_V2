-- =============================================================================
-- seed_valuation_data.sql
-- Run once after migration to populate initial static data.
-- Usage: psql -U <user> -d <db> -f seed_valuation_data.sql
-- =============================================================================

-- ── EPS Estimates ─────────────────────────────────────────────────────────────
INSERT INTO eps_estimates (year, value, type, source) VALUES
    (2024, 249.13, 'actual',   'S&P'),
    (2025, 271.30, 'actual',   'Yardeni'),
    (2026, 330.00, 'estimate', 'Yardeni'),
    (2027, 375.00, 'estimate', 'Yardeni')
ON CONFLICT (year) DO UPDATE SET
    value      = EXCLUDED.value,
    type       = EXCLUDED.type,
    source     = EXCLUDED.source,
    updated_at = NOW();

-- ── System Config ─────────────────────────────────────────────────────────────
INSERT INTO system_config (key, value, data_type, description) VALUES
    ('growth_ksa',                '4.0',          'float',  'KSA assumed GDP growth rate (%)'),
    ('dividend_yield_sp500',      '1.07',         'float',  'S&P 500 annual dividend yield (%)'),
    ('dividend_per_share_annual', '81.15',        'float',  'S&P 500 annual dividend per index point'),
    ('required_earnings_yield',   '7.53',         'float',  'Required EY threshold for fair value (%)'),
    ('tasi_max_weight_cap',       '10.221976',    'float',  'Max single-stock weight in TASI after cap (%)'),
    ('required_margin_over_bonds','3.0',          'float',  'Equity risk premium over A bond yield (%)'),
    ('sp500_fixed_pe_15',         '15',           'int',    'Fixed P/E scenario — conservative'),
    ('sp500_fixed_pe_17',         '17',           'int',    'Fixed P/E scenario — below average'),
    ('sp500_fixed_pe_20',         '20',           'int',    'Fixed P/E scenario — moderate'),
    ('sp500_fixed_pe_25',         '25',           'int',    'Fixed P/E scenario — historical median range'),
    ('sp500_tvm_years_2',         '2',            'int',    'TVM horizon — short term (years)'),
    ('sp500_tvm_years_3',         '3',            'int',    'TVM horizon — medium term (years)'),
    ('sp500_pe_history_years',    '7',            'int',    'Number of years used for historical P/E stats'),
    ('unemployment_positive_threshold',  '4.5',  'float',  'Unemployment rate below which verdict is Positive (%)'),
    ('payrolls_positive_threshold',      '60000','float',  'Monthly payrolls above which verdict is Positive'),
    ('initial_claims_positive_threshold','260000','float', '4-week MA initial claims below which verdict is Positive'),
    ('ey_a_ratio_attractive',     '1.5',          'float',  'EY/A ratio above which market is Attractive'),
    ('ey_a_ratio_neutral',        '1.0',          'float',  'EY/A ratio above which market is Neutral (below = Watch)')
ON CONFLICT (key) DO UPDATE SET
    value      = EXCLUDED.value,
    data_type  = EXCLUDED.data_type,
    description= EXCLUDED.description,
    updated_at = NOW();

-- ── Valuation Zones ───────────────────────────────────────────────────────────
-- price_to = 999999 means "and above" for the last zone
TRUNCATE valuation_zones RESTART IDENTITY;

INSERT INTO valuation_zones (label, label_ar, price_from, price_to, return_pct_low, return_pct_high, color_code, description, order_seq) VALUES
    ('Rare Golden Zone', 'منطقة ذهبية نادرة',    0,      4400,   30, 50, 'green',       'Extremely rare opportunity — market deeply undervalued', 1),
    ('Golden Zone',      'المنطقة الذهبية',      4400,   4700,   21, 24, 'green',       'Strong historical buying opportunity', 2),
    ('Strong Buy',       'شراء قوي',             5000,   5300,   16, 19, 'yellowgreen', 'Market offers attractive risk-adjusted returns', 3),
    ('Buy',              'شراء',                 5600,   6400,   10, 14, 'yellow',      'Market fairly priced relative to earnings', 4),
    ('Fair Value',       'قيمة عادلة',           6400,   7000,    6, 10, 'yellow',      'Market approaching fair value based on current yields', 5),
    ('Fair',             'قيمة منصفة',           7000,   7500,    4,  6, 'orange',      'Moderate return expected; proceed with caution', 6),
    ('Overvalued',       'مبالغ في التقييم',     7500,   8100,    0,  4, 'red',         'Market stretched vs bond yields; selective positioning', 7),
    ('Expensive',        'مرتفع جداً',           8100,   999999, -5,  2, 'red',         'Market expensive relative to current rate environment', 8);
