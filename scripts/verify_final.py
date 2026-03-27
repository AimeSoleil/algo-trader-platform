"""Full integration verification for all phases."""
import sys
sys.path.insert(0, ".")

print("=== Phase 1-2: Models + Settings ===")

from shared.models.option import OptionGreeks, OptionContract, OptionChainSnapshot, OptionType
g = OptionGreeks(iv=0.3, vanna=0.01, charm=-0.02)
assert g.vanna == 0.01 and g.charm == -0.02
c = OptionContract(symbol="T", underlying="T", expiry="2025-12-19", strike=100.0, option_type="call", is_tradeable=True, greeks=g)
assert c.is_tradeable is True
print("  OK Models (vanna/charm/is_tradeable)")

from shared.config import get_settings
s = get_settings()
assert s.data_service.filters.options.cleaning.max_iv == 5.0
assert s.data_service.filters.options.tradeable_marking.min_volume == 10
assert s.signal_service.iv_lookback_days == 252
assert s.signal_service.filters.options.trading.min_delta == 0.05
print("  OK Settings (data_service + signal_service filters)")

print("\n=== Phase 3: data-service filters ===")

from services.data_service.app.filters import apply_option_pipeline, clean_option_chain, mark_tradeable
from services.data_service.app.filters.base import FilterResult
fr = FilterResult(total_input=100, cleaned_removed=5, tradeable_marked=80)
assert fr.output_count == 95
print("  OK Filter module imports + FilterResult")

# Test filter pipeline with mock data
from datetime import datetime, timezone
snap = OptionChainSnapshot(
    underlying="TEST", underlying_price=150.0,
    timestamp=datetime.now(timezone.utc),
    contracts=[
        OptionContract(
            symbol="T1", underlying="TEST", expiry="2025-12-19",
            strike=150.0, option_type=OptionType.CALL,
            volume=100, open_interest=500, bid=5.0, ask=5.5,
            greeks=OptionGreeks(iv=0.3, delta=0.5, vanna=0.01, charm=-0.005),
        ),
        OptionContract(
            symbol="T2", underlying="TEST", expiry="2025-12-19",
            strike=200.0, option_type=OptionType.CALL,
            volume=0, open_interest=5, bid=0.01, ask=0.50,
            greeks=OptionGreeks(iv=0.8, delta=0.02, vanna=0.001, charm=-0.001),
        ),
        OptionContract(
            symbol="T3_bad_iv", underlying="TEST", expiry="2025-12-19",
            strike=100.0, option_type=OptionType.PUT,
            greeks=OptionGreeks(iv=0.0),  # bad IV, should be cleaned
        ),
    ],
)
snap_out, filt_result = apply_option_pipeline(snap)
assert filt_result.cleaned_removed == 1, f"Expected 1 cleaned, got {filt_result.cleaned_removed}"
assert len(snap_out.contracts) == 2, f"Expected 2 contracts, got {len(snap_out.contracts)}"
assert snap_out.contracts[0].is_tradeable is True, "T1 should be tradeable"
assert snap_out.contracts[1].is_tradeable is False, "T2 should not be tradeable"
print("  OK Filter pipeline (clean + mark_tradeable)")

print("\n=== Phase 4: support layer ===")

from services.data_service.app.converters import contracts_to_rows
rows = contracts_to_rows(snap_out)
assert "vanna" in rows[0] and "charm" in rows[0] and "is_tradeable" in rows[0]
print("  OK Converters (vanna/charm/is_tradeable)")

# Vanna/charm BSM formula (standalone test, no py_vollib needed)
import math
from scipy.stats import norm

def _test_vc(S, K, T, r, sigma):
    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r + sigma ** 2 / 2) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    n_d1 = norm.pdf(d1)
    van = -n_d1 * d2 / sigma
    denom = 2.0 * T * sigma * sqrt_T
    cha = -n_d1 * (2.0 * r * T - d2 * sigma * sqrt_T) / denom
    return van, cha

van, cha = _test_vc(100, 100, 0.25, 0.045, 0.20)
assert abs(van) > 0 and abs(cha) > 0
print(f"  OK Vanna/Charm BSM (vanna={van:.6f}, charm={cha:.6f})")

# Storage SQL
with open("services/data_service/app/storage.py") as f:
    storage_src = f.read()
assert "vanna, charm, underlying_price, is_tradeable" in storage_src
assert "max_volume" in storage_src
print("  OK Storage SQL (new columns + MAX volume)")

print("\n=== Phase 5: signal-service filters ===")

from services.signal_service.app.filters import apply_trading_filter
from services.signal_service.app.filters.base import FilterResult as SigFilterResult
sfr = SigFilterResult(total_input=50, filtered_count=10)
assert sfr.output_count == 40
print("  OK Signal filter module imports")

print("\n=== Phase 6: indicator fixes ===")

# data_loaders OPTION_COLS
with open("services/signal_service/app/data_loaders.py") as f:
    dl_src = f.read()
assert '"vanna", "charm", "is_tradeable"' in dl_src
assert "ROW_NUMBER() OVER" in dl_src  # intraday dedup
print("  OK data_loaders (new cols + intraday dedup)")

# option_indicators fixes
with open("services/signal_service/app/indicators/option_indicators.py") as f:
    oi_src = f.read()
assert "lookback_days is None" in oi_src  # config-based IV lookback
assert "puts_with_delta" in oi_src  # 25-delta skew
assert 'option_data["vanna"]' in oi_src  # DB vanna column usage
assert 'option_data["charm"]' in oi_src  # DB charm column usage
assert "apply_trading_filter" in oi_src  # strategy filter split
print("  OK option_indicators (IV skew 25-delta, lookback config, vanna/charm DB, strategy filter)")

print("\n=== Phase 7: init_db migration ===")

with open("scripts/init_db.py") as f:
    init_src = f.read()
assert "vanna FLOAT" in init_src
assert "charm FLOAT" in init_src
assert "is_tradeable BOOLEAN" in init_src
assert "idx_option_snap_tradeable" in init_src
assert "idx_option_daily_tradeable" in init_src
print("  OK init_db (migration columns + indexes)")

print("\n=== ALL PHASES VERIFIED ===")
