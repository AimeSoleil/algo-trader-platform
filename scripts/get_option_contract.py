"""Get a specific option contract quote from yfinance by OCC symbol.

Example:
    uv run python -m scripts.get_option_contract META260323C00607500
"""
from __future__ import annotations

import argparse
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import pandas as pd
import yfinance as yf


_OCC_PATTERN = re.compile(r"^([A-Z]{1,6})(\d{6})([CP])(\d{8})$")


@dataclass
class OCCContract:
    contract_symbol: str
    underlying: str
    expiry: str
    option_type: str
    strike: float


def parse_occ_contract(contract_symbol: str) -> OCCContract:
    """Parse OCC option symbol like META260323C00607500."""
    normalized = contract_symbol.strip().upper()
    match = _OCC_PATTERN.fullmatch(normalized)
    if not match:
        raise ValueError(
            "Invalid OCC contract symbol. Expected format like META260323C00607500"
        )

    underlying, yymmdd, cp_flag, strike_raw = match.groups()
    expiry = datetime.strptime(yymmdd, "%y%m%d").date().isoformat()
    strike = int(strike_raw) / 1000.0
    option_type = "call" if cp_flag == "C" else "put"

    return OCCContract(
        contract_symbol=normalized,
        underlying=underlying,
        expiry=expiry,
        option_type=option_type,
        strike=strike,
    )


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return value


def fetch_contract_quote(contract: OCCContract) -> dict[str, Any]:
    ticker = yf.Ticker(contract.underlying)

    try:
        chain = ticker.option_chain(contract.expiry)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to fetch option chain for {contract.underlying} expiry={contract.expiry}: {exc}"
        ) from exc

    table = chain.calls if contract.option_type == "call" else chain.puts
    if table.empty:
        raise RuntimeError(
            f"No {contract.option_type} contracts found for {contract.underlying} on expiry={contract.expiry}"
        )

    row = table.loc[table["contractSymbol"] == contract.contract_symbol]
    if row.empty:
        # Fallback by strike for users who pass a valid OCC symbol but contract not present in chain response
        strike_row = table.loc[table["strike"] == contract.strike]
        if strike_row.empty:
            raise RuntimeError(
                "Contract not found in option chain. "
                f"symbol={contract.contract_symbol}, underlying={contract.underlying}, "
                f"expiry={contract.expiry}, type={contract.option_type}, strike={contract.strike}"
            )
        row = strike_row.head(1)

    raw_record = row.iloc[0].to_dict()
    record = {k: _clean_value(v) for k, v in raw_record.items()}

    underlying_price = None
    try:
        hist = ticker.history(period="1d")
        if not hist.empty:
            underlying_price = _clean_value(float(hist["Close"].iloc[-1]))
    except Exception:
        underlying_price = None

    return {
        "requested": asdict(contract),
        "underlying_price": underlying_price,
        "quote": record,
        "raw_quote": raw_record,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Get a specific option contract quote from yfinance by OCC symbol"
    )
    parser.add_argument(
        "contract_symbol",
        help="OCC contract symbol, e.g. META260323C00607500",
    )
    args = parser.parse_args()

    contract = parse_occ_contract(args.contract_symbol)
    payload = fetch_contract_quote(contract)
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
