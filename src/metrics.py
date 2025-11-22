from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class MarketPayload:
    market_tick: Dict[str, Any]
    technicals: Dict[str, Any]


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _make_timestamp(ms: Any) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


def _sma(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    window = values[-period:]
    return sum(window) / period


def _ema(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = sum(values[:period]) / period
    for val in values[period:]:
        ema = (val - ema) * multiplier + ema
    return ema


def _rsi(values: List[float], period: int = 14) -> Optional[float]:
    if len(values) <= period:
        return None
    gains = []
    losses = []
    for prev, curr in zip(values[-period - 1 : -1], values[-period:]):
        change = curr - prev
        if change >= 0:
            gains.append(change)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(change))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _macd(values: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[Optional[float], Optional[float]]:
    if len(values) < slow + signal:
        return None, None
    macd_line_values = []
    multiplier_fast = 2 / (fast + 1)
    multiplier_slow = 2 / (slow + 1)
    ema_fast_val = sum(values[:fast]) / fast
    ema_slow_val = sum(values[:slow]) / slow

    for price in values[fast:slow]:
        ema_fast_val = (price - ema_fast_val) * multiplier_fast + ema_fast_val

    for price in values[slow:]:
        ema_fast_val = (price - ema_fast_val) * multiplier_fast + ema_fast_val
        ema_slow_val = (price - ema_slow_val) * multiplier_slow + ema_slow_val
        macd_line_values.append(ema_fast_val - ema_slow_val)

    if len(macd_line_values) < signal:
        return None, None

    signal_line = _ema(macd_line_values, signal)
    macd_line = macd_line_values[-1]
    return macd_line, signal_line


def _log_returns(values: List[float]) -> List[float]:
    returns: List[float] = []
    for prev, curr in zip(values[:-1], values[1:]):
        if prev <= 0 or curr <= 0:
            continue
        returns.append(math.log(curr / prev))
    return returns


def _annualized_volatility(log_returns: List[float], periods_per_day: int) -> Optional[float]:
    if not log_returns:
        return None
    mean_return = sum(log_returns) / len(log_returns)
    variance = sum((r - mean_return) ** 2 for r in log_returns) / len(log_returns)
    return math.sqrt(variance * periods_per_day)


def _sum_depth(levels: List[List[str]], top_n: int) -> Dict[str, float]:
    top_levels = levels[:top_n]
    base_qty = 0.0
    quote_qty = 0.0
    for price_str, qty_str in top_levels:
        price = _to_float(price_str) or 0.0
        qty = _to_float(qty_str) or 0.0
        base_qty += qty
        quote_qty += price * qty
    return {"base_qty": base_qty, "quote_qty": quote_qty}


def _format_depth(levels: List[List[str]], top_n: int) -> Dict[str, Any]:
    formatted_levels = []
    for price_str, qty_str in levels[:top_n]:
        formatted_levels.append(
            {
                "price": _to_float(price_str),
                "quantity": _to_float(qty_str),
            }
        )
    sums = _sum_depth(levels, top_n)
    return {"levels": formatted_levels, "base_total": sums["base_qty"], "quote_total": sums["quote_qty"]}


def build_payload(snapshot: Dict[str, Any]) -> MarketPayload:
    symbol = snapshot["symbol"].upper()
    ticker = snapshot["ticker"]
    orderbook = snapshot["orderbook"]
    klines = snapshot["klines"]

    latest_kline = klines[-1]
    closes = [
        _to_float(kline[4]) or 0.0 for kline in klines if _to_float(kline[4]) is not None
    ]

    exchange_ts = _make_timestamp(latest_kline[6])

    last_close = _to_float(latest_kline[4])
    open_price = _to_float(latest_kline[1])
    high_price = max((_to_float(k[2]) or 0.0) for k in klines[-60:]) if len(klines) else None
    low_price = min((_to_float(k[3]) or 0.0) for k in klines[-60:]) if len(klines) else None

    price_1h = None
    if len(closes) >= 60:
        price_1h = closes[-60]

    price_change_pct_1h = None
    if last_close and price_1h and price_1h != 0:
        price_change_pct_1h = (last_close - price_1h) / price_1h * 100

    bid_levels = orderbook.get("bids", [])
    ask_levels = orderbook.get("asks", [])
    bid_top = bid_levels[0] if bid_levels else None
    ask_top = ask_levels[0] if ask_levels else None

    bid_price = _to_float(bid_top[0]) if bid_top else None
    ask_price = _to_float(ask_top[0]) if ask_top else None
    bid_ask_spread = None
    if bid_price and ask_price:
        bid_ask_spread = ask_price - bid_price

    moving_average_7 = _sma(closes, 7)
    moving_average_30 = _sma(closes, 30)
    rsi_14 = _rsi(closes, 14)
    macd_line, macd_signal = _macd(closes)

    log_returns = _log_returns(closes[-1440:])
    rolling_volatility_24h = _annualized_volatility(log_returns, periods_per_day=1440)

    rolling_return_1h = None
    if last_close and price_1h:
        rolling_return_1h = (last_close / price_1h) - 1

    high_24h = _to_float(ticker.get("highPrice"))
    low_24h = _to_float(ticker.get("lowPrice"))
    high_low_range_24h = None
    if high_24h is not None and low_24h is not None:
        high_low_range_24h = high_24h - low_24h

    bid_depth = _format_depth(bid_levels, 5)
    ask_depth = _format_depth(ask_levels, 5)

    market_payload = {
        "symbol": symbol,
        "exchange": "BINANCE",
        "window_interval": "1m",
        "exchange_ts": exchange_ts.isoformat(),
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": last_close,
        "last_price": _to_float(ticker.get("lastPrice")),
        "price_change_pct_1h": price_change_pct_1h,
        "price_change_pct_24h": _to_float(ticker.get("priceChangePercent")),
        "volume_24h_quote": _to_float(ticker.get("quoteVolume")),
        "volume_change_pct_24h": None,
        "market_cap": None,
        "circulating_supply": None,
        "bid_ask_spread": bid_ask_spread,
        "bid_depth": bid_depth,
        "ask_depth": ask_depth,
        "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
    }

    technical_payload = {
        "symbol": symbol,
        "window_interval": "1m",
        "exchange_ts": exchange_ts.isoformat(),
        "rolling_return_1h": rolling_return_1h,
        "rolling_return_24h": (_to_float(ticker.get("priceChangePercent")) or 0.0) / 100
        if ticker.get("priceChangePercent")
        else None,
        "rolling_volatility_24h": rolling_volatility_24h,
        "high_low_range_24h": high_low_range_24h,
        "moving_average_7": moving_average_7,
        "moving_average_30": moving_average_30,
        "rsi_14": rsi_14,
        "macd": macd_line,
        "macd_signal": macd_signal,
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }

    return MarketPayload(market_tick=market_payload, technicals=technical_payload)
