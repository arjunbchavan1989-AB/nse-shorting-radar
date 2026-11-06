#!/usr/bin/env python3
"""
NSE Shorting Radar — data pipeline.

Universe: NSE F&O stocks (futures & options) — the only practical shorting
universe in India (naked intraday shorts / futures / SLB).

Framework mapped to four traders:
  - Gil Morales (O'Neil Disciples): 50-day MA high-volume breaks, wedging
    rallies into declining 50-day, climax runs, distribution days, former
    leaders 8-12 weeks off peak, liquidity minimums.
  - Qullamaggie (Kristjan Kullamagi): parabolic short — extended run-ups
    (50%+ large cap / 100%+ small cap in days-weeks), 3-5+ up days, first
    crack / red day, cover into 10/20-day MAs.
  - Martin Luk: short at resistance not breakdowns, declining 9/21/50 EMA
    stack, first bounce into declining EMAs, weak stock in weak sector,
    multi-timeframe confirmation, 2-3 day holds.
  - Simon Russo: parabolic rallies in structurally weak names, capitulation
    volume/price acceleration, constant-risk pyramiding, cover into panic,
    shorts last 1-3 days.

Output: data/shorts.json + data/history.json (for drawer charts)
"""

import csv
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf

ROOT = Path(__file__).resolve().parent
NSE_FO_CSV = "https://archives.nseindia.com/content/fo/fo_mktlots.csv"

# --- config -------------------------------------------------------------------
MIN_TURNOVER = 50_000_000          # ₹50 cr default 30-day avg turnover (UI offers more)
VOL_SPIKE_RATIO = 1.5              # volume confirmation for breaks/distribution
RS_LOOKBACK = 63                    # ~3 months
DIST_WINDOW = 20                    # distribution-day lookback
RUNUP_WINDOWS = {"runup10": 10, "runup20": 20}
CHUNK = 60


def log(msg):
    print(msg, flush=True)


# --- universe -----------------------------------------------------------------
def fetch_fno():
    """Bundled fno.csv, refreshed from NSE lot-size file when reachable (CI)."""
    rows = []
    with open(ROOT / "fno.csv", newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r.get("Symbol"):
                rows.append({
                    "symbol": r["Symbol"].strip(),
                    "sector": r.get("Sector", "").strip(),
                    "company": r.get("Company", "").strip(),
                })
    try:
        r = requests.get(NSE_FO_CSV, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        r.raise_for_status()
        fresh = []
        for line in r.text.splitlines():
            cells = [c.strip().strip('"') for c in line.split(",")]
            if len(cells) < 3 or not cells[0].isalpha() or cells[0].isdigit():
                continue
            sym = cells[0]
            if sym in ("SYMBOL", "Symbol"):
                continue
            fresh.append({"symbol": sym, "sector": "", "company": cells[1]})
        if len(fresh) > 150:
            known = {r["sector"] for r in rows}
            sec_map = {r["symbol"]: r["sector"] for r in rows if r["sector"]}
            merged = []
            for r in fresh:
                merged.append({
                    "symbol": r["symbol"],
                    "sector": sec_map.get(r["symbol"], ""),
                    "company": r["company"],
                })
            log(f"fno list: {len(merged)} symbols (NSE)")
            return merged
    except Exception as e:
        log(f"fno refresh failed ({e}); using bundled list")
    log(f"fno list: {len(rows)} symbols (bundled)")
    return rows


# --- prices -------------------------------------------------------------------
def fetch_prices(symbols, index=False):
    out = {}
    syms = [s + ".NS" for s in symbols]
    if index:
        syms = symbols
    for i in range(0, len(syms), CHUNK):
        sub = syms[i:i + CHUNK]
        try:
            data = yf.download(sub, period="1y", interval="1d",
                               auto_adjust=True, group_by="ticker",
                               progress=False, threads=True)
        except Exception as e:
            log(f"chunk {i // CHUNK} failed: {e}")
            continue
        for j, s in enumerate(sub):
            raw = s[:-3] if not index else s
            try:
                d = data[s] if isinstance(data.columns, pd.MultiIndex) else data
                if len(d) and d["Close"].notna().any():
                    out[raw] = d
            except Exception:
                continue
        time.sleep(1)
    return out


# --- metrics ------------------------------------------------------------------
def ema(series, span):
    return series.ewm(span=span, adjust=False).mean()


def compute(sym, df, meta, nifty):
    d = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
    if len(d) < 210:
        return None
    closes, vols, opens = d["Close"], d["Volume"], d["Open"]
    last = float(closes.iloc[-1])
    prev = float(closes.iloc[-2])
    o = float(opens.iloc[-1])

    e9 = float(ema(closes, 9).iloc[-1])
    e21 = float(ema(closes, 21).iloc[-1])
    e50 = float(ema(closes, 50).iloc[-1])
    e200 = float(ema(closes, 200).iloc[-1])
    e50_prev = float(ema(closes, 50).iloc[-11])

    avg_vol50 = float(vols.iloc[-50:].mean())
    avg_vol30 = float(vols.iloc[-30:].mean())
    avg_turn30 = float((closes * vols).iloc[-30:].mean())
    vol_ratio = float(vols.iloc[-1]) / max(avg_vol50, 1)

    day_chg = (last / prev - 1) * 100
    hi52 = float(closes.max())
    pct_hi = (last / hi52 - 1) * 100

    # relative strength vs Nifty (~3 months)
    nc = nifty["Close"].dropna()
    stock_ret = last / float(closes.iloc[-RS_LOOKBACK]) - 1
    nift_ret = float(nc.iloc[-1]) / float(nc.iloc[-RS_LOOKBACK]) - 1
    rs3m = (stock_ret - nift_ret) * 100

    # distribution days: down days on >=1.5x volume in last 20
    dist_days = 0
    for k in range(-DIST_WINDOW, 0):
        if closes.iloc[k] < closes.iloc[k - 1] and vols.iloc[k] >= VOL_SPIKE_RATIO * avg_vol50:
            dist_days += 1

    # run-ups
    run = {}
    for name, w in RUNUP_WINDOWS.items():
        run[name] = (last / float(closes.iloc[-w - 1]) - 1) * 100

    # consecutive up days ending at the most recent peak (today if green, else yesterday)
    peak_i = len(closes) - 1 if last >= prev else len(closes) - 2
    up_streak = 0
    k = peak_i
    while k > 0 and closes.iloc[k] > closes.iloc[k - 1]:
        up_streak += 1
        k -= 1

    # recent low / bounce
    low20 = float(closes.iloc[-21:-1].min())
    bounce_from_low = (last / low20 - 1) * 100

    # setups ---------------------------------------------------------------
    setups = []

    # Qullamaggie / Russo: parabolic exhaustion + first crack
    parabolic = (run["runup20"] >= 30 or run["runup10"] >= 18) and up_streak >= 3
    first_crack = last < prev and last < float(d["Close"].iloc[-3])
    if parabolic:
        setups.append("PARABOLIC")
    if parabolic and (first_crack or (o > prev and last < o)):
        setups.append("FIRST CRACK")

    # Morales: climax reversal — biggest range day of the run on >=2x volume
    rng = float(d["High"].iloc[-1]) - float(d["Low"].iloc[-1])
    avg_rng = float((d["High"] - d["Low"]).iloc[-11:-1].mean())
    if (run["runup20"] >= 20 and rng >= 1.5 * avg_rng and vol_ratio >= 2.0
            and day_chg < 0):
        setups.append("CLIMAX REV")

    # Morales: high-volume 50-day MA break
    e50_y = float(ema(closes, 50).iloc[-2])
    if prev >= e50_y and last < e50 and vol_ratio >= VOL_SPIKE_RATIO:
        setups.append("50-DMA BREAK")

    # Morales / Luk: wedging rally into declining 50-day (short-at-resistance)
    rally = float(closes.iloc[-4:].max()) / low20 - 1 if len(closes) > 4 else 0
    vol_fade = float(vols.iloc[-3:].mean()) < float(vols.iloc[-10:-3].mean())
    near_e50 = abs(last / e50 - 1) <= 0.04
    if (last < e200 and e50 < e50_prev and rally >= 0.04 and vol_fade and near_e50):
        setups.append("WEDGE RALLY")

    # Luk: first bounce into declining 9/21 EMA after breakdown
    if (last < e200 and e21 < float(ema(closes, 21).iloc[-6])
            and bounce_from_low >= 3 and last <= e21 * 1.03 and up_streak >= 2):
        setups.append("BOUNCE SELL")

    # Luk: bear stack 9 < 21 < 50
    if e9 < e21 < e50 and last < e9:
        setups.append("BEAR STACK")

    # Morales / O'Neil: distribution
    if dist_days >= 4:
        setups.append("DISTRIBUTION")

    # structure breaks
    if last < float(closes.iloc[-21:-1].min()):
        setups.append("20D LOW BREAK")
    gap = (o / prev - 1) * 100
    if gap <= -2:
        setups.append("GAP DOWN")

    # Russo / Morales: broken former leader (far off 52w high, below everything)
    if pct_hi <= -35 and last < e50 < e200:
        setups.append("EX-LEADER")

    # --- score -------------------------------------------------------------
    score = 0
    score += 10 if last < e50 else 0
    score += 10 if last < e200 else 0
    score += 15 if "BEAR STACK" in setups else 0
    score += 15 if "PARABOLIC" in setups else 0
    score += 10 if "FIRST CRACK" in setups else 0
    score += 10 if "CLIMAX REV" in setups else 0
    score += 10 if "50-DMA BREAK" in setups else 0
    score += 10 if "WEDGE RALLY" in setups else 0
    score += 10 if "BOUNCE SELL" in setups else 0
    score += 5 if "DISTRIBUTION" in setups else 0
    score += 5 if "20D LOW BREAK" in setups else 0
    score += 5 if "EX-LEADER" in setups else 0
    score += 5 if "GAP DOWN" in setups else 0
    if rs3m <= -10:
        score += 15
    elif rs3m <= -5:
        score += 10
    elif rs3m < 0:
        score += 5
    if vol_ratio >= 2:
        score += 10
    elif vol_ratio >= VOL_SPIKE_RATIO:
        score += 5

    return {
        "symbol": sym,
        "company": meta.get("company", ""),
        "sector": meta.get("sector", ""),
        "close": round(last, 2),
        "dayChangePct": round(day_chg, 2),
        "ema9": round(e9, 2), "ema21": round(e21, 2),
        "ema50": round(e50, 2), "ema200": round(e200, 2),
        "avgVol30": int(avg_vol30),
        "avgTurnover30": int(avg_turn30),
        "volRatio": round(vol_ratio, 2),
        "runup10": round(run["runup10"], 1),
        "runup20": round(run["runup20"], 1),
        "upStreak": up_streak,
        "distDays": dist_days,
        "rs3m": round(rs3m, 1),
        "pctFrom52wHigh": round(pct_hi, 1),
        "bounceFromLow": round(bounce_from_low, 1),
        "setups": setups,
        "shortScore": min(score, 100),
        "parabolic": bool(parabolic),
        "candidate": bool(setups and avg_turn30 >= MIN_TURNOVER),
    }


def market_regime(nifty):
    c = nifty["Close"].dropna()
    v = nifty["Volume"].dropna()
    last = float(c.iloc[-1])
    e50 = float(ema(c, 50).iloc[-1])
    e200 = float(ema(c, 200).iloc[-1])
    avg_v50 = float(v.iloc[-50:].mean())
    dist = 0
    for k in range(-DIST_WINDOW, 0):
        if c.iloc[k] < c.iloc[k - 1] and v.iloc[k] >= VOL_SPIKE_RATIO * avg_v50:
            dist += 1
    below50 = last < e50
    below200 = last < e200
    if below50 and below200:
        regime = "BEAR — shorts favoured (Morales Rule 1: short only in a clear downtrend)"
    elif below50:
        regime = "CORRECTION — trade shorts only at resistance, keep stops tight"
    elif dist >= 4:
        regime = "DISTRIBUTING — leaders breaking down; prepare short watchlist"
    else:
        regime = "BULL — Morales Rule 1: avoid fresh shorts in a bull trend"
    return {
        "niftyClose": round(last, 1),
        "niftyDayPct": round((last / float(c.iloc[-2]) - 1) * 100, 2),
        "below50": below50, "below200": below200,
        "distDays": dist,
        "pctFrom52wHigh": round((last / float(c.max()) - 1) * 100, 1),
        "regime": regime,
    }


HISTORY_BARS = 140


def build_history(prices):
    hist = {}
    for sym, df in prices.items():
        if sym == "^NSEI":
            continue
        try:
            d = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
        except KeyError:
            continue
        if len(d) < 30:
            continue
        d = d.iloc[-HISTORY_BARS:]
        hist[sym] = {
            "t": [int(ts.timestamp() // 86400) for ts in d.index],
            "o": [round(float(x), 2) for x in d["Open"]],
            "h": [round(float(x), 2) for x in d["High"]],
            "l": [round(float(x), 2) for x in d["Low"]],
            "c": [round(float(x), 2) for x in d["Close"]],
            "v": [int(x) for x in d["Volume"]],
        }
    return hist


def main():
    universe = fetch_fno()
    symbols = [r["symbol"] for r in universe]
    meta = {r["symbol"]: r for r in universe}

    log(f"fetching prices for {len(symbols)} F&O symbols + NIFTY…")
    prices = fetch_prices(symbols)
    nifty = fetch_prices(["^NSEI"], index=True).get("^NSEI")
    if nifty is None:
        log("FATAL: NIFTY index fetch failed")
        sys.exit(1)

    stocks = []
    for sym in symbols:
        df = prices.get(sym)
        if df is None:
            continue
        rec = compute(sym, df, meta.get(sym, {}), nifty)
        if rec:
            stocks.append(rec)
    log(f"computed {len(stocks)} stocks, {len(symbols) - len(stocks)} failed/too short")

    regime = market_regime(nifty)
    candidates = [s for s in stocks if s["candidate"]]
    log(f"regime: {regime['regime']}")
    log(f"candidates: {len(candidates)}")

    doc = {
        "asOf": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "market": regime,
        "universeSize": len(symbols),
        "stocks": sorted(stocks, key=lambda s: -s["shortScore"]),
        "stats": {
            "computed": len(stocks),
            "candidates": len(candidates),
            "parabolic": len([s for s in stocks if s["parabolic"]]),
            "dmaBreaks": len([s for s in stocks if "50-DMA BREAK" in s["setups"]]),
            "newLows": len([s for s in stocks if "20D LOW BREAK" in s["setups"]]),
        },
    }

    out = ROOT / "data" / "shorts.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, separators=(",", ":"))
    log(f"wrote {out} ({out.stat().st_size / 1024:.0f} KB)")

    hist_out = ROOT / "data" / "history.json"
    with open(hist_out, "w", encoding="utf-8") as f:
        json.dump(build_history(prices), f, separators=(",", ":"))
    log(f"wrote {hist_out} ({hist_out.stat().st_size / 1024:.0f} KB)")

    # quality gate for CI
    if len(stocks) < 120:
        log(f"QUALITY GATE FAIL: only {len(stocks)} computed")
        sys.exit(2)
    log("quality gate passed")


if __name__ == "__main__":
    main()
