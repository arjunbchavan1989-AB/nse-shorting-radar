# NSE Shorting Radar

Daily short-selling screener for **NSE F&O stocks** (the only practical shorting
universe in India), built on the published methodologies of four traders:
**Gil Morales** (O'Neil Disciples), **Qullamaggie** (Kristjan Kullamägi),
**Martin Luk** and **Simon Russo** (Market Wizards: The Next Generation).

Live: https://arjunbchavan1989-AB.github.io/nse-shorting-radar/

## What it does

Every trading day at **19:00 IST** (GitHub Actions, plus a 14:00 IST hedge run)
the pipeline downloads 1 year of daily OHLCV for all ~220 F&O stocks plus the
NIFTY index, computes trend structure and setup tags, and republishes the site.

### Market regime (Morales Rule 1)

The banner computes the NIFTY's position vs its 50/200 EMA, distance from the
52-week high and distribution-day count, and labels the market BEAR /
CORRECTION / DISTRIBUTING / BULL. Morales: short only in a clear downtrend.

### Setup tags → trader mapping

| Tag | Trigger | Source |
|---|---|---|
| PARABOLIC | +18% / 10d or +30% / 20d run with 3+ up-day streak | Qullamaggie / Russo |
| FIRST CRACK | Parabolic closing below prior two closes | Qullamaggie |
| CLIMAX REV | Red day after 20%+ run, 1.5× range on 2× volume | Morales / O'Neil |
| 50-DMA BREAK | First close below 50-day EMA on ≥1.5× volume | Morales |
| WEDGE RALLY | Bounce on fading volume into declining 50 EMA below the 200 | Morales / Luk |
| BOUNCE SELL | Bounce off the low into a declining 21 EMA below the 200 | Martin Luk |
| BEAR STACK | 9 < 21 < 50 EMA, price below all | Martin Luk |
| DISTRIBUTION | 4+ down days on ≥1.5× volume in 20 sessions | Morales / O'Neil |
| 20D LOW BREAK | Close below the prior 20-session low close | structure |
| GAP DOWN | Open ≥2% below prior close | structure |
| EX-LEADER | 35%+ off 52w high below falling 50 < 200 stack | Morales / Russo |

### Short score (0–100)

Trend structure (35) + triggered setups (up to 40) + 3-month relative strength
vs NIFTY (15) + volume confirmation (10). Candidates = at least one setup tag
and 30-day average turnover above the liquidity filter (₹50/100/250 cr).

## Data

- Universe: NSE F&O list (bundled `fno.csv`, refreshed from NSE's lot-size
  file when reachable)
- Prices: Yahoo Finance daily OHLCV, 1 year, auto-adjusted
- No news/catalyst pipeline — the shorting framework is technicals-first

## Local run

```bash
pip install -r requirements.txt
python pipeline.py            # writes data/shorts.json + data/history.json
python3 -m http.server        # serve and open index.html
```

## Disclaimer

Research and education only — not investment advice. Shorting carries
unlimited-loss risk; F&O positions can gap through stops.
