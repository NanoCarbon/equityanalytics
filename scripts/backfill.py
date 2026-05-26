"""
scripts/backfill.py — Parameterized backfill for any ticker list and date range.

Idempotent: skips tickers already present in each target table so re-running
after a partial failure is safe — no duplicates will be created.

Usage
-----
# Backfill every ticker missing from RAW.PRICES (the common post-expansion case):
  python scripts/backfill.py --missing --start 2010-01-01

# Specific tickers, all data types:
  python scripts/backfill.py --tickers "AZN.L,RY.TO,7203.T" --start 2015-01-01

# From a file, prices only:
  python scripts/backfill.py --tickers-file my_tickers.txt \\
      --start 2010-01-01 --types prices

# International Phase 3 tickers only:
  python scripts/backfill.py --source ftse100,tsx60,asx200,nikkei225,dax40 \\
      --start 2010-01-01

Rate-limiting behaviour
-----------------------
  Prices (yf.download bulk):    batch_size=100, 30 s between batches
  Per-ticker endpoints (.info,  2 s per ticker, 30 s pause every 100 tickers
  .income_stmt, etc.):          (handled inside each extractor function)

The --batch-size and --batch-delay flags override the prices batch defaults.
Per-ticker endpoint delays are set via --ticker-delay and --ticker-batch-delay.
"""

import argparse
import logging
import sys
import time
from datetime import date, datetime
from pathlib import Path

# Resolve project root so ingestion.* imports work regardless of cwd
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Load .env if present (local dev — Airflow containers inject env vars directly)
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            import os; os.environ.setdefault(k.strip(), v.strip())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Defaults ──────────────────────────────────────────────────────────────────

PRICE_BATCH_SIZE  = 100   # tickers per yf.download call
PRICE_BATCH_DELAY = 30    # seconds between price download batches

INFO_TICKER_DELAY      = 2    # seconds between individual .info / statement calls
INFO_BATCH_SIZE        = 100  # tickers before the longer pause in per-ticker extractors
INFO_BATCH_DELAY       = 30   # seconds for that longer pause


# ── Ticker resolution ─────────────────────────────────────────────────────────

def resolve_tickers(args) -> list[str]:
    """
    Return the ordered list of tickers to process.

    Three modes (mutually exclusive CLI args):
      --tickers  "A,B,C"       inline comma-separated list
      --tickers-file path.txt  one ticker per line, # lines ignored
      --missing                auto-diff: every active ticker in
                               TICKER_UNIVERSE that has NO rows in
                               RAW.PRICES (or --missing-table)
      --source   "ftse100,..."  active tickers from specific source labels
    """
    from ingestion.load import get_connection, get_loaded_tickers

    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
        logger.info("Ticker list: %d tickers from --tickers flag", len(tickers))
        return tickers

    if args.tickers_file:
        path = Path(args.tickers_file)
        if not path.exists():
            raise FileNotFoundError(f"--tickers-file not found: {path}")
        tickers = [
            ln.strip() for ln in path.read_text().splitlines()
            if ln.strip() and not ln.strip().startswith("#")
        ]
        logger.info("Ticker list: %d tickers from %s", len(tickers), path)
        return tickers

    if args.source:
        sources = [s.strip() for s in args.source.split(",") if s.strip()]
        conn = get_connection()
        try:
            cur = conn.cursor()
            placeholders = ", ".join(f"'{s}'" for s in sources)
            cur.execute(f"""
                SELECT ticker FROM EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
                WHERE  is_active = TRUE
                  AND  source IN ({placeholders})
                ORDER  BY source, ticker
            """)
            tickers = [r[0] for r in cur.fetchall()]
        finally:
            conn.close()
        logger.info(
            "Ticker list: %d tickers from source(s) %s", len(tickers), sources
        )
        return tickers

    if args.missing:
        table = args.missing_table.upper()
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT ticker FROM EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
                WHERE  is_active = TRUE
                ORDER  BY ticker
            """)
            universe = [r[0] for r in cur.fetchall()]
        finally:
            conn.close()

        loaded = get_loaded_tickers(table)
        tickers = [t for t in universe if t not in loaded]
        logger.info(
            "Missing mode (table=%s): universe=%d | already loaded=%d | to backfill=%d",
            table, len(universe), len(loaded), len(tickers),
        )
        return tickers

    raise ValueError(
        "Specify one of: --tickers, --tickers-file, --source, or --missing"
    )


# ── Prices ────────────────────────────────────────────────────────────────────

def backfill_prices(
    tickers: list[str],
    start: str,
    end: str,
    batch_size: int,
    batch_delay: int,
) -> None:
    """
    Backfill OHLCV prices for tickers that have NO rows in RAW.PRICES.

    Uses yf.download() which fetches multiple tickers in a single HTTP call,
    making it much faster than per-ticker calls.  Batches of `batch_size`
    tickers are downloaded sequentially with `batch_delay` seconds between
    them to stay within Yahoo Finance rate limits.

    Skip logic: any ticker already present in RAW.PRICES is excluded entirely.
    To extend the history of a ticker that already has partial data, pass its
    symbol explicitly via --tickers and remove it from the skip set manually
    (or simply re-run without --missing — the staging QUALIFY handles dupes).
    """
    from ingestion.extract import extract_prices
    from ingestion.load import load_dataframe, get_loaded_tickers

    loaded = get_loaded_tickers("PRICES")
    to_backfill = [t for t in tickers if t not in loaded]
    skipped_count = len(tickers) - len(to_backfill)

    if not to_backfill:
        logger.info(
            "Prices: all %d tickers already in RAW.PRICES — nothing to do",
            len(tickers),
        )
        return

    logger.info(
        "Prices: %d to download | %d already loaded (skipped) | range %s → %s",
        len(to_backfill), skipped_count, start, end,
    )

    batches = [
        to_backfill[i : i + batch_size]
        for i in range(0, len(to_backfill), batch_size)
    ]
    total_rows  = 0
    failed_batches: list[dict] = []

    for idx, batch in enumerate(batches, 1):
        logger.info(
            "Prices batch %d/%d — %d tickers (%s … %s)",
            idx, len(batches), len(batch), batch[0], batch[-1],
        )
        try:
            df = extract_prices(
                tickers=batch,
                start_date_str=start,
                end_date_str=end,
            )
            if df is None or df.empty:
                logger.info("  batch %d: no data returned", idx)
            else:
                rows = load_dataframe(df, "PRICES", overwrite=False)
                total_rows += rows
                logger.info(
                    "  batch %d: %d rows loaded (running total: %d)",
                    idx, rows, total_rows,
                )
        except Exception as exc:
            logger.error("  batch %d FAILED: %s — continuing", idx, exc)
            failed_batches.append(
                {"batch": idx, "tickers": batch, "error": str(exc)}
            )

        if idx < len(batches):
            logger.info("  sleeping %ds before next batch…", batch_delay)
            time.sleep(batch_delay)

    if failed_batches:
        logger.warning(
            "Prices: %d/%d batches failed (re-run is safe — skip logic will "
            "only retry tickers still missing): %s",
            len(failed_batches), len(batches),
            [f["batch"] for f in failed_batches],
        )
    logger.info(
        "Prices done: %d rows loaded across %d batches (%d failed)",
        total_rows, len(batches), len(failed_batches),
    )


# ── Company info ──────────────────────────────────────────────────────────────

def backfill_company_info(
    tickers: list[str],
    ticker_delay: float,
    batch_size: int,
    batch_delay: float,
) -> None:
    """
    Extract company metadata (sector, industry, market cap) for tickers not
    yet in RAW.COMPANY_INFO.

    Uses yf.Ticker().info — one HTTP call per ticker.  Rate limiting is
    enforced inside extract_company_info() via ticker_delay and batch pauses.

    Note: equity_daily overwrites RAW.COMPANY_INFO nightly for all tickers,
    so this backfill only needs to cover the period until the next nightly run.
    """
    from ingestion.extract import extract_company_info
    from ingestion.load import load_dataframe, get_loaded_tickers

    loaded = get_loaded_tickers("COMPANY_INFO")
    to_fetch = [t for t in tickers if t not in loaded]

    if not to_fetch:
        logger.info(
            "Company info: all %d tickers already in RAW.COMPANY_INFO — skipping",
            len(tickers),
        )
        return

    logger.info(
        "Company info: %d to fetch | %d already loaded (skipped)",
        len(to_fetch), len(tickers) - len(to_fetch),
    )

    df = extract_company_info(
        to_fetch,
        delay_seconds=ticker_delay,
        batch_size=batch_size,
        batch_pause=batch_delay,
    )

    if df is None or df.empty:
        logger.warning("Company info: no data returned")
        return

    rows = load_dataframe(df, "COMPANY_INFO", overwrite=False)
    logger.info("Company info done: %d rows loaded", rows)


# ── Financial statements ──────────────────────────────────────────────────────

def backfill_financials(
    tickers: list[str],
    ticker_delay: float,
    batch_size: int,
    batch_delay: float,
) -> None:
    """
    Extract income statement, balance sheet, and cash flow (annual + quarterly)
    for equity tickers not yet in RAW.FINANCIAL_STATEMENTS.

    ETFs are filtered out automatically — they have no SEC filings.

    Many small-cap and international tickers will return no data from yfinance;
    these are logged at WARNING level and silently skipped.

    Note: fundamentals_weekly overwrites RAW.FINANCIAL_STATEMENTS each Saturday
    for all equities, so appending here is safe — no permanent duplication risk.
    """
    from ingestion.extract_fundamentals import extract_financial_statements
    from ingestion.load import load_dataframe, get_loaded_tickers, get_connection

    # Filter to equities only (ETFs have no financials)
    ticker_set = set(tickers)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT ticker FROM EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
            WHERE  is_active = TRUE AND is_equity = TRUE
            ORDER  BY ticker
        """)
        equity_tickers = [r[0] for r in cur.fetchall() if r[0] in ticker_set]
    finally:
        conn.close()

    if not equity_tickers:
        logger.info("Financials: no equity tickers in the provided list — skipping")
        return

    loaded = get_loaded_tickers("FINANCIAL_STATEMENTS")
    to_fetch = [t for t in equity_tickers if t not in loaded]

    if not to_fetch:
        logger.info(
            "Financials: all %d equity tickers already in "
            "RAW.FINANCIAL_STATEMENTS — skipping",
            len(equity_tickers),
        )
        return

    logger.info(
        "Financials: %d equity tickers to fetch | %d already loaded (skipped) "
        "| %d non-equity tickers excluded",
        len(to_fetch),
        len(equity_tickers) - len(to_fetch),
        len(ticker_set) - len(equity_tickers),
    )

    df = extract_financial_statements(
        to_fetch,
        delay_seconds=ticker_delay,
        batch_size=batch_size,
        batch_pause=batch_delay,
    )

    if df is None or df.empty:
        logger.warning(
            "Financials: no data returned — many small-cap / international "
            "tickers are not covered by yfinance"
        )
        return

    rows = load_dataframe(df, "FINANCIAL_STATEMENTS", overwrite=False)
    logger.info("Financials done: %d rows loaded", rows)


# ── Valuation metrics ─────────────────────────────────────────────────────────

def backfill_valuation(
    tickers: list[str],
    ticker_delay: float,
    batch_size: int,
    batch_delay: float,
) -> None:
    """
    Extract today's valuation snapshot (PE, P/B, margins, beta, etc.) for
    tickers not yet in RAW.VALUATION_METRICS.

    Uses yf.Ticker().info — same endpoint as company_info but loaded to a
    separate table because the grain differs (daily time series vs. SCD).

    Skip logic: any ticker already present in VALUATION_METRICS is excluded.
    Downstream stg_valuation_metrics uses QUALIFY to keep the latest snapshot
    per (ticker, snapshot_date), so duplicate snapshot rows are not a concern.

    Note: valuation_daily appends a fresh row per ticker every weekday, so
    this backfill only needs to seed tickers that have never appeared there.
    """
    from ingestion.extract_fundamentals import extract_valuation_metrics
    from ingestion.load import load_dataframe, get_loaded_tickers

    loaded = get_loaded_tickers("VALUATION_METRICS")
    to_fetch = [t for t in tickers if t not in loaded]

    if not to_fetch:
        logger.info(
            "Valuation: all %d tickers already in RAW.VALUATION_METRICS — skipping",
            len(tickers),
        )
        return

    logger.info(
        "Valuation: %d to fetch | %d already loaded (skipped)",
        len(to_fetch), len(tickers) - len(to_fetch),
    )

    df = extract_valuation_metrics(
        to_fetch,
        delay_seconds=ticker_delay,
        batch_size=batch_size,
        batch_pause=batch_delay,
    )

    if df is None or df.empty:
        logger.warning("Valuation: no data returned")
        return

    rows = load_dataframe(df, "VALUATION_METRICS", overwrite=False)
    logger.info("Valuation done: %d rows loaded", rows)


# ── CLI ───────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="backfill.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # ── Ticker source (mutually exclusive) ────────────────────────────────────
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument(
        "--tickers",
        metavar="A,B,C",
        help="Comma-separated ticker symbols",
    )
    src.add_argument(
        "--tickers-file",
        metavar="PATH",
        help="Path to a text file with one ticker per line (# lines ignored)",
    )
    src.add_argument(
        "--source",
        metavar="LABEL,...",
        help=(
            "Comma-separated TICKER_UNIVERSE source labels "
            "(e.g. ftse100,tsx60,asx200,nikkei225,dax40,nasdaq_trader)"
        ),
    )
    src.add_argument(
        "--missing",
        action="store_true",
        help=(
            "Auto-select every active ticker in TICKER_UNIVERSE that has no "
            "rows in --missing-table (default: PRICES)"
        ),
    )

    p.add_argument(
        "--missing-table",
        default="PRICES",
        metavar="TABLE",
        help="RAW table to diff against when --missing is used (default: PRICES)",
    )

    # ── Date range (for prices) ───────────────────────────────────────────────
    p.add_argument(
        "--start",
        default="2010-01-01",
        metavar="YYYY-MM-DD",
        help="Price backfill start date (default: 2010-01-01)",
    )
    p.add_argument(
        "--end",
        default=date.today().isoformat(),
        metavar="YYYY-MM-DD",
        help="Price backfill end date (default: today)",
    )

    # ── Data types ────────────────────────────────────────────────────────────
    p.add_argument(
        "--types",
        nargs="+",
        choices=["prices", "company_info", "financials", "valuation"],
        default=["prices", "company_info", "financials", "valuation"],
        metavar="TYPE",
        help=(
            "Space-separated list of data types to backfill.  "
            "Choices: prices company_info financials valuation.  "
            "Default: all four."
        ),
    )

    # ── Rate-limit tuning ─────────────────────────────────────────────────────
    p.add_argument(
        "--batch-size",
        type=int,
        default=PRICE_BATCH_SIZE,
        metavar="N",
        help=f"Tickers per yf.download batch for prices (default: {PRICE_BATCH_SIZE})",
    )
    p.add_argument(
        "--batch-delay",
        type=int,
        default=PRICE_BATCH_DELAY,
        metavar="SECS",
        help=f"Seconds to sleep between price batches (default: {PRICE_BATCH_DELAY})",
    )
    p.add_argument(
        "--ticker-delay",
        type=float,
        default=INFO_TICKER_DELAY,
        metavar="SECS",
        help=(
            f"Seconds between individual .info/.income_stmt calls "
            f"(default: {INFO_TICKER_DELAY})"
        ),
    )
    p.add_argument(
        "--ticker-batch-size",
        type=int,
        default=INFO_BATCH_SIZE,
        metavar="N",
        help=(
            f"Tickers before a longer pause in per-ticker extractors "
            f"(default: {INFO_BATCH_SIZE})"
        ),
    )
    p.add_argument(
        "--ticker-batch-delay",
        type=float,
        default=INFO_BATCH_DELAY,
        metavar="SECS",
        help=(
            f"Seconds for the longer per-ticker-batch pause "
            f"(default: {INFO_BATCH_DELAY})"
        ),
    )

    return p


def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()

    # Validate date strings early
    for label, val in [("--start", args.start), ("--end", args.end)]:
        try:
            datetime.strptime(val, "%Y-%m-%d")
        except ValueError:
            parser.error(f"{label} must be YYYY-MM-DD, got: {val!r}")

    logger.info("=" * 60)
    logger.info("Backfill starting")
    logger.info("  types        : %s", args.types)
    logger.info("  date range   : %s → %s", args.start, args.end)
    logger.info("  price batch  : %d tickers / %ds delay", args.batch_size, args.batch_delay)
    logger.info(
        "  ticker delay : %gs per ticker / %gs every %d tickers",
        args.ticker_delay, args.ticker_batch_delay, args.ticker_batch_size,
    )
    logger.info("=" * 60)

    tickers = resolve_tickers(args)
    if not tickers:
        logger.info("No tickers to process — exiting")
        return

    logger.info("Total tickers to consider: %d", len(tickers))

    if "prices" in args.types:
        logger.info("\n--- PRICES ---")
        backfill_prices(
            tickers,
            start=args.start,
            end=args.end,
            batch_size=args.batch_size,
            batch_delay=args.batch_delay,
        )

    if "company_info" in args.types:
        logger.info("\n--- COMPANY INFO ---")
        backfill_company_info(
            tickers,
            ticker_delay=args.ticker_delay,
            batch_size=args.ticker_batch_size,
            batch_delay=args.ticker_batch_delay,
        )

    if "financials" in args.types:
        logger.info("\n--- FINANCIAL STATEMENTS ---")
        backfill_financials(
            tickers,
            ticker_delay=args.ticker_delay,
            batch_size=args.ticker_batch_size,
            batch_delay=args.ticker_batch_delay,
        )

    if "valuation" in args.types:
        logger.info("\n--- VALUATION METRICS ---")
        backfill_valuation(
            tickers,
            ticker_delay=args.ticker_delay,
            batch_size=args.ticker_batch_size,
            batch_delay=args.ticker_batch_delay,
        )

    logger.info("\n" + "=" * 60)
    logger.info("Backfill complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
