"""Kalshi collector: the same raw-JSONL -> hive-partitioned parquet -> DuckDB pipeline as the
trade[XYZ] collector (recorder.hl_*), with market families declared in `adapters.py`.

    recorder.kalshi.adapters     what to collect (add a market family here)
    recorder.kalshi.recorder     websocket recorder: books, trades, tickers, market metadata
    recorder.kalshi.to_parquet   raw -> parquet/kalshi/<table>/date=/series=/part.parquet
    recorder.kalshi.backfill     REST history: settled markets + outcomes, 1-min candles
    recorder.kalshi.db           kalshi.duckdb catalog, storage report, raw retention
"""
