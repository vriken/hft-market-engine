#!/usr/bin/env python3
"""
Phase 3: Batch Reconciliation

Batch job for 100% accurate candles - run after market close or hourly.

Unlike streaming:
- No watermark needed (we have all data)
- Perfect ordering (sort by event time)
- No late arrivals (batch runs after all data collected)

This is the "truth" layer for researchers and backtesting.
"""

from datetime import timedelta
from pathlib import Path

import polars as pl


# Window configurations (same as realtime)
WINDOW_CONFIGS = [
    ("1s", timedelta(seconds=1)),
    ("10s", timedelta(seconds=10)),
    ("1m", timedelta(minutes=1)),
]


def batch_reconcile_candles(
    df: pl.DataFrame,
    window_size: timedelta,
    window_name: str,
) -> pl.DataFrame:
    """
    Batch job: 100% accurate candles.
    
    Uses Polars group_by_dynamic for perfect aggregation:
    - Sorted by event time
    - No late arrivals
    - Complete data
    """
    # Sort by event time - eliminates all disorder
    df = df.sort("timestamp")
    
    # Perfect OHLCV aggregation using group_by_dynamic
    candles = df.group_by_dynamic(
        "timestamp",
        every=window_size,
        group_by="instrument",
        closed="left",
        label="left",
    ).agg([
        pl.col("price").first().alias("open"),
        pl.col("price").max().alias("high"),
        pl.col("price").min().alias("low"),
        pl.col("price").last().alias("close"),
        pl.col("volume").sum().alias("volume"),
        pl.len().alias("trade_count"),
    ])
    
    # Rename and add metadata
    candles = candles.rename({"timestamp": "window_start"})
    candles = candles.with_columns([
        (pl.col("window_start") + window_size).alias("window_end"),
        pl.lit(False).alias("is_realtime"),
        pl.lit(window_name).alias("window_size"),
    ])
    
    return candles


def main():
    """Run batch reconciliation for all window sizes."""
    
    print("=" * 60)
    print("Batch Reconciliation (Phase 3)")
    print("=" * 60)
    
    # Paths
    input_path = Path(__file__).parent.parent / "output" / "raw_trades_disordered.parquet"
    output_path = Path(__file__).parent.parent / "output" / "candles_batch.parquet"
    
    # Load data
    print(f"\n📂 Loading trades from: {input_path}")
    if not input_path.exists():
        print("❌ Input file not found. Run data_simulator.py first.")
        return
    
    df = pl.read_parquet(input_path)
    print(f"   Loaded {len(df):,} trades")
    
    # Run batch reconciliation for each window size
    print("\n⚙️  Reconciling candles...")
    
    all_candles = []
    for name, window_size in WINDOW_CONFIGS:
        print(f"   {name}...", end=" ")
        candles = batch_reconcile_candles(df, window_size, name)
        print(f"{len(candles):,} candles")
        all_candles.append(candles)
    
    # Combine all candles
    combined = pl.concat(all_candles)
    
    # Save
    combined.write_parquet(output_path)
    print(f"\n💾 Saved: {output_path}")
    
    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    
    summary = combined.group_by("window_size").agg([
        pl.len().alias("candle_count"),
        pl.col("trade_count").sum().alias("total_trades"),
    ]).sort("window_size")
    
    print(summary)
    
    print("\n✅ Batch reconciliation complete!")
    print("   Run drift_detector.py to compare with real-time candles.")


if __name__ == "__main__":
    main()
