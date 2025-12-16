#!/usr/bin/env python3
"""
Phase 3: Drift Detection

Compares real-time candles against batch truth to monitor accuracy.

Detects:
- Missing candles (in batch but not real-time)
- Extra candles (in real-time but not batch)
- Value differences (close price, volume, trade count)

Alerts if drift exceeds configurable thresholds.
"""

from datetime import datetime
from pathlib import Path

import polars as pl


# Alert thresholds
THRESHOLDS = {
    "missing_pct": 0.5,      # Alert if >0.5% candles missing
    "close_drift_pct": 1.0,  # Alert if any candle close >1% off
    "volume_drift_pct": 5.0, # Alert if volume >5% off
}


def load_candles(path: Path) -> pl.DataFrame:
    """Load candles and normalize column types."""
    
    df = pl.read_parquet(path)
    
    # Ensure datetime columns are parsed
    for col in ["window_start", "window_end"]:
        if col in df.columns:
            if df[col].dtype == pl.Utf8:
                df = df.with_columns([
                    pl.col(col).str.to_datetime()
                ])
    
    return df


def measure_drift(
    realtime_candles: pl.DataFrame,
    batch_candles: pl.DataFrame,
) -> pl.DataFrame:
    """
    Compare real-time candles against batch truth.
    
    Joins on (instrument, window_start, window_size) and calculates
    differences in all OHLCV values.
    """
    # Normalize datetime precision to microseconds for join compatibility
    realtime = realtime_candles.with_columns([
        pl.col("window_start").cast(pl.Datetime("us"))
    ])
    batch = batch_candles.with_columns([
        pl.col("window_start").cast(pl.Datetime("us"))
    ])
    
    # Join on natural key
    joined = realtime.join(
        batch,
        on=["instrument", "window_start", "window_size"],
        suffix="_batch",
        how="full",
    )
    
    # Calculate drift metrics
    drift = joined.with_columns([
        # Missing flags
        pl.col("close").is_null().alias("missing_realtime"),
        pl.col("close_batch").is_null().alias("missing_batch"),
        
        # Price drifts
        ((pl.col("open") - pl.col("open_batch")) / pl.col("open_batch") * 100)
            .alias("open_drift_pct"),
        ((pl.col("high") - pl.col("high_batch")) / pl.col("high_batch") * 100)
            .alias("high_drift_pct"),
        ((pl.col("low") - pl.col("low_batch")) / pl.col("low_batch") * 100)
            .alias("low_drift_pct"),
        ((pl.col("close") - pl.col("close_batch")) / pl.col("close_batch") * 100)
            .alias("close_drift_pct"),
        
        # Volume drift
        ((pl.col("volume") - pl.col("volume_batch")) / pl.col("volume_batch") * 100)
            .alias("volume_drift_pct"),
        
        # Trade count difference
        (pl.col("trade_count") - pl.col("trade_count_batch"))
            .alias("trade_count_diff"),
    ])
    
    return drift


def generate_drift_report(drift: pl.DataFrame) -> dict:
    """Generate summary statistics for drift analysis."""
    
    total_candles = len(drift)
    missing_realtime = drift.filter(pl.col("missing_realtime")).height
    missing_batch = drift.filter(pl.col("missing_batch")).height
    
    # Only calculate drift stats for candles present in both
    matched = drift.filter(
        ~pl.col("missing_realtime") & ~pl.col("missing_batch")
    )
    
    if len(matched) == 0:
        return {
            "total_candles": total_candles,
            "missing_in_realtime": missing_realtime,
            "missing_in_batch": missing_batch,
            "matched_candles": 0,
            "error": "No matched candles for comparison",
        }
    
    report = {
        "total_candles": total_candles,
        "missing_in_realtime": missing_realtime,
        "missing_in_realtime_pct": 100 * missing_realtime / total_candles if total_candles > 0 else 0,
        "missing_in_batch": missing_batch,
        "matched_candles": len(matched),
        
        # Close price drift
        "close_drift_mean_pct": matched["close_drift_pct"].mean(),
        "close_drift_max_pct": matched["close_drift_pct"].abs().max(),
        "close_drift_std_pct": matched["close_drift_pct"].std(),
        
        # Volume drift
        "volume_drift_mean_pct": matched["volume_drift_pct"].mean(),
        "volume_drift_max_pct": matched["volume_drift_pct"].abs().max(),
        
        # Trade count
        "candles_with_trade_diff": matched.filter(
            pl.col("trade_count_diff") != 0
        ).height,
        "trade_count_diff_mean": matched["trade_count_diff"].mean(),
    }
    
    return report


def check_alerts(report: dict) -> list[str]:
    """Check if any metrics exceed alert thresholds."""
    
    alerts = []
    
    # Missing candles
    if report.get("missing_in_realtime_pct", 0) > THRESHOLDS["missing_pct"]:
        alerts.append(
            f"⚠️  HIGH MISSING RATE: {report['missing_in_realtime_pct']:.2f}% "
            f"candles missing in real-time (threshold: {THRESHOLDS['missing_pct']}%)"
        )
    
    # Close price drift
    max_close_drift = report.get("close_drift_max_pct")
    if max_close_drift and abs(max_close_drift) > THRESHOLDS["close_drift_pct"]:
        alerts.append(
            f"⚠️  HIGH CLOSE DRIFT: {max_close_drift:.2f}% max drift "
            f"(threshold: {THRESHOLDS['close_drift_pct']}%)"
        )
    
    # Volume drift
    max_volume_drift = report.get("volume_drift_max_pct")
    if max_volume_drift and abs(max_volume_drift) > THRESHOLDS["volume_drift_pct"]:
        alerts.append(
            f"⚠️  HIGH VOLUME DRIFT: {max_volume_drift:.2f}% max drift "
            f"(threshold: {THRESHOLDS['volume_drift_pct']}%)"
        )
    
    return alerts


def main():
    """Run drift detection between real-time and batch candles."""
    
    print("=" * 60)
    print("Drift Detection (Phase 3)")
    print("=" * 60)
    
    # Paths
    realtime_path = Path(__file__).parent.parent / "output" / "candles_realtime.parquet"
    batch_path = Path(__file__).parent.parent / "output" / "candles_batch.parquet"
    output_path = Path(__file__).parent.parent / "output" / "drift_report.parquet"
    
    # Load candles
    print("\n📂 Loading candles...")
    
    if not realtime_path.exists():
        print(f"❌ Real-time candles not found: {realtime_path}")
        print("Run realtime_aggregator.py first.")
        return
    
    if not batch_path.exists():
        print(f"❌ Batch candles not found: {batch_path}")
        print("Run batch_reconciliation.py first.")
        return
    
    realtime = load_candles(realtime_path)
    batch = load_candles(batch_path)
    
    print(f"   Real-time: {len(realtime):,} candles")
    print(f"   Batch:     {len(batch):,} candles")
    
    # Measure drift
    print("\n⚙️  Measuring drift...")
    drift = measure_drift(realtime, batch)
    
    # Generate report
    report = generate_drift_report(drift)
    
    # Save detailed drift data
    drift.write_parquet(output_path)
    print(f"\n💾 Saved detailed drift: {output_path}")
    
    # Print report
    print("\n" + "=" * 60)
    print("Drift Report")
    print("=" * 60)
    
    print(f"\n  📊 Coverage:")
    print(f"     Total candles:        {report['total_candles']:,}")
    print(f"     Matched candles:      {report['matched_candles']:,}")
    print(f"     Missing (real-time):  {report['missing_in_realtime']:,} "
          f"({report.get('missing_in_realtime_pct', 0):.4f}%)")
    print(f"     Missing (batch):      {report['missing_in_batch']:,}")
    
    print(f"\n  📈 Close Price Drift:")
    print(f"     Mean:  {report.get('close_drift_mean_pct', 0):.4f}%")
    print(f"     Max:   {report.get('close_drift_max_pct', 0):.4f}%")
    print(f"     Std:   {report.get('close_drift_std_pct', 0):.4f}%")
    
    print(f"\n  📦 Volume Drift:")
    print(f"     Mean:  {report.get('volume_drift_mean_pct', 0):.4f}%")
    print(f"     Max:   {report.get('volume_drift_max_pct', 0):.4f}%")
    
    print(f"\n  🔢 Trade Count:")
    print(f"     Candles with diff: {report.get('candles_with_trade_diff', 0):,}")
    print(f"     Mean diff:         {report.get('trade_count_diff_mean', 0):.2f}")
    
    # Check alerts
    alerts = check_alerts(report)
    
    if alerts:
        print("\n" + "=" * 60)
        print("ALERTS")
        print("=" * 60)
        for alert in alerts:
            print(f"  {alert}")
    else:
        print("\n  ✅ All metrics within acceptable thresholds!")
    
    # Summary per window size
    print("\n" + "=" * 60)
    print("By Window Size")
    print("=" * 60)
    
    by_window = drift.group_by("window_size").agg([
        pl.len().alias("total"),
        pl.col("missing_realtime").sum().alias("missing_rt"),
        pl.col("close_drift_pct").mean().alias("close_drift_mean"),
        pl.col("trade_count_diff").abs().mean().alias("trade_diff_mean"),
    ]).sort("window_size")
    
    print(by_window)
    
    print("\n✅ Drift detection complete!")


if __name__ == "__main__":
    main()
