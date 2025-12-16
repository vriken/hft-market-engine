#!/usr/bin/env python3
"""
Phase 1: Data Quality Profiler

Profiles HFT trade data for:
1. Gap Detection - Multi-frequency (500ms, 5s, 30s thresholds)
2. Outlier Detection - Fat finger errors (>5% from rolling 50-trade average)
3. Deduplication - Fingerprint-based duplicate detection

Key principle: "Tell me what you dropped" - full audit trail for all exclusions.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import polars as pl


# Configuration
GAP_THRESHOLDS = [
    ("HFT", timedelta(milliseconds=500)),
    ("Fast", timedelta(seconds=5)),
    ("Standard", timedelta(seconds=30)),
]

OUTLIER_ROLLING_WINDOW = 50  # trades
OUTLIER_DEVIATION_THRESHOLD = 0.05  # 5%


@dataclass
class QualityReport:
    """Summary statistics for data quality report"""
    total_trades: int
    date_range: tuple[datetime, datetime]
    instruments: list[str]
    
    # Gap stats per threshold
    gaps_by_threshold: dict[str, int]
    
    # Outlier stats
    outlier_count: int
    outlier_pct: float
    
    # Dedup stats
    duplicate_count: int
    duplicate_pct: float
    
    # Clean data stats
    clean_trade_count: int


def detect_gaps(
    df: pl.DataFrame,
    gap_threshold: timedelta,
    threshold_name: str,
) -> pl.DataFrame:
    """
    Detect gaps in trade data per instrument.
    
    Args:
        df: Trades with timestamp column
        gap_threshold: Minimum gap to flag
        threshold_name: Human-readable threshold name
    
    Returns:
        DataFrame with gap details
    """
    threshold_ms = gap_threshold.total_seconds() * 1000
    
    return (
        df
        .sort(["instrument", "timestamp"])
        .with_columns([
            pl.col("timestamp")
                .shift(1)
                .over("instrument")
                .alias("prev_trade_time")
        ])
        .with_columns([
            (pl.col("timestamp") - pl.col("prev_trade_time"))
                .dt.total_milliseconds()
                .alias("gap_ms")
        ])
        .filter(pl.col("gap_ms") > threshold_ms)
        .with_columns([
            pl.lit(threshold_name).alias("threshold_name"),
            pl.lit(threshold_ms).alias("threshold_ms"),
        ])
        .select([
            "instrument",
            pl.col("prev_trade_time").alias("gap_start"),
            pl.col("timestamp").alias("gap_end"),
            "gap_ms",
            (pl.col("gap_ms") / 1000.0).alias("gap_seconds"),
            "threshold_name",
            "threshold_ms",
        ])
    )


def detect_all_gaps(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """
    Run gap detection at all threshold levels.
    
    Returns:
        Dictionary mapping threshold name to gaps DataFrame
    """
    gaps = {}
    for name, threshold in GAP_THRESHOLDS:
        gap_df = detect_gaps(df, threshold, name)
        gaps[name] = gap_df
        print(f"  {name} gaps (>{threshold}): {len(gap_df):,}")
    return gaps


def detect_outliers(
    df: pl.DataFrame,
    window_size: int = OUTLIER_ROLLING_WINDOW,
    deviation_threshold: float = OUTLIER_DEVIATION_THRESHOLD,
) -> pl.DataFrame:
    """
    Detect outliers using rolling average comparison.
    
    A trade is flagged if its price deviates >threshold from the
    rolling average of the previous N trades for that instrument.
    
    Args:
        df: Trades with price column
        window_size: Number of preceding trades for rolling average
        deviation_threshold: Deviation percentage to flag (0.05 = 5%)
    
    Returns:
        DataFrame with outlier details
    """
    return (
        df
        .sort(["instrument", "timestamp"])
        .with_columns([
            pl.col("price")
                .shift(1)
                .rolling_mean(window_size=window_size)
                .over("instrument")
                .alias("rolling_avg")
        ])
        .with_columns([
            ((pl.col("price") - pl.col("rolling_avg")).abs() / pl.col("rolling_avg"))
                .alias("deviation_pct")
        ])
        .filter(
            (pl.col("rolling_avg").is_not_null()) & 
            (pl.col("deviation_pct") > deviation_threshold)
        )
        .with_columns([
            pl.lit(window_size).alias("window_size"),
            pl.lit(deviation_threshold).alias("threshold_pct"),
            pl.lit("fat_finger").alias("outlier_type"),
        ])
        .select([
            "trade_id",
            "instrument",
            "timestamp",
            "price",
            "rolling_avg",
            "deviation_pct",
            "volume",
            "side",
            "outlier_type",
            "window_size",
            "threshold_pct",
        ])
    )


def detect_duplicates(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Detect duplicate trades using fingerprinting.
    
    Fingerprint: timestamp + instrument + price + volume + side
    
    Returns:
        Tuple of (duplicates_df, deduplicated_df)
    """
    # Add row number within each fingerprint group
    with_row_num = (
        df
        .with_columns([
            pl.struct(["timestamp", "instrument", "price", "volume", "side"])
                .hash()
                .alias("fingerprint")
        ])
        .with_columns([
            pl.col("trade_id")
                .cum_count()
                .over("fingerprint")
                .alias("row_num")
        ])
    )
    
    # Duplicates are rows where row_num > 1
    duplicates = (
        with_row_num
        .filter(pl.col("row_num") > 1)
        .with_columns([
            pl.lit("duplicate").alias("exclusion_reason"),
        ])
        .select([
            "trade_id",
            "instrument",
            "timestamp",
            "price",
            "volume",
            "side",
            "sequence_id",
            "row_num",
            "exclusion_reason",
        ])
    )
    
    # Deduplicated keeps only first occurrence
    deduplicated = (
        with_row_num
        .filter(pl.col("row_num") == 1)
        .drop(["fingerprint", "row_num"])
    )
    
    return duplicates, deduplicated


def generate_markdown_report(
    report: QualityReport,
    gaps: dict[str, pl.DataFrame],
    outliers: pl.DataFrame,
    duplicates: pl.DataFrame,
    output_path: Path,
) -> None:
    """Generate executive summary markdown report."""
    
    # Get top outliers for sample
    top_outliers = outliers.sort("deviation_pct", descending=True).head(10)
    
    # Get gap distribution by instrument (use HFT threshold)
    hft_gaps = gaps.get("HFT", pl.DataFrame())
    if len(hft_gaps) > 0:
        gap_by_instrument = (
            hft_gaps
            .group_by("instrument")
            .agg(pl.len().alias("gap_count"))
            .sort("gap_count", descending=True)
        )
    else:
        gap_by_instrument = pl.DataFrame()
    
    content = f"""# Data Quality Report

**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  
**Date Range:** {report.date_range[0]} to {report.date_range[1]}  
**Total Trades:** {report.total_trades:,}  
**Instruments:** {len(report.instruments)}

---

## Executive Summary

| Metric | Count | Percentage |
|--------|-------|------------|
| Total Trades | {report.total_trades:,} | 100% |
| Outliers (Fat Finger) | {report.outlier_count:,} | {report.outlier_pct:.4f}% |
| Duplicates | {report.duplicate_count:,} | {report.duplicate_pct:.4f}% |
| **Clean Trades** | **{report.clean_trade_count:,}** | **{100 - report.outlier_pct - report.duplicate_pct:.4f}%** |

---

## Gap Detection

Gaps detected at multiple frequency thresholds:

| Threshold | Gap Count | Description |
|-----------|-----------|-------------|
| HFT (>500ms) | {report.gaps_by_threshold.get('HFT', 0):,} | Sub-second gaps for HFT signals |
| Fast (>5s) | {report.gaps_by_threshold.get('Fast', 0):,} | Dashboard-noticeable gaps |
| Standard (>30s) | {report.gaps_by_threshold.get('Standard', 0):,} | Standard monitoring threshold |

"""
    
    if len(gap_by_instrument) > 0:
        content += """### Gaps by Instrument (HFT Threshold)

| Instrument | Gap Count |
|------------|-----------|
"""
        for row in gap_by_instrument.iter_rows(named=True):
            content += f"| {row['instrument']} | {row['gap_count']:,} |\n"
    
    content += f"""
---

## Outlier Detection

**Method:** Rolling {OUTLIER_ROLLING_WINDOW}-trade average  
**Threshold:** >{OUTLIER_DEVIATION_THRESHOLD*100:.0f}% deviation from rolling average

### Top 10 Outliers

| Instrument | Timestamp | Price | Rolling Avg | Deviation |
|------------|-----------|-------|-------------|-----------|
"""
    
    for row in top_outliers.iter_rows(named=True):
        content += f"| {row['instrument']} | {row['timestamp']} | {row['price']:.2f} | {row['rolling_avg']:.2f} | {row['deviation_pct']*100:.1f}% |\n"
    
    content += f"""
---

## Deduplication

**Method:** Fingerprint-based (timestamp + instrument + price + volume + side)  
**Duplicates Found:** {report.duplicate_count:,} ({report.duplicate_pct:.4f}%)

All duplicates preserved in `output/duplicates.parquet` for audit.

---

## Output Files

| File | Description | Rows |
|------|-------------|------|
| `gaps.parquet` | All detected gaps (HFT threshold) | {report.gaps_by_threshold.get('HFT', 0):,} |
| `outliers.parquet` | Flagged suspect trades | {report.outlier_count:,} |
| `duplicates.parquet` | Duplicate inventory | {report.duplicate_count:,} |
| `clean_trades.parquet` | Deduplicated, outlier-free data | {report.clean_trade_count:,} |

---

## Audit Trail

> **Manager's Ask: "Tell me what you dropped"**  
> Every exclusion is documented with:
> - Full row data
> - Reason for exclusion
> - Threshold violated

No silent `dropna()`. See individual parquet files for complete audit trail.
"""
    
    output_path.write_text(content)
    print(f"\n📄 Report saved: {output_path}")


def main():
    """Run data quality profiling pipeline."""
    
    print("=" * 60)
    print("Data Quality Profiler (Phase 1)")
    print("=" * 60)
    
    # Paths
    input_path = Path(__file__).parent.parent / "output" / "raw_trades_disordered.parquet"
    output_dir = Path(__file__).parent.parent / "output"
    output_dir.mkdir(exist_ok=True)
    
    # Load data
    print(f"\n📂 Loading trades from: {input_path}")
    if not input_path.exists():
        print("❌ Input file not found. Run data_simulator.py first.")
        return
    
    df = pl.read_parquet(input_path)
    print(f"   Loaded {len(df):,} trades")
    
    # Get basic stats
    instruments = df.select("instrument").unique().to_series().to_list()
    date_range = (
        df.select(pl.col("timestamp").min()).item(),
        df.select(pl.col("timestamp").max()).item(),
    )
    
    print(f"   Instruments: {len(instruments)}")
    print(f"   Date range: {date_range[0]} to {date_range[1]}")
    
    # === Challenge 1: Gap Detection ===
    print("\n🔍 Challenge 1: Gap Detection")
    gaps = detect_all_gaps(df)
    
    # Save HFT-level gaps (most granular)
    hft_gaps = gaps["HFT"]
    hft_gaps.write_parquet(output_dir / "gaps.parquet")
    print(f"   Saved: gaps.parquet ({len(hft_gaps):,} rows)")
    
    # === Challenge 2: Outlier Detection ===
    print("\n🔍 Challenge 2: Outlier Detection")
    outliers = detect_outliers(df)
    print(f"   Outliers found: {len(outliers):,} ({100*len(outliers)/len(df):.4f}%)")
    
    outliers.write_parquet(output_dir / "outliers.parquet")
    print(f"   Saved: outliers.parquet")
    
    # Show sample outliers
    if len(outliers) > 0:
        print("\n   Top 5 outliers by deviation:")
        top5 = outliers.sort("deviation_pct", descending=True).head(5)
        print(top5.select(["instrument", "price", "rolling_avg", "deviation_pct"]))
    
    # === Challenge 3: Deduplication ===
    print("\n🔍 Challenge 3: Deduplication")
    duplicates, deduplicated = detect_duplicates(df)
    print(f"   Duplicates found: {len(duplicates):,} ({100*len(duplicates)/len(df):.4f}%)")
    
    duplicates.write_parquet(output_dir / "duplicates.parquet")
    print(f"   Saved: duplicates.parquet")
    
    # === Create Clean Dataset ===
    print("\n🧹 Creating clean dataset...")
    
    # Remove outliers from deduplicated data
    outlier_ids = set(outliers.select("trade_id").to_series().to_list())
    clean_df = deduplicated.filter(~pl.col("trade_id").is_in(list(outlier_ids)))
    
    clean_df.write_parquet(output_dir / "clean_trades.parquet")
    print(f"   Clean trades: {len(clean_df):,}")
    print(f"   Saved: clean_trades.parquet")
    
    # === Generate Report ===
    print("\n📊 Generating quality report...")
    
    report = QualityReport(
        total_trades=len(df),
        date_range=date_range,
        instruments=instruments,
        gaps_by_threshold={name: len(gap_df) for name, gap_df in gaps.items()},
        outlier_count=len(outliers),
        outlier_pct=100 * len(outliers) / len(df),
        duplicate_count=len(duplicates),
        duplicate_pct=100 * len(duplicates) / len(df),
        clean_trade_count=len(clean_df),
    )
    
    generate_markdown_report(
        report, gaps, outliers, duplicates,
        output_dir / "data_quality_report.md"
    )
    
    # === Summary ===
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Total trades:     {report.total_trades:,}")
    print(f"  Outliers:         {report.outlier_count:,} ({report.outlier_pct:.4f}%)")
    print(f"  Duplicates:       {report.duplicate_count:,} ({report.duplicate_pct:.4f}%)")
    print(f"  Clean trades:     {report.clean_trade_count:,}")
    print(f"  Data loss:        {report.total_trades - report.clean_trade_count:,} trades")
    
    print("\n✅ Phase 1 complete!")


if __name__ == "__main__":
    main()
