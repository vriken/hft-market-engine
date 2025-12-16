#!/usr/bin/env python3
"""
Phase 2: Partitioned Storage Writer

Writes cleaned trade data to Hive-style partitioned Parquet:
- Partition by year/month/day
- Zstd compression
- Verify partition pruning with DuckDB

Goal: Single-day queries should be 5-10x faster than full scan.
"""

import shutil
import time
from pathlib import Path

import duckdb
import polars as pl


# Configuration
COMPRESSION = "zstd"
COMPRESSION_LEVEL = 3


def add_partition_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Add year, month, day partition columns from timestamp."""
    
    return df.with_columns([
        pl.col("timestamp").dt.year().alias("year"),
        pl.col("timestamp").dt.month().alias("month"),
        pl.col("timestamp").dt.day().alias("day"),
    ])


def write_partitioned_parquet(
    df: pl.DataFrame,
    output_dir: Path,
    partition_cols: list[str] = ["year", "month", "day"],
) -> dict:
    """
    Write DataFrame as Hive-style partitioned Parquet.
    
    Args:
        df: DataFrame with partition columns
        output_dir: Root directory for partitioned output
        partition_cols: Columns to partition by
    
    Returns:
        Statistics about the written data
    """
    # Clean output directory if exists
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)
    
    # Get unique partitions
    partitions = (
        df
        .select(partition_cols)
        .unique()
        .sort(partition_cols)
        .to_dicts()
    )
    
    stats = {
        "partitions": 0,
        "total_rows": 0,
        "total_bytes": 0,
    }
    
    print(f"\n   Writing {len(partitions)} partitions...")
    
    for partition in partitions:
        # Build partition path: data/year=2024/month=12/day=15/
        partition_path = output_dir
        for col in partition_cols:
            partition_path = partition_path / f"{col}={partition[col]}"
        
        partition_path.mkdir(parents=True, exist_ok=True)
        
        # Filter data for this partition
        filter_expr = pl.lit(True)
        for col in partition_cols:
            filter_expr = filter_expr & (pl.col(col) == partition[col])
        
        partition_df = df.filter(filter_expr)
        
        # Drop partition columns from output (they're in the path)
        partition_df = partition_df.drop(partition_cols)
        
        # Write parquet file
        file_path = partition_path / "part-0.parquet"
        partition_df.write_parquet(
            file_path,
            compression=COMPRESSION,
            compression_level=COMPRESSION_LEVEL,
        )
        
        file_size = file_path.stat().st_size
        stats["partitions"] += 1
        stats["total_rows"] += len(partition_df)
        stats["total_bytes"] += file_size
        
        print(f"     {partition_path.relative_to(output_dir)}: "
              f"{len(partition_df):,} rows, {file_size/1024:.1f} KB")
    
    return stats


def verify_partition_pruning(data_dir: Path) -> dict:
    """
    Verify partition pruning using DuckDB.
    
    Compares single-partition query vs full scan performance.
    
    Returns:
        Benchmark results
    """
    # Get partition info
    parquet_pattern = str(data_dir / "*/*/*/**.parquet")
    
    conn = duckdb.connect(":memory:")
    
    # Get all unique days
    days_result = conn.execute(f"""
        SELECT DISTINCT year, month, day 
        FROM read_parquet('{parquet_pattern}', hive_partitioning=true)
        ORDER BY year, month, day
    """).fetchall()
    
    if len(days_result) == 0:
        return {"error": "No data found"}
    
    year, month, day = days_result[0]
    
    print(f"\n   Testing partition: year={year}/month={month}/day={day}")
    
    # Benchmark: Full scan
    start = time.perf_counter()
    full_count = conn.execute(f"""
        SELECT COUNT(*) as cnt
        FROM read_parquet('{parquet_pattern}', hive_partitioning=true)
    """).fetchone()[0]
    full_scan_time = time.perf_counter() - start
    
    # Benchmark: Single partition query
    start = time.perf_counter()
    partition_count = conn.execute(f"""
        SELECT COUNT(*) as cnt
        FROM read_parquet('{parquet_pattern}', hive_partitioning=true)
        WHERE year = {year} AND month = {month} AND day = {day}
    """).fetchone()[0]
    partition_time = time.perf_counter() - start
    
    # Calculate speedup
    speedup = full_scan_time / partition_time if partition_time > 0 else 0
    
    return {
        "full_scan_rows": full_count,
        "full_scan_time_ms": full_scan_time * 1000,
        "partition_rows": partition_count,
        "partition_time_ms": partition_time * 1000,
        "speedup": speedup,
        "test_partition": f"year={year}/month={month}/day={day}",
    }


def show_explain_analyze(data_dir: Path):
    """Show DuckDB EXPLAIN ANALYZE for partition-pruned query."""
    
    parquet_pattern = str(data_dir / "*/*/*/**.parquet")
    conn = duckdb.connect(":memory:")
    
    # Get first partition
    days_result = conn.execute(f"""
        SELECT DISTINCT year, month, day 
        FROM read_parquet('{parquet_pattern}', hive_partitioning=true)
        ORDER BY year, month, day
        LIMIT 1
    """).fetchall()
    
    year, month, day = days_result[0]
    
    print("\n   EXPLAIN ANALYZE (partition-pruned query):")
    print("-" * 60)
    
    explain = conn.execute(f"""
        EXPLAIN ANALYZE
        SELECT COUNT(*) 
        FROM read_parquet('{parquet_pattern}', hive_partitioning=true)
        WHERE year = {year} AND month = {month} AND day = {day}
    """).fetchall()
    
    for row in explain:
        print(f"   {row[0]}")


def main():
    """Run partitioned storage writer."""
    
    print("=" * 60)
    print("Partitioned Storage Writer (Phase 2)")
    print("=" * 60)
    
    # Paths
    input_path = Path(__file__).parent.parent / "output" / "clean_trades.parquet"
    data_dir = Path(__file__).parent.parent / "data"
    
    # Load clean trades
    print(f"\n[1/4] Loading clean trades...")
    if not input_path.exists():
        print(f"❌ Input file not found: {input_path}")
        print("Run data_quality_profiler.py first.")
        return
    
    df = pl.read_parquet(input_path)
    print(f"   Loaded {len(df):,} trades")
    
    # Calculate uncompressed CSV size estimate
    csv_size_estimate = len(df) * 100  # ~100 bytes per row
    
    # Add partition columns
    print("\n[2/4] Adding partition columns...")
    df = add_partition_columns(df)
    
    # Show partition distribution
    partition_counts = (
        df
        .group_by(["year", "month", "day"])
        .agg(pl.len().alias("count"))
        .sort(["year", "month", "day"])
    )
    
    print("   Partition distribution:")
    for row in partition_counts.iter_rows(named=True):
        print(f"     {row['year']}/{row['month']:02d}/{row['day']:02d}: {row['count']:,} rows")
    
    # Write partitioned parquet
    print("\n[3/4] Writing Hive-style partitioned Parquet...")
    stats = write_partitioned_parquet(df, data_dir)
    
    print(f"\n   Total: {stats['total_bytes']/1024/1024:.2f} MB written")
    print(f"   Compression ratio: ~{csv_size_estimate/stats['total_bytes']:.1f}x vs CSV")
    
    # Verify partition pruning
    print("\n[4/4] Verifying partition pruning with DuckDB...")
    
    results = verify_partition_pruning(data_dir)
    
    print(f"\n   Benchmark Results:")
    print(f"   • Full scan: {results['full_scan_rows']:,} rows in {results['full_scan_time_ms']:.1f}ms")
    print(f"   • Single partition ({results['test_partition']}): "
          f"{results['partition_rows']:,} rows in {results['partition_time_ms']:.1f}ms")
    print(f"   • Speedup: {results['speedup']:.1f}x")
    
    if results['speedup'] >= 1.5:
        print(f"\n   ✅ Partition pruning verified! ({results['speedup']:.1f}x speedup)")
    else:
        print(f"\n   ⚠️  Speedup lower than expected ({results['speedup']:.1f}x)")
        print("      This may be due to small dataset size or caching.")
    
    # Show EXPLAIN ANALYZE
    show_explain_analyze(data_dir)
    
    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Input:        {len(df):,} trades")
    print(f"  Partitions:   {stats['partitions']}")
    print(f"  Output size:  {stats['total_bytes']/1024/1024:.2f} MB")
    print(f"  Location:     {data_dir}")
    
    print("\n✅ Phase 2 complete!")


if __name__ == "__main__":
    main()
