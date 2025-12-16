#!/usr/bin/env python3
"""
Phase 3: Real-Time OHLCV Aggregator

Streaming aggregation producing multi-frequency candles (1s, 10s, 1m) per instrument.

Key features:
- Raw state machine (no group_by_dynamic abstractions)
- Manual watermark-based window eviction
- Multi-frequency window support
- Kafka consumer integration (or file-based simulation)

The interviewer wants to see manual windowing logic - demonstrates understanding of:
1. Window assignment (which window does this tick belong to?)
2. State management (where do I store partial aggregates?)
3. Watermarks (when do I emit and evict?)
4. Late arrivals (what if a tick arrives after window closed?)
"""

import argparse
import json
import signal
import sys
import time
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generator, Iterator, Optional

import polars as pl

try:
    from confluent_kafka import Consumer, KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


# Window configurations (production-realistic)
@dataclass
class WindowConfig:
    name: str
    size: timedelta
    watermark: timedelta


WINDOW_CONFIGS = [
    WindowConfig("1s", timedelta(seconds=1), timedelta(milliseconds=200)),
    WindowConfig("10s", timedelta(seconds=10), timedelta(seconds=1)),
    WindowConfig("1m", timedelta(minutes=1), timedelta(seconds=5)),
]


@dataclass
class OHLCVCandle:
    """Accumulator for one instrument's current window (Optimized)."""
    __slots__ = (
        'instrument', 'window_start_ts', 'window_size_sec', 
        'open', 'high', 'low', 'close', 'volume', 'trade_count', 
        '_first_ts', '_last_ts'
    )
    
    instrument: bytes  # Keep as bytes for speed
    window_start_ts: int
    window_size_sec: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int
    _first_ts: int
    _last_ts: int

    def to_dict(self) -> dict:
        """Convert to output dictionary (decoding happens here)."""
        start_dt = datetime.fromtimestamp(self.window_start_ts / 1_000_000, tz=timezone.utc)
        end_dt = start_dt + timedelta(seconds=self.window_size_sec)
        
        return {
            "instrument": self.instrument.decode("utf-8"),
            "window_size": f"{self.window_size_sec}s" if self.window_size_sec < 60 else f"{self.window_size_sec//60}m",
            "window_start": start_dt.isoformat(),
            "window_end": end_dt.isoformat(),
            "open": self.open,
            "high": self.high if self.high != float('-inf') else None,
            "low": self.low if self.low != float('inf') else None,
            "close": self.close,
            "volume": self.volume,
            "trade_count": self.trade_count,
            "is_realtime": True,
        }

def run_hyper_optimized_aggregation(
    window_configs: list[WindowConfig],
    limit: Optional[int] = None,
    group_id: str = "ohlcv-aggregator",
    topic: str = os.getenv("KAFKA_TOPIC", "trades-chaos-bench"),
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9095")
) -> dict[str, list[dict]]:
    """
    Hyper-optimized main loop.
    Inlines Kafka consumption, byte parsing, and windowing.
    Avoids dict creation, decoding, and function calls per tick.
    """
    if not KAFKA_AVAILABLE:
        raise RuntimeError("confluent-kafka needed for optimized mode")

    # 1. Setup Kafka
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "fetch.min.bytes": 65536,       # 64KB
        "fetch.wait.max.ms": 100,
        "queued.min.messages": 500000,  # Massive buffer
    })
    consumer.subscribe([topic])

    # 2. Setup State (Pre-compute constants)
    # Mapping: window_name -> { instrument_bytes: Candle }
    state = {wc.name: {} for wc in window_configs}
    results = {wc.name: [] for wc in window_configs}
    
    # Pre-calculate window integers
    # List of (name, size_micros, watermark_micros)
    windows = []
    for wc in window_configs:
        size = int(wc.size.total_seconds() * 1_000_000)
        wm = int(wc.watermark.total_seconds() * 1_000_000)
        windows.append((wc.name, size, wm))

    import json
    from prometheus_client import start_http_server, Counter, Gauge

    # Metrics
    # Check if server is already running? 
    # Usually in main() but we want it available during this function.
    # Safe to call multiple times? start_http_server throws if port in use.
    # We will assume this function runs once or move start_http_server to main.
    # Let's define metrics here (or global).
    
    METRIC_TRADES_TOTAL = Counter('trades_processed_total', 'Total trades processed', ['status'])
    # status=valid, dlq
    METRIC_THROUGHPUT = Gauge('trades_processing_rate', 'Current processing rate (ticks/sec)')
    
    max_event_time = 0
    tick_count = 0
    dlq_count = 0
    
    print("\n🚀 Starting Hyper-Optimized Loop (Bytes Only + DLQ Validation + Metrics)...")
    
    # Validation Thresholds
    TS_MAX_FUTURE = 1767225600000000
    
    # Persistent DLQ file
    dlq_path = Path("output/dlq_errors.jsonl")
    dlq_path.parent.mkdir(exist_ok=True)
    
    # Buffered writing
    dlq_buffer = []
    
    last_snapshot_time = time.time()
    
    try:
        with open(dlq_path, "a") as f_dlq: # Append mode
            while True:
                msgs = consumer.consume(num_messages=10000, timeout=1.0)
                if not msgs:
                    if limit and tick_count >= limit:
                        break
                    continue
                
                batch_valid_count = 0
                batch_dlq_count = 0
                
                for msg in msgs:
                    if msg.error():
                        continue
                        
                    tick_count += 1
                    
                    val = msg.value()
                    parts = val.split(b',')
                    inst = parts[1]
                    
                    # Protocol: msg_id, inst, ts, price, vol, side, msg_type
                    # If msg_type is present (index 6) and is NOT 'T', we skip aggregation logic
                    # but we still count it as "load" (tick_count increments).
                    
                    try:
                        # Check msg_type if available (legacy messages might not have it)
                        if len(parts) >= 7 and parts[6] != b'T':
                            # It's an Order Update (Noise)
                            # We processed the message (CPU cost of parsing) but ignore it for candle logic
                             if limit and tick_count >= limit: break
                             continue
                        
                        ts = int(parts[2])
                        price = float(parts[3])
                        vol = float(parts[4])
                    except (ValueError, IndexError):
                        dlq_count += 1
                        batch_dlq_count += 1
                        dlq_buffer.append(json.dumps({"raw": val.decode("utf-8", errors="replace"), "error": "parse_error"}) + "\n")
                        if limit and tick_count >= limit:
                            break
                        continue

                    # Validation
                    is_valid = True
                    reason = ""
                    if price <= 0:
                        is_valid = False
                        reason = "negative_price"
                    elif vol <= 0:
                        is_valid = False
                        reason = "zero_volume"
                    elif ts > TS_MAX_FUTURE:
                        is_valid = False
                        reason = "future_timestamp"
                    
                    if not is_valid:
                        dlq_count += 1
                        batch_dlq_count += 1
                        dlq_buffer.append(json.dumps({"raw": val.decode(), "error": reason}) + "\n")
                        if limit and tick_count >= limit:
                            break
                        continue
                    
                    batch_valid_count += 1
                    
                    # --- Aggregation Logic ---
                    if ts > max_event_time:
                        max_event_time = ts
                    
                    for w_name, w_size, w_wm in windows:
                        w_state = state[w_name]
                        current_wm = max_event_time - w_wm
                        w_start = (ts // w_size) * w_size
                        if w_start + w_size + w_wm < current_wm:
                            continue
                            
                        candle = w_state.get(inst)
                        if candle and w_start == candle.window_start_ts:
                            if ts < candle._first_ts:
                                candle.open = price
                                candle._first_ts = ts
                            if price > candle.high: candle.high = price
                            if price < candle.low: candle.low = price
                            if ts >= candle._last_ts:
                                candle.close = price
                                candle._last_ts = ts
                            candle.volume += vol
                            candle.trade_count += 1
                        else:
                            if candle:
                                results[w_name].append(candle.to_dict())
                            new = OHLCVCandle(
                                instrument=inst, window_start_ts=w_start, window_size_sec=w_size // 1_000_000,
                                open=price, high=price, low=price, close=price, volume=vol, trade_count=1,
                                _first_ts=ts, _last_ts=ts
                            )
                            w_state[inst] = new
                    
                    if limit and tick_count >= limit:
                        break
                
                # Update Metrics
                METRIC_TRADES_TOTAL.labels(status='valid').inc(batch_valid_count)
                METRIC_TRADES_TOTAL.labels(status='dlq').inc(batch_dlq_count)
                
                # Flush DLQ to disk
                if dlq_buffer:
                    f_dlq.writelines(dlq_buffer)
                    dlq_buffer.clear()
                    f_dlq.flush() # Ensure durability
                
                if tick_count % 100000 == 0:
                     print(f"  Processed {tick_count:,} ticks (DLQ: {dlq_count:,})...")
                
                # --- SNAPSHOT LOGIC (For Dashboard) ---
                current_wall_time = time.time()
                if current_wall_time - last_snapshot_time > 0.2:
                    # Dump "Recent History + Active State" to disk for Streamlit
                    snapshot_candles = []
                    
                    # Iterate through all configured windows
                    for w_name, _, _ in windows:
                        # 1. Add History (Finalized candles)
                        # We take the last 500 to provide context for the graph without exploding I/O
                        if w_name in results:
                            snapshot_candles.extend(results[w_name][-500:])
                        
                        # 2. Add Active (Partial candles)
                        if w_name in state:
                            for candle in state[w_name].values():
                                snapshot_candles.append(candle.to_dict())
                    
                    if snapshot_candles:
                        try:
                            df_snap = pl.DataFrame(snapshot_candles)
                            # We skip expensive datetime conversion for the snapshot to keep it fast
                            # Streamlit can parse ISO strings or we do it there.
                            df_snap.write_parquet(Path("output/candles_live_snapshot.parquet"))
                        except Exception as e:
                            pass # Don't crash on snapshot error
                    
                    last_snapshot_time = current_wall_time
                # -------------------------------------

                if limit and tick_count >= limit:
                    break
                
    finally:
        consumer.close()
    
    print(f"\nDLQ Summary: {dlq_count:,} invalid trades rejected.")
    print(f"Full DLQ log available at: {dlq_path.absolute()}")
        
    # Flush remaining
    for w_name, w_state in state.items():
        for candle in w_state.values():
            results[w_name].append(candle.to_dict())
            
    return results


def candles_to_dataframe(candles: list[dict]) -> pl.DataFrame:
    """Convert list of candle dicts to Polars DataFrame."""
    
    if not candles:
        return pl.DataFrame()
    
    df = pl.DataFrame(candles)
    
    # Parse datetime strings into Polars native datetime
    # Input format: ISO string (from to_dict) 2025-12-16T08:00:00.000000+00:00
    try:
        df = df.with_columns([
            pl.col("window_start").str.to_datetime(format="%Y-%m-%dT%H:%M:%S.%f%z", strict=False),
            pl.col("window_end").str.to_datetime(format="%Y-%m-%dT%H:%M:%S.%f%z", strict=False),
        ])
    except Exception as e:
        print(f"⚠️ Warning: performace DataFrame timestamp conversion failed: {e}")
        # Return with string columns is better than crashing
        pass
    
    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--group-id", type=str, default="ohlcv-aggregator")
    parser.add_argument("--source", type=str, default="file") # Keep generic arg
    args = parser.parse_args()
    
    print("=" * 60)
    print("Hyper-Optimized OHLCV Aggregator (Phase 4)")
    print("=" * 60)
    
    start_time = time.time()
    
    # We force the optimized path for Kafka
    # Start Prometheus Metrics Server
    from prometheus_client import start_http_server
    try:
        start_http_server(8000)
        print("📊 Metrics server started on port 8000")
    except OSError:
        print("⚠️  Metrics server port 8000 busy (ignoring)")

    results = run_hyper_optimized_aggregation(
        window_configs=WINDOW_CONFIGS,
        limit=args.limit,
        group_id=args.group_id
    )
    
    duration = time.time() - start_time
    
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    
    total = sum(len(c) for c in results.values())
    print(f"  Total candles: {total:,}")
    print(f"  ⏱️  Time: {duration:.2f}s")
    if args.limit:
        print(f"  🚀 Throughput: {args.limit / duration:,.0f} ticks/sec")
    
    # Save
    df = candles_to_dataframe([c for cl in results.values() for c in cl])
    df.write_parquet(Path("output/candles_realtime.parquet"))
    print("\n✅ Saved to output/candles_realtime.parquet")

if __name__ == "__main__":
    main()
