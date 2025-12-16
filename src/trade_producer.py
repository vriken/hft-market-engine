#!/usr/bin/env python3
"""
Phase 0: Trade Producer for Kafka

Reads trades from parquet and publishes to Kafka for real-time streaming tests.
Supports:
- Real-time replay (with actual delays)
- Fast-forward mode (for testing)
- Partition by instrument for parallel processing
"""

import argparse
import json
import signal
import sys
import time
import random
from pathlib import Path
from datetime import datetime, timezone

import polars as pl
from confluent_kafka import Producer, KafkaError


import os

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9095")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "trades-chaos-bench")

# Speed modes
REALTIME_SPEED = 1.0    # 1x = real-time replay
FAST_FORWARD = 100.0    # 100x = fast replay for testing


def delivery_callback(err, msg):
    """Callback for Kafka message delivery confirmation"""
    if err:
        print(f"❌ Message delivery failed: {err}")
    # Uncomment for verbose logging:
    # else:
    #     print(f"✓ Delivered to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}")


def create_producer() -> Producer:
    """Create Kafka producer with optimal settings"""
    
    # Optimized producer configuration for maximum throughput
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "trade-producer",
        "batch.size": 65536,        # 64KB batches
        "linger.ms": 5,             # Wait 5ms to batch more
        "compression.type": "lz4",  # Fast compression
        "queue.buffering.max.messages": 1000000, # Max messages in producer queue
        # Reliability
        "acks": "all",  # Wait for all replicas
        "retries": 3,
    }
    
    return Producer(config)


def instrument_to_partition(instrument: str, num_partitions: int = 6) -> int:
    """Deterministic partition assignment by instrument hash"""
    return hash(instrument) % num_partitions


def trade_to_json(row: dict) -> str:
    """Convert trade row to JSON string"""
    
    # Convert timestamp to ISO string if it's a datetime
    if hasattr(row["timestamp"], "isoformat"):
        row["timestamp"] = row["timestamp"].isoformat()
    
    return json.dumps(row)


def publish_trades(
    producer: Producer,
    df: pl.DataFrame,
    speed_multiplier: float = FAST_FORWARD,
    limit: int = None,
    infinite: bool = False
) -> int:
    """Publish trades to Kafka with optional infinite replay"""
    
    # Sort by timestamp
    df = df.sort("timestamp")
    
    if limit and not infinite:
        df = df.head(limit)
    
    trades = df.to_dicts()
    original_trades_len = len(trades)
    
    print(f"\n📤 Publishing {original_trades_len:,} trades (Infinite: {infinite})...")
    print(f"Speed: {speed_multiplier}x")
    print()
    
    start_time = time.time()
    prev_trade_ts_str = None
    published = 0
    
    # Micro-optimization: Precompute keys and partitions
    instruments = set(t["instrument"] for t in trades)
    keys = {inst: inst.encode("utf-8") for inst in instruments}
    partitions = {inst: instrument_to_partition(inst) for inst in instruments}
    produce = producer.produce
    poll = producer.poll
    
    # Determine Cycle Duration for Timestamp Shifting
    first_row = trades[0]["timestamp"]
    last_row = trades[-1]["timestamp"]
    
    # helper
    def to_dt(val):
        if isinstance(val, str):
            d = datetime.fromisoformat(val.replace("Z", "+00:00"))
        else:
            d = val
        if d.tzinfo is None: d = d.replace(tzinfo=timezone.utc)
        return d
    
    first_dt = to_dt(first_row)
    last_dt = to_dt(last_row)
    cycle_duration_sec = (last_dt - first_dt).total_seconds()
    
    # We add 1s buffer between cycles
    cycle_duration_offset = int((cycle_duration_sec + 1.0) * 1_000_000)
    
    base_time_offset_micros = 0
    iteration = 0
    price_level_multiplier = 1.0
    
    while True: # Infinite loop or single pass
        iteration += 1
        
        # Reset per-cycle tracker
        prev_trade_ts_dt = None
        
        for i, trade in enumerate(trades):
            # 1. Throttle Speed
            current_ts_val = trade["timestamp"]
            current_dt = to_dt(current_ts_val)
            
            if prev_trade_ts_dt:
                delay_s = (current_dt - prev_trade_ts_dt).total_seconds()
                adjusted_delay = delay_s / speed_multiplier
                if adjusted_delay > 0:
                     time.sleep(adjusted_delay)
            
            prev_trade_ts_dt = current_dt
            
            # 2. Shift Timestamp
            ts_int = int(current_dt.timestamp() * 1_000_000)
            final_ts = ts_int + base_time_offset_micros
            
            # 3. Construct Message
            # Protocol: msg_id,instrument,timestamp_int,price,volume,side,msg_type
            
            # Legacy support: if 'msg_type' missing, assume 'T'
            m_type = trade.get("msg_type", "T")
            
            # Legacy support: 'msg_id' vs 'trade_id'
            m_id = trade.get("msg_id", trade.get("trade_id", ""))
            
            # 4. Apply Cycle Drift (Make history unique)
            # We drift the price level slightly for each cycle so visual patterns shift
            final_price = trade["price"] * price_level_multiplier
            
            # Optional: Add tiny jitter to every trade (L3 Micro-structure noise)
            # This ensures even the "shape" is slightly organic
            if infinite:
                final_price *= random.uniform(0.9995, 1.0005)
            
            message = (
                f'{m_id},{trade["instrument"]},{final_ts},'
                f'{final_price:.2f},{trade["volume"]},{trade["side"]},{m_type}'
            )
            
            inst = trade["instrument"]
            
            try:
                produce(
                    topic=KAFKA_TOPIC,
                    key=keys[inst],
                    value=message.encode("utf-8"),
                    partition=partitions[inst]
                )
                published += 1
                
                if published % 10000 == 0:
                    poll(0)
                    elapsed = time.time() - start_time
                    rate = published / elapsed if elapsed > 0 else 0
                    if published % 50000 == 0:
                         print(f"  {published:,} trades ({rate:.0f}/sec) [Cycle {iteration} | Price x{price_level_multiplier:.2f}]", end="\r")
            
            except BufferError:
                poll(0.1)
                # Retry once
                produce(topic=KAFKA_TOPIC, key=keys[inst], value=message.encode("utf-8"), partition=partitions[inst])
                published += 1
                
            except Exception as e:
                print(f"Error: {e}")

            if limit and not infinite and published >= limit:
                 break
        
        if not infinite:
            break
        
        # Prepare for next cycle
        base_time_offset_micros += cycle_duration_offset
        
        # Random Walk the Price Level for next cycle (Mean Reverting towards 1.0)
        # If > 1.2, bias down. If < 0.8, bias up. 
        drift = random.uniform(0.9, 1.1)
        if price_level_multiplier > 1.2: drift = 0.95
        if price_level_multiplier < 0.8: drift = 1.05
        
        price_level_multiplier *= drift
        
        print(f"\n🔄 Cycle {iteration} complete. Loop +1s... (New Price Level: x{price_level_multiplier:.2f})")
        time.sleep(1.0 / speed_multiplier)
    
    # Flush remaining messages
    print("\n\n⏳ Flushing remaining messages...")
    producer.flush(timeout=10)
    
    elapsed = time.time() - start_time
    rate = published / elapsed if elapsed > 0 else 0
    
    print(f"\n✅ Published {published:,} trades in {elapsed:.1f}s ({rate:.0f}/sec)")
    
    return published


def main():
    """Main entry point"""
    
    parser = argparse.ArgumentParser(
        description="Publish trades to Kafka for real-time streaming tests"
    )
    parser.add_argument(
        "--input", "-i",
        type=Path,
        default=Path(__file__).parent.parent / "output" / "raw_market_data_l3.parquet",
        help="Input parquet file (default: output/raw_market_data_l3.parquet)"
    )
    parser.add_argument(
        "--speed", "-s",
        type=float,
        default=FAST_FORWARD,
        help=f"Replay speed multiplier (default: {FAST_FORWARD})"
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=None,
        help="Limit number of trades to publish"
    )
    parser.add_argument(
        "--realtime", "-r",
        action="store_true",
        help="Real-time replay (1x speed)"
    )
    parser.add_argument(
        "--infinite",
        action="store_true",
        help="Loop forever (simulating strictly increasing time)"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Trade Producer for Kafka")
    print("=" * 60)
    
    # Load trades
    if not args.input.exists():
        print(f"\n❌ Input file not found: {args.input}")
        print("Run data_simulator.py first to generate trades.")
        sys.exit(1)
    
    print(f"\n📂 Loading trades from: {args.input}")
    df = pl.read_parquet(args.input)
    print(f"   Loaded {len(df):,} trades")
    
    # Create producer
    print(f"\n🔌 Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "trade-producer",
        "batch.size": 65536,        # 64KB batches
        "linger.ms": 5,             # Wait 5ms to batch more
        "compression.type": "lz4",  # Fast compression
        "queue.buffering.max.messages": 1000000, # Max messages in producer queue
        # Reliability
        "acks": "all",  # Wait for all replicas
        "retries": 3,
    }
    
    try:
        producer = Producer(conf)
        # Check connection
        producer.list_topics(timeout=5)
    except Exception as e:
        print(f"❌ Failed to create Kafka producer: {e}") 
        print(f"Make sure Kafka is running: docker compose up -d")
        sys.exit(1)
    
    # Handle Ctrl+C gracefully
    def signal_handler(sig, frame):
        print("\n\n⚠️  Interrupted! Flushing messages...")
        producer.flush(timeout=5)
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    # Publish trades
    speed = REALTIME_SPEED if args.realtime else args.speed
    publish_trades(producer, df, speed_multiplier=speed, limit=args.limit, infinite=args.infinite)
    
    print("\n✅ Producer complete!")


if __name__ == "__main__":
    main()
