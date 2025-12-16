import subprocess
import time
import polars as pl
from pathlib import Path
import os
import signal

def run_test():
    print("🚀 Starting Headless Integration Test (Phase 9)...")
    
    # 1. Start Producer (Speed 1000x -> ~86s duration for full file)
    # We use a specific topic to avoid noise
    env = os.environ.copy()
    env["KAFKA_TOPIC"] = "trades-viz-test"
    
    print("  [1/4] Launching Producer (Speed 1000x)...")
    p_prod = subprocess.Popen(
        ["python", "src/trade_producer.py", "--speed", "1000", "--infinite"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    
    # 2. Start Aggregator
    print("  [2/4] Launching Aggregator (Snapshot Mode)...")
    p_agg = subprocess.Popen(
        ["python", "src/realtime_aggregator.py", "--group-id", "viz-test-group"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    
    # 3. Monitor Loop (10 seconds)
    print("  [3/4] Monitoring Snapshot File (10s)...")
    snapshot_path = Path("output/candles_live_snapshot.parquet")
    
    try:
        if snapshot_path.exists():
            snapshot_path.unlink()
            
        for i in range(10):
            time.sleep(1.0)
            if snapshot_path.exists():
                try:
                    df = pl.read_parquet(snapshot_path)
                    count = len(df)
                    # Check if we have history
                    # We expect > 0
                    print(f"    T+{i}s: Snapshot found! Rows: {count} (Sample: {df['close'].tail(1).item()})")
                except Exception as e:
                    print(f"    T+{i}s: Read Error (Writing?): {e}")
            else:
                print(f"    T+{i}s: Waiting for snapshot...")
                
    except KeyboardInterrupt:
        pass
    finally:
        print("  [4/4] Cleaning up...")
        p_prod.terminate()
        p_agg.terminate()
        p_prod.wait()
        p_agg.wait()
        print("✅ Test Complete.")

if __name__ == "__main__":
    run_test()
