# 🏗 Architecture & Engineering Decisions

## 1. Ultra-Low Latency Optimization
**Goal**: Achieve >500,000 TPS on a single core (Sub-Second processing for 500k trades).

### ❌ Initial Bottlenecks
-   **JSON Parsing**: `json.loads` is too slow for 500k ops/sec.
-   **Datetime Objects**: Python's `datetime` creation overhead is significant (microseconds add up).
-   **Object Allocation**: Creating `Trade` objects for every tick triggers GC pressure.

### ✅ The Solution: "Integer Protocol"
We switched to a raw byte-processing pipeline:
1.  **Protocol**: CSV over Kafka (`id,inst,ts_int,price,vol`).
2.  **Timestamps**: Sent as **Integer Microseconds** (avoiding ISO string parsing).
3.  **Zero-Copy**: The Aggregator parses `bytes` directly (`msg.value().split(b',')`).
4.  **Inlined Loop**: The windowing logic is inlined into the consumer loop to remove function call overhead.

**Result**: Throughput increased from ~150k TPS to **~600k TPS** (300% Speedup).

---

## 2. Chaos Engineering (Resilience)
**Goal**: Ensure the pipeline remains stable under bad data input.

### 🧪 Error Injection
We simulate "real-world" network issues in `data_simulator.py`:
-   **Negative Prices**: `-10.5`
-   **Zero Volume**: `0.0`
-   **Future Timestamps**: `Now + 1 Year`
-   **Unknown Instruments**

### 🛡 Dead Letter Queue (DLQ)
Instead of crashing or polluting the aggregate state, invalid trades are trapped:
1.  **Validation**: A generic `validate_tick()` check runs in the hot loop.
2.  **Routing**: Invalid rows are serialized to JSON and appended to `output/dlq_errors.jsonl`.
3.  **Metrics**: `trades_processed_total{status="dlq"}` is incremented for alerting.

---

## 3. Storage Strategy
-   **Format**: Parquet + Zstd (Level 3).
-   **Partitioning**: Hive-style (`year=YYYY/month=MM/day=DD`).
-   **Engine**: DuckDB is used for verification, ensuring partition pruning works.
