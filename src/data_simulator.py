#!/usr/bin/env python3
"""
Phase 0: HFT Trade Data Simulator

Generates realistic high-frequency trade data with:
- 6 energy instruments (power, gas, carbon)
- Millisecond timestamp precision
- Poisson inter-arrival times
- Geometric Brownian Motion price dynamics
- Log-normal volume distribution
- 3-5% message disorder (simulates network delays)
"""

import json
import random
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generator

import numpy as np
import polars as pl

# Instrument configurations
INSTRUMENTS = {
    "POWER_DE_SPOT": {"base_price": 85.50, "volatility": 0.02, "avg_trades_per_sec": 15},
    "POWER_FR_SPOT": {"base_price": 82.30, "volatility": 0.018, "avg_trades_per_sec": 12},
    "GAS_TTF_SPOT": {"base_price": 42.75, "volatility": 0.025, "avg_trades_per_sec": 20},
    "GAS_NBP_SPOT": {"base_price": 38.90, "volatility": 0.022, "avg_trades_per_sec": 8},
    "CARBON_EUA_DEC": {"base_price": 78.20, "volatility": 0.015, "avg_trades_per_sec": 10},
    "CARBON_UKA_DEC": {"base_price": 45.60, "volatility": 0.02, "avg_trades_per_sec": 6},
}

# Simulation parameters
TRADING_HOURS = 8  # Hours of trading data to generate
DISORDER_PERCENTAGE = 0.04  # 4% of messages arrive out of order
MAX_DISORDER_MS = 500  # Maximum delay for disordered messages


@dataclass
class Trade:
    """Single trade event"""
    trade_id: str
    instrument: str
    timestamp: str  # ISO format with ms precision
    price: float
    volume: float
    side: str  # BUY or SELL
    sequence_id: int


def generate_gbm_price(current_price: float, volatility: float, dt: float) -> float:
    """Generate next price using Geometric Brownian Motion"""
    # dS = S * (mu*dt + sigma*dW)
    # Simplified: assume mu=0 (no drift for short-term HFT)
    dw = np.random.normal(0, np.sqrt(dt))
    new_price = current_price * np.exp(volatility * dw)
    return round(new_price, 2)


def generate_volume() -> float:
    """Generate trade volume using log-normal distribution"""
    # Mean around 50 MWh, with heavy tail
    volume = np.random.lognormal(mean=3.9, sigma=0.8)
    return round(volume, 1)


def generate_trades_for_instrument(
    instrument: str,
    config: dict,
    start_time: datetime,
    duration_hours: int,
    start_sequence: int
) -> Generator[Trade, None, None]:
    """Generate trades for a single instrument using Poisson arrivals"""
    
    current_time = start_time
    end_time = start_time + timedelta(hours=duration_hours)
    current_price = config["base_price"]
    sequence_id = start_sequence
    
    # Poisson rate: average trades per second
    lambda_rate = config["avg_trades_per_sec"]
    
    while current_time < end_time:
        # Poisson inter-arrival time (exponential distribution)
        inter_arrival_ms = np.random.exponential(1000 / lambda_rate)
        current_time += timedelta(milliseconds=inter_arrival_ms)
        
        if current_time >= end_time:
            break
        
        # Update price using GBM
        dt = inter_arrival_ms / 1000 / 3600  # Convert to hours for volatility scaling
        current_price = generate_gbm_price(current_price, config["volatility"], dt)
        
        # Generate trade
        trade = Trade(
            trade_id=f"{instrument[:3]}-{sequence_id:08d}",
            instrument=instrument,
            timestamp=current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            price=current_price,
            volume=generate_volume(),
            side=random.choice(["BUY", "SELL"]),
            sequence_id=sequence_id
        )
        
        sequence_id += 1
        yield trade


def add_disorder(trades: list[Trade], disorder_pct: float, max_delay_ms: int) -> list[Trade]:
    """Add network disorder by delaying some messages"""
    
    disordered = []
    num_to_disorder = int(len(trades) * disorder_pct)
    disorder_indices = set(random.sample(range(len(trades)), num_to_disorder))
    
    for i, trade in enumerate(trades):
        if i in disorder_indices:
            # Parse timestamp, add random delay, reformat
            ts = datetime.strptime(trade.timestamp[:-1], "%Y-%m-%dT%H:%M:%S.%f")
            delay_ms = random.randint(50, max_delay_ms)
            delayed_ts = ts + timedelta(milliseconds=delay_ms)
            
            # Create new trade with delayed timestamp (keep original sequence_id for tracing)
            delayed_trade = Trade(
                trade_id=trade.trade_id,
                instrument=trade.instrument,
                timestamp=delayed_ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                price=trade.price,
                volume=trade.volume,
                side=trade.side,
                sequence_id=trade.sequence_id
            )
            disordered.append(delayed_trade)
        else:
            disordered.append(trade)
    
    # Sort by timestamp to simulate arrival order
    disordered.sort(key=lambda t: t.timestamp)
    return disordered


def inject_chaos(trades: list[Trade], chaos_pct: float) -> list[Trade]:
    """
    Inject corrupted data for Chaos Engineering testing.
    Types of errors:
    - Negative Price
    - Zero Volume
    - Future Timestamp (Far future)
    - Unknown Instrument
    """
    chaos_trades = []
    num_to_corrupt = int(len(trades) * chaos_pct)
    corrupt_indices = set(random.sample(range(len(trades)), num_to_corrupt))
    
    error_types = ["NEG_PRICE", "ZERO_VOL", "FUTURE_TS", "BAD_INST"]
    
    for i, trade in enumerate(trades):
        if i in corrupt_indices:
            # Create a copy to corrupt
            bad_trade = Trade(**asdict(trade))
            etype = random.choice(error_types)
            
            if etype == "NEG_PRICE":
                bad_trade.price = -10.5
            elif etype == "ZERO_VOL":
                bad_trade.volume = 0.0
            elif etype == "FUTURE_TS":
                # Add 365 days
                ts = datetime.strptime(bad_trade.timestamp[:-1], "%Y-%m-%dT%H:%M:%S.%f")
                bad_ts = ts + timedelta(days=365)
                bad_trade.timestamp = bad_ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            elif etype == "BAD_INST":
                bad_trade.instrument = "UNKNOWN_COIN"
                
            chaos_trades.append(bad_trade)
        else:
            chaos_trades.append(trade)
            
    # Re-sort? Usually chaos trades arrive mixed in.
    # Future timestamps will naturally be at the end if sorted, 
    # but for streaming simulation we might want them to arrive "early" relative to their timestamp?
    # Actually, if we sort by timestamp, future trades appear at the end of the file.
    # If we DON'T sort, they appear at their "insertion" time (simulating clock skew but arriving now).
    # Let's keep them in valid order for now, meaning future trades will be 'late' or 'early' 
    # depending on how aggregator views 'now'.
    # Actually, future TS usually means "clock skew ahead".
    # Let's NOT resort for chaos to simulate "arrival time" being correct but "payload time" being wrong.
    # But `trade_producer` reads sequentially. 
    # If we want to simulate "bad timestamp in payload", we usually just change payload.
    return chaos_trades

@dataclass
class MarketMessage:
    """Unified L1/L3 market event"""
    msg_id: str
    instrument: str
    timestamp: str          # ISO format
    price: float
    volume: float
    side: str               # BUY/SELL
    msg_type: str           # T=Trade, A=Add, C=Cancel
    sequence_id: int

def simulate_hawkes_intensity(mu, alpha, beta, last_event_time, current_time, last_intensity):
    """
    Calculate Hawkes intensity at current_time given last event.
    lambda(t) = mu + (lambda(t_last) - mu) * exp(-beta * (t - t_last)) + alpha * exp(-beta * (t - t_last))
    Actually, immediately AFTER an event: lambda(t+) = lambda(t-) + alpha.
    Decay happens between events.
    """
    dt = (current_time - last_event_time).total_seconds()
    decay = np.exp(-beta * dt)
    return mu + (last_intensity - mu) * decay

def generate_l3_session(
    instrument: str,
    config: dict,
    start_time: datetime,
    duration_hours: int,
    start_sequence: int
) -> Generator[MarketMessage, None, None]:
    """
    Generate L3 Market Data (Trades + Orders) using Hawkes Process for Trades
    and Poisson noise for Order Updates.
    """
    current_time = start_time
    end_time = start_time + timedelta(hours=duration_hours)
    
    current_price = config["base_price"]
    sequence_id = start_sequence
    
    # Hawkes Params (Vol clustering parameters)
    mu = config["avg_trades_per_sec"]   # Base rate
    alpha = mu * 0.8                    # Excitation (high alpha = bursty)
    beta = mu * 1.2                     # Decay (must be > alpha for stability)
    current_intensity = mu
    last_trade_time = start_time
    
    # L3 Noise Params
    updates_per_trade = 50  # 50 updates between trades ~ 1% trade ratio
    
    while current_time < end_time:
        # 1. Determine next TRADE time via Thinning Algorithm (Ogata)
        # Simplified: Use current max intensity to bound expectation
        # or just adaptive rate.
        # Let's use a simpler "variable rate Poisson" approximation for simulation speed.
        rate_bound = current_intensity
        dt_sec = np.random.exponential(1.0 / rate_bound)
        tentative_time = current_time + timedelta(seconds=dt_sec)
        
        # Exact thinning check would go here, but for this demo, we assume valid transition
        # Decay intensity to this point
        decay_factor = np.exp(-beta * dt_sec)
        current_intensity = mu + (current_intensity - mu) * decay_factor
        
        # Jump!
        current_intensity += alpha
        next_trade_time = tentative_time
        
        if next_trade_time >= end_time:
            break
            
        # 2. GBM Price Update
        time_step_hours = dt_sec / 3600
        current_price = generate_gbm_price(current_price, config["volatility"], time_step_hours)
        
        # 3. Inject L3 NOISE (Orders) in the gap [current_time, next_trade_time)
        # We fill the gap with uniform random L3 messages
        gap_duration_ms = (next_trade_time - current_time).total_seconds() * 1000
        if gap_duration_ms > 0:
            num_updates = np.random.poisson(updates_per_trade)
            
            # Times for updates
            update_offsets = sorted(np.random.uniform(0, gap_duration_ms, num_updates))
            
            for offset in update_offsets:
                l3_ts = current_time + timedelta(milliseconds=offset)
                msg_type = random.choice(["A", "C"]) # Add / Cancel
                
                # Order logic: Adds are near price, Cancels are existing
                l3_price = current_price + np.random.normal(0, 0.05) if msg_type == 'A' else current_price
                l3_vol = generate_volume()
                
                yield MarketMessage(
                    msg_id=f"{instrument[:3]}-{sequence_id:09d}",
                    instrument=instrument,
                    timestamp=l3_ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                    price=round(l3_price, 2),
                    volume=l3_vol,
                    side=random.choice(["BUY", "SELL"]),
                    msg_type=msg_type,
                    sequence_id=sequence_id
                )
                sequence_id += 1
        
        # 4. Emit TRADE
        yield MarketMessage(
            msg_id=f"{instrument[:3]}-{sequence_id:09d}",
            instrument=instrument,
            timestamp=next_trade_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            price=current_price,
            volume=generate_volume(),
            side=random.choice(["BUY", "SELL"]),
            msg_type="T", # Trade
            sequence_id=sequence_id
        )
        sequence_id += 1
        
        current_time = next_trade_time
        last_trade_time = current_time

def messages_to_dataframe(msgs: list[MarketMessage]) -> pl.DataFrame:
    """Convert messages to DataFrame"""
    return pl.DataFrame([asdict(m) for m in msgs]).with_columns([
        pl.col("timestamp").str.to_datetime("%Y-%m-%dT%H:%M:%S%.3fZ"),
        pl.col("price").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
    ])

def main():
    """Generate Simulated L3 Market Data (Hawkes + Noise)"""
    print("=" * 60)
    print("Phase 10: Advanced Market Simulator (L3 + Hawkes)")
    print("=" * 60)
    
    random.seed(42)
    np.random.seed(42)
    
    start_time = datetime.now(timezone.utc).replace(hour=8, minute=0, second=0, microsecond=0)
    
    # We reduce duration to 5 minutes for the L3 demo
    # 15 trades/sec * 300s = 4.5k trades * 50 noise = 225k msgs per instrument
    # Total ~1.3M messages. Fast to generate, heavy to process.
    SIM_DURATION_MINUTES = 5
    
    print(f"\nGenerating {SIM_DURATION_MINUTES}m of L3 Market Data (Regime: Hawkes)...")
    
    all_msgs: list[MarketMessage] = []
    seq_id = 1
    
    for inst, config in INSTRUMENTS.items():
        print(f"  Simulating {inst}...", end="\r")
        msgs = list(generate_l3_session(inst, config, start_time, SIM_DURATION_MINUTES / 60.0, seq_id))
        all_msgs.extend(msgs)
        seq_id += len(msgs) + 1
        print(f"  Simulating {inst}: {len(msgs):,} msgs (Trades & Orders)")
    
    # Sort
    print("\nSorting global message stream...")
    all_msgs.sort(key=lambda m: m.timestamp)
    
    # Save
    df = messages_to_dataframe(all_msgs)
    output_path = Path(__file__).parent.parent / "output" / "raw_market_data_l3.parquet"
    print(f"\nWriting {len(df):,} rows to {output_path}...")
    df.write_parquet(output_path)
    
    counts = df["msg_type"].value_counts()
    print("\nMessage Distribution:")
    print(counts)
    
    print("\n✅ Phase 10 Data Generation Complete.")

if __name__ == "__main__":
    main()
