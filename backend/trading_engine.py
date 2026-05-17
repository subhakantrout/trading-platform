import json
import numpy as np
from datetime import datetime, timezone, timedelta
from sqlalchemy import select

from database import AsyncSessionLocal, Trade, BacktestRun, Candle
from config import DEFAULT_RISK_PER_TRADE, MAX_POSITION_RISK, BACKTEST_DEFAULT_CAPITAL

###############################################################################
# RISK MANAGEMENT ENGINE
###############################################################################

def calculate_position_size(
    account_equity: float,
    entry_price: float,
    stop_loss: float,
    risk_percent: float = DEFAULT_RISK_PER_TRADE,
    max_risk_percent: float = MAX_POSITION_RISK,
    atr: float = None,
    volatility_scaling: bool = True
) -> dict:
    risk_percent = min(risk_percent, max_risk_percent)
    dollar_risk = account_equity * risk_percent
    price_risk = abs(entry_price - stop_loss)
    if price_risk <= 0:
        return {"quantity": 0, "dollar_risk": 0, "warning": "Invalid stop loss"}

    base_quantity = dollar_risk / price_risk

    # Volatility scaling: reduce size in high volatility
    if volatility_scaling and atr and atr > 0:
        vol_ratio = atr / entry_price
        if vol_ratio > 0.02:
            scale = max(0.3, 0.02 / vol_ratio)
            base_quantity *= scale

    # Kelly Criterion adjustment (simplified)
    kelly_fraction = 0.25
    adjusted_quantity = base_quantity * kelly_fraction

    total_risk = adjusted_quantity * price_risk
    risk_pct = total_risk / account_equity * 100 if account_equity > 0 else 0

    return {
        "quantity": round(adjusted_quantity, 4),
        "dollar_risk": round(total_risk, 2),
        "risk_percent": round(risk_pct, 2),
        "entry_price": entry_price,
        "stop_loss": stop_loss,
        "kelly_fraction": kelly_fraction,
        "volatility_scaled": volatility_scaling and atr is not None
    }

def calculate_var(returns: list, confidence: float = 0.95) -> dict:
    if not returns or len(returns) < 20:
        return {"var_95": 0, "var_99": 0, "cvar": 0}
    arr = np.array(returns)
    mean = np.mean(arr)
    std = np.std(arr)
    var_95 = float(np.percentile(arr, 5))
    var_99 = float(np.percentile(arr, 1))
    cvar = float(arr[arr <= var_95].mean()) if len(arr[arr <= var_95]) > 0 else var_95
    return {
        "var_95": round(var_95, 4),
        "var_99": round(var_99, 4),
        "cvar": round(cvar, 4),
        "volatility": round(float(std), 4),
        "mean_return": round(float(mean), 4)
    }

def calculate_sharpe_ratio(returns: list, risk_free_rate: float = 0.05) -> float:
    if not returns or len(returns) < 2:
        return 0.0
    arr = np.array(returns)
    excess = arr - (risk_free_rate / 252)
    if np.std(arr) == 0:
        return 0.0
    return float(np.mean(excess) / np.std(arr) * np.sqrt(252))

def calculate_sortino_ratio(returns: list, risk_free_rate: float = 0.05) -> float:
    if not returns or len(returns) < 2:
        return 0.0
    arr = np.array(returns)
    excess = arr - (risk_free_rate / 252)
    downside = arr[arr < 0]
    if len(downside) == 0 or np.std(downside) == 0:
        return float(np.mean(excess) * np.sqrt(252)) if np.mean(excess) > 0 else 0.0
    return float(np.mean(excess) / np.std(downside) * np.sqrt(252))

def calculate_max_drawdown(equity_curve: list) -> dict:
    if not equity_curve or len(equity_curve) < 2:
        return {"max_drawdown": 0, "max_drawdown_pct": 0}
    arr = np.array(equity_curve)
    peak = np.maximum.accumulate(arr)
    drawdown = (arr - peak) / peak * 100
    max_dd = float(np.min(drawdown))
    max_dd_idx = int(np.argmin(drawdown))
    return {
        "max_drawdown": round(max_dd, 2),
        "max_drawdown_pct": round(abs(max_dd), 2),
        "max_drawdown_index": max_dd_idx,
        "current_drawdown": round(float(drawdown[-1]), 2)
    }

def calculate_profit_factor(trades: list) -> float:
    gross_profit = sum(t["profit"] for t in trades if t.get("profit", 0) > 0)
    gross_loss = abs(sum(t["profit"] for t in trades if t.get("profit", 0) < 0))
    if gross_loss == 0:
        return 999.0 if gross_profit > 0 else 0.0
    return round(gross_profit / gross_loss, 2)

###############################################################################
# BACKTESTING ENGINE
###############################################################################

async def run_backtest(
    symbol: str,
    timeframe: str,
    strategy_name: str = "ai_multi_agent",
    initial_capital: float = BACKTEST_DEFAULT_CAPITAL,
    risk_per_trade: float = DEFAULT_RISK_PER_TRADE,
    start_date: datetime = None,
    end_date: datetime = None
) -> dict:
    async with AsyncSessionLocal() as session:
        stream_id = f"{symbol}_{timeframe}"
        query = select(Candle).where(Candle.stream_id == stream_id).order_by(Candle.time)
        if start_date:
            query = query.where(Candle.ingested_at >= start_date)
        if end_date:
            query = query.where(Candle.ingested_at <= end_date)
        result = await session.execute(query)
        all_candles = result.scalars().all()

    if len(all_candles) < 50:
        return {"error": f"Insufficient data for {stream_id}. Need >=50 candles, got {len(all_candles)}"}

    candles_list = [
        {"time": c.time, "open": c.open, "high": c.high,
         "low": c.low, "close": c.close, "volume": c.volume or 0}
        for c in all_candles
    ]

    capital = initial_capital
    equity_curve = [capital]
    trades = []
    in_position = False
    position = {}
    lookback = 30

    for i in range(lookback, len(candles_list)):
        current = candles_list[i]
        window = candles_list[i - lookback:i]

        closes = np.array([w["close"] for w in window], dtype=float)
        highs = np.array([w["high"] for w in window], dtype=float)
        lows = np.array([w["low"] for w in window], dtype=float)

        # Simple SMA crossover strategy
        sma_fast = np.mean(closes[-5:])
        sma_slow = np.mean(closes[-20:])
        rsi_val = 50.0
        if len(closes) >= 15:
            gains = losses = 0.0
            for j in range(1, 15):
                diff = closes[-j] - closes[-j - 1]
                if diff > 0: gains += diff
                else: losses -= diff
            avg_gain = gains / 14
            avg_loss = losses / 14
            if avg_loss > 0:
                rsi_val = 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)

        atr_val = float(np.mean([h - l for h, l in zip(highs[-14:], lows[-14:])])) if len(highs) >= 14 else 0

        signal = None
        if not in_position:
            if sma_fast > sma_slow and rsi_val > 40 and rsi_val < 70:
                signal = "BUY"
            elif sma_fast < sma_slow and rsi_val < 60 and rsi_val > 30:
                signal = "SELL"
        else:
            stop_dist = abs(position["entry_price"] - position["stop_loss"])
            take_dist = stop_dist * 2
            if position["direction"] == "LONG":
                if current["low"] <= position["stop_loss"]:
                    signal = "STOP_HIT"
                elif current["high"] >= position["entry_price"] + take_dist:
                    signal = "TAKE_PROFIT"
            else:
                if current["high"] >= position["stop_loss"]:
                    signal = "STOP_HIT"
                elif current["low"] <= position["entry_price"] - take_dist:
                    signal = "TAKE_PROFIT"

        if signal == "BUY" and not in_position:
            stop = current["close"] - atr_val * 1.5
            risk = abs(current["close"] - stop)
            dollar_risk = capital * risk_per_trade
            qty = dollar_risk / risk if risk > 0 else 0
            qty = max(qty, 0)
            in_position = True
            position = {
                "direction": "LONG",
                "entry_price": current["close"],
                "stop_loss": stop,
                "quantity": qty,
                "entry_time": current["time"]
            }
        elif signal == "SELL" and not in_position:
            stop = current["close"] + atr_val * 1.5
            risk = abs(stop - current["close"])
            dollar_risk = capital * risk_per_trade
            qty = dollar_risk / risk if risk > 0 else 0
            qty = max(qty, 0)
            in_position = True
            position = {
                "direction": "SHORT",
                "entry_price": current["close"],
                "stop_loss": stop,
                "quantity": qty,
                "entry_time": current["time"]
            }
        elif in_position and signal in ("STOP_HIT", "TAKE_PROFIT"):
            exit_price = position["stop_loss"] if signal == "STOP_HIT" else (
                position["entry_price"] + abs(position["entry_price"] - position["stop_loss"]) * 2
                if position["direction"] == "LONG" else
                position["entry_price"] - abs(position["stop_loss"] - position["entry_price"]) * 2
            )
            if position["direction"] == "LONG":
                profit = (exit_price - position["entry_price"]) * position["quantity"]
            else:
                profit = (position["entry_price"] - exit_price) * position["quantity"]
            capital += profit
            trades.append({
                "direction": position["direction"],
                "entry_price": position["entry_price"],
                "exit_price": exit_price,
                "quantity": position["quantity"],
                "profit": profit,
                "entry_time": position["entry_time"],
                "exit_time": current["time"],
                "exit_reason": signal
            })
            in_position = False
            position = {}
            equity_curve.append(capital)

    if in_position:
        exit_price = candles_list[-1]["close"]
        if position["direction"] == "LONG":
            profit = (exit_price - position["entry_price"]) * position["quantity"]
        else:
            profit = (position["entry_price"] - exit_price) * position["quantity"]
        capital += profit
        trades.append({
            "direction": position["direction"],
            "entry_price": position["entry_price"],
            "exit_price": exit_price,
            "quantity": position["quantity"],
            "profit": profit,
            "entry_time": position["entry_time"],
            "exit_time": candles_list[-1]["time"],
            "exit_reason": "FORCE_CLOSE"
        })
        equity_curve.append(capital)

    returns = []
    for j in range(1, len(equity_curve)):
        if equity_curve[j - 1] > 0:
            returns.append((equity_curve[j] - equity_curve[j - 1]) / equity_curve[j - 1])

    trade_profits = [t["profit"] for t in trades]
    wins = [t for t in trades if t["profit"] > 0]

    equity_curve_pct = [(e / initial_capital - 1) * 100 for e in equity_curve]
    dd_info = calculate_max_drawdown(equity_curve)

    metrics = {
        "total_return": round((capital - initial_capital) / initial_capital * 100, 2),
        "final_capital": round(capital, 2),
        "total_trades": len(trades),
        "winning_trades": len(wins),
        "win_rate": round(len(wins) / len(trades) * 100, 2) if trades else 0,
        "sharpe_ratio": round(calculate_sharpe_ratio(returns), 2),
        "sortino_ratio": round(calculate_sortino_ratio(returns), 2),
        "max_drawdown_pct": dd_info["max_drawdown_pct"],
        "profit_factor": calculate_profit_factor(trades),
        "avg_profit": round(np.mean(trade_profits), 2) if trade_profits else 0,
        "avg_win": round(np.mean([t["profit"] for t in wins]), 2) if wins else 0,
        "avg_loss": round(np.mean([t["profit"] for t in trades if t["profit"] <= 0]), 2) if any(t["profit"] <= 0 for t in trades) else 0,
        "largest_win": round(max(trade_profits), 2) if trade_profits else 0,
        "largest_loss": round(min(trade_profits), 2) if trade_profits else 0,
        "equity_curve": [round(e, 2) for e in equity_curve],
        "equity_curve_pct": [round(e, 2) for e in equity_curve_pct],
        "var_95": calculate_var(returns).get("var_95", 0),
        "cvar": calculate_var(returns).get("cvar", 0),
    }

    async with AsyncSessionLocal() as session:
        backtest_entry = BacktestRun(
            symbol=symbol, timeframe=timeframe,
            strategy_name=strategy_name,
            start_date=start_date or datetime.now(timezone.utc) - timedelta(days=30),
            end_date=end_date or datetime.now(timezone.utc),
            initial_capital=initial_capital,
            final_capital=metrics["final_capital"],
            total_return=metrics["total_return"],
            sharpe_ratio=metrics["sharpe_ratio"],
            sortino_ratio=metrics["sortino_ratio"],
            max_drawdown=metrics["max_drawdown_pct"],
            win_rate=metrics["win_rate"],
            total_trades=metrics["total_trades"],
            profit_factor=metrics["profit_factor"],
            metrics_json=json.dumps(metrics, default=str),
            trades_json=json.dumps(trades, default=str)
        )
        session.add(backtest_entry)
        await session.commit()

    return {"metrics": metrics, "trades": trades}

###############################################################################
# TRADE JOURNAL
###############################################################################

async def record_trade(
    stream_id: str, symbol: str, direction: str,
    entry_price: float, stop_loss: float, take_profit: float,
    quantity: float, strategy: str = "manual",
    risk_percent: float = DEFAULT_RISK_PER_TRADE,
    analysis_id: int = None, notes: str = None
) -> dict:
    async with AsyncSessionLocal() as session:
        trade = Trade(
            stream_id=stream_id, symbol=symbol,
            direction=direction.upper(),
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            quantity=quantity,
            status="open",
            strategy=strategy,
            analysis_id=analysis_id,
            notes=notes,
            risk_percent=risk_percent
        )
        session.add(trade)
        await session.commit()
        await session.refresh(trade)
        return {
            "id": trade.id,
            "symbol": trade.symbol,
            "direction": trade.direction,
            "entry_price": trade.entry_price,
            "quantity": trade.quantity,
            "status": trade.status
        }

async def close_trade(trade_id: int, exit_price: float, notes: str = None) -> dict:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Trade).where(Trade.id == trade_id))
        trade = result.scalar_one_or_none()
        if not trade:
            return {"error": f"Trade {trade_id} not found"}
        if trade.status != "open":
            return {"error": f"Trade {trade_id} already closed"}

        if trade.direction == "LONG":
            trade.profit = (exit_price - trade.entry_price) * trade.quantity
        else:
            trade.profit = (trade.entry_price - exit_price) * trade.quantity

        trade.profit_pips = (exit_price - trade.entry_price) if trade.direction == "LONG" else (trade.entry_price - exit_price)
        trade.exit_price = exit_price
        trade.exit_time = datetime.now(timezone.utc)
        trade.status = "closed"
        if notes:
            trade.notes = (trade.notes or "") + f" | Close: {notes}"
        entry_exit_diff = abs(trade.entry_price - exit_price)
        stop_diff = abs(trade.entry_price - trade.stop_loss) if trade.stop_loss else entry_exit_diff
        trade.r_multiple = round(trade.profit / (trade.quantity * stop_diff), 2) if (stop_diff > 0 and trade.quantity > 0) else 0

        await session.commit()
        return {
            "id": trade.id,
            "symbol": trade.symbol,
            "profit": trade.profit,
            "r_multiple": trade.r_multiple,
            "status": "closed"
        }

async def get_open_trades(current_prices: dict = None) -> list:
    """Return open trades with real unrealized P/L when current_prices are provided.
    current_prices: {symbol: {bid: float, ask: float}}
    """
    current_prices = current_prices or {}
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Trade).where(Trade.status == "open").order_by(Trade.entry_time.desc())
        )
        trades = result.scalars().all()
        output = []
        for t in trades:
            # Calculate real unrealized P/L from current market price
            upl = 0.0
            prices = current_prices.get(t.symbol, {})
            if prices:
                # LONG exits at bid, SHORT exits at ask
                current = prices.get("bid", 0) if t.direction == "LONG" else prices.get("ask", 0)
                if current > 0:
                    if t.direction == "LONG":
                        upl = (current - t.entry_price) * t.quantity
                    else:
                        upl = (t.entry_price - current) * t.quantity
            output.append({
                "id": t.id, "stream_id": t.stream_id, "symbol": t.symbol,
                "direction": t.direction, "entry_price": t.entry_price,
                "stop_loss": t.stop_loss, "take_profit": t.take_profit,
                "quantity": t.quantity, "strategy": t.strategy,
                "entry_time": t.entry_time.isoformat() if t.entry_time else None,
                "unrealized_pl": round(upl, 2)
            })
        return output

async def get_trade_history(limit: int = 50) -> list:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Trade).where(Trade.status == "closed").order_by(Trade.exit_time.desc()).limit(limit)
        )
        trades = result.scalars().all()
        return [
            {
                "id": t.id, "symbol": t.symbol, "direction": t.direction,
                "entry_price": t.entry_price, "exit_price": t.exit_price,
                "quantity": t.quantity, "profit": t.profit,
                "r_multiple": t.r_multiple, "strategy": t.strategy,
                "entry_time": t.entry_time.isoformat() if t.entry_time else None,
                "exit_time": t.exit_time.isoformat() if t.exit_time else None
            }
            for t in trades
        ]

async def get_portfolio_summary() -> dict:
    async with AsyncSessionLocal() as session:
        open_trades = await get_open_trades()
        closed_result = await session.execute(
            select(Trade).where(Trade.status == "closed")
        )
        closed_trades = closed_result.scalars().all()

        total_profit = sum(t.profit or 0 for t in closed_trades)
        profits_list = [t.profit for t in closed_trades if t.profit is not None]
        wins = [p for p in profits_list if p > 0]
        losses = [p for p in profits_list if p <= 0]

        return {
            "open_positions": len(open_trades),
            "closed_positions": len(closed_trades),
            "total_profit": round(total_profit, 2),
            "win_rate": round(len(wins) / len(profits_list) * 100, 2) if profits_list else 0,
            "total_wins": len(wins),
            "total_losses": len(losses),
            "avg_win": round(np.mean(wins), 2) if wins else 0,
            "avg_loss": round(np.mean(losses), 2) if losses else 0,
            "profit_factor": calculate_profit_factor(
                [{"profit": p} for p in profits_list]
            ),
            "open_trades": open_trades
        }
