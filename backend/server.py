import json
import os
import re
import asyncio
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, Request, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import select, func, delete
from sqlalchemy.ext.asyncio import AsyncSession

from config import OLLAMA_BASE_URL, DEFAULT_OLLAMA_MODEL
from database import (
    init_db, get_db, AsyncSessionLocal, init_sync_db,
    MarketSnapshot, Candle, Analysis, SystemEvent,
    MTFData, CorrelationData, EconomicCalendar
)
from ai_engine import (
    run_multi_agent_analysis, run_single_agent_analysis,
    analyze_mtf, detect_patterns, fetch_ollama_models
)
from trading_engine import (
    calculate_position_size, calculate_var,
    run_backtest, record_trade, close_trade,
    get_open_trades, get_trade_history, get_portfolio_summary
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("quant.server")

###############################################################################
# STATE
###############################################################################

market_states = {}
visual_objects = {}
connected_clients = set()
selected_model = DEFAULT_OLLAMA_MODEL
active_analyses = {}
trade_signals = {} 
last_auto_analysis = {} 
last_mtf_analysis = {}
is_auto_trade_enabled = True
ai_semaphore = None

FRONTEND_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend", "index.html")

def extract_and_queue_drawings(symbol, text):
    """Parses text for commands like [DRAW_HLINE: 65000, 'Resistance'] or [DRAW_ZONE: 64000, 64500, 'Demand']"""
    global visual_objects
    if symbol not in visual_objects: visual_objects[symbol] = []
    
    hlines = re.findall(r"\[DRAW_HLINE:\s*([\d\.]+)(?:,\s*['\"]([^'\"]+)['\"])?\]", text)
    for price, label in hlines:
        try:
            price_val = float(price)
        except ValueError:
            continue
        visual_objects[symbol].append({
            "type": "HLINE", 
            "price": price_val, 
            "label": label or "AI Level",
            "color": "clrAqua"
        })
        
    zones = re.findall(r"\[DRAW_ZONE:\s*([\d\.]+),\s*([\d\.]+)(?:,\s*['\"]([^'\"]+)['\"])?\]", text)
    for p1, p2, label in zones:
        try:
            p1_val, p2_val = float(p1), float(p2)
        except ValueError:
            continue
        visual_objects[symbol].append({
            "type": "RECT", 
            "price1": p1_val, 
            "price2": p2_val,
            "label": label or "AI Zone",
            "color": "clrDodgerBlue"
        })
    
    if len(visual_objects[symbol]) > 0:
        logger.info(f"Queued {len(visual_objects[symbol])} visual objects for {symbol}")

###############################################################################
# APP
###############################################################################

@asynccontextmanager
async def lifespan(app: FastAPI):
    global ai_semaphore, selected_model
    ai_semaphore = asyncio.Semaphore(1)
    logger.info("Initializing database...")
    await init_db()
    init_sync_db()
    try:
        available = await fetch_ollama_models()
        if available:
            PREFERRED = ["llama3:8b", "llama3", "deepseek-r1:7b", "gemma4:latest", "qwen3.5:latest"]
            picked = next((m for m in PREFERRED if m in available), available[0])
            selected_model = picked
            logger.info(f"Ollama connected. Auto-selected model: {selected_model}")
        else:
            logger.warning("No Ollama models found. Using default: %s", selected_model)
    except Exception as e:
        logger.warning(f"Failed to connect to Ollama: {e}")
    logger.info("Quantum Terminal initialized.")
    yield
    logger.info("Shutdown complete.")

app = FastAPI(title="AI Quant Terminal", version="4.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

###############################################################################
# HELPERS
###############################################################################

async def broadcast(payload: dict):
    """Send a JSON payload to all connected WebSocket clients safely."""
    try:
        message = json.dumps(payload, default=str)
    except Exception as e:
        logger.error(f"Broadcast serialization failed: {e}")
        return
    dead = set()
    for client in list(connected_clients):
        try:
            await client.send_text(message)
        except Exception:
            dead.add(client)
    connected_clients.difference_update(dead)

def _safe_background_task(coro):
    """Schedule a coroutine as a background task with error logging."""
    task = asyncio.create_task(coro)
    def _done(t):
        if t.cancelled():
            return
        exc = t.exception()
        if exc:
            logger.error(f"Background task failed: {exc}")
    task.add_done_callback(_done)
    return task

###############################################################################
# MODELS
###############################################################################

class ModelSelect(BaseModel):
    model: str

class AnalyzeRequest(BaseModel):
    stream_id: str
    use_multi_agent: bool = True

class TradeRequest(BaseModel):
    stream_id: str
    symbol: str | None = None
    direction: str
    entry_price: float
    stop_loss: float = 0.0
    take_profit: float = 0.0
    quantity: float
    strategy: str = "manual"
    risk_percent: float = 0.02
    notes: str | None = None

class CloseTradeRequest(BaseModel):
    trade_id: int
    exit_price: float
    notes: str | None = None

class BacktestRequest(BaseModel):
    symbol: str
    timeframe: str
    strategy_name: str = "ai_multi_agent"
    initial_capital: float = 10000.0
    risk_per_trade: float = 0.02

class LogEvent(BaseModel):
    event_type: str
    message: str
    details: dict | None = None

###############################################################################
# CORE ROUTES
###############################################################################

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    if not os.path.exists(FRONTEND_PATH):
        return "<h1>Frontend not found!</h1>"
    with open(FRONTEND_PATH, "r", encoding="utf-8") as f:
        return f.read()

@app.post("/mt5/update")
async def mt5_update(request: Request):
    global market_states
    try:
        data = await request.json()
        symbol = data.get("symbol", "UNKNOWN")
        timeframe = data.get("timeframe", "UNKNOWN")
        stream_id = f"{symbol}_{timeframe}"
        data["stream_id"] = stream_id
        data["timestamp"] = datetime.now(timezone.utc).timestamp()
        market_states[stream_id] = data

        is_extended = data.get("extended", False)

        async with AsyncSessionLocal() as session:
            snapshot = MarketSnapshot(
                symbol=symbol, timeframe=timeframe, stream_id=stream_id,
                bid=data.get("bid"), ask=data.get("ask"), spread=data.get("spread"),
                last=data.get("last"), change=data.get("change"),
                indicators_json=json.dumps(data.get("indicators", {}), default=str),
                account_json=json.dumps(data.get("account", {}), default=str),
                structure_json=json.dumps(data.get("structure", {}), default=str) if is_extended else None,
                volume_profile_json=json.dumps(data.get("volume_profile", {}), default=str) if is_extended else None,
                patterns_json=json.dumps(data.get("patterns", []), default=str) if is_extended else None,
                advanced_metrics_json=json.dumps(data.get("advanced_metrics", {}), default=str) if is_extended else None,
                session_json=json.dumps(data.get("session", {}), default=str) if is_extended else None,
                symbol_info_json=json.dumps(data.get("symbol_info", {}), default=str) if is_extended else None,
            )
            session.add(snapshot)

            for c in data.get("recent_candles", []):
                if not all(k in c for k in ("time", "open", "high", "low", "close")):
                    logger.warning(f"Skipping malformed candle: {c}")
                    continue
                stmt = select(Candle).where(
                    Candle.stream_id == stream_id, Candle.time == int(c["time"])
                )
                existing = (await session.execute(stmt)).scalar_one_or_none()
                if not existing:
                    candle = Candle(
                        stream_id=stream_id, symbol=symbol, timeframe=timeframe,
                        time=int(c["time"]),
                        open=float(c["open"]),
                        high=float(c["high"]),
                        low=float(c["low"]),
                        close=float(c["close"]),
                        volume=float(c.get("volume", 0))
                    )
                    session.add(candle)

            if is_extended:
                mtf_entry = MTFData(
                    symbol=symbol, stream_id=stream_id,
                    mtf_json=json.dumps(data.get("mtf", {}), default=str)
                )
                session.add(mtf_entry)

                corr_entry = CorrelationData(
                    symbol=symbol,
                    correlation_json=json.dumps(data.get("correlation", {}), default=str)
                )
                session.add(corr_entry)

                cal_events = data.get("calendar", [])
                if cal_events:
                    cal_entry = EconomicCalendar(
                        symbol=symbol,
                        events_json=json.dumps(cal_events, default=str)
                    )
                    session.add(cal_entry)

            count_q = select(func.count(Candle.id)).where(Candle.stream_id == stream_id)
            total_candles = (await session.execute(count_q)).scalar()
            if total_candles and total_candles > 5000:
                subq = select(Candle.id).where(Candle.stream_id == stream_id).order_by(Candle.time).limit(total_candles - 4000)
                ids_to_delete = (await session.execute(subq)).scalars().all()
                if ids_to_delete:
                    await session.execute(
                        delete(Candle).where(Candle.id.in_(ids_to_delete))
                    )
            await session.commit()

        if is_extended:
            await broadcast({
                "type": "extended_data",
                "stream_id": stream_id,
                "mtf": data.get("mtf", {}),
                "correlation": data.get("correlation", {}),
                "calendar": data.get("calendar", []),
                "structure": data.get("structure", {}),
                "volume_profile": data.get("volume_profile", {}),
                "patterns": data.get("patterns", []),
                "advanced_metrics": data.get("advanced_metrics", {}),
                "session": data.get("session", {}),
                "symbol_info": data.get("symbol_info", {}),
                "pending_orders": data.get("pending_orders", []),
                "recent_trades": data.get("recent_trades", [])
            })
        else:
            await broadcast({"type": "market_data", "stream_id": stream_id, "data": data})

        last_ana = last_auto_analysis.get(stream_id, 0)
        current_time = datetime.now(timezone.utc).timestamp()
        
        if current_time - last_ana > 300 and ai_semaphore is not None:
            last_auto_analysis[stream_id] = current_time
            _safe_background_task(analyze_market(AnalyzeRequest(stream_id=stream_id, use_multi_agent=True)))
            logger.info(f"Auto-analysis triggered for {stream_id}")

        now_dt = datetime.now(timezone.utc)
        if symbol not in last_mtf_analysis or (now_dt - last_mtf_analysis[symbol]).total_seconds() > 900:
            last_mtf_analysis[symbol] = now_dt
            available_tfs = {}
            for sid in market_states:
                if sid.startswith(symbol):
                    tf = sid.split('_')[1] if '_' in sid else "N/A"
                    available_tfs[tf] = market_states[sid]
            if len(available_tfs) >= 2:
                logger.info(f"Triggering MTF Strategic Overlook for {symbol}")
                _safe_background_task(analyze_mtf(f"{symbol}_MTF", available_tfs, model=selected_model))

        return {"status": "ok", "stream_id": stream_id}
    except Exception as e:
        logger.error(f"mt5_update error: {e}")
        return {"status": "error", "message": str(e)}

###############################################################################
# AI ROUTES
###############################################################################

@app.get("/api/models")
async def get_models():
    models = await fetch_ollama_models()
    if not models:
        models = [DEFAULT_OLLAMA_MODEL]
    return {"models": models, "selected": selected_model}

@app.post("/api/select_model")
async def select_model(payload: ModelSelect):
    global selected_model
    if ":cloud" in payload.model:
        raise HTTPException(400, "Cloud-only models cannot be used. Select a local model.")
    selected_model = payload.model
    return {"status": "success", "selected_model": selected_model}

@app.post("/api/analyze")
async def analyze_market(payload: AnalyzeRequest):
    if ai_semaphore is None:
        raise HTTPException(503, "Server still initializing, please try again in a moment.")
    async with ai_semaphore:
        stream_id = payload.stream_id
        target_symbol = stream_id.split('_')[0] if '_' in stream_id else stream_id
        if stream_id not in market_states:
            return {"analysis": f"No data for {stream_id}.", "bias": "NEUTRAL", "confidence": 0}

        data = market_states[stream_id]
        candles = data.get("recent_candles", [])
        indicators = data.get("indicators", {})
        bid = data.get("bid", 0)
        ask = data.get("ask", 0)
        spread = data.get("spread", 0)

        if not candles:
            return {"analysis": "No candle data available.", "bias": "NEUTRAL", "confidence": 0}

        patterns = detect_patterns(candles, indicators)

        if payload.use_multi_agent:
            active_analyses[stream_id] = "RUNNING"
            try:
                result = await run_multi_agent_analysis(
                    stream_id, candles, indicators, bid, ask, spread,
                    patterns=patterns, model=selected_model
                )
                analysis_text = json.dumps(result, indent=2, default=str)
                bias = result.get("final_bias", "NEUTRAL")
                confidence = result.get("overall_confidence", 50)

                payload_for_broadcast = {
                    "type": "analysis_complete",
                    "stream_id": stream_id,
                    "bias": bias,
                    "confidence": confidence,
                    "rationale": result.get("rationale", ""),
                    "entry_zone": result.get("entry_zone", {}),
                    "stop_loss": result.get("stop_loss"),
                    "take_profit": result.get("take_profit"),
                    "key_levels": result.get("key_levels", {}),
                    "risk_assessment": result.get("risk_assessment", "MEDIUM"),
                    "agent_reports": [
                        {"agent": a.get("agent"), "bias": a.get("bias"),
                         "confidence": a.get("confidence"), "elo": round(a.get("elo", 1200), 0),
                         "reasoning": a.get("reasoning", "")}
                        for a in result.get("agents", [])
                    ],
                    "patterns": patterns
                }

                await broadcast(payload_for_broadcast)

                active_analyses[stream_id] = result

                raw_text = result.get("rationale", "")
                extract_and_queue_drawings(target_symbol, raw_text)

                f_bias = result.get("final_bias", "NEUTRAL")
                confidence = result.get("overall_confidence", 0)

                if not is_auto_trade_enabled:
                    logger.info(f"Signal generated for {target_symbol} but Auto-Trade is DISABLED globally.")
                elif f_bias in ["STRONG_BUY", "BUY", "SELL", "STRONG_SELL"] and confidence >= 70:
                    direction = "BUY" if "BUY" in f_bias else "SELL"
                    entry_price = (bid if direction == "SELL" else ask) or data.get("last", 0)
                    if result.get("entry_zone") and result["entry_zone"].get("low") is not None and result["entry_zone"].get("high") is not None:
                        entry_price = (result["entry_zone"]["low"] + result["entry_zone"]["high"]) / 2

                    trade_signals[target_symbol] = {
                        "symbol": target_symbol,
                        "direction": direction,
                        "entry_price": float(entry_price),
                        "stop_loss": float(result.get("stop_loss", 0)),
                        "take_profit": float(result.get("take_profit", 0)),
                        "confidence": confidence,
                        "strategy": "multi_agent_v4"
                    }
                    logger.info(f"QUEUED: {direction} signal for {target_symbol} @ {entry_price} (Conf: {confidence}%)")
                else:
                    logger.info(f"Signal ignored for {target_symbol}: Bias={f_bias}, Confidence={confidence}% (Threshold: 70%)")

                return payload_for_broadcast
            except Exception as e:
                logger.exception(f"Multi-agent analysis failed: {e}")
                active_analyses[stream_id] = f"Error: {str(e)}"
                return {"analysis": f"Multi-agent error: {str(e)}", "bias": "NEUTRAL", "confidence": 0}
        else:
            try:
                result = await run_single_agent_analysis(
                    stream_id, candles, indicators, bid, ask, spread,
                    patterns=patterns, model=selected_model
                )
            except Exception as e:
                logger.exception(f"Single-agent analysis failed: {e}")
                return {"analysis": f"Analysis error: {str(e)}", "bias": "NEUTRAL", "confidence": 0}

            extract_and_queue_drawings(target_symbol, result["analysis"])

            payload_for_broadcast = {
                "type": "analysis_complete",
                "stream_id": stream_id,
                "analysis": result["analysis"],
                "bias": result.get("bias", "NEUTRAL"),
                "confidence": result.get("confidence", 0),
                "patterns": patterns
            }
            await broadcast(payload_for_broadcast)
            return payload_for_broadcast

@app.post("/api/analyze_mtf")
async def multi_timeframe_analysis(payload: AnalyzeRequest):
    if ai_semaphore is None:
        raise HTTPException(503, "Server still initializing, please try again in a moment.")
    async with ai_semaphore:
        stream_id = payload.stream_id
        target_symbol = stream_id.split("_")[0] if "_" in stream_id else stream_id

        mtf_data = {}
        for sid, data in market_states.items():
            if sid.startswith(target_symbol):
                tf = sid.split("_")[1] if "_" in sid else "N/A"
                mtf_data[tf] = {
                    "bid": data.get("bid"),
                    "indicators": data.get("indicators"),
                    "candles": data.get("recent_candles", [])[-50:]
                }

        if not mtf_data:
            return {"analysis": "No multi-timeframe data available.", "bias": "NEUTRAL", "confidence": 0}

        result = await analyze_mtf(stream_id, mtf_data, model=selected_model)

        broadcast_payload = {
            "type": "mtf_analysis_complete",
            "stream_id": stream_id,
            "analysis": result["analysis"],
            "bias": result.get("bias", "NEUTRAL"),
            "confidence": result.get("confidence", 50)
        }
        await broadcast(broadcast_payload)
        return broadcast_payload

###############################################################################
# TRADING ROUTES
###############################################################################

@app.post("/api/trade/open")
async def open_trade(req: TradeRequest):
    try:
        symbol = req.symbol or (req.stream_id.split("_")[0] if "_" in req.stream_id else req.stream_id)
        result = await record_trade(
            stream_id=req.stream_id, symbol=symbol,
            direction=req.direction, entry_price=req.entry_price,
            stop_loss=req.stop_loss, take_profit=req.take_profit or 0,
            quantity=req.quantity, strategy=req.strategy,
            risk_percent=req.risk_percent, notes=req.notes
        )
        evt = SystemEvent(
            event_type="trade_opened", message=f"{req.direction} {symbol}",
            details_json=json.dumps(result, default=str)
        )
        async with AsyncSessionLocal() as session:
            session.add(evt)
            await session.commit()
        return {"status": "success", **result}
    except Exception as e:
        raise HTTPException(400, str(e))

@app.post("/api/trade/close")
async def close_trade_route(req: CloseTradeRequest):
    try:
        result = await close_trade(req.trade_id, req.exit_price, req.notes)
        if "error" in result:
            raise HTTPException(404, result["error"])
        evt = SystemEvent(
            event_type="trade_closed", message=f"Trade {req.trade_id} closed",
            details_json=json.dumps(result, default=str)
        )
        async with AsyncSessionLocal() as session:
            session.add(evt)
            await session.commit()
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, str(e))

@app.get("/api/trades/open")
async def list_open_trades():
    current_prices = {}
    for sid, data in market_states.items():
        sym = data.get("symbol")
        if sym and sym not in current_prices:
            current_prices[sym] = {"bid": data.get("bid", 0), "ask": data.get("ask", 0)}
    return await get_open_trades(current_prices=current_prices)

@app.get("/api/signals/pending")
async def get_pending_signals(symbol: str = Query(...)):
    if symbol in trade_signals:
        signal = trade_signals.pop(symbol)
        return [signal]
    return []

@app.get("/api/trades/history")
async def list_trade_history(limit: int = 50):
    return await get_trade_history(limit)

@app.get("/api/portfolio")
async def portfolio_summary():
    result = await get_portfolio_summary()
    if market_states:
        latest = max(market_states.values(), key=lambda d: d.get('timestamp', 0), default=None)
        if latest and latest.get('account'):
            acct = latest.get('account', {})
            result['total_equity'] = acct.get('equity', 0)
            result['balance'] = acct.get('balance', 0)
            result['free_margin'] = acct.get('free_margin', 0)
            result['floating_profit'] = acct.get('floating_profit', 0)
    return result

@app.get("/api/mt5/drawings")
async def get_mt5_drawings(symbol: str = Query(...)):
    if symbol in visual_objects:
        commands = visual_objects[symbol]
        visual_objects[symbol] = []
        return {"status": "ok", "commands": commands}
    return {"status": "ok", "commands": []}

###############################################################################
# BACKTESTING ROUTES
###############################################################################

@app.post("/api/backtest")
async def backtest(req: BacktestRequest):
    result = await run_backtest(
        symbol=req.symbol, timeframe=req.timeframe,
        strategy_name=req.strategy_name,
        initial_capital=req.initial_capital,
        risk_per_trade=req.risk_per_trade
    )
    evt = SystemEvent(
        event_type="backtest_complete",
        message=f"Backtest {req.symbol} {req.timeframe}",
        details_json=json.dumps(result.get("metrics", {}), default=str)
    )
    async with AsyncSessionLocal() as session:
        session.add(evt)
        await session.commit()
    return result

@app.get("/api/backtest/history")
async def backtest_history(limit: int = 20):
    from database import BacktestRun
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(BacktestRun).order_by(BacktestRun.created_at.desc()).limit(limit)
        )
        runs = result.scalars().all()
        return [
            {
                "id": r.id, "symbol": r.symbol, "timeframe": r.timeframe,
                "strategy": r.strategy_name,
                "total_return": r.total_return, "sharpe": r.sharpe_ratio,
                "max_drawdown": r.max_drawdown, "win_rate": r.win_rate,
                "total_trades": r.total_trades,
                "created_at": r.created_at.isoformat() if r.created_at else None
            }
            for r in runs
        ]

###############################################################################
# RISK ROUTES
###############################################################################

class RiskCalcRequest(BaseModel):
    account_equity: float
    entry_price: float
    stop_loss: float
    atr: float = None
    risk_percent: float = 0.02

@app.post("/api/risk/position_size")
async def calc_position_size(req: RiskCalcRequest):
    return calculate_position_size(
        account_equity=req.account_equity,
        entry_price=req.entry_price,
        stop_loss=req.stop_loss,
        risk_percent=req.risk_percent,
        atr=req.atr
    )

@app.post("/api/risk/var")
async def calc_var(data: dict):
    returns = data.get("returns", [])
    confidence = data.get("confidence", 0.95)
    return calculate_var(returns, confidence)

###############################################################################
# DATA ROUTES
###############################################################################

@app.get("/api/streams")
async def get_streams():
    return {
        stream_id: {
            "symbol": d.get("symbol"),
            "timeframe": d.get("timeframe"),
            "bid": d.get("bid"),
            "ask": d.get("ask"),
            "spread": d.get("spread"),
            "indicators": d.get("indicators", {}),
            "account": d.get("account", {})
        }
        for stream_id, d in market_states.items()
    }

@app.get("/api/streams/{stream_id}/candles")
async def get_stream_candles(stream_id: str, limit: int = 100):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Candle).where(Candle.stream_id == stream_id)
            .order_by(Candle.time.desc()).limit(limit)
        )
        candles = result.scalars().all()
        return [
            {"time": c.time, "open": c.open, "high": c.high,
             "low": c.low, "close": c.close, "volume": c.volume}
            for c in reversed(candles)
        ]

@app.get("/api/mtf")
async def get_mtf_data(symbol: str = Query(...)):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(MTFData).where(MTFData.symbol == symbol)
            .order_by(MTFData.timestamp.desc()).limit(1)
        )
        entry = result.scalar_one_or_none()
        if entry:
            return json.loads(entry.mtf_json)
        return {}

@app.get("/api/correlation")
async def get_correlation_data(symbol: str = Query(...)):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(CorrelationData).where(CorrelationData.symbol == symbol)
            .order_by(CorrelationData.timestamp.desc()).limit(1)
        )
        entry = result.scalar_one_or_none()
        if entry:
            return json.loads(entry.correlation_json)
        return {}

@app.get("/api/calendar")
async def get_calendar_data(symbol: str = Query(...)):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(EconomicCalendar).where(EconomicCalendar.symbol == symbol)
            .order_by(EconomicCalendar.timestamp.desc()).limit(1)
        )
        entry = result.scalar_one_or_none()
        if entry:
            return json.loads(entry.events_json)
        return []

@app.get("/api/structure")
async def get_market_structure(stream_id: str = Query(...)):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(MarketSnapshot).where(MarketSnapshot.stream_id == stream_id)
            .order_by(MarketSnapshot.timestamp.desc()).limit(1)
        )
        entry = result.scalar_one_or_none()
        if entry and entry.structure_json:
            return json.loads(entry.structure_json)
        return {}

@app.get("/api/volume_profile")
async def get_volume_profile(stream_id: str = Query(...)):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(MarketSnapshot).where(MarketSnapshot.stream_id == stream_id)
            .order_by(MarketSnapshot.timestamp.desc()).limit(1)
        )
        entry = result.scalar_one_or_none()
        if entry and entry.volume_profile_json:
            return json.loads(entry.volume_profile_json)
        return {}

@app.get("/api/patterns")
async def get_patterns(stream_id: str = Query(...)):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(MarketSnapshot).where(MarketSnapshot.stream_id == stream_id)
            .order_by(MarketSnapshot.timestamp.desc()).limit(1)
        )
        entry = result.scalar_one_or_none()
        if entry and entry.patterns_json:
            return json.loads(entry.patterns_json)
        return []

@app.get("/api/advanced_metrics")
async def get_advanced_metrics(stream_id: str = Query(...)):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(MarketSnapshot).where(MarketSnapshot.stream_id == stream_id)
            .order_by(MarketSnapshot.timestamp.desc()).limit(1)
        )
        entry = result.scalar_one_or_none()
        if entry and entry.advanced_metrics_json:
            return json.loads(entry.advanced_metrics_json)
        return {}

@app.get("/api/session")
async def get_session_info(stream_id: str = Query(...)):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(MarketSnapshot).where(MarketSnapshot.stream_id == stream_id)
            .order_by(MarketSnapshot.timestamp.desc()).limit(1)
        )
        entry = result.scalar_one_or_none()
        if entry and entry.session_json:
            return json.loads(entry.session_json)
        return {}

@app.get("/api/symbol_info")
async def get_symbol_info(stream_id: str = Query(...)):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(MarketSnapshot).where(MarketSnapshot.stream_id == stream_id)
            .order_by(MarketSnapshot.timestamp.desc()).limit(1)
        )
        entry = result.scalar_one_or_none()
        if entry and entry.symbol_info_json:
            return json.loads(entry.symbol_info_json)
        return {}

@app.get("/api/analyses/history")
async def analysis_history(limit: int = 20, stream_id: str = None):
    async with AsyncSessionLocal() as session:
        query = select(Analysis).order_by(Analysis.created_at.desc())
        if stream_id:
            query = query.where(Analysis.stream_id == stream_id)
        result = await session.execute(query.limit(limit))
        analyses = result.scalars().all()
        return [
            {
                "id": a.id, "stream_id": a.stream_id, "type": a.analysis_type,
                "model": a.model, "bias": a.bias, "confidence": a.confidence,
                "created_at": a.created_at.isoformat() if a.created_at else None,
                "result_preview": (a.result or "")[:300]
            }
            for a in analyses
        ]

@app.get("/api/analyses/{analysis_id}")
async def get_analysis(analysis_id: int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Analysis).where(Analysis.id == analysis_id)
        )
        a = result.scalar_one_or_none()
        if not a:
            raise HTTPException(404, "Analysis not found")
        return {
            "id": a.id, "stream_id": a.stream_id, "type": a.analysis_type,
            "model": a.model, "bias": a.bias, "confidence": a.confidence,
            "result": a.result, "agent_votes": json.loads(a.agent_votes_json) if a.agent_votes_json else None,
            "created_at": a.created_at.isoformat() if a.created_at else None
        }

###############################################################################
# SYSTEM ROUTES
###############################################################################

@app.post("/api/system/log")
async def system_log(evt: LogEvent):
    async with AsyncSessionLocal() as session:
        entry = SystemEvent(
            event_type=evt.event_type,
            message=evt.message,
            details_json=json.dumps(evt.details) if evt.details else None
        )
        session.add(entry)
        await session.commit()
    return {"status": "logged"}

@app.get("/api/system/events")
async def system_events(limit: int = 50, event_type: str = None):
    async with AsyncSessionLocal() as session:
        query = select(SystemEvent).order_by(SystemEvent.created_at.desc())
        if event_type:
            query = query.where(SystemEvent.event_type == event_type)
        result = await session.execute(query.limit(limit))
        return [
            {
                "id": e.id, "type": e.event_type, "message": e.message,
                "details": json.loads(e.details_json) if e.details_json else None,
                "created_at": e.created_at.isoformat() if e.created_at else None
            }
            for e in result.scalars().all()
        ]

@app.get("/api/settings")
async def get_settings():
    return {"is_auto_trade_enabled": is_auto_trade_enabled}

@app.post("/api/settings")
async def update_settings(data: dict):
    global is_auto_trade_enabled
    is_auto_trade_enabled = bool(data.get("is_auto_trade_enabled", is_auto_trade_enabled))
    logger.info(f"Master Auto-Trade set to: {is_auto_trade_enabled}")
    return {"status": "ok", "is_auto_trade_enabled": is_auto_trade_enabled}

@app.get("/api/system/health")
async def system_health():
    ollama_ok = False
    try:
        import httpx
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{OLLAMA_BASE_URL}/api/tags")
            ollama_ok = resp.status_code == 200
    except Exception:
        pass
    return {
        "status": "operational",
        "ollama_connected": ollama_ok,
        "active_streams": len(market_states),
        "connected_clients": len(connected_clients),
        "active_model": selected_model,
        "uptime": datetime.now(timezone.utc).isoformat()
    }

###############################################################################
# WEBSOCKET
###############################################################################

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    logger.info(f"WS client connected. Total: {len(connected_clients)}")

    for stream_id, data in market_states.items():
        try:
            await websocket.send_text(
                json.dumps({"type": "market_data", "stream_id": stream_id, "data": data}, default=str)
            )
        except Exception:
            break

    try:
        while True:
            msg = await websocket.receive_text()
            try:
                cmd = json.loads(msg)
                if cmd.get("action") == "ping":
                    await websocket.send_text(json.dumps({"type": "pong", "time": datetime.now(timezone.utc).isoformat()}))
                elif cmd.get("action") == "get_streams":
                    await websocket.send_text(json.dumps({
                        "type": "streams_list",
                        "streams": list(market_states.keys())
                    }))
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({"type": "error", "message": "Invalid JSON"}))
    except Exception:
        pass
    finally:
        connected_clients.discard(websocket)
        logger.info(f"WS client disconnected. Total: {len(connected_clients)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False)
