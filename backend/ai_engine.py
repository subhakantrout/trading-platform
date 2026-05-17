import json
import os
import re
import asyncio
import logging
from datetime import datetime, timezone

import httpx
import numpy as np
from sqlalchemy import select

from config import OLLAMA_BASE_URL, DEFAULT_OLLAMA_MODEL
from database import AsyncSessionLocal, Analysis, AgentPerformance

logger = logging.getLogger("quant.ai")

_ollama_url = OLLAMA_BASE_URL.rstrip("/")

async def fetch_ollama_models():
    """Fetch available models from local Ollama instance, filtering cloud-only models."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{_ollama_url}/api/tags")
            if resp.status_code == 200:
                data = resp.json()
                all_models = [m["name"] for m in data.get("models", [])]
                local_models = [m for m in all_models if ":cloud" not in m]
                return local_models if local_models else all_models
    except Exception as e:
        logger.error(f"Failed to fetch Ollama models: {e}")
    return []

async def _get_ai_response(model: str, messages: list, temperature: float = 0.2, max_tokens: int = 2048) -> str:
    """Call Ollama's chat API and return the assistant's response text."""
    payload = {
        "model": model,
        "messages": messages,
        "stream": False,
        "options": {
            "temperature": temperature,
            "num_predict": max_tokens,
        }
    }
    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.post(f"{_ollama_url}/api/chat", json=payload)
        resp.raise_for_status()
        data = resp.json()
        return data.get("message", {}).get("content", "")

try:
    import chromadb
    from chromadb.config import Settings as ChromaSettings
    CHROMA_AVAILABLE = True
except ImportError:
    CHROMA_AVAILABLE = False
    logger.warning("chromadb not available, RAG disabled")

from config import CHROMA_PATH

if CHROMA_AVAILABLE:
    _chroma_client = chromadb.PersistentClient(
        path=CHROMA_PATH,
        settings=ChromaSettings(anonymized_telemetry=False)
    )
    _rag_collection = _chroma_client.get_or_create_collection(
        name="market_analyses",
        metadata={"hnsw:space": "cosine"}
    )
else:
    _chroma_client = None
    _rag_collection = None

###############################################################################
# AGENT ELO SYSTEM
###############################################################################

_agent_elo_cache = {}

def _repair_json(json_str: str) -> str:
    """Attempt to repair common JSON malformations like trailing commas and missing closing braces."""
    json_str = re.sub(r',\s*\}', '}', json_str)
    json_str = re.sub(r',\s*\]', ']', json_str)
    
    open_braces = json_str.count('{')
    close_braces = json_str.count('}')
    open_brackets = json_str.count('[')
    close_brackets = json_str.count(']')
    
    if open_braces > close_braces:
        json_str = json_str.rstrip()
        if json_str.endswith(','):
            json_str = json_str[:-1]
        
        stack = []
        for char in json_str:
            if char == '{':
                stack.append('}')
            elif char == '[':
                stack.append(']')
            elif char == '}':
                if stack and stack[-1] == '}':
                    stack.pop()
            elif char == ']':
                if stack and stack[-1] == ']':
                    stack.pop()
        
        json_str += "".join(reversed(stack))
        
    return json_str

def _validate_signal(final: dict, bid: float, ask: float) -> dict:
    """Sanitize and validate trade signals to prevent hallucinations."""
    bias = final.get("final_bias", "NEUTRAL")
    entry_zone = final.get("entry_zone", {"low": bid, "high": ask})
    
    if not isinstance(entry_zone, dict):
        entry_zone = {"low": bid, "high": ask}
    else:
        try:
            entry_zone["low"] = float(entry_zone.get("low", bid))
            entry_zone["high"] = float(entry_zone.get("high", ask))
        except (ValueError, TypeError):
            entry_zone = {"low": bid, "high": ask}
            
    final["entry_zone"] = entry_zone

    try:
        sl = float(final.get("stop_loss", 0))
    except (ValueError, TypeError):
        sl = 0.0
        
    try:
        tp = float(final.get("take_profit", 0))
    except (ValueError, TypeError):
        tp = 0.0

    final["stop_loss"] = sl
    final["take_profit"] = tp

    entry_mid = (entry_zone["low"] + entry_zone["high"]) / 2

    if "BUY" in bias:
        if sl >= entry_mid:
            final["stop_loss"] = entry_mid * 0.99
            final["rationale"] = final.get("rationale", "") + " [SL Corrected]"
    elif "SELL" in bias:
        if sl <= entry_mid:
            final["stop_loss"] = entry_mid * 1.01
            final["rationale"] = final.get("rationale", "") + " [SL Corrected]"
    
    return final

def _extract_json(text: str) -> dict:
    """Robustly extract JSON from text even if surrounded by conversational noise."""
    try:
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1:
            json_str = text[start:end+1]
            try:
                return json.loads(json_str)
            except Exception:
                repaired = _repair_json(json_str)
                return json.loads(repaired)
        return json.loads(text)
    except Exception:
        try:
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group())
                except Exception:
                    repaired = _repair_json(match.group())
                    return json.loads(repaired)
        except Exception:
            pass
    return None

async def _get_elo(session, agent_name: str) -> float:
    if agent_name in _agent_elo_cache:
        return _agent_elo_cache[agent_name]
    result = await session.execute(
        select(AgentPerformance).where(AgentPerformance.agent_name == agent_name)
    )
    ap = result.scalar_one_or_none()
    elo = ap.elo_rating if ap else 1200.0
    _agent_elo_cache[agent_name] = elo
    return elo

async def _update_elo_batch(session, agent_name: str, was_correct: bool, opponent_elo: float = 1200.0):
    result = await session.execute(
        select(AgentPerformance).where(AgentPerformance.agent_name == agent_name)
    )
    ap = result.scalar_one_or_none()
    if not ap:
        ap = AgentPerformance(agent_name=agent_name, elo_rating=1200.0)
        session.add(ap)

    ap.total_predictions += 1
    if was_correct:
        ap.correct_predictions += 1
    ap.accuracy = ap.correct_predictions / max(ap.total_predictions, 1)

    cur = ap.elo_rating
    expected = 1.0 / (1.0 + 10.0 ** ((opponent_elo - cur) / 400.0))
    K = 32
    ap.elo_rating = cur + K * ((1.0 if was_correct else 0.0) - expected)
    _agent_elo_cache[agent_name] = ap.elo_rating

###############################################################################
# CHART PATTERN RECOGNITION
###############################################################################

def detect_patterns(candles: list, indicators: dict = None) -> list:
    if not candles or len(candles) < 30:
        return []
    indicators = indicators or {}
    patterns = []
    closes = np.array([c.get("close", 0) for c in candles], dtype=float)
    highs = np.array([c.get("high", 0) for c in candles], dtype=float)
    lows = np.array([c.get("low", 0) for c in candles], dtype=float)
    opens = np.array([c.get("open", 0) for c in candles], dtype=float)
    volumes = np.array([c.get("volume", 0) for c in candles], dtype=float)

    atr = indicators.get("atr_14", (highs.max() - lows.min()) / 50)
    volatility_pct = (atr / closes[-1]) if closes[-1] > 0 else 0.01
    pattern_threshold = max(0.001, min(0.005, volatility_pct * 0.5))

    lookback = min(60, len(candles))
    recent_highs = highs[-lookback:]
    peak1_idx = np.argmax(recent_highs[:lookback//2])
    peak2_idx = np.argmax(recent_highs[lookback//2:]) + lookback//2
    peak1, peak2 = recent_highs[peak1_idx], recent_highs[peak2_idx]
    
    if abs(peak1 - peak2) / max(peak1, 0.0001) < pattern_threshold:
        patterns.append({
            "pattern": "DOUBLE_TOP", "price": float(peak2),
            "confidence": 85,
            "description": f"Volatility-confirmed Double Top at {peak2:.5f}."
        })

    recent_lows = lows[-lookback:]
    bottom1_idx = np.argmin(recent_lows[:lookback//2])
    bottom2_idx = np.argmin(recent_lows[lookback//2:]) + lookback//2
    bottom1, bottom2 = recent_lows[bottom1_idx], recent_lows[bottom2_idx]
    
    if abs(bottom1 - bottom2) / max(bottom1, 0.0001) < pattern_threshold:
        patterns.append({
            "pattern": "DOUBLE_BOTTOM", "price": float(bottom2),
            "confidence": 85,
            "description": f"Volatility-confirmed Double Bottom at {bottom2:.5f}."
        })

    if len(candles) >= 2:
        c1, c2 = candles[-2], candles[-1]
        if (c2["open"] < c1["close"] and c2["close"] > c1["open"] and
            c2["close"] > c2["open"] and c1["close"] < c1["open"]):
            patterns.append({
                "pattern": "BULLISH_ENGULFING", "price": c2["close"],
                "confidence": 75,
                "description": "Bullish engulfing on last 2 candles"
            })
        if (c2["open"] > c1["close"] and c2["close"] < c1["open"] and
            c2["close"] < c2["open"] and c1["close"] > c1["open"]):
            patterns.append({
                "pattern": "BEARISH_ENGULFING", "price": c2["close"],
                "confidence": 75,
                "description": "Bearish engulfing on last 2 candles"
            })

    if len(candles) >= 20:
        rsi_vals = []
        for i in range(14, len(closes)):
            gains = losses = 0.0
            for j in range(i - 13, i + 1):
                diff = closes[j] - closes[j - 1]
                if diff > 0: gains += diff
                else: losses -= diff
            avg_gain = gains / 14
            avg_loss = losses / 14
            if avg_loss == 0: rsi_vals.append(100.0)
            else: rsi_vals.append(100.0 - 100.0 / (1.0 + avg_gain / avg_loss))
        if len(rsi_vals) >= 10:
            recent_rsi = rsi_vals[-10:]
            recent_prices = closes[-10:]
            if (recent_prices[-1] < recent_prices[0] and recent_rsi[-1] > recent_rsi[0]):
                patterns.append({
                    "pattern": "BULLISH_DIVERGENCE", "price": float(closes[-1]),
                    "confidence": 65,
                    "description": "Bullish RSI divergence detected"
                })
            if (recent_prices[-1] > recent_prices[0] and recent_rsi[-1] < recent_rsi[0]):
                patterns.append({
                    "pattern": "BEARISH_DIVERGENCE", "price": float(closes[-1]),
                    "confidence": 65,
                    "description": "Bearish RSI divergence detected"
                })

    return patterns

###############################################################################
# RAG PIPELINE
###############################################################################

async def _store_rag_entry(stream_id: str, analysis_type: str, bias: str,
                           confidence: float, analysis_text: str, metadata: dict = None):
    if not CHROMA_AVAILABLE or _rag_collection is None:
        return
    try:
        def add_entry():
            _rag_collection.add(
                documents=[analysis_text[:2000]],
                metadatas=[{
                    "stream_id": stream_id,
                    "analysis_type": analysis_type,
                    "bias": bias,
                    "confidence": confidence,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    **(metadata or {})
                }],
                ids=[f"{stream_id}_{analysis_type}_{datetime.now(timezone.utc).timestamp()}"]
            )
        await asyncio.to_thread(add_entry)
    except Exception as e:
        logger.warning(f"RAG store failed: {e}")

async def _query_rag_context(stream_id: str, market_summary: dict = None, top_k: int = 3) -> str:
    if not CHROMA_AVAILABLE or _rag_collection is None:
        return ""
    try:
        query_text = stream_id
        if market_summary and "candles_summary" in market_summary:
            s = market_summary["candles_summary"]
            query_text += f" volatility {s.get('recent_volatility', 0)} change {s.get('price_change_pct', 0)}"
            
        def run_query():
            return _rag_collection.query(
                query_texts=[query_text],
                n_results=top_k
            )
        results = await asyncio.to_thread(run_query)
        docs = results.get("documents") if results else None
        metas = results.get("metadatas") if results else None
        if docs and metas and docs[0] and metas[0] and len(docs[0]) == len(metas[0]):
            ctx = []
            for doc, meta in zip(docs[0], metas[0]):
                ctx.append(f"[{meta.get('bias','?')} {meta.get('confidence',0)}%] {doc[:300]}")
            return "\n---\n".join(ctx)
    except Exception as e:
        logger.warning(f"RAG query failed: {e}")
    return ""

###############################################################################
# AGENT PROMPTS
###############################################################################

AGENT_ROLES = {
    "technical": """You are a SENIOR TECHNICAL ANALYST at a top hedge fund.
Analyze the provided market data and produce a structured JSON assessment.

INSTRUCTIONS:
- Evaluate technical indicators (RSI, MACD, ATR, Bollinger Bands, SMA)
- Identify trend direction, momentum, volatility regime
- Detect support/resistance levels
- Incorporate detected chart patterns
- Provide a clear bias (BULLISH/BEARISH/NEUTRAL) with confidence 0-100
- Output ONLY valid JSON with keys: bias, confidence, reasoning, key_levels (object with support, resistance, pivot), pattern_notes""",

    "sentiment": """You are a SENTIMENT ANALYST at a quant fund.
Analyze the market data for sentiment clues and provide a structured JSON assessment.

INSTRUCTIONS:
- Evaluate price action sentiment (buying/selling pressure)
- Assess volatility regime and volume characteristics
- Look for exhaustion or momentum signals in the data
- Provide a clear bias (BULLISH/BEARISH/NEUTRAL) with confidence 0-100
- Output ONLY valid JSON with keys: bias, confidence, reasoning, sentiment_score (0-100, bearish to bullish), volume_insight""",

    "risk": """You are a RISK MANAGER at an institutional trading desk.
Evaluate the risk profile of trading this instrument and provide structured JSON.

INSTRUCTIONS:
- Analyze ATR for volatility-based risk
- Evaluate spread for liquidity risk
- Assess current market structure for stop-loss placement
- Determine appropriate risk level
- Provide a risk assessment: bias (LOW/MEDIUM/HIGH risk), confidence 0-100
- Output ONLY valid JSON with keys: bias, confidence, reasoning, suggested_stop_pips, max_risk_percent, volatility_regime""",

    "macro": """You are a MACRO STRATEGIST analyzing cross-asset context.
Assess the broader market implications from the data and provide structured JSON.

INSTRUCTIONS:
- Consider the instrument's behavior in context
- Identify potential regime shifts or trend changes
- Evaluate if this aligns with or diverges from broader market narrative
- Provide a clear bias (BULLISH/BEARISH/NEUTRAL) with confidence 0-100
- Output ONLY valid JSON with keys: bias, confidence, reasoning, macro_context, regime_assessment"""
}

###############################################################################
# MULTI-AGENT DEBATE ENGINE
###############################################################################

async def _run_agent(agent_name: str, prompt_template: str,
                     market_summary: dict, model: str) -> dict:
    context = json.dumps(market_summary, indent=2, default=str)
    full_prompt = f"{prompt_template}\n\n## MARKET DATA:\n{context}\n\n## YOUR ANALYSIS (JSON ONLY):"
    
    try:
        text = await _get_ai_response(
            model=model,
            messages=[{"role": "user", "content": full_prompt}],
            temperature=0.2,
            max_tokens=1024
        )
        result = _extract_json(text)
        if result is None:
            raise ValueError("No valid JSON found in response")
        result["agent"] = agent_name
        return result
    except Exception as e:
        logger.error(f"Agent {agent_name} failed: {e}")
        return {"agent": agent_name, "bias": "NEUTRAL", "confidence": 0,
                "reasoning": f"Agent error: {str(e)}"}

ARBITER_PROMPT = """You are the CHIEF INVESTMENT OFFICER synthesizing a panel of expert agents into a final trading decision.

## AGENT REPORTS:
{agent_reports}

## INSTRUCTIONS:
Synthesize the agent reports into a final actionable trading decision.
- Weight each agent's opinion by their confidence and historical Elo rating
- Resolve conflicts between agents with reasoned judgment
- If a pattern was detected, factor it heavily
- Provide a clear final bias with overall confidence

Output ONLY valid JSON with keys:
- final_bias: "STRONG_BUY" / "BUY" / "NEUTRAL" / "SELL" / "STRONG_SELL"
- overall_confidence: 0-100
- entry_zone: {"low": price, "high": price}
- stop_loss: price
- take_profit: price
- rationale: concise synthesis of the decision
- risk_assessment: "LOW" / "MEDIUM" / "HIGH"
- key_levels: {"support": price, "resistance": price, "pivot": price}
- agent_consensus: summary of agreement/disagreement
- position_sizing_note: recommended position size as fraction of capital

## CRITICAL SANITY RULES:
1. For STRONG_BUY/BUY: Stop Loss MUST be BELOW Entry Zone.
2. For STRONG_SELL/SELL: Stop Loss MUST be ABOVE Entry Zone.
3. If entry zone is not specified, use current bid/ask.
4. Prices must be realistic based on recent price action provided."""

async def run_multi_agent_analysis(stream_id: str, candles: list,
                                   indicators: dict, bid: float, ask: float,
                                   spread: float, patterns: list = None,
                                   model: str = DEFAULT_OLLAMA_MODEL) -> dict:
    model = model or DEFAULT_OLLAMA_MODEL
    market_summary = {
        "stream_id": stream_id,
        "bid": bid, "ask": ask, "spread": spread,
        "indicators": indicators,
        "recent_price_action": [
            {"o": c.get("open", 0), "h": c.get("high", 0), "l": c.get("low", 0), "c": c.get("close", 0)} 
            for c in candles[-20:]
        ],
        "candles_summary": {
            "latest_close": candles[-1]["close"] if candles else None,
            "latest_high": candles[-1]["high"] if candles else None,
            "latest_low": candles[-1]["low"] if candles else None,
            "candle_count": len(candles),
            "price_change_pct": ((candles[-1]["close"] - candles[0]["close"]) / candles[0]["close"] * 100)
            if len(candles) > 1 and candles[0].get("close", 0) != 0 else 0,
            "avg_volume": float(np.mean([c.get("volume", 0) for c in candles[-20:]])) if len(candles) >= 20 else 0,
            "recent_volatility": float(np.std([c["close"] for c in candles[-20:]])) if len(candles) >= 20 else 0
        },
        "patterns": patterns or []
    }

    agent_tasks = []
    for agent_name, agent_prompt in AGENT_ROLES.items():
        agent_tasks.append(_run_agent(agent_name, agent_prompt, market_summary, model))

    agent_results = await asyncio.gather(*agent_tasks, return_exceptions=True)
    valid_agents = []
    for r in agent_results:
        if isinstance(r, dict) and "agent" in r:
            valid_agents.append(r)

    async with AsyncSessionLocal() as session:
        for agent_res in valid_agents:
            agent_name = agent_res.get("agent", "unknown")
            conf = agent_res.get("confidence", 50)
            elo = await _get_elo(session, agent_name)
            agent_res["elo"] = elo
            agent_res["weighted_confidence"] = conf * (elo / 1200.0)

    agent_reports = json.dumps(valid_agents, indent=2)
    rag_context = await _query_rag_context(stream_id, market_summary=market_summary)

    arbiter_prompt = ARBITER_PROMPT.replace("{agent_reports}", agent_reports)
    if rag_context:
        arbiter_prompt += f"\n\n## SIMILAR PAST SCENARIOS:\n{rag_context}"

    try:
        text = await _get_ai_response(
            model=model,
            messages=[{"role": "user", "content": arbiter_prompt}],
            temperature=0.1,
            max_tokens=2048
        )
        final = _extract_json(text)
        if final is None:
            raise ValueError("Arbiter returned invalid JSON")
    except Exception as e:
        logger.error(f"Arbiter failed: {e}")
        final = {
            "final_bias": "NEUTRAL", "overall_confidence": 30,
            "rationale": f"Synthesis error: {str(e)}",
            "risk_assessment": "HIGH", "agent_consensus": "Error reaching consensus",
            "entry_zone": {"low": bid, "high": ask},
            "stop_loss": 0, "take_profit": 0,
            "key_levels": {"support": 0, "resistance": 0, "pivot": 0},
            "position_sizing_note": "No trade"
        }

    final["agents"] = valid_agents
    final["patterns"] = patterns or []

    final = _validate_signal(final, bid, ask)

    bias_map = {"STRONG_BUY": "BULLISH", "BUY": "BULLISH", "NEUTRAL": "NEUTRAL",
                "SELL": "BEARISH", "STRONG_SELL": "BEARISH"}
    simple_bias = bias_map.get(final.get("final_bias", "NEUTRAL"), "NEUTRAL")

    async with AsyncSessionLocal() as session:
        analysis_entry = Analysis(
            stream_id=stream_id,
            symbol=stream_id.split("_")[0] if "_" in stream_id else stream_id,
            timeframe=stream_id.split("_")[1] if "_" in stream_id else "N/A",
            analysis_type="multi_agent",
            model=model,
            prompt=arbiter_prompt[:500],
            result=json.dumps(final, default=str),
            bias=simple_bias,
            confidence=final.get("overall_confidence", 50),
            agent_votes_json=json.dumps(valid_agents, default=str)
        )
        session.add(analysis_entry)
        await session.commit()

    await _store_rag_entry(stream_id, "multi_agent", simple_bias,
                           final.get("overall_confidence", 50),
                           json.dumps(final, default=str))

    return final

async def run_single_agent_analysis(stream_id: str, candles: list,
                                     indicators: dict, bid: float, ask: float,
                                     spread: float, patterns: list = None,
                                     model: str = DEFAULT_OLLAMA_MODEL) -> dict:
    model = model or DEFAULT_OLLAMA_MODEL
    patterns = patterns or detect_patterns(candles, indicators)
    market_summary = {
        "stream_id": stream_id,
        "bid": bid, "ask": ask, "spread": spread,
        "indicators": indicators,
        "candles_summary": {
            "latest_close": candles[-1]["close"] if candles else None,
            "candle_count": len(candles),
            "recent_range": float(np.max([c["high"] for c in candles[-20:]]) - np.min([c["low"] for c in candles[-20:]]))
            if len(candles) >= 20 else 0,
        },
        "patterns": patterns
    }

    rag_context = await _query_rag_context(stream_id, market_summary=market_summary)

    prompt = f"""# QUANTITATIVE STRATEGY REPORT: {stream_id}

You are a Senior Quantitative Strategist at a top-tier hedge fund.
Provide a professional, structured report in MARKDOWN.

## CURRENT MARKET STATE:
{json.dumps(market_summary, indent=2, default=str)}

{f"## SIMILAR HISTORICAL CONTEXT:\n{rag_context}\n" if rag_context else ""}

## REPORT STRUCTURE:
1. **EXECUTIVE SUMMARY** - Current bias, confidence, primary catalyst
2. **KEY PRICE LEVELS** - Resistance, Support, Pivot
3. **TECHNICAL CONFLUENCE** - Indicator analysis, pattern significance
4. **STRATEGIC EXECUTION** - Entry zone, Stop loss, Take profit, Bias
5. **RISK ASSESSMENT** - Volatility regime, recommended position sizing

Use pure numerical format for prices. Be specific and actionable."""

    try:
        result_text = await _get_ai_response(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=2048
        )
    except Exception as e:
        result_text = f"Analysis error: {str(e)}"

    bias = "NEUTRAL"
    bias_lower = result_text.lower()
    if any(w in bias_lower for w in ["bullish", "strong_buy", "strong buy"]):
        bias = "BULLISH"
    elif any(w in bias_lower for w in ["bearish", "strong_sell", "strong sell"]):
        bias = "BEARISH"

    confidence = 50
    conf_match = re.search(r'confidence[:\s]*(\d+)', bias_lower)
    if conf_match:
        confidence = min(100, int(conf_match.group(1)))

    async with AsyncSessionLocal() as session:
        analysis_entry = Analysis(
            stream_id=stream_id,
            symbol=stream_id.split("_")[0] if "_" in stream_id else stream_id,
            timeframe=stream_id.split("_")[1] if "_" in stream_id else "N/A",
            analysis_type="single",
            model=model,
            prompt=prompt[:500],
            result=result_text,
            bias=bias,
            confidence=confidence
        )
        session.add(analysis_entry)
        await session.commit()

    await _store_rag_entry(stream_id, "single", bias, confidence, result_text)

    return {
        "analysis": result_text,
        "bias": bias,
        "confidence": confidence,
        "model": model,
        "patterns": patterns or []
    }

async def analyze_mtf(stream_id: str, mtf_data: dict, model: str = None) -> dict:
    model = model or DEFAULT_OLLAMA_MODEL
    target_symbol = stream_id.split("_")[0]

    rag_context = await _query_rag_context(stream_id)

    mtf_json = {}
    for tf, data in mtf_data.items():
        mtf_json[tf] = {
            "bid": data.get("bid"),
            "indicators": data.get("indicators"),
            "candle_count": len(data.get("candles", [])),
            "pattern_notes": detect_patterns(data.get("candles", []), data.get("indicators", {}))
        }

    prompt = f"""# INSTITUTIONAL TOP-DOWN ANALYSIS: {target_symbol}

You are a Hedge Fund Portfolio Manager. Analyze the timeframe cluster for a SYNTHESIZED strategy.

## TIMEFRAME CLUSTER DATA:
{json.dumps(mtf_json, indent=2, default=str)}

{f"## SIMILAR PAST SCENARIOS:\n{rag_context}\n" if rag_context else ""}

## ANALYSIS REQUIREMENTS:
1. **BIAS ALIGNMENT** - Higher vs Lower timeframe comparison
2. **CONFLUENCE ZONES** - Overlapping levels across TFs
3. **SMC/ICT STRUCTURE** - BOS/CHoCH identification
4. **EXECUTION PLAN** - Specific entry, SL, TP with prices

Output in MARKDOWN. Be specific with prices."""

    try:
        result_text = await _get_ai_response(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.25,
            max_tokens=2048
        )
    except Exception as e:
        result_text = f"MTF Analysis error: {str(e)}"

    bias = "NEUTRAL"
    bias_lower = result_text.lower()
    if any(w in bias_lower for w in ["bullish", "strong_buy"]):
        bias = "BULLISH"
    elif any(w in bias_lower for w in ["bearish", "strong_sell"]):
        bias = "BEARISH"

    confidence = 50
    conf_match = re.search(r'confidence[:\s]*(\d+)', bias_lower)
    if conf_match:
        confidence = min(100, int(conf_match.group(1)))

    async with AsyncSessionLocal() as session:
        analysis_entry = Analysis(
            stream_id=stream_id,
            symbol=target_symbol,
            timeframe="MTF",
            analysis_type="mtf",
            model=model,
            prompt=prompt[:500],
            result=result_text,
            bias=bias,
            confidence=confidence
        )
        session.add(analysis_entry)
        await session.commit()

    return {"analysis": result_text, "bias": bias, "confidence": confidence, "model": model}
