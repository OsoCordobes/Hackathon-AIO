# src/agent.py
# Deterministic router with human-readable output (local time, clean bullets).

import re, json
from typing import Dict, Any, List
from datetime import datetime
from zoneinfo import ZoneInfo

from langchain_openai import ChatOpenAI  # fallback only
from src.agent_tools import (
    recommend_action_missing_sku,
    impacted_orders_by_sku,
    impacted_orders_by_component,
    simulate_component_stockout,
    reroute_block,
    predict_coverage,
)

# ---------- formatting helpers ----------
LOCAL_TZ = ZoneInfo("Europe/Copenhagen")

def _loads(s: str):
    try: return json.loads(s)
    except Exception: return s

def _to_dt(x):
    if x is None: return None
    if isinstance(x, datetime): return x.astimezone(LOCAL_TZ)
    sx = str(x)
    try:
        # handle pandas/ISO strings
        dt = datetime.fromisoformat(sx.replace("Z", "+00:00"))
        return dt.astimezone(LOCAL_TZ) if dt.tzinfo else dt.replace(tzinfo=ZoneInfo("UTC")).astimezone(LOCAL_TZ)
    except Exception:
        return None

def _fmt_ts(x):
    dt = _to_dt(x)
    return dt.strftime("%d %b %Y, %H:%M %Z") if dt else "n/a"

def _fmt_pct(x):
    try:
        return f"{float(x):.1f}%"
    except Exception:
        return "?"

def _fmt_late(h):
    try:
        h = float(h)
    except Exception:
        return ""
    return "on time" if h <= 1e-6 else f"late by {h:.1f} h"

def _line(r: Dict[str, Any]) -> str:
    oid = r.get("order_id", "?")
    cust = r.get("customer_id", "?")
    dst = r.get("dest_loc_id", "?")
    src = r.get("source_loc_id") or "n/a"
    strat = (r.get("strategy") or "").replace("-", " ")
    eta = _fmt_ts(r.get("ETA_ts"))
    late = _fmt_late(r.get("lateness_h"))
    return f"- Order {oid} → {dst} (customer {cust}). Source {src}. {strat}. ETA {eta}. {late}."

def _fmt_rows(rows: List[Dict[str, Any]], limit: int = 5) -> str:
    if not rows: return "none."
    take = rows[:limit]
    out = "\n".join(_line(r) for r in take)
    more = "" if len(rows) <= limit else f"\n… and {len(rows)-limit} more."
    return out + more

# ---------- intent parsing ----------
SKU_RE = r"(product_\d+)"
PLANT_RE = r"(plant_\d+)"

KW_MISSING = r"(missing|out of stock|delay|late|shortage|falta|agotad|retras|sin stock)"
KW_COMPONENT = r"(component|componente)"
KW_ROUTE = r"(route|ruta)"
KW_BLOCKED = r"(blocked|bloquead|closed|cerrad)"
KW_COVERAGE = r"(coverage|stockout[s]?|predict|horizon|riesgo|alerta)"

def _parse(text: str) -> Dict[str, Any]:
    t = text.strip().lower()
    msku = re.findall(SKU_RE, t)
    mplant = re.findall(PLANT_RE, t)
    out: Dict[str, Any] = {"raw": text}

    if re.search(KW_ROUTE, t) and re.search(KW_BLOCKED, t):
        out["type"] = "route_block"
        out["origin"] = mplant[0] if len(mplant) >= 1 else None
        out["dest"] = mplant[1] if len(mplant) >= 2 else None
        out["sku"] = msku[0] if msku else ""
        return out

    if re.search(KW_COMPONENT, t) and re.search(KW_MISSING, t) and msku:
        out["type"] = "component_missing"
        out["code"] = msku[0]
        return out

    if re.search(KW_COMPONENT, t) and ("stockout" in t or "simulate" in t or "simular" in t or "falt" in t) and msku:
        out["type"] = "component_stockout"
        out["code"] = msku[0]
        return out

    if re.search(KW_MISSING, t) and msku:
        out["type"] = "sku_missing"
        out["sku"] = msku[0]
        return out

    if ("who is affected" in t or "clientes" in t or "affected" in t) and msku:
        out["type"] = "impacted_by_sku"
        out["sku"] = msku[0]
        return out

    if re.search(KW_COVERAGE, t):
        n = re.findall(r"(\d+)\s*(day|día|dias|días)?", t)
        horizon = int(n[0][0]) if n else 7
        out["type"] = "coverage"
        out["horizon"] = max(1, min(60, horizon))
        return out

    out["type"] = "fallback"
    return out

# ---------- handlers ----------
def _handle_sku_missing(sku: str) -> str:
    res = _loads(recommend_action_missing_sku(sku))
    if isinstance(res, dict):
        top = res.get("recommended_action") or "No feasible plan. Inform customers of delay."
        # beautify any ETA inside the top string if present
        top = top.replace("ETA ", "ETA ").replace("+00:00", "")
        kpi = res.get("kpi", {})
        rows = res.get("per_order", [])
        k1 = _fmt_pct(kpi.get("on_time_pct"))
        k2 = _fmt_pct(kpi.get("on_time_orders_pct"))
        # try to rewrite ETA in top with local time if present as ISO
        for token in re.findall(r"\d{4}-\d{2}-\d{2}[^ ]+", top):
            top = top.replace(token, _fmt_ts(token))
        return (
            f"Missing SKU: {sku}\n"
            f"Recommended: {top}\n"
            f"KPI lines on-time: {k1} · orders full-kit on-time: {k2}\n"
            f"{_fmt_rows(rows)}"
        )
    return str(res)

def _handle_impacted_by_sku(sku: str) -> str:
    res = _loads(impacted_orders_by_sku(sku))
    if isinstance(res, list):
        return f"Impacted orders for {sku}: {len(res)}\n{_fmt_rows(res)}"
    return str(res)

def _handle_component_missing(code: str) -> str:
    res = _loads(impacted_orders_by_component(code))
    count = len(res) if isinstance(res, list) else 0
    sim = _loads(simulate_component_stockout(code))
    lines = sim if isinstance(sim, list) else []
    return (
        f"Component missing: {code}\n"
        f"Affected finished-good orders: {count}\n"
        f"{_fmt_rows(lines)}"
    )

def _handle_component_stockout(code: str) -> str:
    res = _loads(simulate_component_stockout(code))
    if isinstance(res, list):
        return f"Stockout simulation for {code}: {len(res)} replanned lines\n{_fmt_rows(res)}"
    return str(res)

def _handle_route_block(origin: str, dest: str, sku: str) -> str:
    if not origin or not dest:
        return "Need origin and destination plants like plant_201 and plant_203."
    res = _loads(reroute_block(origin, dest, sku))
    if isinstance(res, dict):
        rows = res.get("rows", [])
        k = _fmt_pct(res.get("on_time_pct"))
        return f"Route blocked {origin} → {dest}. On-time after reroute: {k}\n{_fmt_rows(rows)}"
    return str(res)

def _handle_coverage(h: int) -> str:
    res = _loads(predict_coverage(h))
    if isinstance(res, list):
        rows = sorted(res, key=lambda r: r.get("risk", 0), reverse=True)[:10]
        lines = [
            f"- {r.get('sku','?')}: demand {r.get('demand_in_window','?')} vs on_hand {r.get('on_hand','?')} → risk {r.get('risk','?')}"
            for r in rows
        ]
        return f"Coverage alerts next {h} days:\n" + "\n".join(lines)
    return str(res)

# ---------- public API ----------
def build_agent(model: str = "gpt-4o-mini"):
    llm = ChatOpenAI(model=model, temperature=0)  # fallback only

    class RouterAgent:
        def invoke(self, payload):
            text = str(payload.get("input", "")).strip()
            intent = _parse(text)

            t = intent["type"]
            if t == "sku_missing":
                return {"output": _handle_sku_missing(intent["sku"])}
            if t == "impacted_by_sku":
                return {"output": _handle_impacted_by_sku(intent["sku"])}
            if t == "component_missing":
                return {"output": _handle_component_missing(intent["code"])}
            if t == "component_stockout":
                return {"output": _handle_component_stockout(intent["code"])}
            if t == "route_block":
                return {"output": _handle_route_block(intent.get("origin"), intent.get("dest"), intent.get("sku",""))}
            if t == "coverage":
                return {"output": _handle_coverage(intent["horizon"])}

            # fallback for chit-chat
            sys = ("You are Jarvis, a supply chain assistant. Keep answers short. "
                   "When no valid SKU/plant intent is present, ask for exact codes.")
            msg = [{"role":"system","content":sys},{"role":"user","content":text}]
            out = llm.invoke(msg)
            return {"output": out.content}

    return RouterAgent()
