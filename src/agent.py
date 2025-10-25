# src/agent.py
import os, re, json
from typing import Any, Dict
from src.agent_tools import (
    recommend_action_missing_sku,
    impacted_orders_by_component,
    reroute_block,
    predict_coverage,
    handle_plant_down,
)

# ---------- IO coercion ----------
def _to_text(x) -> str:
    if isinstance(x, str):
        return x
    if isinstance(x, dict):
        for k in ("text", "message", "query", "input", "prompt"):
            v = x.get(k)
            if isinstance(v, str):
                return v
        return " ".join(str(v) for v in x.values())
    return str(x)

# ---------- optional LLM ----------
USE_LLM = bool(os.getenv("OPENAI_API_KEY"))
LLM = None
if USE_LLM:
    try:
        from langchain_openai import ChatOpenAI
        LLM = ChatOpenAI(model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"), temperature=0.2)
    except Exception:
        USE_LLM = False

SYSTEM_PROMPT = (
    "You are a supply-chain assistant. Detect intent (missing_sku, missing_component, "
    "reroute, coverage, unknown). Extract sku/component and quantity. "
    "Return ONLY JSON: {\"intent\":..., \"sku\":..., \"component\":..., \"quantity\":...}"
)

# ---------- parsers ----------
def _regex_parse(x: Any) -> Dict[str, Any]:
    t = _to_text(x).strip()
    sku = None; comp = None; qty = None
    m = re.search(r"(?:product|sku)_[A-Za-z0-9\-]+", t, re.I)
    if m: sku = m.group(0)
    m = re.search(r"(?:component|cmp)_[A-Za-z0-9\-]+", t, re.I)
    if m: comp = m.group(0)
    m = re.search(r"(\d+)\s*(units|pcs|qty)?", t, re.I)
    if m:
        try: qty = int(m.group(1))
        except: qty = None
    intent = "unknown"
    low = t.lower()
    if any(k in low for k in ["missing", "stockout", "unavailable"]):
        intent = "missing_component" if comp and not sku else "missing_sku"
    elif "reroute" in low: intent = "reroute"
    elif "coverage" in low or "how long" in low: intent = "coverage"
    return {"intent": intent, "sku": sku, "component": comp, "quantity": qty}

def _llm_parse(x: Any) -> Dict[str, Any]:
    if not USE_LLM or LLM is None:
        return _regex_parse(x)
    try:
        text = _to_text(x)
        out = LLM.invoke([
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": text},
        ]).content
        j = re.search(r"\{.*\}", out, re.S)
        return json.loads(j.group(0)) if j else _regex_parse(text)
    except Exception:
        return _regex_parse(x)

def _parse(x: Any) -> Dict[str, Any]:
    return _llm_parse(x)

# ---------- agent ----------
class SmartAgent:
    def _wrap(self, text: str, suggestions=None) -> dict:
        return {"text": text, "suggestions": suggestions or []}

    def invoke(self, payload: Any) -> dict:
        p = _parse(payload)
        intent = p.get("intent", "unknown")
        sku = p.get("sku")
        comp = p.get("component")
        qty = p.get("quantity") or 50

        # detect plant outage phrasing
        msg = _to_text(payload).strip()
        mplant = re.search(r"(?:plant|site|wh)_[A-Za-z0-9\-]+", msg, re.I)
        plant_id = mplant.group(0) if mplant else None
        if plant_id and any(k in msg.lower() for k in ["down","not working","outage","offline","closed"]):
            return handle_plant_down(plant_id)

        if intent == "missing_sku" and sku:
            return self._wrap(
                recommend_action_missing_sku(f"{sku} is missing"),
                [f"Try alternative plant for {sku}", f"Export plan for {sku}"],
            )

        if intent == "missing_component" and comp:
            n = len(impacted_orders_by_component(comp))
            return self._wrap(
                f"Component {comp} shortage detected. {n} impacted order(s) estimated. Provide parent SKU to plan recovery.",
                [f"Plan recovery for parent of {comp}", f"Show orders using {comp}"],
            )

        if intent == "reroute":
            return self._wrap(str(reroute_block(payload)))

        if intent == "coverage":
            return self._wrap(str(predict_coverage(payload)))

        # small talk fallback
        return self._wrap(
            f"I’m online. Example: 'product_556490 is missing' or 'plant_253 is not working'. You said: '{msg}'.",
            ["product_556490 is missing", "plant_253 is not working"],
        )

def build_agent():
    return SmartAgent()
