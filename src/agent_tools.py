# src/agent_tools.py
import re
import pandas as pd
from src.planner import plan_recovery as plan_for_delay
from src.load_data import (
    load_inventory,
    load_orders,
    load_plants,
    load_plant_material,
)

# ---------- helpers ----------
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

def _extract_sku(text_like) -> str | None:
    t = _to_text(text_like).strip()
    m = re.search(r"(?:product|sku)_[A-Za-z0-9\-]+", t, flags=re.I)
    return m.group(0) if m else None

def _fmt_plan(sku: str, plans: pd.DataFrame, kpi: dict) -> str:
    if plans.empty:
        return f"No affected orders found for {sku}. All deliveries on track."
    lines = [
        f"📦 Recovery plan for {sku}",
        f"- Affected orders: {kpi.get('affected_orders',0)}",
        f"- Stock available: {kpi.get('available_stock',0)}",
        f"- Allocated now: {kpi.get('recovered',0)}",
        f"- Still missing: {kpi.get('missing',0)}",
        "",
    ]
    for _, r in plans.iterrows():
        lines.append(
            f"• Order {r.get('order_id')} → Customer {r.get('customer_id')} | "
            f"Ship {int(r.get('ship_qty',0))} from {r.get('from')} "
            f"({r.get('method')}, {r.get('transit_days')}d)"
        )
    return "\n".join(lines)

# ---------- main actions ----------
def recommend_action_missing_sku(input_text) -> str:
    sku = _extract_sku(input_text)
    if not sku:
        return "Tell me the product code. Example: 'product_556490 is missing'."

    inv = load_inventory("data/inventory.csv")
    plants = load_plants("data/plants.csv")
    orders = load_orders("data/orders.csv")
    plant_material = load_plant_material("data/plant_material.csv")

 # pick real source plant from inventory for this SKU
    stock_rows = inv.copy()
    stock_rows.columns = [c.lower() for c in stock_rows.columns]
    sku_col = next(c for c in ("sku","product","material","product_id") if c in stock_rows.columns)
    plant_col = next(c for c in ("plant_id","location_id","warehouse_id","site","plant") if c in stock_rows.columns)
    qty_col = next(c for c in ("stock","available_qty","qty","quantity","on_hand","balance","current_level") if c in stock_rows.columns)
    
    rows = stock_rows[stock_rows[sku_col].astype(str) == str(sku)]
    origin = rows.sort_values(qty_col, ascending=False)[plant_col].iloc[0] if not rows.empty else "unknown"
    
    delay_event = {"shipment_id": f"S_{sku}", "sku": sku, "qty_unavailable": 50, "origin": origin}


def impacted_orders_by_sku(sku: str):
    orders = load_orders("data/orders.csv")
    now = pd.Timestamp.now(tz="UTC")
    df = orders.copy()
    # tolerant selection of columns
    sku_col = next((c for c in df.columns if c.lower() in ("sku","product","material","product_id")), None)
    time_col = next((c for c in df.columns if c.lower() in ("need_by_ts_utc","need_by_ts","need_by","due_date","need_date")), None)
    if sku_col is None:
        return pd.DataFrame()
    if time_col:
        df[time_col] = pd.to_datetime(df[time_col], errors="coerce", utc=True)
        return df[(df[sku_col].astype(str)==str(sku)) & (df[time_col] >= now)]
    return df[df[sku_col].astype(str)==str(sku)]

def impacted_orders_by_component(component_id: str):
    # minimal alias for now
    return impacted_orders_by_sku(component_id)

def simulate_component_stockout(component_id: str):
    return impacted_orders_by_component(component_id)

def reroute_block(*args, **kwargs):
    return "Reroute planner not implemented yet. Provide order_id and target plant to simulate."

def predict_coverage(*args, **kwargs):
    return "Coverage model not implemented yet. Provide SKU and daily demand to estimate days of cover."

# ---------- plant outage ----------
def handle_plant_down(plant_id: str) -> dict:
    """
    Minimal report for a plant outage. Counts orders shipping from that plant
    and proposes next actions.
    """
    orders = load_orders("data/orders.csv")
    df = orders.copy()
    # find a plant column
    lower_map = {c.lower(): c for c in df.columns}
    plant_col = next((lower_map[c] for c in ("plant_id","delivery_plant","dest_loc_id","plant") if c in lower_map), None)
    if not plant_col:
        return {"text": f"Plant {plant_id}: no plant column in orders.csv.", "suggestions": []}

    impacted = df[df[plant_col].astype(str).str.lower() == str(plant_id).lower()]
    n = len(impacted)

    suggestions = [
        f"Show impacted orders at {plant_id}",
        f"Reroute orders from {plant_id} to nearest plant",
        "Estimate coverage for top SKUs at this plant"
    ]
    return {"text": f"Plant {plant_id} outage detected. {n} order(s) ship from this plant.", "suggestions": suggestions}
