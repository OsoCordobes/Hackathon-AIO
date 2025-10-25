import pandas as pd
from typing import Optional

from src.planner import plan_recovery
from src.load_data import (
    load_inventory,
    load_orders,
    load_plants,
    load_plant_material,
)

def _norm(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    x.columns = [str(c).strip().lower() for c in x.columns]
    return x

def _pick(df: pd.DataFrame, names) -> Optional[str]:
    for n in names:
        if n in df.columns:
            return n
    return None

def _to_num(s):
    return pd.to_numeric(s, errors="coerce").fillna(0)

def _origin_for_sku(inv: pd.DataFrame, sku: str) -> str:
    invn = _norm(inv)
    sku_c   = _pick(invn, ["sku","product","material","product_id"])
    plant_c = _pick(invn, ["plant_id","location_id","warehouse_id","site","plant"])
    stock_c = _pick(invn, ["stock","available_qty","qty","quantity","on_hand","balance","current_level"])
    if not (sku_c and stock_c):
        return "unknown"
    rows = invn.loc[invn[sku_c].astype(str) == str(sku)].copy()
    if rows.empty:
        return "unknown"
    rows[stock_c] = _to_num(rows[stock_c])
    rows.sort_values(stock_c, ascending=False, inplace=True)
    return str(rows.iloc[0][plant_c]) if plant_c and plant_c in rows else "unknown"

def _fmt_plan_rows(plans: pd.DataFrame, kpi: dict, sku: str) -> str:
    lines = [
        f"📦 Recovery plan for {sku}",
        f"- Affected orders: {kpi.get('affected_orders',0)}",
        f"- Stock available: {kpi.get('available_stock',0)}",
        f"- Allocated now: {kpi.get('recovered',0)}",
        f"- Still missing: {kpi.get('missing',0)}",
        ""
    ]
    for _, r in plans.iterrows():
        lines.append(
            f"• Order {r.get('order_id')} → Customer {r.get('customer_id')} | "
            f"Ship {int(r.get('ship_qty',0))} from {r.get('from')} "
            f"({r.get('method')}, {r.get('transit_days')}d)"
        )
    return "\n".join(lines)

def delayed_orders_md() -> str:
    orders = _norm(load_orders("data/orders.csv"))
    now = pd.Timestamp.now(tz="UTC")

    need   = _pick(orders, ["need_by_ts_utc","need_by_ts","need_by","due_date","need_date"])
    status = _pick(orders, ["status","order_status"])
    shipped= _pick(orders, ["shipped_qty","delivered_qty"])
    qty    = _pick(orders, ["qty","quantity","ordered_qty","order_qty"])
    oid    = _pick(orders, ["order_id","order","so_id","sales_order"])
    cust   = _pick(orders, ["customer_id","customer","client_id","client"])
    plant  = _pick(orders, ["plant_id","delivery_plant","dest_loc_id","plant"])
    sku_c  = _pick(orders, ["sku","product","material","product_id"])

    if need:
        orders[need] = pd.to_datetime(orders[need], errors="coerce", utc=True)

    cond = pd.Series(False, index=orders.index)
    if status:
        cond |= orders[status].astype(str).str.lower().isin(["delayed","late","overdue"])
    if need and qty:
        shipped_series = _to_num(orders[shipped]) if shipped else 0
        qty_series     = _to_num(orders[qty])
        cond |= (orders[need] < now) & (shipped_series < qty_series)

    delayed = orders.loc[cond].copy()
    if delayed.empty:
        return "No delayed orders detected in the CSVs."

    delayed.sort_values(need or oid, inplace=True)
    lines = ["🕒 Delayed orders:", ""]
    for _, r in delayed.iterrows():
        lines.append(
            f"- Order {r.get(oid)} → Customer {r.get(cust)} | Plant {r.get(plant)} "
            f"| Need-by {r.get(need)} | SKU {r.get(sku_c)}"
        )
    return "\n".join(lines)

def plan_missing_sku_md(sku: str) -> str:
    if not sku:
        return "Need a SKU like product_556490."
    inv    = load_inventory("data/inventory.csv")
    plants = load_plants("data/plants.csv")
    orders = load_orders("data/orders.csv")
    pm     = load_plant_material("data/plant_material.csv")

    origin = _origin_for_sku(inv, sku)
    delay_event = {"shipment_id": f"S_{sku}", "sku": sku, "qty_unavailable": 50, "origin": origin}
    plans, kpi = plan_recovery(delay_event, inv, plants, orders, pm)
    if plans.empty:
        return f"No affected orders found for {sku}."
    return _fmt_plan_rows(plans, kpi, sku)

def impacted_by_plant_md(plant_id: str) -> str:
    if not plant_id:
        return "Need a plant id like plant_253 or a numeric id."
    orders = _norm(load_orders("data/orders.csv"))
    now = pd.Timestamp.now(tz="UTC")

    need  = _pick(orders, ["need_by_ts_utc","need_by_ts","need_by","due_date","need_date"])
    plant = _pick(orders, ["plant_id","delivery_plant","dest_loc_id","plant"])
    oid   = _pick(orders, ["order_id","order","so_id","sales_order"])
    cust  = _pick(orders, ["customer_id","customer","client_id","client"])
    sku_c = _pick(orders, ["sku","product","material","product_id"])

    if need:
        orders[need] = pd.to_datetime(orders[need], errors="coerce", utc=True)
    if not plant:
        return "orders.csv has no plant column."

    impacted = orders.loc[orders[plant].astype(str).str.lower() == str(plant_id).lower()].copy()
    if need:
        impacted = impacted.loc[impacted[need] >= now]
    if impacted.empty:
        return f"No upcoming orders found for plant {plant_id}."

    impacted.sort_values(need or oid, inplace=True)
    lines = [f"🏭 Orders shipping from plant {plant_id}:", ""]
    for _, r in impacted.iterrows():
        lines.append(f"- Order {r.get(oid)} → Customer {r.get(cust)} | Need-by {r.get(need)} | SKU {r.get(sku_c)}")
    return "\n".join(lines)

def order_details_md(order_id: str) -> str:
    if not order_id:
        return "Need an order id like order_311579."
    raw = str(order_id)
    mnum = pd.Series([raw]).astype(str).str.extract(r"(\d+)")
    num = mnum.iloc[0, 0] if not mnum.isna().all(axis=None) else None
    if not num:
        return f"Could not parse an id from '{order_id}'."

    orders = _norm(load_orders("data/orders.csv"))
    oid   = _pick(orders, ["order_id","order","so_id","sales_order"])
    if not oid:
        return "orders.csv missing an order id column."

    row = orders.loc[orders[oid].astype(str) == str(num)]
    if row.empty:
        return f"Order {num} not found."
    r = row.iloc[0]

    need   = _pick(orders, ["need_by_ts_utc","need_by_ts","need_by","due_date","need_date"])
    plant  = _pick(orders, ["plant_id","delivery_plant","dest_loc_id","plant"])
    qty    = _pick(orders, ["qty","quantity","ordered_qty","order_qty"])
    shipped= _pick(orders, ["shipped_qty","delivered_qty"])
    sku_c  = _pick(orders, ["sku","product","material","product_id"])
    cust   = _pick(orders, ["customer_id","customer","client_id","client"])
    status = _pick(orders, ["status","order_status"])

    return (
        f"🧾 Order {r.get(oid)}\n"
        f"- Customer: {r.get(cust)}\n"
        f"- SKU: {r.get(sku_c)}\n"
        f"- Plant: {r.get(plant)}\n"
        f"- Need-by: {r.get(need)}\n"
        f"- Qty: {r.get(qty)} | Shipped: {r.get(shipped)}\n"
        f"- Status: {r.get(status)}"
    )

def plan_all_delayed_orders_md() -> str:
    orders = _norm(load_orders("data/orders.csv"))
    inv    = load_inventory("data/inventory.csv")
    plants = load_plants("data/plants.csv")
    pm     = load_plant_material("data/plant_material.csv")

    now   = pd.Timestamp.now(tz="UTC")
    need  = _pick(orders, ["need_by_ts_utc","need_by_ts","need_by","due_date","need_date"])
    status= _pick(orders, ["status","order_status"])
    shipped=_pick(orders, ["shipped_qty","delivered_qty"])
    qty   = _pick(orders, ["qty","quantity","ordered_qty","order_qty"])
    oid   = _pick(orders, ["order_id","order","so_id","sales_order"])
    sku_c = _pick(orders, ["sku","product","material","product_id"])

    if need:
        orders[need] = pd.to_datetime(orders[need], errors="coerce", utc=True)

    cond = pd.Series(False, index=orders.index)
    if status:
        cond |= orders[status].astype(str).str.lower().isin(["delayed","late","overdue"])
    if need and qty:
        shipped_series = _to_num(orders[shipped]) if shipped else 0
        qty_series     = _to_num(orders[qty])
        cond |= (orders[need] < now) & (shipped_series < qty_series)

    delayed = orders.loc[cond].copy()
    if delayed.empty:
        return "No delayed orders to plan."

    delayed.sort_values(need or oid, inplace=True)
    out = ["🧭 Planning for delayed orders", ""]
    for _, r in delayed.iterrows():
        order_id = r.get(oid)
        sku      = str(r.get(sku_c))
        origin   = _origin_for_sku(inv, sku)

        ordered = _to_num(pd.Series([r.get(qty, 0)])).iloc[0] if qty else 0
        shipped_val = _to_num(pd.Series([r.get(shipped, 0)])).iloc[0] if shipped else 0
        qty_missing = max(0, int((ordered or 0) - (shipped_val or 0)))

        delay_event = {"shipment_id": f"S_{sku}",
                       "sku": sku,
                       "qty_unavailable": qty_missing,
                       "origin": origin}
        plans, kpi = plan_recovery(delay_event, inv, plants, orders, pm)

        if plans.empty:
            out.append(f"- Order {order_id} | SKU {sku}: no allocation available.")
        else:
            out.append(f"- Order {order_id} | SKU {sku}:")
            for _, pr in plans.iterrows():
                out.append(
                    f"    • Ship {int(pr.get('ship_qty',0))} from {pr.get('from')} "
                    f"({pr.get('method')}, {pr.get('transit_days')}d)"
                )
    return "\n".join(out)
