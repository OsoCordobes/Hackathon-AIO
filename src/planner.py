# src/planner.py
import pandas as pd

# ---------- helpers ----------
def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df

def _pick_col(df: pd.DataFrame, candidates, *, numeric=False, exclude=None):
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    if numeric:
        ex = set(exclude or [])
        for c in df.columns:
            if c in ex:
                continue
            if pd.api.types.is_numeric_dtype(df[c]):
                return c
    return None

# ---------- main ----------
def plan_recovery(delay_event, inventory, plants, orders, plant_material, horizon_days=7):
    """
    Minimal CSV-only planner.
    - Detects column names dynamically (lowercased).
    - Finds orders for the missing SKU due now→+horizon.
    - Allocates from plant stock first, else global stock.
    """
    sku = str(delay_event.get("sku"))
    qty_missing = int(delay_event.get("qty_unavailable", 0))
    origin = delay_event.get("origin", "unknown")

    # normalize headers
    inventory = _norm_cols(inventory)
    orders = _norm_cols(orders)

    # ---- orders columns
    sku_o   = _pick_col(orders, ["sku", "product", "material", "product_id"])
    qty_o   = _pick_col(orders, ["qty", "quantity", "ordered_qty", "order_qty"], numeric=True)
    ord_o   = _pick_col(orders, ["order_id", "order", "so_id", "sales_order"])
    cust_o  = _pick_col(orders, ["customer_id", "customer", "client_id", "client"])
    need_o  = _pick_col(orders, ["need_by_ts_utc", "need_by_ts", "need_by", "due_date", "need_date"])
    plant_o = _pick_col(orders, ["plant_id", "delivery_plant", "dest_loc_id", "plant"])

    if sku_o is None or qty_o is None:
        return pd.DataFrame(), {"affected_orders": 0, "available_stock": 0, "recovered": 0, "missing": qty_missing}

    if need_o is None:
        orders["need_by_ts_utc"] = pd.Timestamp.now(tz="UTC") + pd.Timedelta(hours=24)
        need_o = "need_by_ts_utc"
    else:
        orders[need_o] = pd.to_datetime(orders[need_o], errors="coerce", utc=True)\
                           .fillna(pd.Timestamp.now(tz="UTC") + pd.Timedelta(hours=24))

    orders[qty_o] = pd.to_numeric(orders[qty_o], errors="coerce").fillna(0).astype(int)

    # ---- inventory columns
    sku_i   = _pick_col(inventory, ["sku", "product", "material", "product_id"])
    plant_i = _pick_col(inventory, ["plant_id", "location_id", "warehouse_id", "site", "plant"])
    stock_i = _pick_col(
        inventory,
        ["stock", "available_qty", "qty", "quantity", "on_hand", "balance", "current_level", "available"],
        numeric=True,
        exclude=[sku_i, plant_i],
    )
    if stock_i is None:
        inventory["stock"] = 0
        stock_i = "stock"
    inventory[stock_i] = pd.to_numeric(inventory[stock_i], errors="coerce").fillna(0).astype(int)

    # ---- impacted orders in horizon
    now = pd.Timestamp.now(tz="UTC")
    end = now + pd.Timedelta(days=horizon_days)

    impacted = orders.loc[
        (orders[sku_o].astype(str) == sku) & (orders[need_o].between(now, end))
    ].copy()

    if impacted.empty:
        return pd.DataFrame(), {"affected_orders": 0, "available_stock": 0, "recovered": 0, "missing": qty_missing}

    impacted.sort_values(need_o, inplace=True)

    # ---- available stock
    if sku_i is None:
        total_available = 0
        source_loc = origin
    else:
        stock_rows = inventory.loc[inventory[sku_i].astype(str) == sku].copy()
        total_available = int(stock_rows[stock_i].sum()) if not stock_rows.empty else 0
        source_loc = (
            stock_rows[plant_i].iloc[0] if (plant_i and not stock_rows.empty and plant_i in stock_rows) else origin
        )

    def _avail_at_plant(sku_, plant_):
        if not (sku_i and plant_i and plant_):
            return 0
        rows = inventory.loc[
            (inventory[sku_i].astype(str) == str(sku_)) & (inventory[plant_i].astype(str) == str(plant_))
        ]
        return int(rows[stock_i].sum()) if not rows.empty else 0

    plans = []
    recovered = 0
    global_left = total_available

    for _, r in impacted.iterrows():
        if global_left <= 0:
            break
        need = int(r[qty_o])
        if need <= 0:
            continue

        delivery_plant = str(r.get(plant_o)) if plant_o else None
        local_avail = _avail_at_plant(sku, delivery_plant) if delivery_plant else 0

        use_local = min(need, local_avail, global_left)
        ship_qty = use_local if use_local > 0 else min(need, global_left)
        if ship_qty <= 0:
            continue

        recovered += ship_qty
        global_left -= ship_qty

        plans.append({
            "order_id": r.get(ord_o),
            "customer_id": r.get(cust_o),
            "sku": sku,
            "need": need,
            "ship_qty": ship_qty,
            "from": delivery_plant if use_local > 0 else source_loc,
            "method": "plant" if use_local > 0 else "warehouse",
            "transit_days": 1 if use_local > 0 else 3,
        })

    kpi = {
        "affected_orders": len(impacted),
        "available_stock": total_available,
        "recovered": recovered,
        "missing": max(0, qty_missing - recovered),
    }

    return pd.DataFrame(plans), kpi
