from __future__ import annotations
import math
from typing import Dict, Tuple, Optional, List
import pandas as pd
from pandas import Timestamp

# -----------------------------
# Small helpers
# -----------------------------
ROAD_KMPH = 60.0  # default road speed
SLA_ON_TIME_H = 0  # lateness threshold in hours

def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    """Great-circle distance in kilometers."""
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlmb/2)**2
    return 2*r*math.atan2(math.sqrt(a), math.sqrt(1 - a))

def _travel_hours(plants: pd.DataFrame, src: str, dst: str) -> float:
    """Road travel time in hours between two plant ids."""
    try:
        a = plants.loc[src]
        b = plants.loc[dst]
    except KeyError:
        return float("inf")
    km = _haversine_km(a["lat"], a["lng"], b["lat"], b["lng"])
    return km / ROAD_KMPH

def _lead_time_hours(lead: pd.DataFrame, src: str, sku: str) -> float:
    """Production lead time in hours for a sku at a plant if defined; else inf."""
    try:
        return float(lead.loc[(src, sku)]["lead_time_h"])
    except KeyError:
        return float("inf")

def _on_hand(inv: pd.DataFrame, src: str, sku: str) -> float:
    try:
        return float(inv.loc[(sku, src)]["on_hand"])
    except KeyError:
        return 0.0

# -----------------------------
# Public utility
# -----------------------------
def affected_orders_if_missing(
    sku: str,
    orders: pd.DataFrame,
    horizon_days: Optional[int] = None,
) -> pd.DataFrame:
    """
    Return the subset of orders that require this SKU,
    optionally limited to a horizon in days from now.
    """
    df = orders[orders["sku"] == sku].copy()
    if horizon_days is not None:
        now = pd.Timestamp.utcnow().tz_localize("UTC")
        end = now + pd.Timedelta(days=horizon_days)
        df = df[(df["need_by_ts_utc"] >= now) & (df["need_by_ts_utc"] <= end)]
    return df.sort_values("need_by_ts_utc")

# -----------------------------
# Core planner
# -----------------------------
def _best_source_for_line(
    sku: str,
    qty: float,
    dest_loc_id: str,
    need_by: Timestamp,
    inv: pd.DataFrame,
    plants: pd.DataFrame,
    lead: pd.DataFrame,
    forbid_loc: Optional[str] = None,
) -> Tuple[Optional[str], float, str]:
    """
    Pick the fastest source plant for this order line.
    Returns (source_loc_id, ETA_ts (UTC POSIX hours from now), strategy_text).
    If none feasible, returns (None, inf, reason).
    """
    now = pd.Timestamp.utcnow().tz_localize("UTC")

    candidates: List[Tuple[str, float, str]] = []

    # Iterate all plants known (index of plants)
    for src in plants.index:
        if forbid_loc and src == forbid_loc:
            continue

        stock = _on_hand(inv, src, sku)
        travel_h = _travel_hours(plants, src, dest_loc_id)

        # Option A: ship-from-stock if enough units
        if stock >= qty and math.isfinite(travel_h):
            eta = now + pd.Timedelta(hours=travel_h)
            candidates.append((src, (eta - now).total_seconds()/3600, f"stock-now"))
            continue

        # Option B: produce then ship
        lt_h = _lead_time_hours(lead, src, sku)
        if math.isfinite(lt_h) and math.isfinite(travel_h):
            eta = now + pd.Timedelta(hours=lt_h + travel_h)
            candidates.append((src, (eta - now).total_seconds()/3600, f"make-{int(round(lt_h))}h-then-ship"))

    if not candidates:
        return None, float("inf"), "no-feasible-source"

    # Choose minimal ETA hours
    candidates.sort(key=lambda x: x[1])
    src, eta_h, strategy = candidates[0]
    return src, eta_h, strategy

def plan_recovery(
    delay: Dict[str, str],
    inv_raw: pd.DataFrame,
    plants_raw: pd.DataFrame,
    orders_raw: pd.DataFrame,
    plant_material_raw: pd.DataFrame,
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """
    Build a simple recovery plan for all affected orders of a missing SKU.
    delay = {"sku":..., "qty_unavailable": <float>, "origin_loc_id": <str or None>}
    Returns (plans_df, kpi_dict).
    """
    # --- Normalize indices for fast lookup
    inv = inv_raw.copy()
    if not {"sku","loc_id","on_hand"}.issubset(inv.columns):
        raise ValueError("inventory must have columns: sku, loc_id, on_hand")
    inv["on_hand"] = inv["on_hand"].clip(lower=0)
    inv.set_index(["sku","loc_id"], inplace=True, drop=False)

    plants = plants_raw.copy()
    if not {"loc_id","lat","lng"}.issubset(plants.columns):
        raise ValueError("plants must have columns: loc_id, lat, lng")
    plants.set_index("loc_id", inplace=True, drop=False)

    orders = orders_raw.copy()
    need_cols = {"order_id","customer_id","sku","qty","dest_loc_id","need_by_ts_utc"}
    if not need_cols.issubset(orders.columns):
        raise ValueError(f"orders must have columns: {need_cols}")
    # ensure tz-aware UTC
    orders["need_by_ts_utc"] = pd.to_datetime(orders["need_by_ts_utc"], utc=True)

    lead = plant_material_raw.copy()
    if not {"loc_id","sku","lead_time_h"}.issubset(lead.columns):
        raise ValueError("plant_material must have columns: loc_id, sku, lead_time_h")
    lead.set_index(["loc_id","sku"], inplace=True, drop=False)

    # --- Inputs
    sku = delay.get("sku")
    origin = delay.get("origin_loc_id")
    # qty_unavailable kept for UI, but planning is per order-line qty

    # --- Find impacted orders for this SKU (>= now)
    now = pd.Timestamp.utcnow().tz_localize("UTC")
    impacted = orders[(orders["sku"] == sku) & (orders["need_by_ts_utc"] >= now)].copy()
    impacted.sort_values("need_by_ts_utc", inplace=True)

    if impacted.empty:
        return pd.DataFrame(columns=["order_id","customer_id","sku","qty","dest_loc_id",
                                     "source_loc_id","ETA_ts","strategy","lateness_h"]), {
            "on_time_pct": 100.0, "late_orders": 0
        }

    # --- Plan per order line
    plan_rows = []
    late_count = 0
    for _, row in impacted.iterrows():
        qty = float(row["qty"])
        dest = str(row["dest_loc_id"])
        need_by = row["need_by_ts_utc"]

        src, eta_h, strategy = _best_source_for_line(
            sku=sku, qty=qty, dest_loc_id=dest, need_by=need_by,
            inv=inv, plants=plants, lead=lead, forbid_loc=origin
        )

        if src is None or not math.isfinite(eta_h):
            # no feasible source, mark very late
            lateness_h = float("inf")
            eta_ts = pd.NaT
            source_id = "NONE"
            strategy = "no-source"
        else:
            eta_ts = now + pd.Timedelta(hours=eta_h)
            lateness_h = max(0.0, (eta_ts - need_by).total_seconds()/3600.0)
            source_id = src

        if lateness_h > SLA_ON_TIME_H:
            late_count += 1

        plan_rows.append({
            "order_id": row["order_id"],
            "customer_id": row.get("customer_id","CUST_UNKNOWN"),
            "sku": sku,
            "qty": qty,
            "dest_loc_id": dest,
            "source_loc_id": source_id,
            "ETA_ts": eta_ts,
            "strategy": strategy,
            "lateness_h": 0.0 if not math.isfinite(lateness_h) else round(lateness_h, 2)
        })

    plans = pd.DataFrame(plan_rows)
    on_time_pct = 100.0 * (1.0 - late_count / max(1, len(plans)))
    kpi = {"on_time_pct": round(on_time_pct, 1), "late_orders": int(late_count)}
    return plans, kpi

# Backwards-compatible alias for older scripts
def plan_for_delay(*args, **kwargs):
    return plan_recovery(*args, **kwargs)