# src/agent_tools.py
# StructuredTool set. Uses lead_time_h, greedy stock reservation via planner,
# route-block simulation, robust stockout simulation, and SKU-optional rerouting.

from functools import lru_cache
import json
import pandas as pd
from langchain_core.tools import StructuredTool

from src.load_data import load_inventory, load_plants, load_orders, load_plant_material
from src.planner import plan_for_delay
from src.impact import affected_orders_if_missing
from src.bom import load_bom, products_using
from src.predict import coverage_alerts


def _j(o) -> str:
    return json.dumps(o, default=str)


@lru_cache(maxsize=1)
def _load_all():
    inv = load_inventory("data/inventory.csv")
    plants = load_plants("data/plants.csv")
    orders = load_orders("data/orders.csv")
    plant_material = load_plant_material("data/plant_material.csv")
    try:
        bom_df = load_bom("data/material_component.csv", "data/material_component_small.csv")
    except Exception:
        bom_df = pd.DataFrame(columns=["component", "material"])
    return inv, plants, orders, plant_material, bom_df


# ---------- helpers ----------
def _bom_detect(code: str, bom_df: pd.DataFrame) -> str:
    """Return 'component' if code is in component col, 'material' if in material col, else 'none'."""
    s = str(code)
    if not bom_df.empty:
        if s in set(bom_df["component"].astype(str)):
            return "component"
        if s in set(bom_df["material"].astype(str)):
            return "material"
    return "none"


# ---------- tool functions ----------
def list_skus() -> str:
    """Return up to 200 SKUs found in both orders and inventory."""
    inv, _, orders, _, _ = _load_all()
    both = sorted(set(orders["sku"]).intersection(set(inv["sku"])))
    return _j(both[:200])


def plants_for_sku(sku: str) -> str:
    """Return plant IDs that currently hold stock for the given SKU."""
    inv, _, __, ___, ____ = _load_all()
    pls = sorted(
        inv.loc[inv["sku"].astype(str) == str(sku), "loc_id"]
        .astype(str).unique().tolist()
    )
    return _j(pls)


def impacted_orders_by_sku(sku: str) -> str:
    """List orders and customers affected if a finished-good SKU is missing."""
    _, _, orders, __, ___ = _load_all()
    df = affected_orders_if_missing(str(sku), orders)
    return df.to_json(orient="records", date_format="iso")


def impacted_orders_by_component(component_code: str) -> str:
    """Expand a missing COMPONENT via the BOM and list affected finished-good orders."""
    _, _, orders, __, bom_df = _load_all()
    if bom_df.empty:
        return "BOM not loaded"
    skus = products_using(str(component_code), bom_df)
    df = orders[orders["sku"].astype(str).isin(skus)][
        ["order_id", "customer_id", "sku", "qty", "dest_loc_id", "need_by_ts_utc"]
    ].copy()
    return df.to_json(orient="records", date_format="iso")


def plan_delay(sku: str, qty_unavailable: int, origin_loc_id: str) -> str:
    """Compute a single-source recovery plan using stock or plant lead time; returns KPI and plan rows."""
    inv, plants, orders, pm, _ = _load_all()
    plans, kpi = plan_for_delay(
        {"sku": str(sku), "qty_unavailable": int(qty_unavailable), "origin_loc_id": str(origin_loc_id)},
        inv, plants, orders, plant_material=pm
    )
    out = {
        "kpi": kpi,  # includes line-level and full-kit order KPIs
        "plans": plans[[
            "order_id", "customer_id", "sku", "qty", "dest_loc_id",
            "source_loc_id", "ETA_ts", "strategy", "lateness_h"
        ]].to_dict(orient="records")
    }
    return _j(out)


def recommend_action_missing_sku(sku: str) -> str:
    """
    Recommend concrete action for a missing finished-good SKU.
    - If stock exists: ship now from the best source plant(s) with ETAs.
    - Else: produce at the fastest capable plant (uses lead_time_h) then ship.
    Output includes a top-level 'recommended_action' string plus per-order actions.
    """
    inv, plants, orders, pm, _ = _load_all()
    sku = str(sku)

    # Affected orders
    affected = affected_orders_if_missing(sku, orders)

    # Plan using greedy allocator (stock or produce) for every affected line
    plans, kpi = plan_for_delay(
        {"sku": sku, "qty_unavailable": 10**9, "origin_loc_id": "NA"},
        inv, plants, orders, plant_material=pm
    )

    if plans.empty:
        return _j({
            "sku": sku,
            "affected_orders": 0 if affected.empty else int(len(affected)),
            "message": "No open orders found for this SKU.",
            "kpi": kpi
        })

    # Lead-time lookup for produce strategy
    lt_map = {}
    if not pm.empty:
        lt_map = (pm[pm["sku"].astype(str) == sku]
                  .set_index("loc_id")["lead_time_h"].astype(float).to_dict())

    # Build per-order actionable rows
    rows = []
    for _, r in plans.iterrows():
        src = str(r["source_loc_id"]) if pd.notna(r["source_loc_id"]) else None
        strat = str(r["strategy"])
        eta = r["ETA_ts"]
        if strat == "stock-now" and src:
            action = f"Ship now from {src} · ETA {eta}"
        elif strat == "produce" and src:
            lt = lt_map.get(src, 72.0)
            action = f"Produce at {src} (LT≈{int(round(lt))}h) then ship · ETA {eta}"
        else:
            action = "No feasible source. Inform customer of delay."
        rows.append({
            "order_id": str(r["order_id"]),
            "customer_id": str(r["customer_id"]),
            "dest_loc_id": str(r["dest_loc_id"]),
            "qty": int(r["qty"]),
            "action": action,
            "strategy": strat,
            "source_loc_id": src,
            "ETA_ts": eta,
            "lateness_h": float(r["lateness_h"]),
        })

    df = pd.DataFrame(rows)

    # Decide a single top recommendation:
    # Prefer any stock-now option with minimum lateness, else fastest produce.
    ship = df[df["strategy"] == "stock-now"]
    if not ship.empty:
        best = ship.sort_values(["lateness_h", "ETA_ts"]).iloc[0]
        top = f"Ship now from {best['source_loc_id']} for earliest ETA {best['ETA_ts']}."
    else:
        prod = df[df["strategy"] == "produce"]
        if not prod.empty:
            best = prod.sort_values(["lateness_h", "ETA_ts"]).iloc[0]
            lt = lt_map.get(best["source_loc_id"], 72.0)
            top = f"Produce at {best['source_loc_id']} (LT≈{int(round(lt))}h) then ship. Earliest ETA {best['ETA_ts']}."
        else:
            top = "No feasible plant. Inform customers of delay."

    # Summaries by source
    by_src = (df.dropna(subset=["source_loc_id"])
                .groupby(["strategy", "source_loc_id"])
                .agg(lines=("order_id","count"),
                     min_eta=("ETA_ts","min"))
                .reset_index()
                .sort_values(["strategy","min_eta"]))

    return _j({
        "sku": sku,
        "affected_orders": int(len(affected)) if not affected.empty else 0,
        "affected_customers": int(affected["customer_id"].nunique()) if not affected.empty else 0,
        "kpi": kpi,  # includes line and full-kit order KPIs
        "recommended_action": top,
        "by_source": by_src.to_dict(orient="records"),
        "per_order": rows
    })



def simulate_component_stockout(code: str) -> str:
    """Simulate stockout for a COMPONENT or a finished-good code and replan using stock or lead time."""
    inv, plants, orders, pm, bom_df = _load_all()
    role = _bom_detect(code, bom_df)

    if role == "component":
        skus = products_using(str(code), bom_df)
    elif role == "material":
        skus = [str(code)]
    else:
        s = str(code)
        if s in set(orders["sku"].astype(str)):
            skus = [s]
        else:
            return _j({"message": f"Code '{s}' not found in BOM or orders. Provide a component or a valid SKU."})

    inv_sim = inv.copy()
    inv_sim.loc[inv_sim["sku"].astype(str).isin(skus), "on_hand"] = 0

    all_plans = []
    for s in skus:
        plans, _ = plan_for_delay(
            {"sku": s, "qty_unavailable": 10**9, "origin_loc_id": "NA"},
            inv_sim, plants, orders, plant_material=pm
        )
        if not plans.empty:
            plans["sim_sku"] = s
            all_plans.append(plans)

    if not all_plans:
        return _j({"message": "No open orders for the mapped SKU(s), nothing to replan."})

    res = pd.concat(all_plans, ignore_index=True)
    view = res[[
        "sim_sku", "order_id", "customer_id", "qty", "dest_loc_id",
        "source_loc_id", "ETA_ts", "strategy", "lateness_h"
    ]]
    return view.to_json(orient="records", date_format="iso")


def reroute_block(origin_loc_id: str, dest_loc_id: str, sku: str = "") -> str:
    """
    Simulate a blocked origin→dest route.
    If SKU is omitted, consider ALL SKUs with open orders to the destination.
    """
    inv, plants, orders, pm, _ = _load_all()

    if sku and str(sku).strip():
        sku_set = [str(sku)]
    else:
        sku_set = sorted(
            orders.loc[orders["dest_loc_id"].astype(str) == str(dest_loc_id), "sku"]
                  .astype(str).unique().tolist()
        )

    if not sku_set:
        return _j({
            "message": f"No open orders delivering to {dest_loc_id}.",
            "blocked": [str(origin_loc_id), str(dest_loc_id)]
        })

    all_plans = []
    for s in sku_set:
        plans, _k = plan_for_delay(
            {"sku": s, "qty_unavailable": 10**9, "origin_loc_id": "NA"},
            inv, plants, orders, plant_material=pm,
            blocked_routes=[(str(origin_loc_id), str(dest_loc_id))]
        )
        if not plans.empty:
            plans["sim_sku"] = s
            all_plans.append(plans)

    if not all_plans:
        return _j({
            "message": "No feasible alternative route for the selected scope.",
            "blocked": [str(origin_loc_id), str(dest_loc_id)]
        })

    result = pd.concat(all_plans, ignore_index=True)

    want = [
        "sim_sku", "order_id", "customer_id", "sku", "qty", "dest_loc_id",
        "source_loc_id", "ETA_ts", "strategy", "lateness_h"
    ]
    cols = [c for c in want if c in result.columns]

    kpi_ontime = (
        round((result["lateness_h"] <= 1e-6).mean() * 100, 2)
        if "lateness_h" in result.columns and len(result) else None
    )

    sort_keys = [k for k in ["sim_sku", "lateness_h", "ETA_ts"] if k in cols]
    out = {
        "blocked": [str(origin_loc_id), str(dest_loc_id)],
        "on_time_pct": kpi_ontime,
        "rows": result[cols].sort_values(sort_keys if sort_keys else cols).to_dict(orient="records"),
    }
    return _j(out)


def predict_coverage(horizon_days: int = 7) -> str:
    """Compute coverage alerts per SKU for the next N days."""
    inv, _, orders, __, ___ = _load_all()
    df = coverage_alerts(inv, orders, int(horizon_days)).reset_index()
    return df.to_json(orient="records", date_format="iso")


def component_production_hint(component_code: str) -> str:
    """
    Suggest fastest plant to produce a missing component, and fastest assembly plants
    for its parents that have open orders.
    """
    inv, plants, orders, pm, bom_df = _load_all()
    if bom_df.empty:
        return _j({"message": "BOM not loaded"})

    comp = str(component_code)
    parents = products_using(comp, bom_df)
    if not parents:
        return _j({"message": f"No parent products found for component {comp}."})

    # fastest plant for the component
    comp_cap = pm[pm["sku"].astype(str) == comp] if not pm.empty else pd.DataFrame()
    if comp_cap.empty:
        comp_best = {"plant": None, "lead_time_h": 72.0}
    else:
        row = comp_cap.sort_values("lead_time_h").iloc[0]
        comp_best = {"plant": str(row["loc_id"]), "lead_time_h": float(row["lead_time_h"])}

    # parent orders
    parent_orders = orders[orders["sku"].astype(str).isin(parents)].copy()
    if parent_orders.empty:
        return _j({"component": comp, "parents": len(parents), "orders": 0, "hint_component": comp_best})

    # fastest assembly plant per parent SKU (lead time only)
    hints = []
    for s in sorted(set(parent_orders["sku"].astype(str))):
        cap = pm[pm["sku"].astype(str) == s] if not pm.empty else pd.DataFrame()
        if cap.empty:
            hints.append({"parent_sku": s, "assembly_plant": None, "parent_lt_h": 72.0})
        else:
            r = cap.sort_values("lead_time_h").iloc[0]
            hints.append({"parent_sku": s, "assembly_plant": str(r["loc_id"]), "parent_lt_h": float(r["lead_time_h"])})

    return _j({
        "component": comp,
        "parents": len(parents),
        "orders": int(len(parent_orders)),
        "hint_component": comp_best,
        "hint_parents": hints
    })


# ---------- StructuredTool registry ----------
TOOLS = [
    StructuredTool.from_function(
        list_skus, name="list_skus",
        description="Return up to 200 SKUs present in both orders and inventory."
    ),
    StructuredTool.from_function(
        plants_for_sku, name="plants_for_sku",
        description="Return plant IDs that currently hold stock for the given SKU."
    ),
    StructuredTool.from_function(
        impacted_orders_by_sku, name="impacted_orders_by_sku",
        description="List orders and customers affected if a finished-good SKU is missing."
    ),
    StructuredTool.from_function(
        impacted_orders_by_component, name="impacted_orders_by_component",
        description="Expand a missing COMPONENT via the BOM and list affected finished-good orders."
    ),
    StructuredTool.from_function(
        plan_delay, name="plan_delay",
        description="Compute a single-source recovery plan using stock or plant lead time."
    ),
    StructuredTool.from_function(
        recommend_action_missing_sku, name="recommend_action_missing_sku",
        description="Summarise affected counts and recommend top source plants if a SKU is missing."
    ),
    StructuredTool.from_function(
        simulate_component_stockout, name="simulate_component_stockout",
        description="Simulate stockout for a component or a finished-good code and replan."
    ),
    StructuredTool.from_function(
        reroute_block, name="reroute_block",
        description="Simulate a blocked origin→dest route. SKU optional; if omitted, replan for all SKUs to the destination."
    ),
    StructuredTool.from_function(
        predict_coverage, name="predict_coverage",
        description="Compute coverage alerts per SKU for the next N days."
    ),
    StructuredTool.from_function(
        component_production_hint, name="component_production_hint",
        description="Suggest fastest plant to produce a missing component and fastest assembly plants for its parents."
    ),
]
