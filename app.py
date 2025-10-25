# app.py
# Jarvis — Danfoss Disruption Responder (MVP)
# Modes: Plan recovery | Affected by missing SKU | Affected by missing COMPONENT | Predict stockouts

import streamlit as st
import pandas as pd

from src.load_data import (
    load_inventory,
    load_plants,
    load_orders,
    load_plant_material,
)
from src.planner import plan_for_delay
from src.impact import affected_orders_if_missing
from src.bom import load_bom, products_using
from src.predict import coverage_alerts

st.set_page_config(page_title="Jarvis — Danfoss Disruption Responder (MVP)", layout="wide")
st.title("Jarvis — Danfoss Disruption Responder (MVP)")

# ---------- Data loading (cached) ----------
@st.cache_data(show_spinner=False)
def _load_all():
    inv = load_inventory("data/inventory.csv")
    plants = load_plants("data/plants.csv")
    orders = load_orders("data/orders.csv")
    plant_material = load_plant_material("data/plant_material.csv")
    # BOM optional: try full, then small
    try:
        bom_df = load_bom("data/material_component.csv", "data/material_component_small.csv")
    except Exception:
        bom_df = None
    return inv, plants, orders, plant_material, bom_df

inv, plants, orders, plant_material, bom_df = _load_all()

def _download_button(label, df: pd.DataFrame, fname: str):
    st.download_button(
        label,
        df.to_csv(index=False).encode("utf-8"),
        file_name=fname,
        mime="text/csv",
    )

# ---------- UI mode selector ----------
mode = st.radio(
    "Choose task",
    [
        "Plan recovery for a delayed SKU",
        "Who is affected if a SKU is missing?",
        "Who is affected if a COMPONENT is missing?",
        "Predict near-term stockouts",
    ],
    index=0,
)

# ---------- Plan recovery ----------
if mode == "Plan recovery for a delayed SKU":
    sku_options = sorted(set(orders["sku"]).intersection(set(inv["sku"])))
    if not sku_options:
        st.error("No overlapping SKUs between orders and inventory.")
    else:
        sku = st.selectbox("SKU", sku_options)
        qty = st.number_input("Qty unavailable", min_value=1, value=50, step=1)

        origin_default = sorted(inv.loc[inv["sku"] == sku, "loc_id"].astype(str).unique())
        allow_any_origin = st.checkbox("Allow any plant as origin", value=False)
        origin_pool = sorted(plants["loc_id"].astype(str).unique()) if allow_any_origin else origin_default
        if not origin_pool:
            st.warning("No plants with stock for this SKU. Enable 'Allow any plant as origin' to simulate.")
            origin_pool = sorted(plants["loc_id"].astype(str).unique())
        origin = st.selectbox("Origin loc_id (where delay happened)", origin_pool)

        if st.button("Compute plan"):
            delay = {"sku": sku, "qty_unavailable": qty, "origin_loc_id": origin}
            plans, kpi = plan_for_delay(
                delay, inv, plants, orders, plant_material=plant_material
            )

            st.subheader("KPI (SLA target 95%)")
            st.metric("On-time %", f"{kpi['on_time_pct']}%")
            st.metric("Late orders", kpi["late_orders"])

            if plans.empty:
                st.info("No open orders for this SKU.")
            else:
                cols = ["order_id","customer_id","sku","qty","dest_loc_id","source_loc_id","ETA_ts","lateness_h"]
                st.dataframe(plans[cols])
                _download_button("Download plan CSV", plans[cols], f"plan_{sku}.csv")

# ---------- Affected by missing SKU ----------
elif mode == "Who is affected if a SKU is missing?":
    sku_all = sorted(orders["sku"].astype(str).unique().tolist())
    if not sku_all:
        st.error("orders.csv has no SKU column or no rows.")
    else:
        sku = st.selectbox("SKU", sku_all)
        if st.button("Show affected"):
            affected = affected_orders_if_missing(sku, orders)
            st.subheader("Affected orders and customers")
            st.write({
                "orders": len(affected),
                "customers": affected["customer_id"].nunique()
            })
            st.dataframe(affected.sort_values("need_by_ts_utc"))
            _download_button("Download affected CSV", affected, f"affected_orders_{sku}.csv")

# ---------- Affected by missing COMPONENT (friendlier + simulate) ----------
elif mode == "Who is affected if a COMPONENT is missing?":
    if bom_df is None or bom_df.empty:
        st.error("BOM not loaded. Put material_component.csv or material_component_small.csv in /data.")
    else:
        comp_codes = sorted(bom_df["component"].astype(str).unique().tolist())
        comp = st.selectbox("Component code", comp_codes)

        # Map component -> finished goods SKUs
        skus = products_using(comp, bom_df)
        open_counts = (
            orders[orders["sku"].astype(str).isin(skus)]
            .groupby("sku")["order_id"].count()
            .rename("open_orders")
            .reset_index()
            .sort_values("open_orders", ascending=False)
        )

        st.subheader("Mapped products")
        c1, c2 = st.columns(2)
        c1.metric("Finished goods mapped", len(skus))
        c2.metric("SKUs with open orders now", int((open_counts["open_orders"] > 0).sum()))
        st.dataframe(open_counts)

        colA, colB = st.columns(2)
        if colA.button("Show affected orders now"):
            affected = orders[orders["sku"].astype(str).isin(skus)][
                ["order_id","customer_id","sku","qty","dest_loc_id","need_by_ts_utc"]
            ].copy().sort_values("need_by_ts_utc")
            st.subheader("Affected finished-goods orders (current open orders)")
            st.write({"orders": len(affected), "customers": affected["customer_id"].nunique()})
            st.dataframe(affected)
            _download_button("Download affected CSV", affected, f"affected_by_component_{comp}.csv")

        simulate = colB.checkbox(
            "Simulate global stockout of mapped products",
            value=False,
            help="Zero on-hand for all mapped SKUs and re-plan per SKU."
        )
        if simulate:
            qty_short = st.number_input(
                "Shortage quantity per SKU for simulation",
                min_value=1, value=10**9, step=1000,
            )
            inv_sim = inv.copy()
            inv_sim.loc[inv_sim["sku"].astype(str).isin(skus), "on_hand"] = 0

            all_plans = []
            for sku_sim in skus:
                delay = {"sku": sku_sim, "qty_unavailable": qty_short, "origin_loc_id": "NA"}
                plans, _k = plan_for_delay(
                    delay, inv_sim, plants, orders, plant_material=plant_material
                )
                if not plans.empty:
                    plans["sim_sku"] = sku_sim
                    all_plans.append(plans)

            if all_plans:
                result = pd.concat(all_plans, ignore_index=True)
                st.subheader("Replan under component stockout (all mapped SKUs)")
                kpi_ontime = round((result["lateness_h"] <= 1e-6).mean() * 100, 2)
                st.metric("On-time %", f"{kpi_ontime}%")
                st.metric("Late orders", int((result["lateness_h"] > 0).sum()))
                cols = ["sim_sku","order_id","customer_id","qty","dest_loc_id","source_loc_id","ETA_ts","lateness_h"]
                st.dataframe(result[cols].sort_values(["sim_sku","lateness_h","ETA_ts"]))
                _download_button("Download replan CSV", result[cols], f"replan_component_{comp}.csv")
            else:
                st.info("No orders to replan for the mapped SKUs at this moment.")

# ---------- Predict stockouts (polished: show risky by default) ----------
else:
    days = st.slider("Horizon (days)", 1, 30, 7)
    alerts = coverage_alerts(inv, orders, days)
    show_all = st.checkbox("Show all SKUs", value=False)

    view = alerts if show_all else alerts[alerts["risk"]]
    view_disp = view.reset_index()  # ensure 'sku' is a column for display and download

    st.subheader("Coverage alerts")
    st.dataframe(view_disp)
    _download_button("Download alerts CSV", view_disp, f"coverage_alerts_{days}d.csv")
