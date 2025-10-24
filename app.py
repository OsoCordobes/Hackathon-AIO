import streamlit as st
from src.load_data import load_inventory, load_plants, load_orders
from src.planner import plan_for_delay
from src.impact import affected_orders_if_missing
from src.bom import load_bom, products_using

st.title("Jarvis — Danfoss Disruption Responder (MVP)")

inv = load_inventory("data/inventory.csv")
plants = load_plants("data/plants.csv")
orders = load_orders("data/orders.csv")

mode = st.radio("What do you want to do?",
                ["Plan recovery for a delayed SKU",
                 "Who is affected if a SKU is missing?",
                 "Who is affected if a COMPONENT is missing?"], index=0)

if mode == "Plan recovery for a delayed SKU":
    sku_options = sorted(set(orders["sku"]).intersection(set(inv["sku"])))
    sku = st.selectbox("SKU", sku_options)
    qty = st.number_input("Qty unavailable", min_value=1, value=50)
    origin_options = sorted(inv.loc[inv["sku"] == sku, "loc_id"].astype(str).unique().tolist())
    origin = st.selectbox("Origin loc_id", origin_options)
    if st.button("Compute recovery plan"):
        delay = {"sku": sku, "qty_unavailable": qty, "origin_loc_id": origin}
        plans, kpi = plan_for_delay(delay, inv, plants, orders)
        st.subheader("KPI (SLA 95%)")
        st.metric("On-time %", f"{kpi['on_time_pct']}%")
        st.metric("Late orders", kpi["late_orders"])
        st.dataframe(plans[["order_id","customer_id","sku","qty","dest_loc_id",
                            "source_loc_id","ETA_ts","lateness_h"]])

elif mode == "Who is affected if a SKU is missing?":
    sku = st.selectbox("SKU", sorted(orders["sku"].astype(str).unique().tolist()))
    if st.button("Show affected"):
        affected = affected_orders_if_missing(sku, orders)
        st.subheader("Affected orders and customers")
        st.write({"orders": len(affected), "customers": affected["customer_id"].nunique()})
        st.dataframe(affected.sort_values("need_by_ts_utc"))
        st.download_button("Download CSV", affected.to_csv(index=False).encode("utf-8"),
                           file_name=f"affected_orders_{sku}.csv", mime="text/csv")

else:  # component impact
    try:
        bom_df = load_bom()
        comp = st.selectbox("Component code", sorted(bom_df["component"].astype(str).unique().tolist()))
        if st.button("Show affected by component"):
            skus = products_using(comp, bom_df)
            affected = orders[orders["sku"].astype(str).isin(skus)][
                ["order_id","customer_id","sku","qty","dest_loc_id","need_by_ts_utc"]
            ].copy()
            st.subheader("Affected finished-goods orders")
            st.write({"component": comp, "mapped_products": len(skus),
                      "orders": len(affected), "customers": affected["customer_id"].nunique()})
            st.dataframe(affected.sort_values("need_by_ts_utc"))
            st.download_button("Download CSV", affected.to_csv(index=False).encode("utf-8"),
                               file_name=f"affected_orders_by_component_{comp}.csv", mime="text/csv")
    except Exception as e:
        st.error(f"BOM not loaded: {e}")
