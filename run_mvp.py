import pandas as pd
from src.load_data import load_inventory, load_plants, load_orders
from src.planner import plan_for_delay

# Load data from /data
inv = load_inventory("data/inventory.csv")
plants = load_plants("data/plants.csv")
orders = load_orders("data/orders.csv")

# --- Helper: list SKUs in both orders and inventory ---
both = set(orders["sku"]).intersection(set(inv["sku"]))
print("Candidate SKUs (first 20):", list(both)[:20])

# --- Helper: list plants that have stock for your chosen SKU ---
sku_test = "product_1631005"                 ### EDIT ME: put one SKU from the printed list
plants_for_sku = inv.loc[inv["sku"] == sku_test, "loc_id"].unique().tolist()
print(f"Plants with stock for {sku_test}:", plants_for_sku[:20])

# --- Delay event: use your chosen SKU and one plant from the line above ---
delay = {"sku": "product_1631005",           ### EDIT ME: same SKU as sku_test
         "qty_unavailable": 50,
         "origin_loc_id": plants_for_sku[0]} ### pick one real plant id

# Run planner
plans, kpi = plan_for_delay(delay, inv, plants, orders)
print("KPI:", kpi)
if plans.empty:
    print("No affected orders for this SKU.")
else:
    cols = ["order_id","customer_id","sku","qty","dest_loc_id","source_loc_id","ETA_ts","lateness_h"]
    print(plans[cols].head(25).to_string(index=False))
