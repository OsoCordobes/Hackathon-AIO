import pandas as pd
from src.load_data import load_inventory, load_plants, load_orders, load_plant_material
from src.planner import plan_recovery as plan_for_delay



inv = load_inventory("data/inventory.csv")
plants = load_plants("data/plants.csv")
orders = load_orders("data/orders.csv")
plant_material = load_plant_material("data/plant_material.csv")

delay = {"sku": "PUT_REAL_SKU", "qty_unavailable": 50, "origin_loc_id": "PUT_ORIGIN"}
plans, kpi = plan_for_delay(delay, inv, plants, orders, plant_material=plant_material)
print("KPI:", kpi)
if not plans.empty:
    cols = ["order_id","customer_id","sku","qty","dest_loc_id","source_loc_id","ETA_ts","lateness_h"]
    print(plans[cols].head(25).to_string(index=False))
else:
    print("No affected orders for this SKU.")
