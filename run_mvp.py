from src.load_data import load_inventory, load_orders, load_plants, load_plant_material
from src.planner import plan_recovery

# --- Load data
inv = load_inventory("data/inventory.csv")
plants = load_plants("data/plants.csv")
orders = load_orders("data/orders.csv")
plant_material = load_plant_material("data/plant_material.csv")

# --- Simulate delay event
delay = {
    "shipment_id": "S100",
    "sku": "product_556490",
    "qty_unavailable": 50,
    "origin": "Plant_A"
}

# --- Run recovery plan
plans, kpi = plan_recovery(
    delay_event=delay,
    inventory=inv,
    plants=plants,
    orders=orders,
    plant_material=plant_material
)

# --- Output results
print("Recovery plan:")
print(plans)
print("\nKPI summary:")
print(kpi)
