# Project constants and mappings for the MVP agent “Jarvis”

AGENT_NAME = "Jarvis"

# Speed for straight-line ETA (km/h). MVP uses one mode for all lanes.
SPEED_KMH = 700

# Column mappings from your CSVs -> normalized schema
# inventory.csv has: Stock, Product, Plant
INVENTORY_MAP = {
    "Stock": "on_hand",
    "Product": "sku",
    "Plant": "loc_id",
}

# Required columns after loading
PLANTS_REQUIRED = ["loc_id", "lat", "lon"]
ORDERS_REQUIRED = ["order_id", "customer_id", "sku", "qty", "need_by_ts_utc", "dest_loc_id"]

# SLA target
SLA_ON_TIME_TARGET = 0.95  # 95%
