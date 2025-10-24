# src/load_data.py
# Normalize CSVs so Jarvis can cross-reference orders, plants, and stock.

import pandas as pd
from .contracts import INVENTORY_MAP

def _clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df

def load_inventory(path: str = "data/inventory.csv") -> pd.DataFrame:
    df = pd.read_csv(path)
    df = _clean_cols(df).rename(columns=INVENTORY_MAP)

    # Defaults required by planner
    df["reserved"] = 0
    df["available_from_ts_utc"] = pd.Timestamp.utcnow()

    out = df[["loc_id", "sku", "on_hand", "reserved", "available_from_ts_utc"]].copy()
    out["loc_id"] = out["loc_id"].astype(str)
    out["sku"] = out["sku"].astype(str)
    out["on_hand"] = pd.to_numeric(out["on_hand"], errors="coerce").fillna(0).astype(int)
    return out

def load_plants(path: str = "data/plants.csv") -> pd.DataFrame:
    df = pd.read_csv(path)
    df = _clean_cols(df)
    lower = {c.lower(): c for c in df.columns}

    def pick(candidates):
        for k in candidates:
            if k in lower:
                return lower[k]
        raise ValueError(f"plants.csv missing any of: {candidates}")

    # Accept many aliases
    id_col  = pick(["loc_id", "plant", "plant_id", "site", "location", "id"])
    lat_col = pick(["lat", "latitude", "y"])
    lon_col = pick(["lon", "longitude", "long", "lng", "x"])

    out = df[[id_col, lat_col, lon_col]].copy()
    out.columns = ["loc_id", "lat", "lon"]
    out["loc_id"] = out["loc_id"].astype(str)
    out["lat"] = pd.to_numeric(out["lat"], errors="coerce")
    out["lon"] = pd.to_numeric(out["lon"], errors="coerce")
    return out

def load_orders(path: str = "data/orders.csv") -> pd.DataFrame:
    df = pd.read_csv(path)
    df = _clean_cols(df)
    lower = {c.lower(): c for c in df.columns}

    # SKU (strict)
    sku_col = next((lower[k] for k in ["sku", "product", "material", "component"] if k in lower), None)
    if not sku_col:
        raise ValueError("orders.csv needs a SKU-like column (sku/product/material/component).")
    df["sku"] = df[sku_col].astype(str)

    # Quantity
    qty_col = next((lower[k] for k in ["qty", "quantity", "order_qty"] if k in lower), None)
    df["qty"] = pd.to_numeric(df[qty_col], errors="coerce").fillna(1).astype(int) if qty_col else 1

    # Destination plant/location
    dest_col = next((lower[k] for k in ["dest_loc_id","destination","dest","plant","location","site"] if k in lower), None)
    df["dest_loc_id"] = df[dest_col].astype(str) if dest_col else "DEST_UNKNOWN"

    # Customer ID
    cust_col = next((lower[k] for k in ["customer_id","customer","cust_id"] if k in lower), None)
    df["customer_id"] = df[cust_col].astype(str) if cust_col else df["dest_loc_id"]

    # Order ID
    oid_col = next((lower[k] for k in ["order_id","ord_id","id"] if k in lower), None)
    df["order_id"] = df[oid_col].astype(str) if oid_col else pd.RangeIndex(1, len(df)+1).astype(str)

    # Need-by / due (UTC) -> always a Series
    nb_col = next((lower[k] for k in lower if any(s in k for s in ["need", "due", "deliver", "require"])), None)
    default_nb = pd.Timestamp.utcnow() + pd.Timedelta(hours=24)
    if nb_col:
        nb_series = pd.to_datetime(df[nb_col], utc=True, errors="coerce")
        df["need_by_ts_utc"] = nb_series.fillna(default_nb)
    else:
        df["need_by_ts_utc"] = pd.Series(default_nb, index=df.index)
    df["need_by_ts_utc"] = pd.to_datetime(df["need_by_ts_utc"], utc=True, errors="coerce")

    out = df[["order_id","customer_id","sku","qty","need_by_ts_utc","dest_loc_id"]].copy()
    out["order_id"] = out["order_id"].astype(str)
    out["customer_id"] = out["customer_id"].astype(str)
    out["dest_loc_id"] = out["dest_loc_id"].astype(str)
    return out
