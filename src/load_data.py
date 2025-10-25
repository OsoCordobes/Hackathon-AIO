# src/load_data.py
from __future__ import annotations
import pandas as pd

# ---------- small helpers ----------
def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={c: c.strip().lower().replace(" ", "_") for c in df.columns})

def _pick(cols, *cands):
    s = {c.lower(): c for c in cols}
    for c in cands:
        if c in s:
            return s[c]
    return None

# ---------- inventory ----------
def load_inventory(path: str) -> pd.DataFrame:
    """
    -> columns: sku, loc_id, on_hand
    """
    df = _norm_cols(pd.read_csv(path))

    sku = _pick(df.columns, "sku","material","material_id","product","product_id","item","item_id")
    loc = _pick(df.columns, "loc_id","location","location_id","plant","plant_id","site","warehouse","wh_id")
    qty = _pick(df.columns, "on_hand","qty","quantity","stock","stock_qty","available","inventory","onhand")

    missing = [k for k,v in {"sku":sku,"loc_id":loc,"on_hand":qty}.items() if v is None]
    if missing:
        raise ValueError(f"inventory: missing {missing}. got {list(df.columns)}")

    out = df[[sku,loc,qty]].copy()
    out.columns = ["sku","loc_id","on_hand"]
    out["sku"] = out["sku"].astype(str)
    out["loc_id"] = out["loc_id"].astype(str)
    out["on_hand"] = pd.to_numeric(out["on_hand"], errors="coerce").fillna(0).clip(lower=0)

    out = out.groupby(["sku","loc_id"], as_index=False)["on_hand"].sum()
    return out

# ---------- plants ----------
def load_plants(path: str) -> pd.DataFrame:
    """
    -> columns: loc_id, lat, lng
    """
    df = _norm_cols(pd.read_csv(path))

    loc = _pick(df.columns, "loc_id","plant","plant_id","location","location_id","site")
    lat = _pick(df.columns, "lat","latitude","lat_dd")
    lng = _pick(df.columns, "lng","lon","long","longitude","lng_dd")

    missing = [k for k,v in {"loc_id":loc,"lat":lat,"lng":lng}.items() if v is None]
    if missing:
        raise ValueError(f"plants: missing {missing}. got {list(df.columns)}")

    out = df[[loc,lat,lng]].copy()
    out.columns = ["loc_id","lat","lng"]
    out["loc_id"] = out["loc_id"].astype(str)
    out["lat"] = pd.to_numeric(out["lat"], errors="coerce")
    out["lng"] = pd.to_numeric(out["lng"], errors="coerce")
    return out.dropna(subset=["lat","lng"])

# ---------- orders ----------
def load_orders(path: str) -> pd.DataFrame:
    """
    -> columns: order_id, customer_id, sku, qty, dest_loc_id, need_by_ts_utc (tz-aware)
    """
    df = _norm_cols(pd.read_csv(path))

    oid = _pick(df.columns, "order_id","order","id")
    cust = _pick(df.columns, "customer_id","customer","client_id","client")
    sku  = _pick(df.columns, "sku","material","product","item","product_id","material_id","item_id")
    qty  = _pick(df.columns, "qty","quantity","order_qty","units")
    dest = _pick(df.columns, "dest_loc_id","destination","plant","ship_to","shipto","location_id","loc_id")
    due  = _pick(df.columns, "need_by_ts_utc","need_by","need_by_date","promise_date","due","due_ts","eta")

    missing = [k for k,v in {"order_id":oid,"customer_id":cust,"sku":sku,"qty":qty,"dest_loc_id":dest,"need_by_ts_utc":due}.items() if v is None]
    if missing:
        raise ValueError(f"orders: missing {missing}. got {list(df.columns)}")

    out = df[[oid,cust,sku,qty,dest,due]].copy()
    out.columns = ["order_id","customer_id","sku","qty","dest_loc_id","need_by_ts_utc"]
    out["order_id"] = out["order_id"].astype(str)
    out["customer_id"] = out["customer_id"].astype(str)
    out["sku"] = out["sku"].astype(str)
    out["dest_loc_id"] = out["dest_loc_id"].astype(str)
    out["qty"] = pd.to_numeric(out["qty"], errors="coerce").fillna(0).clip(lower=0).astype(int)
    out["need_by_ts_utc"] = pd.to_datetime(out["need_by_ts_utc"], utc=True, errors="coerce")
    return out.dropna(subset=["need_by_ts_utc"])

# ---------- plant material (lead time) ----------
def load_plant_material(path: str) -> pd.DataFrame:
    """
    -> columns: loc_id, sku, lead_time_h
    """
    df = _norm_cols(pd.read_csv(path))

    loc = _pick(df.columns, "loc_id","plant","plant_id","location_id","site")
    sku = _pick(df.columns, "sku","material","material_id","product","product_id","item","item_id")
    lt  = _pick(df.columns, "lead_time_h","leadtime_h","lead_time_hours","lead_time","lt_h","leadtime")

    missing = [k for k,v in {"loc_id":loc,"sku":sku,"lead_time_h":lt}.items() if v is None]
    if missing:
        raise ValueError(f"plant_material: missing {missing}. got {list(df.columns)}")

    out = df[[loc,sku,lt]].copy()
    out.columns = ["loc_id","sku","lead_time_h"]
    out["loc_id"] = out["loc_id"].astype(str)
    out["sku"] = out["sku"].astype(str)
    out["lead_time_h"] = pd.to_numeric(out["lead_time_h"], errors="coerce").fillna(float("inf"))
    return out

# ---------- BOM (optional) ----------
def load_bom(path_main: str, path_small: str | None = None) -> pd.DataFrame:
    """
    -> columns: parent_sku, child_sku, usage_qty
    Picks the first file that exists/loads.
    """
    import os
    path = path_main if os.path.exists(path_main) else path_small
    if not path or not os.path.exists(path):
        # return empty BOM gracefully
        return pd.DataFrame(columns=["parent_sku","child_sku","usage_qty"])

    df = _norm_cols(pd.read_csv(path))
    parent = _pick(df.columns, "parent_sku","parent","fg","product","product_id")
    child  = _pick(df.columns, "child_sku","child","component","component_id","material","material_id")
    use    = _pick(df.columns, "usage_qty","qty","quantity","per_parent","consumption")

    missing = [k for k,v in {"parent_sku":parent,"child_sku":child,"usage_qty":use}.items() if v is None]
    if missing:
        raise ValueError(f"bom: missing {missing}. got {list(df.columns)}")

    out = df[[parent,child,use]].copy()
    out.columns = ["parent_sku","child_sku","usage_qty"]
    out["parent_sku"] = out["parent_sku"].astype(str)
    out["child_sku"] = out["child_sku"].astype(str)
    out["usage_qty"] = pd.to_numeric(out["usage_qty"], errors="coerce").fillna(1).astype(int).clip(lower=1)
    return out
