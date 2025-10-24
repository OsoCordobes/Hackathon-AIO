import pandas as pd
from .geo import haversine_km, transit_hours

def plan_for_delay(delay_event, inv, plants, orders):
    """
    delay_event = {"sku": str, "qty_unavailable": int, "origin_loc_id": str}
    Strict SKU match. No partials. No multi-source.
    """
    sku = delay_event["sku"]

    # 1) Affected orders
    at_risk = orders[orders["sku"] == sku].copy()
    if at_risk.empty:
        return pd.DataFrame(), {"on_time_pct": 100.0, "late_orders": 0}

    # 2) Candidate single-source plants with stock now
    candidates = inv[(inv["sku"] == sku) & (inv["on_hand"] > 0)].copy()
    if candidates.empty:
        at_risk["source_loc_id"] = None
        at_risk["ETA_ts"] = pd.NaT
        at_risk["lateness_h"] = 1e9
        return at_risk, _kpi(at_risk)

    # 3) Join coordinates
    p = plants.rename(columns={"loc_id": "_loc"})
    cand = candidates.merge(p, left_on="loc_id", right_on="_loc", how="left") \
                     .rename(columns={"lat": "src_lat", "lon": "src_lon"}) \
                     .drop(columns=["_loc"])
    dest = at_risk.merge(p, left_on="dest_loc_id", right_on="_loc", how="left") \
                  .rename(columns={"lat": "dst_lat", "lon": "dst_lon"}) \
                  .drop(columns=["_loc"])

    # 4) Evaluate each order and pick best source
    plans = []
    now = pd.Timestamp.utcnow()
    for _, o in dest.iterrows():
        feasible = cand[cand["on_hand"] >= o["qty"]].copy()
        if feasible.empty:
            plans.append({**o.to_dict(), "source_loc_id": None, "ETA_ts": pd.NaT, "lateness_h": 1e9})
            continue

        feasible["km"] = haversine_km(feasible["src_lat"], feasible["src_lon"], o["dst_lat"], o["dst_lon"])
        feasible["eta"] = now + pd.to_timedelta(transit_hours(feasible["km"]), unit="h")
        feasible["lateness_h"] = ((feasible["eta"] - o["need_by_ts_utc"]).dt.total_seconds() / 3600).clip(lower=0)

        best = feasible.sort_values(["lateness_h", "km"]).iloc[0]
        plans.append({
            **o.to_dict(),
            "source_loc_id": best["loc_id"],
            "ETA_ts": best["eta"],
            "lateness_h": best["lateness_h"],
        })

    out = pd.DataFrame(plans)
    return out, _kpi(out)

def _kpi(df):
    total = len(df)
    on_time = int((df["lateness_h"] <= 1e-6).sum()) if total else 0
    on_time_pct = 100.0 * on_time / total if total else 100.0
    return {"on_time_pct": round(on_time_pct, 2), "late_orders": int(total - on_time)}
