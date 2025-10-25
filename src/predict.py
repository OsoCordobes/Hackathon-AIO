import pandas as pd
def coverage_alerts(inv: pd.DataFrame, orders: pd.DataFrame, horizon_days: int = 7) -> pd.DataFrame:
    now = pd.Timestamp.utcnow()
    window = orders[orders["need_by_ts_utc"] <= now + pd.Timedelta(days=horizon_days)]
    demand = window.groupby("sku")["qty"].sum().rename("demand_in_window")
    stock  = inv.groupby("sku")["on_hand"].sum().rename("on_hand")
    df = demand.to_frame().join(stock, how="left").fillna({"on_hand":0})
    df["gap"]  = df["on_hand"] - df["demand_in_window"]
    df["risk"] = df["gap"] < 0
    return df.sort_values("gap")
