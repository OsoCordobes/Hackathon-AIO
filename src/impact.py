import pandas as pd
def affected_orders_if_missing(sku: str, orders: pd.DataFrame) -> pd.DataFrame:
    cols = ["order_id","customer_id","sku","qty","dest_loc_id","need_by_ts_utc"]
    return orders.loc[orders["sku"] == sku, cols].copy()
