import pandas as pd

def affected_orders_if_missing(sku: str, orders: pd.DataFrame) -> pd.DataFrame:
    return orders.loc[orders["sku"] == sku,
                      ["order_id","customer_id","sku","qty","dest_loc_id","need_by_ts_utc"]].copy()
