# advisor.py
# Requisitos: Python 3.10+, pandas
# Uso CLI (ejemplos de flags, sin datos inventados):
#   python advisor.py --data-dir data --missing-product <ID> --missing-qty <N> --origin-plant <PLANT>
# Opcionales:
#   --report-json para ver salida en JSON estructurado
#   --verbose para trazas de decisión

import argparse
import json
import math
from pathlib import Path
import pandas as pd

def haversine_km(lat1, lon1, lat2, lon2):
    # Distancia geodésica en km
    R = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlmb/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def load_data(data_dir: Path):
    inv = pd.read_csv(data_dir / "inventory.csv")  # columnas: Stock, Product, Plant
    bom = pd.read_csv(data_dir / "material_component_small.csv")  # columnas: Parent, Child
    orders = pd.read_csv(data_dir / "orders.csv")  # columnas: Product, plant, order_key, sold_to_party
    plant_mat = pd.read_csv(data_dir / "plant_material.csv")  # columnas: Product, Plant, lead_time
    plants = pd.read_csv(data_dir / "plants.csv")  # columnas: Plant, latitude, longitude

    # Normalización mínima de columnas esperadas
    for df, cols in [
        (inv, ["Stock", "Product", "Plant"]),
        (bom, ["Parent", "Child"]),
        (orders, ["Product", "plant", "order_key", "sold_to_party"]),
        (plant_mat, ["Product", "Plant", "lead_time"]),
        (plants, ["Plant", "latitude", "longitude"]),
    ]:
        missing = [c for c in cols if c not in df.columns]
        if missing:
            raise ValueError(f"Faltan columnas {missing} en {df}")

    # Tipos
    inv["Stock"] = pd.to_numeric(inv["Stock"], errors="coerce").fillna(0).astype(int)
    plant_mat["lead_time"] = pd.to_numeric(plant_mat["lead_time"], errors="coerce")
    # Alineamos nombres de columnas de planta entre tablas
    orders = orders.rename(columns={"plant": "Plant"})
    return inv, bom, orders, plant_mat, plants

def build_reverse_bom(bom: pd.DataFrame):
    # Mapa Child -> set(Parents)
    rev = {}
    for _, r in bom.iterrows():
        p, c = str(r["Parent"]), str(r["Child"])
        rev.setdefault(c, set()).add(p)
    return rev

def ancestors_products(missing_product: str, bom: pd.DataFrame):
    # Todos los padres que dependen directa o indirectamente del componente faltante
    rev = build_reverse_bom(bom)
    seen = set()
    stack = [missing_product]
    while stack:
        node = stack.pop()
        for parent in rev.get(node, []):
            if parent not in seen:
                seen.add(parent)
                stack.append(parent)
    return seen  # conjunto de Parents afectados

def affected_orders_and_customers(affected_products: set, orders: pd.DataFrame):
    if not affected_products:
        return orders.iloc[0:0].copy(), []
    mask = orders["Product"].astype(str).isin(affected_products)
    aff = orders[mask].copy()
    customers = sorted(aff["sold_to_party"].astype(str).unique().tolist())
    return aff, customers

def plants_with_stock(inv: pd.DataFrame, product: str, exclude_plants=None):
    df = inv[inv["Product"].astype(str) == str(product)].copy()
    if exclude_plants:
        df = df[~df["Plant"].astype(str).isin(set(exclude_plants))]
    df = df[df["Stock"] > 0]
    return df

def nearest_plants_with_stock(inv: pd.DataFrame, plants: pd.DataFrame, product: str, origin_plant: str):
    origin = plants[plants["Plant"].astype(str) == str(origin_plant)]
    if origin.empty:
        # Si no conocemos lat/lon del origen, devolvemos plantas con stock sin ordenar
        stock_df = plants_with_stock(inv, product)
        merged = stock_df.merge(plants, on="Plant", how="left")
        merged["distance_km"] = float("nan")
        return merged.sort_values(["distance_km", "Stock"], na_position="last", ascending=[True, False])

    o_lat, o_lon = float(origin.iloc[0]["latitude"]), float(origin.iloc[0]["longitude"])
    stock_df = plants_with_stock(inv, product, exclude_plants=[origin_plant]).merge(plants, on="Plant", how="left")
    stock_df["distance_km"] = stock_df.apply(
        lambda r: haversine_km(o_lat, o_lon, float(r["latitude"]), float(r["longitude"]))
        if pd.notnull(r["latitude"]) and pd.notnull(r["longitude"]) else float("inf"),
        axis=1
    )
    return stock_df.sort_values(["distance_km", "Stock"], ascending=[True, False])

def best_production_option(plant_mat: pd.DataFrame, plants: pd.DataFrame, product: str):
    # Elegimos la planta con menor lead_time disponible para ese producto
    pm = plant_mat[plant_mat["Product"].astype(str) == str(product)].copy()
    pm = pm.dropna(subset=["lead_time"])
    if pm.empty:
        return None
    best = pm.sort_values("lead_time", ascending=True).iloc[0]
    plant_row = plants[plants["Plant"].astype(str) == str(best["Plant"])]
    lat = float(plant_row.iloc[0]["latitude"]) if not plant_row.empty and pd.notnull(plant_row.iloc[0]["latitude"]) else None
    lon = float(plant_row.iloc[0]["longitude"]) if not plant_row.empty and pd.notnull(plant_row.iloc[0]["longitude"]) else None
    return {
        "Plant": str(best["Plant"]),
        "lead_time": float(best["lead_time"]),
        "latitude": lat,
        "longitude": lon,
    }

def allocate_from_alt_plants(stock_df_sorted: pd.DataFrame, need_qty: int):
    # Devuelve asignaciones [{Plant, ship_qty, remaining_need}]
    plan = []
    remaining = int(need_qty)
    for _, r in stock_df_sorted.iterrows():
        if remaining <= 0:
            break
        available = int(r["Stock"])
        ship = min(available, remaining)
        if ship > 0:
            plan.append({
                "Plant": str(r["Plant"]),
                "available_qty": available,
                "ship_qty": ship,
                "distance_km": float(r["distance_km"]) if "distance_km" in r and pd.notnull(r["distance_km"]) else None
            })
            remaining -= ship
    return plan, remaining

def plan_recovery(
    data_dir: Path,
    missing_product: str,
    missing_qty: int,
    origin_plant: str,
    verbose: bool=False
):
    inv, bom, orders, plant_mat, plants = load_data(data_dir)

    # 1) Propagar impacto: padres afectados por el componente faltante
    parents = ancestors_products(missing_product, bom)

    # 2) Órdenes y clientes afectados
    aff_orders, customers = affected_orders_and_customers(parents, orders)

    # 3) Reubicación de stock desde otras plantas
    stock_candidates = nearest_plants_with_stock(inv, plants, missing_product, origin_plant)
    reallocate_plan, remaining_need = allocate_from_alt_plants(stock_candidates, missing_qty)

    # 4) Si no alcanza, estimar producción
    production_option = None
    if remaining_need > 0:
        production_option = best_production_option(plant_mat, plants, missing_product)

    # 5) Formar reporte
    result = {
        "input": {
            "missing_product": str(missing_product),
            "missing_qty": int(missing_qty),
            "origin_plant": str(origin_plant),
        },
        "impact": {
            "affected_parent_products": sorted(list(parents)),
            "affected_orders_rows": len(aff_orders),
            "affected_customers": customers,
            # Para mantenerlo 100% basado en CSV: devolvemos las filas crudas si el usuario lo desea inspeccionar
            "orders_sample": aff_orders.head(50).to_dict(orient="records")  # limitar tamaño
        },
        "recovery": {
            "reallocate_plan": reallocate_plan,          # asignaciones por planta con ship_qty
            "remaining_need_after_reallocation": int(remaining_need),
            "production_option_if_needed": production_option  # planta con menor lead_time
        }
    }
    if verbose:
        result["debug"] = {
            "stock_candidates_preview": stock_candidates.head(50).to_dict(orient="records")
        }
    return result

def parse_args():
    ap = argparse.ArgumentParser(description="Supply Chain Advisor basado 100% en CSV.")
    ap.add_argument("--data-dir", type=str, required=True, help="Carpeta que contiene los CSV.")
    ap.add_argument("--missing-product", type=str, required=True, help="ID del producto/componente faltante.")
    ap.add_argument("--missing-qty", type=int, required=True, help="Cantidad faltante.")
    ap.add_argument("--origin-plant", type=str, required=True, help="Planta que reporta la falta.")
    ap.add_argument("--report-json", action="store_true", help="Imprime el reporte en JSON.")
    ap.add_argument("--verbose", action="store_true", help="Incluye trazas de soporte.")
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    out = plan_recovery(
        data_dir=Path(args.data_dir),
        missing_product=args.missing_product,
        missing_qty=args.missing_qty,
        origin_plant=args.origin_plant,
        verbose=args.verbose
    )
    if args.report_json:
        print(json.dumps(out, ensure_ascii=False, indent=2))
    else:
        # Resumen breve legible
        imp = out["impact"]
        rec = out["recovery"]
        print("=== IMPACTO ===")
        print(f"Productos padre afectados: {len(imp['affected_parent_products'])}")
        print(f"Órdenes afectadas: {imp['affected_orders_rows']}")
        print(f"Clientes afectados: {len(imp['affected_customers'])}")
        print("=== PLAN ===")
        if rec["reallocate_plan"]:
            print("Reubicación desde plantas:")
            for a in rec["reallocate_plan"]:
                d = f" ({a['distance_km']:.1f} km)" if a.get("distance_km") is not None else ""
                print(f"- {a['Plant']}: enviar {a['ship_qty']} de {a['available_qty']} disponibles{d}")
        else:
            print("No hay stock alternativo disponible.")
        rem = rec["remaining_need_after_reallocation"]
        print(f"Faltante tras reubicación: {rem}")
        if rem > 0 and rec["production_option_if_needed"]:
            po = rec["production_option_if_needed"]
            print(f"Sugerir producción en {po['Plant']} con lead_time={po['lead_time']}")
        elif rem > 0:
            print("Sin opción de producción conocida en plant_material.csv")
