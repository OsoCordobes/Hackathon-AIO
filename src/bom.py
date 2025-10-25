import pandas as pd

_COMPONENT_CANDS = ["component","component_id","componentcode","comp","comp_id","child","child_id","child_material","child_item","subcomponent"]
_MATERIAL_CANDS  = ["material","material_id","product","product_id","fg","finished_good","parent","parent_id","header_material","header_item"]

def _pick(lower_map, cands):
    for k in cands:
        if k in lower_map: return lower_map[k]
    return None

def load_bom(path="data/material_component.csv", alt="data/material_component_small.csv"):
    try:
        bom = pd.read_csv(path)
    except Exception:
        bom = pd.read_csv(alt)
    bom.columns = [c.strip() for c in bom.columns]
    lower = {c.lower(): c for c in bom.columns}
    comp = _pick(lower, _COMPONENT_CANDS)
    mat  = _pick(lower, _MATERIAL_CANDS)
    if not comp or not mat:
        raise ValueError(f"BOM missing component/material columns. Columns: {list(bom.columns)}")
    out = bom.rename(columns={comp:"component", mat:"material"})[["component","material"]].copy()
    out["component"] = out["component"].astype(str)
    out["material"]  = out["material"].astype(str)
    return out.dropna().drop_duplicates()

def products_using(component_code: str, bom_df: pd.DataFrame):
    return sorted(bom_df.loc[bom_df["component"] == str(component_code), "material"].astype(str).unique().tolist())
