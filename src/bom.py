import pandas as pd

def load_bom(material_component_path="data/material_component.csv",
             alt_small_path="data/material_component_small.csv"):
    try:
        bom = pd.read_csv(material_component_path)
    except Exception:
        bom = pd.read_csv(alt_small_path)
    # normalize expected column names
    lower = {c.lower(): c for c in bom.columns}
    comp = next((c for c in bom.columns if "component" in c.lower()), None)
    mat  = next((c for c in bom.columns if "material"  in c.lower() or "product" in c.lower()), None)
    if not comp or not mat:
        raise ValueError("BOM file needs columns for component and material/product")
    return bom.rename(columns={comp:"component", mat:"material"})

def products_using(component_code: str, bom_df: pd.DataFrame):
    return sorted(bom_df.loc[bom_df["component"] == component_code, "material"].astype(str).unique().tolist())
