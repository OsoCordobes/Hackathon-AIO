# run_mvp.py
import argparse, json
from pathlib import Path
from src.advisor import plan_recovery

def main():
    ap = argparse.ArgumentParser(description="Danfoss MVP advisor 100% basado en CSV")
    ap.add_argument("--data-dir", required=True, help="Carpeta con inventory.csv, material_component_small.csv, orders.csv, plant_material.csv, plants.csv")
    ap.add_argument("--missing-product", required=True)
    ap.add_argument("--missing-qty", type=int, required=True)
    ap.add_argument("--origin-plant", required=True)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    out = plan_recovery(
        data_dir=Path(args.data_dir),
        missing_product=args.missing_product,
        missing_qty=args.missing_qty,
        origin_plant=args.origin_plant,
        verbose=args.verbose
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
