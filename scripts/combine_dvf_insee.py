import pandas as pd
from pathlib import Path

# ----------------------------
# Paths
# ----------------------------
BASE_DIR = Path(__file__).resolve().parent.parent

FORMATTED_INSEE = BASE_DIR / "data" / "formatted" / "insee" / "population_commune_2023.parquet"
FORMATTED_DVF = BASE_DIR / "data" / "formatted" / "dvf" / "price_commune_2023.parquet"

USAGE_DIR = BASE_DIR / "data" / "usage"
USAGE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = USAGE_DIR / "real_estate_commune_2023.parquet"

# ----------------------------
# Load
# ----------------------------
print("Loading INSEE data...")
df_insee = pd.read_parquet(FORMATTED_INSEE)

print("Loading DVF data...")
df_dvf = pd.read_parquet(FORMATTED_DVF)

# ----------------------------
# Harmonize join key (IMPORTANT)
# INSEE commune code must be 5 characters with leading zeros
# ----------------------------
if "code_commune" not in df_insee.columns:
    raise ValueError(f"INSEE file missing 'code_commune'. Columns: {list(df_insee.columns)}")

if "code_commune" not in df_dvf.columns:
    raise ValueError(f"DVF file missing 'code_commune'. Columns: {list(df_dvf.columns)}")

df_insee["code_commune"] = df_insee["code_commune"].astype(str).str.strip().str.zfill(5)
df_dvf["code_commune"] = df_dvf["code_commune"].astype(str).str.strip().str.zfill(5)

# Optional: drop duplicates on key if any (keep first)
df_insee = df_insee.drop_duplicates(subset=["code_commune"])
df_dvf = df_dvf.drop_duplicates(subset=["code_commune"])

# ----------------------------
# Merge (DVF left join INSEE)
# ----------------------------
print("Merging DVF and INSEE data...")
df_combined = df_dvf.merge(df_insee, on="code_commune", how="left")

# ----------------------------
# Quick checks
# ----------------------------
print("---- Checks ----")
print("DVF rows:", len(df_dvf))
print("INSEE rows:", len(df_insee))
print("Combined rows:", len(df_combined))

missing_pop = df_combined["population"].isna().sum() if "population" in df_combined.columns else None
if missing_pop is not None:
    print("Rows with missing population after merge:", missing_pop)

# Show a small preview
print("\nPreview (first 5 rows):")
print(df_combined.head(5))

# ----------------------------
# Save
# ----------------------------
print("\nSaving combined dataset...")
df_combined.to_parquet(OUTPUT_PATH, index=False)

print("Combined file saved to:", OUTPUT_PATH)
