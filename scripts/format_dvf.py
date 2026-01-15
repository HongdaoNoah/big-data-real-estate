import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

RAW_DVF = BASE_DIR / "data" / "raw" / "dvf" / "ValeursFoncieres-2023.txt"
FORMATTED_DVF_DIR = BASE_DIR / "data" / "formatted" / "dvf"
FORMATTED_DVF_DIR.mkdir(parents=True, exist_ok=True)

print("Loading DVF 2023... (big file)")
print("RAW_DVF =", RAW_DVF)

# Preview columns
preview = pd.read_csv(RAW_DVF, sep="|", nrows=1, low_memory=False)
print("DVF columns sample:", preview.columns.tolist())

# Read needed columns (IMPORTANT: department + commune)
cols = ["Code departement", "Code commune", "Valeur fonciere", "Type local", "Date mutation"]
df = pd.read_csv(RAW_DVF, sep="|", usecols=cols, low_memory=False)
print("Rows after read_csv:", len(df))

# Drop NA for keys/price
df = df.dropna(subset=["Valeur fonciere", "Code departement", "Code commune"])
print("Rows after dropna:", len(df))

# Clean Type local
df["Type local"] = (
    df["Type local"]
    .astype(str)
    .str.strip()
    .str.lower()
)
print("Top Type local values:\n", df["Type local"].value_counts().head(10))

df = df[df["Type local"].isin(["appartement", "maison"])]
print("Rows after filtering Type local:", len(df))

# Convert price (comma -> dot)
df["Valeur fonciere"] = (
    df["Valeur fonciere"]
    .astype(str)
    .str.replace(",", ".", regex=False)
)
df["Valeur fonciere"] = pd.to_numeric(df["Valeur fonciere"], errors="coerce")
df = df.dropna(subset=["Valeur fonciere"])
print("Rows after numeric conversion:", len(df))

# Date (optional)
df["Date mutation"] = pd.to_datetime(df["Date mutation"], errors="coerce")

# Build INSEE commune code = dep + commune(3 digits)
dep = df["Code departement"].astype(str).str.strip()
com = df["Code commune"].astype(str).str.strip().str.zfill(3)
df["code_commune"] = dep + com

print("Sample code_commune:", df["code_commune"].head(5).tolist())
print("Unique communes before agg:", df["code_commune"].nunique())

# Aggregate
df_commune = (
    df.groupby("code_commune", as_index=False)
      .agg(
          avg_price=("Valeur fonciere", "mean"),
          transactions=("Valeur fonciere", "count")
      )
)

print("Aggregated communes:", len(df_commune))
print(df_commune.head())

# Save
output_path = FORMATTED_DVF_DIR / "price_commune_2023.parquet"
df_commune.to_parquet(output_path, index=False)
print("DVF formatted file saved:", output_path)

