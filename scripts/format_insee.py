import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_INSEE = BASE_DIR / "data" / "raw" / "insee" / "Insee2023.xlsx"
FORMATTED_INSEE = BASE_DIR / "data" / "formatted" / "insee"

FORMATTED_INSEE.mkdir(parents=True, exist_ok=True)

df = pd.read_excel(
    RAW_INSEE,
    sheet_name="2023",
    header=7
)

df.columns = [c.strip() for c in df.columns]

df = df[["COM", "NCC", "PMUN23"]]

df = df.rename(columns={
    "COM": "code_commune",
    "NCC": "commune_name",
    "PMUN23": "population"
})

df = df.dropna()

output_path = FORMATTED_INSEE / "population_commune_2023.parquet"
df.to_parquet(output_path, index=False)

print("INSEE 2023 formatted file saved:", output_path)
