from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BASE_DIR = Path(__file__).resolve().parent.parent

FORMATTED_INSEE = str(BASE_DIR / "data" / "formatted" / "insee" / "population_commune_2023.parquet")
FORMATTED_DVF = str(BASE_DIR / "data" / "formatted" / "dvf" / "price_commune_2023.parquet")

USAGE_DIR = BASE_DIR / "data" / "usage"
USAGE_DIR.mkdir(parents=True, exist_ok=True)

OUT_DIR = str(USAGE_DIR / "real_estate_commune_2023_csv_tmp")  
OUT_CSV = USAGE_DIR / "real_estate_commune_2023.csv"


def main():
    spark = (
        SparkSession.builder
        .appName("combine_dvf_insee_2023")
        .getOrCreate()
    )

    print("Loading INSEE parquet:", FORMATTED_INSEE)
    df_insee = spark.read.parquet(FORMATTED_INSEE)

    print("Loading DVF parquet:", FORMATTED_DVF)
    df_dvf = spark.read.parquet(FORMATTED_DVF)


    df_insee = df_insee.withColumn("code_commune", F.lpad(F.col("code_commune").cast("string"), 5, "0"))
    df_dvf = df_dvf.withColumn("code_commune", F.lpad(F.col("code_commune").cast("string"), 5, "0"))


    df_insee = df_insee.dropDuplicates(["code_commune"])
    df_dvf = df_dvf.dropDuplicates(["code_commune"])

    df = df_dvf.join(df_insee, on="code_commune", how="left")

  
    df = df.withColumn("department", F.substring(F.col("code_commune"), 1, 2))


    keep_cols = ["code_commune", "avg_price", "transactions", "commune_name", "population", "department"]
    df = df.select(*[c for c in keep_cols if c in df.columns])


    print("Writing CSV to temp dir:", OUT_DIR)
    (
        df.coalesce(1)
          .write.mode("overwrite")
          .option("header", "true")
          .option("encoding", "UTF-8")
          .csv(OUT_DIR)
    )

 
    import os, shutil, glob
    part_files = glob.glob(os.path.join(OUT_DIR, "part-*.csv"))
    if not part_files:
        raise RuntimeError(f"No part csv found in {OUT_DIR}")
    part = part_files[0]

 
    if OUT_CSV.exists():
        OUT_CSV.unlink()

    shutil.move(part, str(OUT_CSV))

    shutil.rmtree(OUT_DIR, ignore_errors=True)

    print("DONE. CSV saved to:", OUT_CSV)

    spark.stop()


if __name__ == "__main__":
    main()
