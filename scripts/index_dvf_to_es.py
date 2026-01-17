import csv
import requests

ES_INDEX = "dvf_insee_2023"
ES_URL = f"http://localhost:9200/{ES_INDEX}/_doc"

csv_path = "../dvf_insee_2023_es.csv"

with open(csv_path, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        doc = {
            "date_mutation": row["date_mutation"],
            "valeur_fonciere": float(row["valeur_fonciere"]),
            "commune": row["commune"],
            "code_departement": row["code_departement"],
            "location": {
                "lat": float(row["latitude"]),
                "lon": float(row["longitude"])
            }
        }

        r = requests.post(ES_URL, json=doc)
        if r.status_code not in (200, 201):
            print("Error:", r.text)
