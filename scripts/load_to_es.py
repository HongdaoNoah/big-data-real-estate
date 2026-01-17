import os
import sys
import math
import pandas as pd
import requests
import json


ES_URL = os.getenv("ES_URL", "http://localhost:9200")
INDEX = os.getenv("ES_INDEX", "dvf_insee_2023")
CSV_PATH = os.getenv("CSV_PATH", "dvf_insee_2023_es.csv")

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "5000"))

def es_ok():
    r = requests.get(f"{ES_URL}")
    r.raise_for_status()
    return r.json()

def create_index_if_needed(columns):
    properties = {}

    def add_if(col, mapping):
        if col in columns:
            properties[col] = mapping

    add_if("date_mutation", {"type": "date"})
    add_if("valeur_fonciere", {"type": "double"})
    add_if("surface_reelle_bati", {"type": "double"})
    add_if("surface_terrain", {"type": "double"})
    add_if("nombre_pieces_principales", {"type": "integer"})
    add_if("type_local", {"type": "keyword"})
    add_if("code_postal", {"type": "keyword"})
    add_if("code_commune", {"type": "keyword"})
    add_if("nom_commune", {"type": "keyword"})
    add_if("departement", {"type": "keyword"})
    add_if("commune", {"type": "keyword"})

    add_if("latitude", {"type": "double"})
    add_if("longitude", {"type": "double"})

    properties["location"] = {"type": "geo_point"}

    payload = {
        "mappings": {
            "properties": properties
        }
    }


    r = requests.get(f"{ES_URL}/{INDEX}")
    if r.status_code == 404:
        print(f"[INFO] Creating index: {INDEX}")
        cr = requests.put(f"{ES_URL}/{INDEX}", json=payload)
        cr.raise_for_status()
    else:
        print(f"[INFO] Index exists: {INDEX}")

def to_number(x):
    if x is None:
        return None
    if isinstance(x, (int, float)) and not (isinstance(x, float) and math.isnan(x)):
        return x
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return None
 
    s = s.replace(",", ".")
    try:
        return float(s)
    except:
        return None

def bulk_index(actions):

    data = "\n".join(actions) + "\n"
    r = requests.post(f"{ES_URL}/_bulk", data=data, headers={"Content-Type": "application/x-ndjson"})
    r.raise_for_status()
    resp = r.json()
    if resp.get("errors"):
       
        items = resp.get("items", [])
        errs = []
        for it in items:
            op = it.get("index", {})
            if op.get("error"):
                errs.append(op["error"])
                if len(errs) >= 3:
                    break
        raise RuntimeError(f"Bulk had errors, examples: {errs}")

def main():
    print("[INFO] Elasticsearch:", es_ok().get("version", {}).get("number"))
    if not os.path.exists(CSV_PATH):
        print(f"[ERROR] CSV not found: {CSV_PATH}")
        print("Set env CSV_PATH or put the file in project root.")
        sys.exit(1)


    preview = pd.read_csv(CSV_PATH, nrows=5)
    columns = list(preview.columns)
    print("[INFO] Columns:", columns[:20], "..." if len(columns) > 20 else "")
    create_index_if_needed(columns)

    total = 0
    for chunk in pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE):
        actions = []
        for _, row in chunk.iterrows():
            doc = row.to_dict()


            for k, v in list(doc.items()):
                if isinstance(v, float) and math.isnan(v):
                    doc[k] = None


            lat = doc.get("latitude")
            lon = doc.get("longitude")
            latn = to_number(lat)
            lonn = to_number(lon)
            if latn is not None and lonn is not None:
                doc["location"] = {"lat": latn, "lon": lonn}

            actions.append(f'{{"index":{{"_index":"{INDEX}"}}}}')
            actions.append(json.dumps(doc, ensure_ascii=False))


        bulk_index(actions)
        total += len(chunk)
        print(f"[INFO] Indexed: {total}")

    print(f"[DONE] Total indexed: {total}")
    print("[NEXT] Check: http://localhost:9200/_cat/indices?v")

if __name__ == "__main__":
    main()
