import os, json, requests
from dotenv import load_dotenv
load_dotenv("/Users/philipbergman/Documents/Coding_Projects/Mirror-Market/.env")
p = {"key": os.environ["USDA_API_KEY"], "source_desc": "SURVEY", "commodity_desc": "SOYBEANS",
     "short_desc": "SOYBEANS - PRODUCTION, MEASURED IN BU", "agg_level_desc": "COUNTY", "state_alpha": "IA",
     "year__GE": "2020", "format": "JSON"}
r = requests.get("https://quickstats.nass.usda.gov/api/api_GET/", params=p, timeout=60)
r.raise_for_status()
d = r.json()["data"]
rows = [{"year": int(x["year"]), "fips": x["state_fips_code"] + x["county_code"], "county": x["county_name"],
         "bu": x["Value"]} for x in d if x["reference_period_desc"] == "YEAR"]
json.dump(rows, open("nass_ia.json", "w"))
print(len(rows), sorted({x["year"] for x in rows}))
