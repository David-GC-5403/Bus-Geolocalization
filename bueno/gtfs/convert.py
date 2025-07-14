import csv
import json

geojson = {
    "type": "FeatureCollection",
    "features": []
}

with open('stops.txt', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(row["stop_lon"]), float(row["stop_lat"])]
            },
            "properties": {
                "stop_id": row["stop_id"],
                "stop_name": row["stop_name"]
            }
        }
        geojson["features"].append(feature)

with open("stops.geojson", "w", encoding='utf-8') as outfile:
    json.dump(geojson, outfile, indent=2)
