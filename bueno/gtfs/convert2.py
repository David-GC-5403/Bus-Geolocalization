import csv
import json
from collections import defaultdict

# Archivos de entrada
routes_file = "routes.txt"
trips_file = "trips.txt"
shapes_file = "shapes.txt"

# Leer trips.txt → relaciona route_id con shape_id
route_to_shape = {}
with open(trips_file, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        route_id = row["route_id"]
        shape_id = row["shape_id"]
        # Nos quedamos con la primera shape_id por route_id
        if route_id not in route_to_shape:
            route_to_shape[route_id] = shape_id

# Leer shapes.txt → agrupar coordenadas por shape_id
shapes = defaultdict(list)
with open(shapes_file, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        shape_id = row["shape_id"]
        lat = float(row["shape_pt_lat"])
        lon = float(row["shape_pt_lon"])
        pt_sequence = int(row["shape_pt_sequence"])
        shapes[shape_id].append((pt_sequence, [lon, lat]))

# Ordenar puntos de cada shape por secuencia
for shape_id in shapes:
    shapes[shape_id].sort()  # ordena por shape_pt_sequence
    shapes[shape_id] = [coord for _, coord in shapes[shape_id]]

# Leer routes.txt → propiedades de cada ruta
routes = {}
with open(routes_file, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        routes[row["route_id"]] = row

# Construir GeoJSON
features = []
for route_id, shape_id in route_to_shape.items():
    if shape_id not in shapes:
        continue
    geometry = {
        "type": "LineString",
        "coordinates": shapes[shape_id]
    }
    properties = routes.get(route_id, {"route_id": route_id})
    features.append({
        "type": "Feature",
        "geometry": geometry,
        "properties": properties
    })

geojson = {
    "type": "FeatureCollection",
    "features": features
}

# Guardar GeoJSON
with open("routes.geojson", "w", encoding="utf-8") as f:
    json.dump(geojson, f, ensure_ascii=False, indent=2)

print("✅ Archivo routes.geojson generado correctamente.")
