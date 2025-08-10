import pandas as pd

coords_ida = []
coords_vuelta = []

df_gtfs = pd.read_csv("https://raw.githubusercontent.com/David-GC-5403/Bus-Geolocalization/refs/heads/Pruebas/bueno/Program/stops_info.csv")

id_ruta_ida = "5_395_90" 
id_ruta_vuelta = "5_410_90" 
'''
data_ida = df[df["trip_id"] == id_ruta_ida]
data_vuelta = df[df["trip_id"] == id_ruta_vuelta]

print(data_ida)

seq_ida = data_ida["stop_id"]

coords_ida.append(data_ida["stop_lat"].tolist())
coords_ida.append(data_ida["stop_lon"].tolist())

coords_vuelta.append(data_vuelta["stop_lat"].tolist())
coords_vuelta.append(data_vuelta["stop_lon"].tolist())

print(coords_ida)
print(coords_vuelta)
'''
    # Coordenadas de las rutas
coords_ida = []
coords_vuelta = []

data_ida = df_gtfs[df_gtfs["trip_id"] == id_ruta_ida] # Info de la ida
data_vuelta = df_gtfs[df_gtfs["trip_id"] == id_ruta_vuelta] # Info de la vuelta

seq_ida = data_ida["stop_id"].tolist() # Secuencia de paradas de la ida
seq_vuelta = data_vuelta["stop_id"] # Secuencia de paradas de la vuelta

# Guarda las coordenadas de las paradas
coords_ida.append(data_ida["stop_lat"].tolist())
coords_ida.append(data_ida["stop_lon"].tolist())

coords_ida = list(zip(data_ida["stop_lat"], data_ida["stop_lon"]))


coords_vuelta.append(data_vuelta["stop_lat"].tolist())
coords_vuelta.append(data_vuelta["stop_lon"].tolist())

#print(coords_ida[0])
print(len(coords_ida))
print(coords_ida)