import pandas as pd

df = pd.read_csv("https://raw.githubusercontent.com/David-GC-5403/Bus-Geolocalization/refs/heads/Pruebas/bueno/Program/stops_info.csv")
print(df)

id_ruta_ida = "5_395_90" 
id_ruta_vuelta = "5_410_90" 

data_ida = df[df["trip_id"] == id_ruta_ida]
data_vuelta = df[df["trip_id"] == id_ruta_vuelta]

print(data_ida)
'''
seq_ida = data_ida["stop_id"]

coords_ida = df_stops[df_stops["stop_id"].isin(seq_ida)]["stop_lat"]
coords_ida = pd.Categorical(coords_ida, categories=df_stops["stop_lat"])

print(coords_ida)

'''