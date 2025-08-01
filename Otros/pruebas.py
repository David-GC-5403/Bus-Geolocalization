import pandas as pd

df_stops = pd.read_csv("https://raw.githubusercontent.com/David-GC-5403/Bus-Geolocalization/refs/heads/main/bueno/Program/paradas.csv")
stop_times = pd.read_csv("https://raw.githubusercontent.com/David-GC-5403/Bus-Geolocalization/refs/heads/main/bueno/Program/stop_times.csv")

# Vamos a hacer la ruta primero de Algeciras a los barrios
id_ruta_ida = "5_395_90" # Trip id de la primera ruta (siempre es la misma)
id_ruta_vuelta = "5_410_90" # Trip id de la ruta de vuelta

data_ida = stop_times[stop_times["trip_id"] == id_ruta_ida]
data_vuelta = stop_times[stop_times["trip_id"] == id_ruta_vuelta]

seq_ida = data_ida["stop_id"]

coords_ida = df_stops[df_stops["stop_id"].isin(seq_ida)]["stop_lat"]
coords_ida = pd.Categorical(coords_ida, categories=df_stops["stop_lat"])

print(coords_ida)

