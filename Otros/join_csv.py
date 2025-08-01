import pandas as pd

df_stops = pd.read_csv("https://raw.githubusercontent.com/David-GC-5403/Bus-Geolocalization/refs/heads/main/bueno/Program/paradas.csv")
stop_times = pd.read_csv("https://raw.githubusercontent.com/David-GC-5403/Bus-Geolocalization/refs/heads/main/bueno/Program/stop_times.csv")

union = pd.merge(stop_times, df_stops, on="stop_id", how="left")

union.to_csv("union.csv", index=False)