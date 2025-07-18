import influxdb_client
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import time
import csv

###########################################################################
# Variables
org = "TFG"
bucket = "Alumnos"
token = "TmYTccKyAKj-meNu-JpQH7-iACOI_5mgWCeasgeyjHsLxFG7azZNJvwARDpyJ8ZNSKD9rqAAxnqBRZXN3Mjl5w=="
url = "http://localhost:8086"

# Creacion del cliente de InfluxDB
client = InfluxDBClient(url = url, token = token, org = org)

# Creacion de la API de escritura
writer = client.write_api()

# Creacion de la API de lectura
reader = client.query_api()

# Query de grafana
query = 'from(bucket: "Alumnos")\
  |> range(start: -1d)\
  |> filter(fn: (r) => r["_measurement"] == "mqtt_consumer")\
  |> filter(fn: (r) => r["_field"] == "uplink_message_decoded_payload_decoded_latitud" or r["_field"] == "uplink_message_decoded_payload_decoded_longitud")'

out = reader.query(query = query, org = org)

latitud = out[0].records
longitud = out[1].records

# p = influxdb_client.Point("mqtt_consumer").tag("Hora_llegada", "Algo").field("saludo", "Hola Mundo").time(datetime.now())
#########################################################

with open("stops.txt", "r") as file:
  reader = csv.DictReader(file)
  for row in reader:
    p = influxdb_client.Point("mqtt_consumer") \
      .tag("stop_id", row["stop_id"]) \
      .field("stop_lat", float(row["stop_lat"])) \
      .field("stop_lon", float(row["stop_lon"])) \
     
  writer.write(bucket=bucket, org=org, record=p)

time.sleep(10)